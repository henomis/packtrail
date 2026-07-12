// Copyright 2026 Simone Vellei
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/henomis/packtrail/internal/dsl"
	"github.com/henomis/packtrail/internal/store"
	"github.com/henomis/packtrail/invoker"
)

// stepFanout starts (or resumes) all branches of a fanout node. A synchronous
// branch Invoker settles inline; an asynchronous one (StatusPending) leaves the
// branch pending to be settled later via CompleteActivity. If any branch is left
// pending, the execution parks at the fanin node (waiting) and branch completions
// drive the join; otherwise it advances to the fanin immediately. Each branch
// result is durably written as it settles, so a crash re-runs only branches
// still pending — no completed work is lost.
func (e *Engine) stepFanout(ctx context.Context, flow *dsl.Flow, node *dsl.Node, exec *store.Execution) error {
	fanin := flow.Successor(node.ID)
	if fanin == "" {
		return fmt.Errorf("fanout node %q has no outgoing edge to a fanin", node.ID)
	}

	// Ensure a pending entry exists for every branch (idempotent on resume). The
	// guard keeps a stale delivery from dispatching branches — and firing their
	// side effects — for an execution that was cancelled or has moved on.
	updated, err := e.store.Mutate(ctx, exec.ID, func(ex *store.Execution) error {
		if !ex.Active() || ex.CurrentNode != node.ID {
			return errSkip
		}

		if ex.Branches == nil {
			ex.Branches = map[string]store.BranchState{}
		}

		for _, b := range node.Branches {
			if _, ok := ex.Branches[b]; !ok {
				ex.Branches[b] = store.BranchState{NodeID: b, Status: store.BranchPending}
			}
		}

		return nil
	})
	if err != nil {
		if errors.Is(err, errSkip) {
			return nil // cancelled or moved on: drop without dispatching
		}

		return err
	}

	branches := make([]string, 0, len(node.Branches))

	for _, b := range node.Branches {
		if updated.Branches[b].Status == store.BranchPending {
			branches = append(branches, b)
		}
	}

	if e.dispatchBranches(ctx, flow, updated, branches) {
		// Async branches outstanding: park at the fanin (waiting); their
		// completions will enqueue fanin_eval. Set CurrentNode so evalFanin
		// recognises the node. One eval is committed with the park itself
		// (transactional outbox): a fast async branch can complete *before* the
		// park lands — its CompleteActivity enqueued a fanin_eval that evalFanin
		// dropped as stale — and if that was the last outstanding branch nothing
		// else would ever re-evaluate the join. With branches still pending the
		// eval harmlessly no-ops; duplicates are state-safe (advanceTo is
		// guarded). This is the fanout counterpart to stepTask's
		// early-completion stash.
		evalItem, marshalErr := json.Marshal(workItem{ExecID: exec.ID, Kind: kindFaninEval})
		if marshalErr != nil {
			return marshalErr
		}

		parked, parkErr := e.store.Mutate(ctx, exec.ID, func(ex *store.Execution) error {
			// Park only if still active at the fanout: a Cancel that landed while
			// branches were dispatching must not be overwritten back to waiting.
			if !ex.Active() || ex.CurrentNode != node.ID {
				return errSkip
			}

			ex.Status = store.StatusWaiting
			ex.CurrentNode = fanin
			ex.Attempt = 0
			ex.AppendWork(evalItem)

			return nil
		})
		if parkErr != nil {
			if errors.Is(parkErr, errSkip) {
				return nil
			}

			return parkErr
		}

		e.emitEvent(ctx, parked)

		return e.flushOutbox(ctx, parked)
	}

	// All branches settled synchronously; move to the fanin to apply the join.
	return e.advanceTo(ctx, exec.ID, node.ID, fanin, nil)
}

// dispatchBranches invokes every branch in parallel and persists each settled
// result as it completes, returning whether any branch went async (pending).
//
// Branch invocations run concurrently, but their CAS writes to the single
// execution document are serialized through a per-fanout mutex: with only one
// writer active at a time there are zero CAS conflicts even for a very wide
// fanout. The previous design had every branch racing the same key, which is
// O(N²) retry work and exhausted the Mutate retry budget once the fan was wide
// enough (a 200-way fanout settling at once). Serializing keeps per-branch
// durability (a completed branch is written before any crash, and is not
// recomputed on takeover) while bounding contention to a single writer.
func (e *Engine) dispatchBranches(
	ctx context.Context, flow *dsl.Flow, exec *store.Execution, branches []string,
) (anyPending bool) {
	// One assembly serves every branch: they all see the same upstream context.
	contextDoc, err := e.assembleContext(ctx, exec)
	if err != nil {
		e.log.Error("assemble branch context", "exec", exec.ID, "err", err)

		return false // nothing dispatched; the redelivered advance retries
	}

	var (
		wg      sync.WaitGroup
		writeMu sync.Mutex
		pending atomic.Bool
	)

	for _, b := range branches {
		wg.Add(1)

		go func(branchID string) {
			defer wg.Done()

			startAttempt := exec.Branches[branchID].Attempt

			o := e.runBranch(ctx, flow, branchID, exec.ID, contextDoc, startAttempt)
			if o.pending {
				pending.Store(true)
				return
			}

			// Data before control: the output entry is written outside the
			// mutex (data-plane puts are CAS-free and parallel-safe), then the
			// settle is serialized like every branch write.
			if len(o.payload) > 0 {
				if putErr := e.store.PutPayload(ctx, store.OutputKey(exec.ID, branchID), o.payload); putErr != nil {
					e.log.Error("persist branch output", "exec", exec.ID, "branch", branchID, "err", putErr)

					return // branch stays pending; the redelivered advance re-runs it
				}
			}

			writeMu.Lock()
			defer writeMu.Unlock()

			e.persistBranch(ctx, exec.ID, branchID, startAttempt, o.state, len(o.payload) > 0)
		}(b)
	}

	wg.Wait()

	return pending.Load()
}

// persistBranch writes a single branch's settled state via CAS. Callers serialize
// concurrent invocations for one execution (see dispatchBranches) so these writes
// do not contend. The write is guarded: it applies only while the execution is
// still active and the branch is still pending at the dispatched attempt, so a
// stale dispatcher (duplicate delivery, lost lease) cannot overwrite a branch that
// was settled or re-dispatched elsewhere.
func (e *Engine) persistBranch(
	ctx context.Context, execID, branchID string, startAttempt int,
	state store.BranchState, hasOutput bool,
) {
	_, err := e.store.Mutate(ctx, execID, func(ex *store.Execution) error {
		if !ex.Active() {
			return errSkip
		}

		bs, ok := ex.Branches[branchID]
		if !ok || bs.Status != store.BranchPending || bs.Attempt != startAttempt {
			return errSkip // settled or re-dispatched elsewhere: stale write
		}

		ex.Branches[branchID] = state

		if hasOutput {
			ex.AddOutput(branchID)
		}

		return nil
	})
	if err != nil && !errors.Is(err, errSkip) {
		e.log.Error("persist branch", "exec", execID, "branch", branchID, "err", err)
	}
}

// branchOutcome is the in-memory result of dispatching one branch. A pending
// outcome carries no state: the branch stays BranchPending until
// CompleteActivity. A completed outcome's payload is written to the data plane
// by dispatchBranches before the settle is persisted.
type branchOutcome struct {
	state   store.BranchState
	payload json.RawMessage
	pending bool
}

// runBranch dispatches a single branch task and returns its outcome WITHOUT
// persisting it (dispatchBranches persists settled outcomes under its write
// mutex). A pending outcome means the branch Invoker reported StatusPending — it
// is left pending for CompleteActivity to settle.
//
// A synchronous Invoker is settled inline: on a transient failure runBranch
// retries within this call, sleeping the node's backoff between attempts. This is
// intentional but means a synchronous branch with a retry policy occupies its
// concurrency slot (and holds the ownership lease, kept alive by the heartbeat)
// for the sum of its backoff windows. It is correct — the lease is renewed and
// the ack window extended, so no other instance double-processes — but it ties up
// a slot. Steer slow or retry-heavy branches to an asynchronous invoker
// (StatusPending): those free the slot immediately and their retries, driven by
// CompleteActivity, never occupy an engine slot (see the task path's
// scheduler-based retry in settleTask, and TestAsyncBranchRetryDoesNotBlock).
func (e *Engine) runBranch(
	ctx context.Context, flow *dsl.Flow,
	branchID, execID string, payload json.RawMessage, startAttempt int,
) branchOutcome {
	node := flow.Node(branchID)
	if node == nil || node.Type != dsl.NodeTask {
		return failedBranch(branchID, startAttempt, "branch is not a task node")
	}

	maxAtt := maxAttempts(node)

	var (
		res     invoker.Result
		callErr error
	)

	for attempt := startAttempt; attempt < maxAtt; attempt++ {
		res, callErr = e.invoke(ctx, node, execID, payload, attempt)
		if callErr == nil && res.Status == invoker.StatusPending {
			return branchOutcome{pending: true} // async: settled later via CompleteActivity
		}

		if callErr == nil && res.Status == invoker.StatusOK {
			return branchOutcome{
				state:   store.BranchState{NodeID: branchID, Status: store.BranchCompleted, Attempt: attempt},
				payload: res.Payload,
			}
		}

		if callErr == nil && res.Status == invoker.StatusError {
			break // permanent failure, no retry
		}

		if attempt < maxAtt-1 {
			select {
			case <-ctx.Done():
				return failedBranch(branchID, attempt, "cancelled")
			case <-time.After(backoff(node, attempt+1, e.cfg.RetryBaseDelay, e.cfg.RetryMaxDelay)):
			}
		}
	}

	return failedBranch(branchID, maxAtt-1, retryReason(res, callErr))
}

// failedBranch builds a settled, failed branch outcome.
func failedBranch(branchID string, attempt int, errMsg string) branchOutcome {
	return branchOutcome{state: store.BranchState{
		NodeID: branchID, Status: store.BranchFailed, Attempt: attempt, Error: errMsg,
	}}
}

// evalFanin applies a fanin node's join policy to the persisted branch states.
// On success it advances (branch outputs are already in the data plane, under
// results.<branch> in the assembled context); if the policy can never be met
// it fails the execution.
func (e *Engine) evalFanin(ctx context.Context, flow *dsl.Flow, exec *store.Execution) error {
	node := flow.Node(exec.CurrentNode)
	if node == nil || node.Type != dsl.NodeFanin {
		return nil // execution already moved on; stale eval
	}

	var completed, failed int

	for _, w := range node.WaitFor {
		switch exec.Branches[w].Status {
		case store.BranchCompleted:
			completed++
		case store.BranchFailed:
			failed++
		}
	}

	total := len(node.WaitFor)
	settled := completed + failed
	kind, quorum := node.JoinKind()

	required := total // JoinAll

	switch kind {
	case dsl.JoinAny:
		required = 1
	case dsl.JoinQuorum:
		required = quorum
	}

	switch {
	case completed >= required:
		return e.advanceTo(ctx, exec.ID, node.ID, flow.Successor(node.ID), nil)
	case settled == total:
		// Everything has settled but the policy was not met.
		reason := fmt.Sprintf("fanin %q: join not satisfied (%d completed, need %d)", node.ID, completed, required)
		return e.failNode(ctx, exec.ID, node.ID, reason)
	default:
		// Not all branches have settled yet; nothing to do until more arrive.
		return nil
	}
}
