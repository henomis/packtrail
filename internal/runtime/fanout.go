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
// drive the join; otherwise it advances to the fanin immediately. Because every
// branch result is durably written as it settles, a crash re-runs only branches
// still pending — no completed work is lost.
func (e *Engine) stepFanout(ctx context.Context, flow *dsl.Flow, node *dsl.Node, exec *store.Execution) error {
	fanin := flow.Successor(node.ID)
	if fanin == "" {
		return fmt.Errorf("fanout node %q has no outgoing edge to a fanin", node.ID)
	}

	// Ensure a pending entry exists for every branch (idempotent on resume).
	updated, err := e.store.Mutate(ctx, exec.ID, func(ex *store.Execution) error {
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
		return err
	}

	// Dispatch the branches that have not settled yet, in parallel.
	var (
		wg         sync.WaitGroup
		anyPending atomic.Bool
	)

	for _, b := range node.Branches {
		if updated.Branches[b].Status != store.BranchPending {
			continue
		}

		wg.Add(1)
		go func(branchID string) {
			defer wg.Done()

			if e.runBranch(ctx, flow, branchID, exec.ID, updated.Payload, updated.Branches[branchID].Attempt) {
				anyPending.Store(true)
			}
		}(b)
	}

	wg.Wait()

	// Async branches outstanding: park at the fanin (waiting); branch completions
	// will enqueue fanin_eval. Set CurrentNode so evalFanin recognises the node.
	if anyPending.Load() {
		parked, parkErr := e.store.Mutate(ctx, exec.ID, func(ex *store.Execution) error {
			ex.Status = store.StatusWaiting
			ex.CurrentNode = fanin
			ex.Attempt = 0

			return nil
		})
		if parkErr != nil {
			return parkErr
		}

		e.emitEvent(ctx, parked)

		return nil
	}

	// All branches settled synchronously; move to the fanin to apply the join.
	return e.advanceTo(ctx, exec.ID, fanin, nil)
}

// runBranch dispatches a single branch task. It returns pending=true if the
// branch Invoker reported StatusPending (the branch is left pending for
// CompleteActivity to settle). A synchronous Invoker is settled inline, retrying
// per the node policy under the current lease, and returns pending=false.
func (e *Engine) runBranch(
	ctx context.Context, flow *dsl.Flow,
	branchID, execID string, payload json.RawMessage, startAttempt int,
) (pending bool) {
	node := flow.Node(branchID)
	if node == nil || node.Type != dsl.NodeTask {
		e.setBranch(ctx, execID, branchID, store.BranchFailed, startAttempt, nil, "branch is not a task node")
		return false
	}

	maxAtt := maxAttempts(node)

	var (
		res     invoker.Result
		callErr error
	)
	for attempt := startAttempt; attempt < maxAtt; attempt++ {
		res, callErr = e.invoke(ctx, node, execID, payload, attempt)
		if callErr == nil && res.Status == invoker.StatusPending {
			return true // async: settled later via CompleteActivity
		}

		if callErr == nil && res.Status == invoker.StatusOK {
			e.setBranch(ctx, execID, branchID, store.BranchCompleted, attempt, res.Payload, "")
			return false
		}

		if callErr == nil && res.Status == invoker.StatusError {
			break // permanent failure, no retry
		}

		if attempt < maxAtt-1 {
			select {
			case <-ctx.Done():
				e.setBranch(ctx, execID, branchID, store.BranchFailed, attempt, nil, "cancelled")
				return false
			case <-time.After(backoff(node, attempt+1, e.cfg.RetryBaseDelay, e.cfg.RetryMaxDelay)):
			}
		}
	}

	e.setBranch(ctx, execID, branchID, store.BranchFailed, maxAtt-1, nil, retryReason(res, callErr))

	return false
}

// setBranch persists a branch's terminal state via CAS.
func (e *Engine) setBranch(
	ctx context.Context, execID, branchID, status string,
	attempt int, result json.RawMessage, errMsg string,
) {
	_, err := e.store.Mutate(ctx, execID, func(ex *store.Execution) error {
		if ex.Branches == nil {
			ex.Branches = map[string]store.BranchState{}
		}

		ex.Branches[branchID] = store.BranchState{
			NodeID:  branchID,
			Status:  status,
			Attempt: attempt,
			Result:  result,
			Error:   errMsg,
		}

		return nil
	})
	if err != nil {
		e.log.Error("persist branch", "exec", execID, "branch", branchID, "err", err)
	}
}

// evalFanin applies a fanin node's join policy to the persisted branch states.
// On success it merges branch results into the shared payload and advances; if
// the policy can never be met it fails the execution.
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
		next := flow.Successor(node.ID)

		return e.advanceTo(ctx, exec.ID, next, func(ex *store.Execution) {
			mergeBranchResults(ex, node.WaitFor)
		})
	case settled == total:
		// Everything has settled but the policy was not met.
		reason := fmt.Sprintf("fanin %q: join not satisfied (%d completed, need %d)", node.ID, completed, required)
		return e.fail(ctx, exec.ID, reason)
	default:
		// Not all branches have settled yet; nothing to do until more arrive.
		return nil
	}
}

// mergeBranchResults writes each branch's result into payload under the key
// "branches" as {branchNodeID: result}.
func mergeBranchResults(ex *store.Execution, branches []string) {
	root := map[string]json.RawMessage{}
	_ = json.Unmarshal(ex.Payload, &root)
	merged := map[string]json.RawMessage{}

	for _, b := range branches {
		if bs, ok := ex.Branches[b]; ok && len(bs.Result) > 0 {
			merged[b] = bs.Result
		}
	}

	if data, err := json.Marshal(merged); err == nil {
		root["branches"] = data
	}

	if data, err := json.Marshal(root); err == nil {
		ex.Payload = data
	}
}
