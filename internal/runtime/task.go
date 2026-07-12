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
	"runtime/debug"
	"time"

	"github.com/henomis/packtrail/internal/dsl"
	"github.com/henomis/packtrail/internal/store"
	"github.com/henomis/packtrail/invoker"
)

const (
	retryRequestedMessage  = "retry requested"
	backoffKindFixed       = "fixed"
	backoffKindExponential = "exponential"
	backoffKindLinear      = "linear"
)

// invoke executes a single task/branch node through the configured Invoker. It
// applies the node timeout as the call deadline (both as a ctx deadline and in
// the request), so individual Invokers do not have to.
//
// A panic from the Invoker is recovered and converted into a StatusError result:
// this runs on the work-consumer goroutine, which has no recovery above it, so an
// unrecovered panic would crash the whole engine process (every other in-flight
// item with it). A panic is treated as a permanent failure rather than a retry —
// a redelivery would likely re-panic — with a logged stack; the dead-letter cap
// bounds any retry regardless. This is the synchronous counterpart to the async
// worker's guard (invoker/asyncqueue Worker.invoke); together they contain a
// buggy Invoker — sync or async — to its own execution.
func (e *Engine) invoke(
	ctx context.Context, node *dsl.Node, execID string,
	payload json.RawMessage, attempt int,
) (res invoker.Result, err error) {
	timeout := node.Timeout.D()
	if timeout <= 0 {
		timeout = e.cfg.DefaultTimeout
	}

	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	defer func() {
		if r := recover(); r != nil {
			e.log.Error("invoker panic",
				"exec", execID, "node", node.ID, "attempt", attempt,
				"panic", r, "stack", string(debug.Stack()))

			res = invoker.Result{
				Status: invoker.StatusError,
				Error:  fmt.Sprintf("invoker panic: %v", r),
			}
			err = nil
		}
	}()

	return e.invoker.Invoke(reqCtx, invoker.Request{
		Invoker:     node.InvokerKind(),
		Target:      dsl.ResolvePlaceholders(node.InvokeTarget(), execID),
		ExecutionID: execID,
		NodeID:      node.ID,
		Payload:     payload,
		Attempt:     attempt,
		Deadline:    time.Now().Add(timeout),
	})
}

// stepTask invokes a task node. A synchronous Invoker settles the node now
// (advance/retry/fail). An asynchronous Invoker returns StatusPending: the
// execution is parked (waiting) and freed from the engine, to be settled later
// via CompleteActivity.
func (e *Engine) stepTask(ctx context.Context, flow *dsl.Flow, node *dsl.Node, exec *store.Execution) error {
	contextDoc, err := e.assembleContext(ctx, exec)
	if err != nil {
		return err
	}

	res, callErr := e.invoke(ctx, node, exec.ID, contextDoc, exec.Attempt)

	// Async dispatch: park until CompleteActivity is called. The work item is
	// acked, freeing the engine slot for the agent's whole runtime. If a
	// completion already arrived (the runner finished before we persisted the
	// wait), consume it and settle now instead of parking.
	if callErr == nil && res.Status == invoker.StatusPending {
		var early *store.ActivityResult

		updated, mutErr := e.store.Mutate(ctx, exec.ID, func(ex *store.Execution) error {
			// Park only if the execution is still active at this node/attempt: a
			// Cancel (or a competing instance's transition) that landed while the
			// dispatch was in flight must not be overwritten back to waiting. The
			// outstanding CompleteActivity then no-ops against the terminal state.
			if !ex.Active() || ex.CurrentNode != node.ID || ex.Attempt != exec.Attempt {
				return errSkip
			}

			if a := ex.Activity; a != nil && a.Node == node.ID && a.Attempt == exec.Attempt {
				early = a
				ex.Activity = nil
				ex.Status = store.StatusRunning // claim for settling

				return nil
			}

			ex.Status = store.StatusWaiting

			return nil
		})
		if mutErr != nil {
			if errors.Is(mutErr, errSkip) {
				return nil // cancelled or moved on while dispatching: drop
			}

			return mutErr
		}

		e.emitEvent(ctx, updated)

		if early != nil {
			r, earlyErr := activityResult(*early)
			return e.settleTask(ctx, flow, node, updated, r, earlyErr)
		}

		return nil
	}

	return e.settleTask(ctx, flow, node, exec, res, callErr)
}

// settleTask applies a task result to the execution: advance on success, fail on
// permanent error, or retry (re-dispatch via the Message Scheduler) on a
// transient failure with attempts remaining. It is shared by the synchronous
// stepTask path and the asynchronous CompleteActivity path.
func (e *Engine) settleTask(
	ctx context.Context, flow *dsl.Flow, node *dsl.Node,
	exec *store.Execution, res invoker.Result, callErr error,
) error {
	if callErr == nil && res.Status == invoker.StatusOK {
		return e.settleTaskSuccess(ctx, flow, node, exec, res)
	}

	// Permanent error from the task: fail immediately, no retry.
	if callErr == nil && res.Status == invoker.StatusError {
		return e.failNode(ctx, exec.ID, node.ID, "task "+node.ID+": "+res.Error)
	}

	// Transient: a transport error (callErr != nil), an explicit retry request,
	// or a re-pending settle. These are re-driven per the node's retry policy.
	if callErr != nil || res.Status == invoker.StatusRetry || res.Status == invoker.StatusPending {
		return e.settleTaskRetry(ctx, node, exec, res, callErr)
	}

	// callErr == nil but the status is none of the known values (e.g. an empty or
	// misspelled status from a buggy worker). Retrying re-hits the same bug and
	// burns the whole retry budget, so fail fast with an actionable reason.
	return e.failNode(ctx, exec.ID, node.ID,
		fmt.Sprintf("task %s: unknown result status %q", node.ID, res.Status))
}

// settleTaskSuccess records the output in the data plane and advances. Data
// before control — the output is readable before the advance commits, so the
// flow can never move past a node whose result is missing; a re-run of the
// same attempt overwrites the entry idempotently. Outputs are namespaced per
// node (results.<node> in the assembled context), so any JSON shape is legal —
// nothing merges into a shared root anymore.
func (e *Engine) settleTaskSuccess(
	ctx context.Context, flow *dsl.Flow, node *dsl.Node, exec *store.Execution, res invoker.Result,
) error {
	if len(res.Payload) > 0 {
		if putErr := e.store.PutPayload(ctx, store.OutputKey(exec.ID, node.ID), res.Payload); putErr != nil {
			if errors.Is(putErr, store.ErrPayloadTooLarge) {
				return e.failNode(ctx, exec.ID, node.ID, putErr.Error())
			}

			return putErr
		}
	}

	next := flow.Successor(node.ID)

	return e.advanceTo(ctx, exec.ID, node.ID, next, func(ex *store.Execution) {
		if len(res.Payload) > 0 {
			ex.AddOutput(node.ID)
		}
	})
}

// settleTaskRetry handles a transient failure (transport error, timeout, or
// explicit retry): retries if attempts remain, scheduling the next attempt via
// the Message Scheduler, or fails the node once retries are exhausted.
func (e *Engine) settleTaskRetry(
	ctx context.Context, node *dsl.Node, exec *store.Execution, res invoker.Result, callErr error,
) error {
	reason := retryReason(res, callErr)
	if exec.Attempt+1 >= maxAttempts(node) {
		return e.failNode(ctx, exec.ID, node.ID, "task "+node.ID+" exhausted retries: "+reason)
	}

	// exec.Attempt is the pre-increment value; the attempt being scheduled is +1.
	delay := backoff(node, exec.Attempt+1, e.cfg.RetryBaseDelay, e.cfg.RetryMaxDelay)

	item, marshalErr := json.Marshal(workItem{ExecID: exec.ID, Kind: kindAdvance})
	if marshalErr != nil {
		return marshalErr
	}

	updated, err := e.store.Mutate(ctx, exec.ID, func(ex *store.Execution) error {
		// Bump the attempt only while the execution is still active at this
		// node/attempt: a cancelled execution must stay cancelled, and a stale or
		// duplicate settlement must not double-bump (and double-schedule) a retry.
		if !ex.Active() || ex.CurrentNode != node.ID || ex.Attempt != exec.Attempt {
			return errSkip
		}

		ex.Status = store.StatusRunning
		ex.Attempt++
		ex.Error = reason
		// RetryAt tells the stall watchdog this quiet period is a scheduled
		// backoff, not a lost work item. The retry's scheduled delivery is
		// committed in this same write (transactional outbox), so a crash can
		// never bump the attempt yet lose its timer.
		ex.RetryAt = time.Now().Add(delay).UTC()
		ex.AppendSched(item, ex.RetryAt)

		return nil
	})
	if err != nil {
		if errors.Is(err, errSkip) {
			return nil // cancelled or moved on: drop the retry
		}

		return err
	}

	return e.flushOutbox(ctx, updated)
}

func maxAttempts(node *dsl.Node) int {
	if node.Retry != nil && node.Retry.MaxAttempts > 0 {
		return node.Retry.MaxAttempts
	}

	return 1
}

func retryReason(res invoker.Result, callErr error) string {
	if callErr != nil {
		return callErr.Error()
	}

	if res.Error != "" {
		return res.Error
	}

	return retryRequestedMessage
}

// maxBackoffShift caps the exponential-backoff shift. base << shift overflows
// int64 for a large enough shift, and an overflow can wrap to a small positive
// value that slips past the maxDelay clamp below — turning the intended long
// backoff into a spuriously short one. Past this many doublings the delay is
// already far beyond any sane maxDelay, so we saturate to maxDelay instead of
// shifting. (dsl validation also caps max_attempts; this is the arithmetic
// backstop.)
const maxBackoffShift = 62

// backoff returns the delay before the next attempt. attempt is the number of
// attempts already made (1-based after the first failure).
func backoff(node *dsl.Node, attempt int, base, maxDelay time.Duration) time.Duration {
	kind := backoffKindFixed
	if node.Retry != nil && node.Retry.Backoff != "" {
		kind = node.Retry.Backoff
	}

	var d time.Duration

	switch kind {
	case backoffKindExponential:
		// Saturate rather than shift once the doublings would overflow int64; an
		// overflow could wrap into (0, maxDelay] and escape the clamp below.
		if shift := attempt - 1; shift >= maxBackoffShift {
			d = maxDelay
		} else {
			d = base << shift // attempt>=1
		}
	case backoffKindLinear:
		d = base * time.Duration(attempt)
	default: // fixed
		d = base
	}

	if d <= 0 || d > maxDelay {
		d = maxDelay
	}

	return d
}
