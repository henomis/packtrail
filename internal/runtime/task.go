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
func (e *Engine) invoke(
	ctx context.Context, node *dsl.Node, execID string,
	payload json.RawMessage, attempt int,
) (invoker.Result, error) {
	timeout := node.Timeout.D()
	if timeout <= 0 {
		timeout = e.cfg.DefaultTimeout
	}

	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

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
	res, callErr := e.invoke(ctx, node, exec.ID, exec.Payload, exec.Attempt)

	// Async dispatch: park until CompleteActivity is called. The work item is
	// acked, freeing the engine slot for the agent's whole runtime. If a
	// completion already arrived (the runner finished before we persisted the
	// wait), consume it and settle now instead of parking.
	if callErr == nil && res.Status == invoker.StatusPending {
		var early *store.ActivityResult

		updated, err := e.store.Mutate(ctx, exec.ID, func(ex *store.Execution) error {
			if a := ex.Activity; a != nil && a.Node == node.ID && a.Attempt == exec.Attempt {
				early = a
				ex.Activity = nil
				ex.Status = store.StatusRunning // claim for settling

				return nil
			}

			ex.Status = store.StatusWaiting

			return nil
		})
		if err != nil {
			return err
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
	// Success: merge the response payload into the shared context and advance.
	if callErr == nil && res.Status == invoker.StatusOK {
		next := flow.Successor(node.ID)

		return e.advanceTo(ctx, exec.ID, next, func(ex *store.Execution) {
			if len(res.Payload) > 0 {
				ex.Payload = res.Payload
			}
		})
	}

	// Permanent error from the task: fail immediately, no retry.
	if callErr == nil && res.Status == invoker.StatusError {
		return e.fail(ctx, exec.ID, "task "+node.ID+": "+res.Error)
	}

	// Transient failure (transport error, timeout, or explicit retry). Retry if
	// attempts remain, scheduling the next attempt via the Message Scheduler.
	reason := retryReason(res, callErr)
	if exec.Attempt+1 >= maxAttempts(node) {
		return e.fail(ctx, exec.ID, "task "+node.ID+" exhausted retries: "+reason)
	}

	if _, err := e.store.Mutate(ctx, exec.ID, func(ex *store.Execution) error {
		ex.Status = store.StatusRunning
		ex.Attempt++
		ex.Error = reason

		return nil
	}); err != nil {
		return err
	}
	// exec.Attempt is the pre-increment value; the attempt just scheduled is +1.
	delay := backoff(node, exec.Attempt+1, e.cfg.RetryBaseDelay, e.cfg.RetryMaxDelay)

	item, marshalErr := json.Marshal(workItem{ExecID: exec.ID, Kind: kindAdvance})
	if marshalErr != nil {
		return marshalErr
	}

	return e.sched.After(ctx, exec.ID, delay, item)
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
		d = base << (attempt - 1) // attempt>=1
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
