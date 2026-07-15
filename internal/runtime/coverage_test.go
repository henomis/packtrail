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
	"strings"
	"testing"
	"time"

	"github.com/henomis/packtrail/internal/store"
	"github.com/henomis/packtrail/invoker"
)

// waitBranchXFailed polls until branch x of execution id fails, or fails.
func waitBranchXFailed(t *testing.T, h *asyncHarness, id string) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ex := h.get(t, id)
		if ex.Branches["x"].Status == store.BranchFailed {
			return
		}

		time.Sleep(20 * time.Millisecond)
	}

	t.Fatalf("branch x of %s never reached failed (have %q)", id, h.get(t, id).Branches["x"].Status)
}

// TestScheduleFlowUnknown verifies ScheduleFlow rejects an unknown flow name.
func TestScheduleFlowUnknown(t *testing.T) {
	h := newHarness(t, linearFlow, Config{})

	if err := h.engine.ScheduleFlow(context.Background(), "sched", "no-such-flow", "* * * * * *", nil); err == nil {
		t.Fatal("ScheduleFlow with unknown flow succeeded, want error")
	}
}

// TestScheduleReconcileFires verifies each reconcile schedule fires its own
// registered callback (active and full are independent).
func TestScheduleReconcileFires(t *testing.T) {
	h := newHarness(t, linearFlow, Config{})

	active := make(chan struct{}, 1)
	full := make(chan struct{}, 1)

	signal := func(ch chan struct{}) func(context.Context) error {
		return func(context.Context) error {
			select {
			case ch <- struct{}{}:
			default:
			}

			return nil
		}
	}

	h.engine.OnReconcileActive(signal(active))
	h.engine.OnReconcileFull(signal(full))

	if err := h.engine.ScheduleReconcileActive(context.Background(), "* * * * * *"); err != nil {
		t.Fatalf("schedule active reconcile: %v", err)
	}

	if err := h.engine.ScheduleReconcileFull(context.Background(), "* * * * * *"); err != nil {
		t.Fatalf("schedule full reconcile: %v", err)
	}

	for _, c := range []struct {
		name string
		ch   chan struct{}
	}{{"active", active}, {"full", full}} {
		select {
		case <-c.ch:
		case <-time.After(15 * time.Second):
			t.Fatalf("%s reconcile callback did not fire", c.name)
		}
	}
}

// TestResumeNotFound verifies Resume surfaces ErrNotFound for a missing execution.
func TestResumeNotFound(t *testing.T) {
	h := newHarness(t, linearFlow, Config{})

	if err := h.engine.Resume(context.Background(), "missing"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Resume(missing) err = %v, want ErrNotFound", err)
	}
}

// TestResumeNotFailed verifies a non-failed (completed) execution cannot be
// resumed.
func TestResumeNotFailed(t *testing.T) {
	h := newHarness(t, linearFlow, Config{})
	h.serve(t, "tasks.a.*", passthrough)
	h.serve(t, "tasks.b.*", passthrough)

	id, err := h.engine.Start(context.Background(), "linear", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	h.waitStatus(t, id, store.StatusCompleted, 10*time.Second)

	if resumeErr := h.engine.Resume(context.Background(), id); resumeErr == nil {
		t.Fatal("Resume of a completed execution succeeded, want error")
	}
}

const signalTimeoutFlow = `
name: sigfail
nodes:
  - {id: start, type: task, subject: "tasks.start.{execution_id}"}
  - {id: wait, type: signal, signal_name: approval, timeout: 500ms}
edges:
  - {from: start, to: wait}
`

// TestSignalTimeoutNoFallbackFails verifies a signal node with no on_timeout
// target fails the execution when its wait times out.
func TestSignalTimeoutNoFallbackFails(t *testing.T) {
	h := newHarness(t, signalTimeoutFlow, Config{})
	h.serve(t, "tasks.start.*", passthrough)

	id, err := h.engine.Start(context.Background(), "sigfail", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	// Parks waiting, then the timeout fires with no on_timeout target → failed.
	h.waitStatus(t, id, store.StatusWaiting, 5*time.Second)

	ex := h.waitStatus(t, id, store.StatusFailed, 10*time.Second)
	if ex.Error == "" {
		t.Fatal("failed execution carried no error message")
	}
}

func TestSignalTimeoutFailRejectsStaleGeneration(t *testing.T) {
	h := newHarness(t, signalTimeoutFlow, Config{})
	ctx := context.Background()

	exec := &store.Execution{
		ID:             "sig-timeout-stale-generation",
		FlowName:       "sigfail",
		Status:         store.StatusWaiting,
		CurrentNode:    "wait",
		NodeGeneration: 2,
		WaitSignal:     "approval",
	}
	if _, err := h.store.Create(ctx, exec); err != nil {
		t.Fatalf("create: %v", err)
	}

	staleSnapshot := *exec
	staleSnapshot.NodeGeneration = 1

	flow := h.engine.flows["sigfail"]
	if err := h.engine.onWaitTimeout(ctx, flow, &staleSnapshot, workItem{
		Kind: kindWaitTimeout, Node: "wait", Generation: 1, Signal: "approval",
	}); err != nil {
		t.Fatalf("stale timeout: %v", err)
	}

	afterStale, err := h.store.Get(ctx, exec.ID)
	if err != nil {
		t.Fatalf("get stale: %v", err)
	}

	if afterStale.Status != store.StatusWaiting ||
		afterStale.CurrentNode != "wait" ||
		afterStale.NodeGeneration != 2 ||
		afterStale.WaitSignal != "approval" {
		t.Fatalf("after stale timeout = %+v, want unchanged signal wait", afterStale)
	}

	if err = h.engine.onWaitTimeout(ctx, flow, afterStale, workItem{
		Kind: kindWaitTimeout, Node: "wait", Generation: 2, Signal: "approval",
	}); err != nil {
		t.Fatalf("fresh timeout: %v", err)
	}

	afterFresh, err := h.store.Get(ctx, exec.ID)
	if err != nil {
		t.Fatalf("get fresh: %v", err)
	}

	if afterFresh.Status != store.StatusFailed || afterFresh.Error == "" {
		t.Fatalf("after fresh timeout = %+v, want failed with error", afterFresh)
	}
}

// drainBranchDispatches reads the two initial fanout branch dispatches.
func drainBranchDispatches(t *testing.T, h *asyncHarness) {
	t.Helper()

	r1, r2 := h.nextReq(t), h.nextReq(t)
	if got := map[string]bool{r1.NodeID: true, r2.NodeID: true}; !got["x"] || !got["y"] {
		t.Fatalf("branch dispatches = %v, want x and y", got)
	}
}

// TestAsyncBranchError settles a fanout branch with StatusError; the branch must
// be marked failed and, since join_policy is "all", the join fails the execution.
func TestAsyncBranchError(t *testing.T) {
	h := newAsyncHarness(t, asyncFanFlow)
	ctx := context.Background()

	id, err := h.engine.Start(ctx, "async-fan", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	drainBranchDispatches(t, h)
	h.waitStatus(t, id, store.StatusWaiting)

	if completeErr := h.engine.CompleteActivity(ctx, id, "x", 0,
		invoker.Result{Status: invoker.StatusError, Error: "branch boom"}); completeErr != nil {
		t.Fatalf("complete x: %v", completeErr)
	}

	waitBranchXFailed(t, h, id)

	// Settle the other branch OK; the "all" join is unmet → execution fails.
	if completeErr := h.engine.CompleteActivity(ctx, id, "y", 0,
		invoker.Result{Status: invoker.StatusOK, Payload: json.RawMessage(`{"branch":"y"}`)}); completeErr != nil {
		t.Fatalf("complete y: %v", completeErr)
	}

	h.waitStatus(t, id, store.StatusFailed)
}

// TestAsyncBranchRetryExhausted settles a fanout branch with StatusRetry when no
// attempts remain (default max_attempts=1); the branch is marked failed.
func TestAsyncBranchRetryExhausted(t *testing.T) {
	h := newAsyncHarness(t, asyncFanFlow)
	ctx := context.Background()

	id, err := h.engine.Start(ctx, "async-fan", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	drainBranchDispatches(t, h)
	h.waitStatus(t, id, store.StatusWaiting)

	if completeErr := h.engine.CompleteActivity(ctx, id, "x", 0,
		invoker.Result{Status: invoker.StatusRetry, Error: "transient"}); completeErr != nil {
		t.Fatalf("complete x: %v", completeErr)
	}

	// No attempts remain for branch x, so retry settles it as failed.
	waitBranchXFailed(t, h, id)
}

func TestAsyncBranchUnknownStatusFailsBranch(t *testing.T) {
	h := newAsyncHarness(t, asyncFanFlow)
	ctx := context.Background()

	id, err := h.engine.Start(ctx, "async-fan", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	drainBranchDispatches(t, h)
	h.waitStatus(t, id, store.StatusWaiting)

	if err = h.engine.CompleteActivity(ctx, id, "x", 0, invoker.Result{Status: invoker.Status("bogus")}); err != nil {
		t.Fatalf("complete x: %v", err)
	}

	waitBranchXFailed(t, h, id)

	if err = h.engine.CompleteActivity(ctx, id, "y", 0,
		invoker.Result{Status: invoker.StatusOK, Payload: json.RawMessage(`{"branch":"y"}`)}); err != nil {
		t.Fatalf("complete y: %v", err)
	}

	ex := h.waitStatus(t, id, store.StatusFailed)
	if !strings.Contains(ex.Branches["x"].Error, "unknown result status") {
		t.Fatalf("branch error = %q, want unknown-status failure", ex.Branches["x"].Error)
	}
}

func TestAsyncBranchOversizedOutputFailsBranch(t *testing.T) {
	h := newAsyncHarness(t, asyncFanFlow)
	h.store.SetMaxPayloadBytes(128)

	ctx := context.Background()

	id, err := h.engine.Start(ctx, "async-fan", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	drainBranchDispatches(t, h)
	h.waitStatus(t, id, store.StatusWaiting)

	big := setField(json.RawMessage(`{}`), "blob", strings.Repeat("x", 256))
	if err = h.engine.CompleteActivity(ctx, id, "x", 0,
		invoker.Result{Status: invoker.StatusOK, Payload: big}); err != nil {
		t.Fatalf("complete x: %v", err)
	}

	waitBranchXFailed(t, h, id)

	if err = h.engine.CompleteActivity(ctx, id, "y", 0,
		invoker.Result{Status: invoker.StatusOK, Payload: json.RawMessage(`{"branch":"y"}`)}); err != nil {
		t.Fatalf("complete y: %v", err)
	}

	ex := h.waitStatus(t, id, store.StatusFailed)
	if !strings.Contains(ex.Branches["x"].Error, "exceeds max size") {
		t.Fatalf("branch error = %q, want payload-size failure", ex.Branches["x"].Error)
	}
}

const asyncFanRetryFlow = `
version: "1.0"
name: async-fan-retry
nodes:
  - {id: fo, type: fanout, branches: [x, y]}
  - {id: x, type: task, subject: "x", retry: {max_attempts: 3}}
  - {id: y, type: task, subject: "y"}
  - {id: join, type: fanin, wait_for: [x, y], join_policy: "all"}
  - {id: done, type: task, subject: "d"}
edges:
  - {from: fo, to: join}
  - {from: join, to: done}
`

// TestAsyncBranchRetryRedispatch settles a branch with StatusRetry while attempts
// remain; the branch must be re-dispatched at the next attempt rather than failed.
func TestAsyncBranchRetryRedispatch(t *testing.T) {
	h := newAsyncHarness(t, asyncFanRetryFlow)
	ctx := context.Background()

	id, err := h.engine.Start(ctx, "async-fan-retry", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	drainBranchDispatches(t, h)
	h.waitStatus(t, id, store.StatusWaiting)

	// Retry branch x with attempts remaining → it is re-dispatched at attempt 1.
	if completeErr := h.engine.CompleteActivity(ctx, id, "x", 0,
		invoker.Result{Status: invoker.StatusRetry, Error: "transient"}); completeErr != nil {
		t.Fatalf("complete x: %v", completeErr)
	}

	redo := h.nextReq(t)
	if redo.NodeID != "x" || redo.Attempt != 1 {
		t.Fatalf("redispatch = %s@%d, want x@1", redo.NodeID, redo.Attempt)
	}

	// The branch is still pending (not failed) at the new attempt.
	if bs := h.get(t, id).Branches["x"]; bs.Status != store.BranchPending || bs.Attempt != 1 {
		t.Fatalf("branch x = %+v, want pending@1", bs)
	}
}
