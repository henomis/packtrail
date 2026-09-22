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

// TestCancelWaitingExecution cancels an execution parked on an async activity and
// verifies it settles to cancelled, the in-flight completion is a stale no-op,
// and a repeat cancel does not change the recorded reason.
func TestCancelWaitingExecution(t *testing.T) {
	h := newAsyncHarness(t, asyncLinearFlow)
	ctx := context.Background()

	id, err := h.engine.Start(ctx, "async-linear", json.RawMessage(`{"n":0}`))
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	_ = h.nextReq(t) // node a dispatched; execution parks waiting
	h.waitStatus(t, id, store.StatusWaiting)

	if cancelErr := h.engine.Cancel(ctx, id, "operator stop"); cancelErr != nil {
		t.Fatalf("cancel: %v", cancelErr)
	}

	ex := h.waitStatus(t, id, store.StatusCancelled)
	if ex.Error != "operator stop" {
		t.Fatalf("reason = %q, want %q", ex.Error, "operator stop")
	}

	// A late completion of the in-flight activity is a stale no-op: it must not
	// advance the cancelled execution.
	if completeErr := h.engine.CompleteActivity(ctx, id, "a", 0,
		invoker.Result{Status: invoker.StatusOK, Payload: json.RawMessage(`{"n":1}`)}); completeErr != nil {
		t.Fatalf("late complete: %v", completeErr)
	}

	select {
	case r := <-h.inv.reqs:
		t.Fatalf("unexpected dispatch after cancel: %+v", r)
	case <-time.After(300 * time.Millisecond):
	}

	if got := h.get(t, id); got.Status != store.StatusCancelled {
		t.Fatalf("status after late completion = %q, want cancelled", got.Status)
	}

	// Idempotent: cancelling an already-cancelled execution keeps the first reason.
	if cancelErr := h.engine.Cancel(ctx, id, "second reason"); cancelErr != nil {
		t.Fatalf("re-cancel: %v", cancelErr)
	}

	if got := h.get(t, id); got.Status != store.StatusCancelled || got.Error != "operator stop" {
		t.Fatalf("re-cancel changed state: status=%q reason=%q", got.Status, got.Error)
	}
}

// TestCancelCompletedIsNoOp verifies cancelling a terminal (completed) execution
// leaves it untouched.
func TestCancelCompletedIsNoOp(t *testing.T) {
	h := newHarness(t, linearFlow, Config{})
	h.serve(t, "tasks.a.*", passthrough)
	h.serve(t, "tasks.b.*", passthrough)

	ctx := context.Background()

	id, err := h.engine.Start(ctx, "linear", json.RawMessage(`{"ok":true}`))
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	h.waitStatus(t, id, store.StatusCompleted, 5*time.Second)

	if cancelErr := h.engine.Cancel(ctx, id, "too late"); cancelErr != nil {
		t.Fatalf("cancel: %v", cancelErr)
	}

	ex, err := h.store.Get(ctx, id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if ex.Status != store.StatusCompleted || ex.Error != "" {
		t.Fatalf("completed execution mutated by cancel: status=%q reason=%q", ex.Status, ex.Error)
	}
}

// TestCancelUnknownExecution: an id naming no execution is an error, not the
// no-op that "cancel is idempotent" covers.
//
// This used to return nil, on the reading that cancelling nothing has already
// achieved cancellation. But idempotence is a statement about *state* — this
// execution has already reached a terminal one — and an unknown id is not a
// state at all. Conflating them meant an operator stopping a runaway execution
// with a mistyped id was told it had worked while it kept running, which is the
// one answer that must not be silent.
func TestCancelUnknownExecution(t *testing.T) {
	h := newHarness(t, linearFlow, Config{})

	err := h.engine.Cancel(context.Background(), "exec-does-not-exist", "x")
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("cancel unknown: %v, want store.ErrNotFound", err)
	}
}

// A failed execution is terminal but resumable, so cancelling it is how an
// operator gives up on one they are never going to fix. Before this it was a
// no-op: the execution could be revived forever and retired never.
func TestCancelFailedExecutionRetiresIt(t *testing.T) {
	h := newHarness(t, linearFlow, Config{})
	ctx := context.Background()

	failed := &store.Execution{
		ID: "hot-failed", FlowName: "linear", CurrentNode: "a",
		Status: store.StatusFailed, Error: "task a: boom",
	}
	if _, err := h.store.Create(ctx, failed); err != nil {
		t.Fatalf("create: %v", err)
	}

	if err := h.engine.Cancel(ctx, "hot-failed", "not worth fixing"); err != nil {
		t.Fatalf("cancel failed execution: %v", err)
	}

	ex, err := h.store.Get(ctx, "hot-failed")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if ex.Status != store.StatusCancelled {
		t.Fatalf("status = %q, want cancelled", ex.Status)
	}

	// Why it failed is the reason the operator was looking at it; cancelling
	// files it away rather than erasing it.
	if !strings.Contains(ex.Error, "not worth fixing") || !strings.Contains(ex.Error, "task a: boom") {
		t.Errorf("error = %q, want both the cancel reason and the original failure", ex.Error)
	}

	// Cancelled is not resumable — that is what retiring it means.
	if resumeErr := h.engine.Resume(ctx, "hot-failed"); resumeErr == nil {
		t.Error("Resume revived a cancelled execution")
	}
}

// With no reason given, the failure is still what the record says.
func TestCancelFailedExecutionKeepsFailureWithoutReason(t *testing.T) {
	h := newHarness(t, linearFlow, Config{})
	ctx := context.Background()

	failed := &store.Execution{
		ID: "hot-failed-2", FlowName: "linear", CurrentNode: "a",
		Status: store.StatusFailed, Error: "task a: boom",
	}
	if _, err := h.store.Create(ctx, failed); err != nil {
		t.Fatalf("create: %v", err)
	}

	if err := h.engine.Cancel(ctx, "hot-failed-2", ""); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	ex, err := h.store.Get(ctx, "hot-failed-2")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if !strings.Contains(ex.Error, "task a: boom") {
		t.Errorf("error = %q, want the original failure preserved", ex.Error)
	}
}
