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

// TestCancelUnknownExecution is a no-op (no execution to cancel), not an error.
func TestCancelUnknownExecution(t *testing.T) {
	h := newHarness(t, linearFlow, Config{})

	if err := h.engine.Cancel(context.Background(), "exec-does-not-exist", "x"); err != nil {
		t.Fatalf("cancel unknown: %v, want nil (no-op)", err)
	}
}
