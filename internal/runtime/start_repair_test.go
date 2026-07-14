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

// TestStartWithIDRepairsMissingEnqueue simulates the crash window between the
// create-with-outbox commit and its flush: the execution document exists with
// the first work item still in its outbox, never published. A retried
// StartWithID with the same id must heal it by re-flushing the outbox
// (previously it returned early on ErrAlreadyExists, leaving the execution
// stuck forever).
func TestStartWithIDRepairsMissingEnqueue(t *testing.T) {
	h := newAsyncHarness(t, asyncLinearFlow)
	ctx := context.Background()

	// The half-started execution: the input landed in the data plane and the
	// create committed the first work item in its outbox — the flush never ran
	// (crash).
	if _, err := h.store.CreatePayload(ctx, store.InputKey("order-1"), json.RawMessage(`{"orig":true}`)); err != nil {
		t.Fatalf("put input: %v", err)
	}

	exec := &store.Execution{
		ID: "order-1", FlowName: "async-linear",
		CurrentNode: "a", Status: store.StatusRunning,
	}

	item, err := json.Marshal(workItem{ExecID: "order-1", Kind: kindAdvance})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	exec.AppendWork(item)

	if _, err = h.store.Create(ctx, exec); err != nil {
		t.Fatalf("create: %v", err)
	}

	// The caller retries its Start with the same idempotency key and the same
	// arguments (the contract for retries).
	id, err := h.engine.StartWithID(ctx, "order-1", "async-linear", json.RawMessage(`{"orig":true}`))
	if err != nil {
		t.Fatalf("retried StartWithID: %v", err)
	}

	if id != "order-1" {
		t.Fatalf("id = %q, want order-1", id)
	}

	// The repair re-enqueued the first step with the original input.
	r := h.nextReq(t)
	if r.NodeID != "a" {
		t.Fatalf("dispatched node = %q, want a", r.NodeID)
	}

	if got := parseCtx(t, r.Payload); string(got.Input) != `{"orig":true}` {
		t.Fatalf("input = %s, want the original (first-write wins)", got.Input)
	}
}

func TestStartReturnsIDWhenInitialFlushFails(t *testing.T) {
	ctx := context.Background()

	inv := invoker.Func(func(context.Context, invoker.Request) (invoker.Result, error) {
		return invoker.Result{Status: invoker.StatusOK}, nil
	})

	st, eng := signalHarness(t, inv)

	realJS := eng.js
	eng.js = &failPublishJS{JetStream: realJS}

	id, err := eng.Start(ctx, "sig-redrive", nil)
	if err == nil {
		t.Fatal("Start with broken publish returned nil error, want surfaced flush error")
	}

	if id == "" {
		t.Fatal("Start returned empty id even though the execution was committed")
	}

	ex, getErr := st.Get(ctx, id)
	if getErr != nil {
		t.Fatalf("committed execution %q not readable: %v", id, getErr)
	}

	if len(ex.Outbox) != 1 {
		t.Fatalf("outbox len = %d, want initial work item durably committed", len(ex.Outbox))
	}
}

// TestStartWithIDRejectsDifferentPayload: reusing an idempotency key with a
// different payload must return an error, not silently report the existing
// execution as this caller's own. Without the check, two concurrent StartWithID
// calls racing the data and control planes could bind one caller's document to
// the other caller's payload — with both callers told success.
func TestStartWithIDRejectsDifferentPayload(t *testing.T) {
	h := newAsyncHarness(t, asyncLinearFlow)
	ctx := context.Background()

	if _, err := h.engine.StartWithID(ctx, "order-3", "async-linear", json.RawMessage(`{"orig":true}`)); err != nil {
		t.Fatalf("first StartWithID: %v", err)
	}

	_, err := h.engine.StartWithID(ctx, "order-3", "async-linear", json.RawMessage(`{"other":true}`))
	if err == nil {
		t.Fatal("StartWithID with a different payload succeeded, want mismatch error")
	}

	// A genuine retry (same arguments) stays idempotent.
	id, err := h.engine.StartWithID(ctx, "order-3", "async-linear", json.RawMessage(`{"orig":true}`))
	if err != nil || id != "order-3" {
		t.Fatalf("same-args retry: id=%q err=%v", id, err)
	}
}

// TestStartWithIDRejectsDifferentFlow: reusing an idempotency key against a
// different flow must return an error rather than idempotent success bound to
// a flow the caller did not name.
func TestStartWithIDRejectsDifferentFlow(t *testing.T) {
	h := newAsyncHarness(t, asyncLinearFlow, sigGuardFlow)
	ctx := context.Background()

	if _, err := h.engine.StartWithID(ctx, "order-4", "async-linear", nil); err != nil {
		t.Fatalf("first StartWithID: %v", err)
	}

	if _, err := h.engine.StartWithID(ctx, "order-4", "sig-guard", nil); err == nil {
		t.Fatal("StartWithID with a different flow succeeded, want mismatch error")
	}
}

// TestStartWithIDNoNudgeWhenProgressed verifies the repair does not fire for an
// execution that has demonstrably progressed past its start node: a casual
// duplicate StartWithID must not re-dispatch anything.
func TestStartWithIDNoNudgeWhenProgressed(t *testing.T) {
	h := newAsyncHarness(t, asyncLinearFlow)
	ctx := context.Background()

	if _, err := h.store.Create(ctx, &store.Execution{
		ID: "order-2", FlowName: "async-linear",
		CurrentNode: "b", Status: store.StatusRunning,
	}); err != nil {
		t.Fatalf("create: %v", err)
	}

	id, err := h.engine.StartWithID(ctx, "order-2", "async-linear", nil)
	if err != nil || id != "order-2" {
		t.Fatalf("duplicate StartWithID: id=%q err=%v", id, err)
	}

	select {
	case r := <-h.inv.reqs:
		t.Fatalf("unexpected dispatch for progressed execution: %+v", r)
	case <-time.After(400 * time.Millisecond):
	}
}

const sigGuardFlow = `
version: "1.0"
name: sig-guard
nodes:
  - {id: gate, type: signal, signal_name: go, timeout: 2s, on_timeout: fb}
  - {id: fb, type: task, subject: "x"}
edges: []
`

// TestSignalReparkIdempotent verifies a redelivered advance for an
// already-parked signal wait neither rewrites the execution (no revision bump)
// nor breaks the timeout semantics: the on_timeout route fires exactly once, at
// the first park's deadline, and the duplicate's re-scheduled timeout no-ops.
func TestSignalReparkIdempotent(t *testing.T) {
	h := newAsyncHarness(t, sigGuardFlow)
	ctx := context.Background()

	id, err := h.engine.Start(ctx, "sig-guard", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	h.waitStatus(t, id, store.StatusWaiting)
	before := h.get(t, id)

	// Simulate an at-least-once redelivery of the advance that parked the wait.
	if enqErr := h.engine.enqueue(ctx, id, workItem{Kind: kindAdvance}); enqErr != nil {
		t.Fatalf("duplicate advance: %v", enqErr)
	}

	time.Sleep(300 * time.Millisecond) // let the duplicate process

	after := h.get(t, id)
	if after.Revision != before.Revision {
		t.Fatalf("duplicate advance re-parked the wait: revision %d -> %d", before.Revision, after.Revision)
	}

	// The original timeout fires and routes to fb exactly once.
	r := h.nextReq(t)
	if r.NodeID != "fb" {
		t.Fatalf("timeout dispatched %q, want fb", r.NodeID)
	}

	if completeErr := h.engine.CompleteActivity(ctx, id, "fb", 0,
		invoker.Result{Status: invoker.StatusOK}); completeErr != nil {
		t.Fatalf("complete fb: %v", completeErr)
	}

	h.waitStatus(t, id, store.StatusCompleted)

	// The duplicate's re-scheduled timeout must no-op, not re-dispatch anything.
	select {
	case stale := <-h.inv.reqs:
		t.Fatalf("stale duplicate timeout dispatched: %+v", stale)
	case <-time.After(1200 * time.Millisecond):
	}
}
