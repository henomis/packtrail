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

	"github.com/henomis/packtrail/internal/dsl"
	"github.com/henomis/packtrail/internal/names"
	"github.com/henomis/packtrail/internal/natstest"
	"github.com/henomis/packtrail/internal/scheduler"
	"github.com/henomis/packtrail/internal/store"
	"github.com/henomis/packtrail/invoker"
)

// blockingInvoker parks every invocation until the test releases it, so a test
// can interleave operations (e.g. Cancel) with an in-flight synchronous invoke.
type blockingInvoker struct {
	started chan invoker.Request
	release chan invoker.Result
}

func (b *blockingInvoker) Invoke(ctx context.Context, req invoker.Request) (invoker.Result, error) {
	b.started <- req

	select {
	case res := <-b.release:
		return res, nil
	case <-ctx.Done():
		return invoker.Result{}, ctx.Err()
	}
}

// blockHarness wires a running engine over a blockingInvoker.
type blockHarness struct {
	store  *store.Store
	engine *Engine
	inv    *blockingInvoker
}

func newBlockHarness(t *testing.T, flowYAML string, cfg Config) *blockHarness {
	t.Helper()

	ctx := context.Background()
	srv := natstest.Start(t)

	st, err := store.Open(ctx, srv.JS, names.New(""))
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	sch := scheduler.New(srv.JS, names.New(""))
	if err := sch.EnsureStream(ctx); err != nil {
		t.Fatalf("scheduler: %v", err)
	}

	flow, err := dsl.Parse([]byte(flowYAML))
	if err != nil {
		t.Fatalf("flow: %v", err)
	}

	inv := &blockingInvoker{started: make(chan invoker.Request, 16), release: make(chan invoker.Result, 16)}

	eng, err := New(inv, st, sch, testSignals(t, st), map[string]*dsl.Flow{flow.Name: flow}, cfg)
	if err != nil {
		t.Fatalf("engine: %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)

	go func() { _ = eng.Run(runCtx) }()

	return &blockHarness{store: st, engine: eng, inv: inv}
}

func (h *blockHarness) awaitInvoke(t *testing.T) invoker.Request {
	t.Helper()

	select {
	case r := <-h.inv.started:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("no invocation started")
		return invoker.Request{}
	}
}

func (h *blockHarness) waitStatus(t *testing.T, id, status string) *store.Execution {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ex, err := h.store.Get(context.Background(), id)
		if err == nil && ex.Status == status {
			return ex
		}

		time.Sleep(15 * time.Millisecond)
	}

	ex, _ := h.store.Get(context.Background(), id)
	if ex != nil {
		t.Fatalf("exec %s: status=%q err=%q, want %q", id, ex.Status, ex.Error, status)
	}

	t.Fatalf("exec %s never reached %q", id, status)

	return nil
}

func (h *blockHarness) get(t *testing.T, id string) *store.Execution {
	t.Helper()

	ex, err := h.store.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("get %s: %v", id, err)
	}

	return ex
}

const guardLinearFlow = `
version: "1.0"
name: guard-linear
nodes:
  - {id: a, type: task, subject: "x"}
  - {id: b, type: task, subject: "y"}
edges:
  - {from: a, to: b}
`

const guardRetryFlow = `
version: "1.0"
name: guard-retry
nodes:
  - {id: a, type: task, subject: "x", retry: {max_attempts: 3}}
edges: []
`

// TestCancelDuringSyncInvoke verifies that cancelling an execution while a
// synchronous invocation is in flight sticks, whatever the invocation's outcome:
// the late settlement must neither resume the flow (OK), flip cancelled to failed
// (Error), nor schedule a retry (Retry).
func TestCancelDuringSyncInvoke(t *testing.T) {
	cases := []struct {
		name string
		flow string
		res  invoker.Result
	}{
		{"ok", guardLinearFlow, invoker.Result{Status: invoker.StatusOK, Payload: json.RawMessage(`{"a":true}`)}},
		{"error", guardLinearFlow, invoker.Result{Status: invoker.StatusError, Error: "boom"}},
		{"retry", guardRetryFlow, invoker.Result{Status: invoker.StatusRetry, Error: "transient"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newBlockHarness(t, tc.flow, Config{RetryBaseDelay: 50 * time.Millisecond})
			ctx := context.Background()

			flowName := "guard-linear"
			if tc.flow == guardRetryFlow {
				flowName = "guard-retry"
			}

			id, err := h.engine.Start(ctx, flowName, json.RawMessage(`{"n":0}`))
			if err != nil {
				t.Fatalf("start: %v", err)
			}

			// Node a's invocation is in flight; cancel while it blocks.
			_ = h.awaitInvoke(t)

			if cancelErr := h.engine.Cancel(ctx, id, "operator stop"); cancelErr != nil {
				t.Fatalf("cancel: %v", cancelErr)
			}

			h.waitStatus(t, id, store.StatusCancelled)

			// Release the blocked invocation; its settlement must be a stale no-op.
			h.inv.release <- tc.res

			// No successor node or retry may be dispatched.
			select {
			case r := <-h.inv.started:
				t.Fatalf("unexpected dispatch after cancel: node=%s attempt=%d", r.NodeID, r.Attempt)
			case <-time.After(500 * time.Millisecond):
			}

			ex := h.get(t, id)
			if ex.Status != store.StatusCancelled || ex.Error != "operator stop" {
				t.Fatalf("after settlement: status=%q reason=%q, want cancelled/operator stop", ex.Status, ex.Error)
			}

			if ex.Attempt != 0 {
				t.Fatalf("attempt = %d, want 0 (no retry bump on a cancelled execution)", ex.Attempt)
			}
		})
	}
}

// newIdleEngine builds an engine without running its consumers, so a test can
// call unexported transitions directly against hand-crafted execution state.
func newIdleEngine(t *testing.T, flowYAML string) (*store.Store, *Engine) {
	t.Helper()

	ctx := context.Background()
	srv := natstest.Start(t)

	st, err := store.Open(ctx, srv.JS, names.New(""))
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	sch := scheduler.New(srv.JS, names.New(""))
	if err := sch.EnsureStream(ctx); err != nil {
		t.Fatalf("scheduler: %v", err)
	}

	flow, err := dsl.Parse([]byte(flowYAML))
	if err != nil {
		t.Fatalf("flow: %v", err)
	}

	inv := &pendingInvoker{reqs: make(chan invoker.Request, 1)}

	eng, err := New(inv, st, sch, testSignals(t, st), map[string]*dsl.Flow{flow.Name: flow}, Config{})
	if err != nil {
		t.Fatalf("engine: %v", err)
	}

	return st, eng
}

// TestStaleAdvanceNoRewind verifies advanceTo's from-node guard: an advance from
// a node the execution has already moved past is a no-op, so a stale instance
// (lost lease, duplicate delivery) cannot rewind CurrentNode.
func TestStaleAdvanceNoRewind(t *testing.T) {
	st, eng := newIdleEngine(t, guardLinearFlow)
	ctx := context.Background()

	exec := &store.Execution{
		ID: "exec-stale", FlowName: "guard-linear",
		CurrentNode: "b", Status: store.StatusRunning,
	}
	if _, err := st.Create(ctx, exec); err != nil {
		t.Fatalf("create: %v", err)
	}

	before, _ := st.Get(ctx, "exec-stale")

	// Stale advance from node a (the execution is at b): must not apply.
	if err := eng.advanceTo(ctx, "exec-stale", "a", "b", func(ex *store.Execution) {
		ex.AddOutput("stale-marker")
	}); err != nil {
		t.Fatalf("stale advance: %v", err)
	}

	after, _ := st.Get(ctx, "exec-stale")
	if after.Revision != before.Revision {
		t.Fatalf("stale advance wrote: revision %d -> %d", before.Revision, after.Revision)
	}

	if after.CurrentNode != "b" || len(after.Outputs) != 0 {
		t.Fatalf("stale advance mutated state: node=%q outputs=%v", after.CurrentNode, after.Outputs)
	}
}

// TestStaleFailNodeNoOverwrite verifies failNode's guards: a failure for a node
// the execution has moved past is dropped, and a terminal (cancelled) execution
// is never flipped to failed (which would make it resumable).
func TestStaleFailNodeNoOverwrite(t *testing.T) {
	st, eng := newIdleEngine(t, guardLinearFlow)
	ctx := context.Background()

	// Moved on: failing node a while the execution is at b is a no-op.
	if _, err := st.Create(ctx, &store.Execution{
		ID: "exec-moved", FlowName: "guard-linear",
		CurrentNode: "b", Status: store.StatusRunning,
	}); err != nil {
		t.Fatalf("create: %v", err)
	}

	if err := eng.failNode(ctx, "exec-moved", "a", "boom"); err != nil {
		t.Fatalf("stale failNode: %v", err)
	}

	if ex, _ := st.Get(ctx, "exec-moved"); ex.Status != store.StatusRunning || ex.Error != "" {
		t.Fatalf("stale failNode applied: status=%q err=%q", ex.Status, ex.Error)
	}

	// Terminal: a cancelled execution stays cancelled even through plain fail.
	if _, err := st.Create(ctx, &store.Execution{
		ID: "exec-cancelled", FlowName: "guard-linear",
		CurrentNode: "a", Status: store.StatusCancelled, Error: "operator stop",
	}); err != nil {
		t.Fatalf("create: %v", err)
	}

	if err := eng.fail(ctx, "exec-cancelled", "late failure"); err != nil {
		t.Fatalf("fail on cancelled: %v", err)
	}

	if ex, _ := st.Get(ctx, "exec-cancelled"); ex.Status != store.StatusCancelled || ex.Error != "operator stop" {
		t.Fatalf("fail overwrote terminal state: status=%q err=%q", ex.Status, ex.Error)
	}
}

// TestSignalTerminalExecutionDropped verifies applySignal's active guard: a
// signal for an execution that has already finished is acked and dropped without
// mutating the terminal document.
func TestSignalTerminalExecutionDropped(t *testing.T) {
	h := newAsyncHarness(t, asyncLinearFlow)
	ctx := context.Background()

	id, err := h.engine.Start(ctx, "async-linear", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	_ = h.nextReq(t)
	h.waitStatus(t, id, store.StatusWaiting)

	if cancelErr := h.engine.Cancel(ctx, id, "stop"); cancelErr != nil {
		t.Fatalf("cancel: %v", cancelErr)
	}

	h.waitStatus(t, id, store.StatusCancelled)
	before := h.get(t, id)

	if sigErr := h.engine.Signal(ctx, id, "late", json.RawMessage(`{"x":1}`)); sigErr != nil {
		t.Fatalf("signal: %v", sigErr)
	}

	time.Sleep(500 * time.Millisecond) // let the signal consumer process (and drop) it

	after := h.get(t, id)
	if after.Revision != before.Revision {
		t.Fatalf("late signal mutated a terminal execution: revision %d -> %d", before.Revision, after.Revision)
	}

	if len(after.Signals) != 0 {
		t.Fatalf("late signal recorded on terminal execution: %v", after.Signals)
	}
}
