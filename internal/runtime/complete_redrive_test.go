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

const redriveFlow = `
version: "1.0"
name: redrive
nodes:
  - {id: fo, type: fanout, branches: [b1, b2]}
  - {id: b1, type: task, subject: "x"}
  - {id: b2, type: task, subject: "y"}
  - {id: join, type: fanin, wait_for: [b1, b2], join_policy: all}
edges:
  - {from: fo, to: join}
`

// TestCompleteActivitySettledBranchRedrivesFanin covers the crash window in
// completeBranch: the branch settle committed — with its follow-on fanin_eval
// in the execution's outbox (transactional outbox) — but the process died
// before the flush published it. The async worker redelivers the completion,
// which finds the branch already settled; the entry re-flush must publish the
// pending eval instead of dropping the duplicate silently.
//
// The test manufactures the post-crash state directly — parked waiting at the
// fanin, every branch settled, the eval committed in the outbox but never
// published — and asserts the redelivered CompleteActivity completes the flow.
func TestCompleteActivitySettledBranchRedrivesFanin(t *testing.T) {
	ctx := context.Background()
	srv := natstest.Start(t)

	st, err := store.Open(ctx, srv.JS, names.New(""))
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	sch := scheduler.New(srv.JS, names.New(""))
	if err = sch.EnsureStream(ctx); err != nil {
		t.Fatalf("scheduler: %v", err)
	}

	flow, err := dsl.Parse([]byte(redriveFlow))
	if err != nil {
		t.Fatalf("flow: %v", err)
	}

	inv := invoker.Func(func(context.Context, invoker.Request) (invoker.Result, error) {
		t.Error("invoker called: the manufactured state has no work to dispatch")

		return invoker.Result{Status: invoker.StatusError, Error: "unexpected"}, nil
	})

	eng, err := New(inv, st, sch, testSignals(t, st), map[string]*dsl.Flow{flow.Name: flow}, Config{})
	if err != nil {
		t.Fatalf("engine: %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)

	go func() { _ = eng.Run(runCtx) }()

	// The post-crash state: both branches settled, execution parked at the
	// fanin, the committed fanin_eval still in the outbox (its flush never ran).
	exec := &store.Execution{
		ID: "redrive-1", FlowName: "redrive",
		Status: store.StatusWaiting, CurrentNode: "join",

		Branches: map[string]store.BranchState{
			"b1": {NodeID: "b1", Status: store.BranchCompleted, Attempt: 0},
			"b2": {NodeID: "b2", Status: store.BranchCompleted, Attempt: 0},
		},
	}

	evalItem, err := json.Marshal(workItem{ExecID: "redrive-1", Kind: kindFaninEval})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	exec.AppendWork(evalItem)

	if _, err = st.Create(ctx, exec); err != nil {
		t.Fatalf("create: %v", err)
	}

	// The redelivered completion for the already-settled branch.
	if err = eng.CompleteActivity(ctx, "redrive-1", "b2", 0,
		invoker.Result{Status: invoker.StatusOK, Payload: json.RawMessage(`{"b2":1}`)}); err != nil {
		t.Fatalf("complete activity: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ex, getErr := st.Get(ctx, "redrive-1")
		if getErr == nil && ex.Status == store.StatusCompleted {
			return
		}

		time.Sleep(15 * time.Millisecond)
	}

	ex, _ := st.Get(ctx, "redrive-1")
	t.Fatalf("execution stranded at the fanin: status=%q node=%q (redelivered completion did not re-drive the join)",
		ex.Status, ex.CurrentNode)
}

// TestCompleteBranchSkipsCancelledExecution: a Cancel that lands between
// CompleteActivity's snapshot read and completeBranch's settle write must win —
// the branch stays pending on the terminal document, nothing is mutated.
// completeBranch is called directly with the stale (still-active) snapshot to
// make the race deterministic.
func TestCompleteBranchSkipsCancelledExecution(t *testing.T) {
	ctx := context.Background()
	srv := natstest.Start(t)

	st, err := store.Open(ctx, srv.JS, names.New(""))
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	sch := scheduler.New(srv.JS, names.New(""))
	if err = sch.EnsureStream(ctx); err != nil {
		t.Fatalf("scheduler: %v", err)
	}

	flow, err := dsl.Parse([]byte(redriveFlow))
	if err != nil {
		t.Fatalf("flow: %v", err)
	}

	inv := invoker.Func(func(context.Context, invoker.Request) (invoker.Result, error) {
		t.Error("invoker called: a skipped completion must not re-dispatch")

		return invoker.Result{Status: invoker.StatusError, Error: "unexpected"}, nil
	})

	eng, err := New(inv, st, sch, testSignals(t, st), map[string]*dsl.Flow{flow.Name: flow}, Config{})
	if err != nil {
		t.Fatalf("engine: %v", err)
	}

	exec := &store.Execution{
		ID: "cancel-race-1", FlowName: "redrive",
		Status: store.StatusWaiting, CurrentNode: "join",

		Branches: map[string]store.BranchState{
			"b1": {NodeID: "b1", Status: store.BranchCompleted, Attempt: 0},
			"b2": {NodeID: "b2", Status: store.BranchPending, Attempt: 0},
		},
	}
	if _, err = st.Create(ctx, exec); err != nil {
		t.Fatalf("create: %v", err)
	}

	// The completion's stale snapshot: taken while still active.
	snapshot, err := st.Get(ctx, "cancel-race-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	// Cancel lands after the snapshot, before the settle write.
	if _, err = st.Mutate(ctx, "cancel-race-1", func(ex *store.Execution) error {
		ex.Status = store.StatusCancelled

		return nil
	}); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	if err = eng.completeBranch(ctx, flow, snapshot, "b2", 0,
		invoker.Result{Status: invoker.StatusOK, Payload: json.RawMessage(`{"b2":1}`)}); err != nil {
		t.Fatalf("completeBranch: %v", err)
	}

	ex, err := st.Get(ctx, "cancel-race-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if ex.Status != store.StatusCancelled {
		t.Fatalf("status = %q, want cancelled (a late completion must not revive the execution)", ex.Status)
	}

	if ex.Branches["b2"].Status != store.BranchPending {
		t.Fatalf("branch b2 = %q, want still pending (terminal document must not be mutated)", ex.Branches["b2"].Status)
	}
}
