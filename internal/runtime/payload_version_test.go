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

	"github.com/henomis/packtrail/internal/dsl"
	"github.com/henomis/packtrail/internal/names"
	"github.com/henomis/packtrail/internal/natstest"
	"github.com/henomis/packtrail/internal/scheduler"
	"github.com/henomis/packtrail/internal/store"
	"github.com/henomis/packtrail/invoker"
)

func TestStaleTaskSuccessCannotOverwriteCommittedOutput(t *testing.T) {
	st, eng := newIdleEngine(t)
	ctx := context.Background()

	const id = "payload-task-stale"

	putVersionedOutput(ctx, t, st, id, "a", "fresh", json.RawMessage(`{"run":"fresh"}`))

	if _, err := st.Create(ctx, &store.Execution{
		ID: "payload-task-stale", FlowName: "guard-linear",
		CurrentNode: "b", Status: store.StatusRunning,
		Outputs:        []string{"a"},
		OutputVersions: map[string]string{"a": "fresh"},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}

	flow := eng.flows["guard-linear"]
	node := flow.Node("a")
	staleSnapshot := &store.Execution{
		ID: id, FlowName: "guard-linear",
		CurrentNode: "a", Status: store.StatusRunning, Attempt: 0,
	}

	if err := eng.settleTaskSuccess(ctx, flow, node, staleSnapshot,
		invoker.Result{Status: invoker.StatusOK, Payload: json.RawMessage(`{"run":"stale"}`)}); err != nil {
		t.Fatalf("settle stale task success: %v", err)
	}

	assertResult(ctx, t, eng, id, "a", `{"run":"fresh"}`)

	ex, err := st.Get(ctx, id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if ex.CurrentNode != "b" || ex.OutputVersion("a") != "fresh" {
		t.Fatalf("execution mutated by stale task success: node=%q output_version=%q",
			ex.CurrentNode, ex.OutputVersion("a"))
	}
}

func TestStaleTaskAttemptCannotCommitOutput(t *testing.T) {
	st, eng := newIdleEngine(t)
	ctx := context.Background()

	const id = "payload-task-attempt-stale"

	if _, err := st.Create(ctx, &store.Execution{
		ID: "payload-task-attempt-stale", FlowName: "guard-linear",
		CurrentNode: "a", Status: store.StatusRunning, Attempt: 1,
	}); err != nil {
		t.Fatalf("create: %v", err)
	}

	flow := eng.flows["guard-linear"]
	node := flow.Node("a")
	staleSnapshot := &store.Execution{
		ID: id, FlowName: "guard-linear",
		CurrentNode: "a", Status: store.StatusRunning, Attempt: 0,
	}

	if err := eng.settleTaskSuccess(ctx, flow, node, staleSnapshot,
		invoker.Result{Status: invoker.StatusOK, Payload: json.RawMessage(`{"run":"stale"}`)}); err != nil {
		t.Fatalf("settle stale task attempt: %v", err)
	}

	ex, err := st.Get(ctx, id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if ex.CurrentNode != "a" || ex.Attempt != 1 || len(ex.Outputs) != 0 {
		t.Fatalf("stale task attempt committed: node=%q attempt=%d outputs=%v",
			ex.CurrentNode, ex.Attempt, ex.Outputs)
	}
}

func TestStaleAsyncBranchCompletionCannotOverwriteCommittedOutput(t *testing.T) {
	ctx, st, eng, flow := newIdleRedriveEngine(t)

	const id = "payload-async-branch-stale"

	putVersionedOutput(ctx, t, st, id, "b2", "fresh", json.RawMessage(`{"run":"fresh"}`))

	if _, err := st.Create(ctx, &store.Execution{
		ID: "payload-async-branch-stale", FlowName: "redrive",
		Status: store.StatusWaiting, CurrentNode: "join",
		Branches: map[string]store.BranchState{
			"b1": {NodeID: "b1", Status: store.BranchCompleted, Attempt: 0},
			"b2": {NodeID: "b2", Status: store.BranchCompleted, Attempt: 0},
		},
		Outputs:        []string{"b2"},
		OutputVersions: map[string]string{"b2": "fresh"},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}

	staleSnapshot := &store.Execution{
		ID: "payload-async-branch-stale", FlowName: "redrive",
		Status: store.StatusWaiting, CurrentNode: "join",
		Branches: map[string]store.BranchState{
			"b1": {NodeID: "b1", Status: store.BranchCompleted, Attempt: 0},
			"b2": {NodeID: "b2", Status: store.BranchPending, Attempt: 0},
		},
	}

	if err := eng.completeBranch(ctx, flow, staleSnapshot, "b2", 0, 0,
		invoker.Result{Status: invoker.StatusOK, Payload: json.RawMessage(`{"run":"stale"}`)}); err != nil {
		t.Fatalf("complete stale branch: %v", err)
	}

	assertResult(ctx, t, eng, id, "b2", `{"run":"fresh"}`)
}

func TestStaleSyncBranchPersistCannotOverwriteCommittedOutput(t *testing.T) {
	ctx, st, eng, _ := newIdleRedriveEngine(t)

	const id = "payload-sync-branch-stale"

	putVersionedOutput(ctx, t, st, id, "b1", "fresh", json.RawMessage(`{"run":"fresh"}`))
	putVersionedOutput(ctx, t, st, id, "b1", "stale", json.RawMessage(`{"run":"stale"}`))

	if _, err := st.Create(ctx, &store.Execution{
		ID: "payload-sync-branch-stale", FlowName: "redrive",
		Status: store.StatusWaiting, CurrentNode: "join",
		Branches: map[string]store.BranchState{
			"b1": {NodeID: "b1", Status: store.BranchCompleted, Attempt: 0},
			"b2": {NodeID: "b2", Status: store.BranchPending, Attempt: 0},
		},
		Outputs:        []string{"b1"},
		OutputVersions: map[string]string{"b1": "fresh"},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}

	if err := eng.persistBranch(ctx, id, "b1", 0, 0,
		store.BranchState{NodeID: "b1", Status: store.BranchCompleted, Attempt: 0}, "stale"); err != nil {
		t.Fatalf("persist stale branch: %v", err)
	}

	assertResult(ctx, t, eng, id, "b1", `{"run":"fresh"}`)
}

func TestPersistBranchRejectsStaleGeneration(t *testing.T) {
	ctx, st, eng, _ := newIdleRedriveEngine(t)

	const id = "persist-branch-stale-generation"

	if _, err := st.Create(ctx, &store.Execution{
		ID:             id,
		FlowName:       "redrive",
		Status:         store.StatusRunning,
		CurrentNode:    "fo",
		NodeGeneration: 2,
		Branches: map[string]store.BranchState{
			"b1": {NodeID: "b1", Status: store.BranchPending, Generation: 2, Attempt: 0},
		},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}

	if err := eng.persistBranch(ctx, id, "b1", 1, 0,
		store.BranchState{NodeID: "b1", Status: store.BranchCompleted, Generation: 1, Attempt: 0}, ""); err != nil {
		t.Fatalf("persist stale generation: %v", err)
	}

	ex, err := st.Get(ctx, id)
	if err != nil {
		t.Fatalf("get stale: %v", err)
	}

	if bs := ex.Branches["b1"]; bs.Status != store.BranchPending || bs.Generation != 2 {
		t.Fatalf("branch after stale persist = %+v, want pending generation 2", bs)
	}

	if err = eng.persistBranch(ctx, id, "b1", 2, 0,
		store.BranchState{NodeID: "b1", Status: store.BranchCompleted, Generation: 2, Attempt: 0}, ""); err != nil {
		t.Fatalf("persist fresh generation: %v", err)
	}

	ex, err = st.Get(ctx, id)
	if err != nil {
		t.Fatalf("get fresh: %v", err)
	}

	if bs := ex.Branches["b1"]; bs.Status != store.BranchCompleted || bs.Generation != 2 {
		t.Fatalf("branch after fresh persist = %+v, want completed generation 2", bs)
	}
}

func newIdleRedriveEngine(t *testing.T) (context.Context, *store.Store, *Engine, *dsl.Flow) {
	t.Helper()

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
		t.Fatal("invoker should not be called by direct stale branch settle tests")
		return invoker.Result{}, nil
	})

	eng, err := New(inv, st, sch, testSignals(t, st), map[string]*dsl.Flow{flow.Name: flow}, Config{})
	if err != nil {
		t.Fatalf("engine: %v", err)
	}

	return ctx, st, eng, flow
}

func putVersionedOutput(
	ctx context.Context,
	t *testing.T,
	st *store.Store,
	execID, node, version string,
	payload json.RawMessage,
) {
	t.Helper()

	if existing, err := st.CreatePayload(ctx, store.OutputVersionKey(execID, node, version), payload); err != nil {
		t.Fatalf("put output %s/%s/%s: %v", execID, node, version, err)
	} else if existing != nil {
		t.Fatalf("output version %s/%s/%s already existed", execID, node, version)
	}
}

func assertResult(ctx context.Context, t *testing.T, eng *Engine, execID, node, want string) {
	t.Helper()

	doc, err := eng.Results(ctx, execID)
	if err != nil {
		t.Fatalf("results: %v", err)
	}

	if got := parseCtx(t, doc); string(got.Results[node]) != want {
		t.Fatalf("results.%s = %s, want %s", node, got.Results[node], want)
	}
}
