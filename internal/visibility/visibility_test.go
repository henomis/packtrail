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

package visibility

import (
	"context"
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/henomis/packtrail/internal/names"
	"github.com/henomis/packtrail/internal/natstest"
	"github.com/henomis/packtrail/internal/store"
)

// setup starts an embedded server, a store and a running indexer.
func setup(t *testing.T) (context.Context, *store.Store, *Indexer) {
	t.Helper()

	ctx := context.Background()
	srv := natstest.Start(t)

	st, err := store.Open(ctx, srv.JS, names.New(""))
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	ix := New(st)

	cc, err := ix.Run(ctx)
	if err != nil {
		t.Fatalf("indexer run: %v", err)
	}

	t.Cleanup(cc.Stop)

	return ctx, st, ix
}

// contains reports whether id is in ids.
func contains(ids []string, id string) bool { return slices.Contains(ids, id) }

const waitIndexTimeout = 3 * time.Second

// waitIndex polls ByStatus until the membership predicate holds, or fails.
func waitIndex(t *testing.T, ix *Indexer, status, id string, want bool) {
	t.Helper()

	ctx := context.Background()

	deadline := time.Now().Add(waitIndexTimeout)
	for time.Now().Before(deadline) {
		ids, err := ix.ByStatus(ctx, status)
		if err == nil && contains(ids, id) == want {
			return
		}

		time.Sleep(20 * time.Millisecond)
	}

	ids, _ := ix.ByStatus(ctx, status)
	t.Fatalf("ByStatus(%q) membership of %s = %v, want %v (have %v)", status, id, !want, want, ids)
}

func mkExec(t *testing.T, st *store.Store, flow string) *store.Execution {
	t.Helper()

	id := "exec-" + flow + "-" + time.Now().Format("150405.000000")

	ex := &store.Execution{ID: id, FlowName: flow, Status: store.StatusRunning, CurrentNode: "start"}
	if _, err := st.Create(context.Background(), ex); err != nil {
		t.Fatalf("create: %v", err)
	}

	if err := st.EmitEvent(context.Background(), ex); err != nil {
		t.Fatalf("emit: %v", err)
	}

	return ex
}

// TestIndexReflectsStateWithinSeconds is the §12 acceptance test: a status-index
// query reflects real state within a few seconds with no manual reconciliation.
func TestIndexReflectsStateWithinSeconds(t *testing.T) {
	ctx, st, ix := setup(t)
	ex := mkExec(t, st, "alpha")

	// Initial running state is indexed promptly.
	waitIndex(t, ix, store.StatusRunning, ex.ID, true)

	// Transition to completed: the new status appears and the old one is gone.
	updated, err := st.Mutate(ctx, ex.ID, func(e *store.Execution) error {
		e.Status = store.StatusCompleted
		e.CurrentNode = ""

		return nil
	})
	if err != nil {
		t.Fatalf("mutate: %v", err)
	}

	_ = st.EmitEvent(ctx, updated)

	waitIndex(t, ix, store.StatusCompleted, ex.ID, true)
	waitIndex(t, ix, store.StatusRunning, ex.ID, false)
}

// TestByFlow verifies the flow index groups executions by flow name.
func TestByFlow(t *testing.T) {
	ctx, st, ix := setup(t)
	a := mkExec(t, st, "beta")
	b := mkExec(t, st, "beta")
	c := mkExec(t, st, "gamma")

	waitIndex(t, ix, store.StatusRunning, a.ID, true)
	waitIndex(t, ix, store.StatusRunning, b.ID, true)
	waitIndex(t, ix, store.StatusRunning, c.ID, true)

	beta, err := ix.ByFlow(ctx, "beta")
	if err != nil {
		t.Fatalf("by flow: %v", err)
	}

	if !contains(beta, a.ID) || !contains(beta, b.ID) || contains(beta, c.ID) {
		t.Fatalf("ByFlow(beta) = %v, want {%s,%s} only", beta, a.ID, b.ID)
	}
}

// TestStaleEventIgnored verifies an out-of-order (lower-revision) event does not
// resurrect a previous status.
func TestStaleEventIgnored(t *testing.T) {
	ctx, st, ix := setup(t)
	ex := mkExec(t, st, "delta") // revision r1, running
	r1 := ex.Revision

	updated, err := st.Mutate(ctx, ex.ID, func(e *store.Execution) error {
		e.Status = store.StatusCompleted
		return nil
	})
	if err != nil {
		t.Fatalf("mutate: %v", err)
	}

	_ = st.EmitEvent(ctx, updated)

	waitIndex(t, ix, store.StatusCompleted, ex.ID, true)

	// Replay the original running event (revision r1 < current): must be ignored.
	stale := store.Event{ExecID: ex.ID, FlowName: ex.FlowName, Status: store.StatusRunning, Revision: r1, Time: time.Now()}
	if indexErr := ix.index(ctx, stale); indexErr != nil {
		t.Fatalf("index stale: %v", indexErr)
	}

	if ids, _ := ix.ByStatus(ctx, store.StatusRunning); contains(ids, ex.ID) {
		t.Fatalf("stale event resurrected running status: %v", ids)
	}
}

// findEvent returns the event for id from evs, or fails.
func findEvent(t *testing.T, evs []store.Event, id string) store.Event {
	t.Helper()

	for _, ev := range evs {
		if ev.ExecID == id {
			return ev
		}
	}

	t.Fatalf("event for %s not found in %v", id, evs)

	return store.Event{}
}

// TestByStatusEvents verifies the status index returns full events (not just
// ids), each carrying the execution's flow, node and error message.
func TestByStatusEvents(t *testing.T) {
	ctx, st, ix := setup(t)
	ex := mkExec(t, st, "zeta")
	waitIndex(t, ix, store.StatusRunning, ex.ID, true)

	evs, err := ix.ByStatusEvents(ctx, store.StatusRunning)
	if err != nil {
		t.Fatalf("by status events: %v", err)
	}

	ev := findEvent(t, evs, ex.ID)
	if ev.FlowName != "zeta" || ev.Status != store.StatusRunning || ev.Node != "start" {
		t.Fatalf("event = %+v, want flow=zeta status=running node=start", ev)
	}
}

// TestByFlowEvents verifies the flow index returns full events grouped by flow.
func TestByFlowEvents(t *testing.T) {
	ctx, st, ix := setup(t)
	a := mkExec(t, st, "eta")
	b := mkExec(t, st, "theta")
	waitIndex(t, ix, store.StatusRunning, a.ID, true)
	waitIndex(t, ix, store.StatusRunning, b.ID, true)

	evs, err := ix.ByFlowEvents(ctx, "eta")
	if err != nil {
		t.Fatalf("by flow events: %v", err)
	}

	if got := findEvent(t, evs, a.ID); got.FlowName != "eta" {
		t.Fatalf("event flow = %q, want eta", got.FlowName)
	}

	for _, ev := range evs {
		if ev.ExecID == b.ID {
			t.Fatalf("ByFlowEvents(eta) leaked execution from another flow: %v", evs)
		}
	}
}

// TestByStatusEventsCarryError verifies a failed execution's error message is
// projected into the index and surfaced through ByStatusEvents, and that a
// Reconcile rebuild preserves it (the reassert path).
func TestByStatusEventsCarryError(t *testing.T) {
	ctx, st, ix := setup(t)
	ex := mkExec(t, st, "iota")
	waitIndex(t, ix, store.StatusRunning, ex.ID, true)

	const wantErr = "boom: task exploded"

	updated, err := st.Mutate(ctx, ex.ID, func(e *store.Execution) error {
		e.Status = store.StatusFailed
		e.Error = wantErr

		return nil
	})
	if err != nil {
		t.Fatalf("mutate: %v", err)
	}

	_ = st.EmitEvent(ctx, updated)

	waitIndex(t, ix, store.StatusFailed, ex.ID, true)

	// Live projection carries the error.
	evs, err := ix.ByStatusEvents(ctx, store.StatusFailed)
	if err != nil {
		t.Fatalf("by status events: %v", err)
	}

	if got := findEvent(t, evs, ex.ID); got.Error != wantErr {
		t.Fatalf("live event error = %q, want %q", got.Error, wantErr)
	}

	// Corrupt and rebuild from the source of truth: the error must survive.
	// Delete the status membership and the flow bookkeeping entry.
	_ = st.IdxStatus().Delete(ctx, store.StatusFailed+sep+ex.ID)
	_ = st.IdxFlow().Delete(ctx, "iota"+sep+ex.ID)

	if reconcileErr := ix.Reconcile(ctx); reconcileErr != nil {
		t.Fatalf("reconcile: %v", reconcileErr)
	}

	evs, err = ix.ByStatusEvents(ctx, store.StatusFailed)
	if err != nil {
		t.Fatalf("by status events after reconcile: %v", err)
	}

	if got := findEvent(t, evs, ex.ID); got.Error != wantErr {
		t.Fatalf("reconciled event error = %q, want %q", got.Error, wantErr)
	}
}

// TestReconcileRepairsDrift verifies Reconcile rebuilds the index from the
// source of truth after manual corruption.
func TestReconcileRepairsDrift(t *testing.T) {
	ctx, st, ix := setup(t)
	ex := mkExec(t, st, "epsilon")
	waitIndex(t, ix, store.StatusRunning, ex.ID, true)

	// Corrupt the index: delete the status membership and flow bookkeeping entries.
	_ = st.IdxStatus().Delete(ctx, store.StatusRunning+sep+ex.ID)

	_ = st.IdxFlow().Delete(ctx, "epsilon"+sep+ex.ID)
	if ids, _ := ix.ByStatus(ctx, store.StatusRunning); contains(ids, ex.ID) {
		t.Fatalf("expected corrupted index to drop %s", ex.ID)
	}

	if err := ix.Reconcile(ctx); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	if ids, _ := ix.ByStatus(ctx, store.StatusRunning); !contains(ids, ex.ID) {
		t.Fatalf("reconcile did not restore %s: %v", ex.ID, ids)
	}
}

// TestGCPrunesOrphans verifies GC deletes index entries whose execution is gone
// from both the hot bucket and the archive, and leaves live entries intact.
func TestGCPrunesOrphans(t *testing.T) {
	ctx, st, ix := setup(t)

	// A live completed execution: indexed and still present in the store.
	live := mkExec(t, st, "kappa")

	updated, err := st.Mutate(ctx, live.ID, func(e *store.Execution) error {
		e.Status = store.StatusCompleted

		return nil
	})
	if err != nil {
		t.Fatalf("mutate: %v", err)
	}

	_ = st.EmitEvent(ctx, updated)

	waitIndex(t, ix, store.StatusCompleted, live.ID, true)

	// An orphan: index entries pointing at an execution that no longer exists
	// (as if its archive entry expired). Write the membership and bookkeeping
	// directly, with an old timestamp so the staleAfter filter selects it.
	orphan := store.Event{
		ExecID: "ghost", FlowName: "kappa", Status: store.StatusCompleted,
		Revision: 1, Time: time.Now().Add(-48 * time.Hour),
	}

	val, err := json.Marshal(orphan)
	if err != nil {
		t.Fatalf("marshal orphan: %v", err)
	}

	if _, err = st.IdxStatus().Put(ctx, store.StatusCompleted+sep+"ghost", val); err != nil {
		t.Fatalf("put orphan status: %v", err)
	}

	if _, err = st.IdxFlow().Put(ctx, "kappa"+sep+"ghost", val); err != nil {
		t.Fatalf("put orphan flow: %v", err)
	}

	if _, err = st.IdxFlow().Put(ctx, metaKey("ghost"), val); err != nil {
		t.Fatalf("put orphan meta: %v", err)
	}

	pruned, err := ix.GC(ctx, 24*time.Hour)
	if err != nil || pruned != 1 {
		t.Fatalf("GC = %d, %v; want 1, nil", pruned, err)
	}

	if ids, _ := ix.ByStatus(ctx, store.StatusCompleted); contains(ids, "ghost") {
		t.Error("GC did not prune the orphan")
	}

	if _, err = st.IdxFlow().Get(ctx, metaKey("ghost")); err == nil {
		t.Error("GC did not prune the orphan's bookkeeping record")
	}

	if ids, _ := ix.ByStatus(ctx, store.StatusCompleted); !contains(ids, live.ID) {
		t.Errorf("GC pruned a live entry: %v", ids)
	}
}

// TestReconcileActiveFixesStaleStatus verifies the cheap active-set pass moves
// an execution whose terminal transition was never projected (the event was
// dropped, so the index still lists it as running) to its real status.
func TestReconcileActiveFixesStaleStatus(t *testing.T) {
	ctx, st, ix := setup(t)
	ex := mkExec(t, st, "zeta")
	waitIndex(t, ix, store.StatusRunning, ex.ID, true)

	// Advance the source of truth to completed but drop the event, so the index
	// is left stale (still running).
	if _, err := st.Mutate(ctx, ex.ID, func(e *store.Execution) error {
		e.Status = store.StatusCompleted
		e.CurrentNode = ""

		return nil
	}); err != nil {
		t.Fatalf("mutate: %v", err)
	}

	if ids, _ := ix.ByStatus(ctx, store.StatusCompleted); contains(ids, ex.ID) {
		t.Fatalf("precondition: %s already indexed completed", ex.ID)
	}

	if err := ix.ReconcileActive(ctx); err != nil {
		t.Fatalf("reconcile active: %v", err)
	}

	if ids, _ := ix.ByStatus(ctx, store.StatusCompleted); !contains(ids, ex.ID) {
		t.Fatalf("active reconcile did not move %s to completed: %v", ex.ID, ids)
	}

	if ids, _ := ix.ByStatus(ctx, store.StatusRunning); contains(ids, ex.ID) {
		t.Fatalf("active reconcile left stale running entry for %s: %v", ex.ID, ids)
	}
}
