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
	"testing"

	"github.com/henomis/packtrail/internal/store"
)

// TestRunSkipsBadEvent verifies the indexer Term's a malformed event (bad JSON)
// without stalling: a valid event published afterwards is still indexed.
func TestRunSkipsBadEvent(t *testing.T) {
	ctx, st, ix := setup(t)

	// Publish a poison message directly to the events stream; the consumer must
	// Term it and carry on.
	if _, err := st.JS().Publish(ctx, st.Names().SubjEventsPrefix+"badexec", []byte("{not json")); err != nil {
		t.Fatalf("publish bad event: %v", err)
	}

	ex := mkExec(t, st, "afterbad")
	waitIndex(t, ix, store.StatusRunning, ex.ID, true)
}

// TestIndexGetError exercises index's non-NotFound error path from the meta Get
// using a cancelled context.
func TestIndexGetError(t *testing.T) {
	_, _, ix := setup(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ev := store.Event{ExecID: "e", FlowName: "f", Status: store.StatusRunning, Revision: 1}
	if err := ix.index(ctx, ev); err == nil {
		t.Fatal("index with cancelled context succeeded, want error")
	}
}

// TestReconcileListError exercises Reconcile's error path when the source-of-truth
// scan fails.
func TestReconcileListError(t *testing.T) {
	_, _, ix := setup(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := ix.Reconcile(ctx); err == nil {
		t.Fatal("Reconcile with cancelled context succeeded, want error")
	}
}

// TestListByPrefixContextError exercises the Watch error path of collectIDs.
func TestListByPrefixContextError(t *testing.T) {
	_, st, _ := setup(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := collectIDs(ctx, st.IdxStatus(), store.StatusRunning+sep, 0); err == nil {
		t.Fatal("collectIDs with cancelled context succeeded, want error")
	}
}

// TestListEventsByPrefixSkipsCorrupt verifies a corrupt (non-JSON) index value is
// skipped rather than failing the whole query.
func TestListEventsByPrefixSkipsCorrupt(t *testing.T) {
	ctx, st, ix := setup(t)

	// Write a membership-shaped key under a flow prefix holding invalid JSON.
	if _, err := st.IdxFlow().Put(ctx, "corruptflow"+sep+"e1", []byte("{not json")); err != nil {
		t.Fatalf("put corrupt: %v", err)
	}

	evs, err := ix.ByFlowEvents(ctx, "corruptflow")
	if err != nil {
		t.Fatalf("by flow events: %v", err)
	}

	if len(evs) != 0 {
		t.Fatalf("corrupt entry was not skipped: %v", evs)
	}
}

// TestListEventsByPrefixContextError exercises the Watch error path of
// collectEvents.
func TestListEventsByPrefixContextError(t *testing.T) {
	_, st, _ := setup(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := collectEvents(ctx, st.IdxFlow(), "anyflow"+sep, 0); err == nil {
		t.Fatal("collectEvents with cancelled context succeeded, want error")
	}
}
