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
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail/internal/names"
	"github.com/henomis/packtrail/internal/natstest"
	"github.com/henomis/packtrail/internal/store"
)

// hookKV wraps the flow-index bucket and fires a hook once, just before the
// first guarded commit (Create or Update) — the window between an indexer's
// read and its write.
type hookKV struct {
	jetstream.KeyValue

	before func()
}

func (h *hookKV) fire() {
	if h.before != nil {
		f := h.before
		h.before = nil

		f()
	}
}

func (h *hookKV) Create(ctx context.Context, key string, value []byte, opts ...jetstream.KVCreateOpt) (uint64, error) {
	h.fire()

	return h.KeyValue.Create(ctx, key, value, opts...)
}

func (h *hookKV) Update(ctx context.Context, key string, value []byte, revision uint64) (uint64, error) {
	h.fire()

	return h.KeyValue.Update(ctx, key, value, revision)
}

func (h *hookKV) Delete(ctx context.Context, key string, opts ...jetstream.KVDeleteOpt) error {
	h.fire()

	return h.KeyValue.Delete(ctx, key, opts...)
}

// TestIndexConcurrentProjectionDoesNotRegress reproduces the multi-instance
// indexer race deterministically: indexer A reads the bookkeeping record for an
// older event, and before A commits, indexer B fully projects a newer event for
// the same execution. A's commit must lose the CAS and its retry must yield to
// the newer record — the index may never regress to the older event's status,
// and A's provisionally written status membership must be cleaned up.
func TestIndexConcurrentProjectionDoesNotRegress(t *testing.T) {
	ctx := context.Background()
	srv := natstest.Start(t)

	st, err := store.Open(ctx, srv.JS, names.New(""))
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	a, b := New(st), New(st)

	// Arm A: in its read→commit window, B projects the newer event.
	evNew := store.Event{ExecID: "race-x", FlowName: "f", Status: store.StatusWaiting, Revision: 7, Time: time.Now().UTC()}
	a.idxFlow = &hookKV{KeyValue: a.idxFlow, before: func() {
		if idxErr := b.index(ctx, evNew); idxErr != nil {
			t.Errorf("B index: %v", idxErr)
		}
	}}

	evOld := store.Event{ExecID: "race-x", FlowName: "f", Status: store.StatusRunning, Revision: 6, Time: time.Now().UTC()}
	if err = a.index(ctx, evOld); err != nil {
		t.Fatalf("A index: %v", err)
	}

	// The bookkeeping record must hold the newer event.
	entry, err := b.idxFlow.Get(ctx, "f"+sep+"race-x")
	if err != nil {
		t.Fatalf("get flow entry: %v", err)
	}

	var rec store.Event
	if err = json.Unmarshal(entry.Value(), &rec); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if rec.Revision != 7 || rec.Status != store.StatusWaiting {
		t.Fatalf("flow record = rev %d status %q, want rev 7 waiting (index regressed to the older event)",
			rec.Revision, rec.Status)
	}

	// Membership: waiting present, the older event's provisional entry cleaned.
	if ids, _ := b.ByStatus(ctx, store.StatusWaiting); !contains(ids, "race-x") {
		t.Fatal("waiting membership missing")
	}

	if ids, _ := b.ByStatus(ctx, store.StatusRunning); contains(ids, "race-x") {
		t.Fatal("stale running membership left behind by the losing projection")
	}
}

func TestStaleSameStatusProjectionReassertsEventSummary(t *testing.T) {
	ctx := context.Background()
	srv := natstest.Start(t)

	st, err := store.Open(ctx, srv.JS, names.New(""))
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	ix := New(st)

	current := store.Event{
		ExecID: "same-status-race", FlowName: "f", Status: store.StatusRunning,
		Node: "new", Error: "new", Revision: 2, Time: time.Now().UTC(),
	}
	if err = ix.index(ctx, current); err != nil {
		t.Fatalf("index current: %v", err)
	}

	stale := store.Event{
		ExecID: "same-status-race", FlowName: "f", Status: store.StatusRunning,
		Node: "old", Error: "old", Revision: 1, Time: time.Now().UTC(),
	}

	data, err := json.Marshal(stale)
	if err != nil {
		t.Fatalf("marshal stale: %v", err)
	}

	if _, err = st.IdxStatus().Put(ctx, store.StatusRunning+sep+stale.ExecID, data); err != nil {
		t.Fatalf("put stale status: %v", err)
	}

	if _, err = st.IdxFlow().Put(ctx, stale.FlowName+sep+stale.ExecID, data); err != nil {
		t.Fatalf("put stale flow: %v", err)
	}

	if err = ix.index(ctx, stale); err != nil {
		t.Fatalf("reindex stale: %v", err)
	}

	evs, err := ix.ByStatusEvents(ctx, store.StatusRunning)
	if err != nil {
		t.Fatalf("by status events: %v", err)
	}

	got := findEvent(t, evs, stale.ExecID)
	if got.Revision != current.Revision || got.Node != current.Node || got.Error != current.Error {
		t.Fatalf("event summary = %+v, want current %+v", got, current)
	}
}

func TestStaleReassertRetriesWhenMetaAdvances(t *testing.T) {
	ctx := context.Background()
	srv := natstest.Start(t)

	st, err := store.Open(ctx, srv.JS, names.New(""))
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	a, b := New(st), New(st)

	current := store.Event{ExecID: "stale-reassert", FlowName: "f", Status: store.StatusRunning, Revision: 2}
	if err = a.index(ctx, current); err != nil {
		t.Fatalf("index current: %v", err)
	}

	newer := store.Event{ExecID: "stale-reassert", FlowName: "f", Status: store.StatusWaiting, Revision: 3}
	a.idxFlow = &hookKV{KeyValue: a.idxFlow, before: func() {
		if idxErr := b.index(ctx, newer); idxErr != nil {
			t.Errorf("B index: %v", idxErr)
		}
	}}

	stale := store.Event{ExecID: "stale-reassert", FlowName: "f", Status: store.StatusRunning, Revision: 1}
	if err = a.index(ctx, stale); err != nil {
		t.Fatalf("A stale index: %v", err)
	}

	evs, err := a.ByStatusEvents(ctx, store.StatusWaiting)
	if err != nil {
		t.Fatalf("by status events: %v", err)
	}

	if got := findEvent(t, evs, stale.ExecID); got.Revision != newer.Revision {
		t.Fatalf("winner revision = %d, want newer revision %d", got.Revision, newer.Revision)
	}

	if ids, _ := a.ByStatus(ctx, store.StatusRunning); contains(ids, stale.ExecID) {
		t.Fatalf("stale running membership survived after retry: %v", ids)
	}
}

// TestIndexMetaMatchesMembership: after a projection, the bookkeeping record
// and the read-model memberships must agree — same revision, same status, and
// the flow membership carries the same event.
func TestIndexMetaMatchesMembership(t *testing.T) {
	ctx := context.Background()
	srv := natstest.Start(t)

	st, err := store.Open(ctx, srv.JS, names.New(""))
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	ix := New(st)

	ev := store.Event{ExecID: "meta-x", FlowName: "f", Status: store.StatusRunning, Revision: 3, Time: time.Now().UTC()}
	if err = ix.index(ctx, ev); err != nil {
		t.Fatalf("index: %v", err)
	}

	var meta, membership store.Event

	entry, err := ix.idxFlow.Get(ctx, metaKey("meta-x"))
	if err != nil {
		t.Fatalf("get meta: %v", err)
	}

	if err = json.Unmarshal(entry.Value(), &meta); err != nil {
		t.Fatalf("unmarshal meta: %v", err)
	}

	entry, err = ix.idxFlow.Get(ctx, "f"+sep+"meta-x")
	if err != nil {
		t.Fatalf("get membership: %v", err)
	}

	if err = json.Unmarshal(entry.Value(), &membership); err != nil {
		t.Fatalf("unmarshal membership: %v", err)
	}

	if meta.Revision != 3 || meta.Status != store.StatusRunning ||
		membership.Revision != meta.Revision || membership.Status != meta.Status {
		t.Fatalf("meta %+v and membership %+v disagree", meta, membership)
	}
}

// TestGCPreservesRecreatedExecution reproduces the GC-vs-re-Start race: GC
// collects a terminal candidate and sees it absent from the store, but before
// it deletes the index entries a re-Start recreates the id (rewriting the meta
// entry and fresh membership). The revision-guarded meta delete must fail and
// GC must leave the recreated execution's fresh index entries intact.
func TestGCPreservesRecreatedExecution(t *testing.T) {
	ctx := context.Background()
	srv := natstest.Start(t)

	st, err := store.Open(ctx, srv.JS, names.New(""))
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	ix := New(st)

	// Index a terminal execution that is NOT in the store, so GC sees it as gone
	// and becomes a delete candidate.
	old := store.Event{ExecID: "gc-x", FlowName: "f", Status: store.StatusCompleted, Revision: 1, Time: time.Now().UTC()}
	if err = ix.index(ctx, old); err != nil {
		t.Fatalf("index old: %v", err)
	}

	// Arm the meta delete: just before GC deletes the meta entry, a second
	// indexer recreates gc-x with a newer event (bumping the meta revision and
	// writing fresh membership) — exactly the re-Start-in-the-window case.
	b := New(st)
	fresh := store.Event{ExecID: "gc-x", FlowName: "f", Status: store.StatusRunning, Revision: 2, Time: time.Now().UTC()}
	ix.idxFlow = &hookKV{KeyValue: ix.idxFlow, before: func() {
		if idxErr := b.index(ctx, fresh); idxErr != nil {
			t.Errorf("recreate index: %v", idxErr)
		}
	}}

	if _, err = ix.GC(ctx, 0); err != nil {
		t.Fatalf("GC: %v", err)
	}

	// The recreated execution's fresh membership must survive the guarded delete.
	ids, err := b.ByFlow(ctx, "f")
	if err != nil {
		t.Fatalf("by flow: %v", err)
	}

	if len(ids) != 1 || ids[0] != "gc-x" {
		t.Fatalf("ByFlow(f) = %v, want [gc-x] (GC clobbered the recreated execution's index)", ids)
	}
}

// TestMetaKeysInvisibleToFlowQueries: bookkeeping records live in the flow
// bucket under the "meta=." prefix, which no flow name can produce — even a
// flow literally named "meta" must not see them in its membership listing.
func TestMetaKeysInvisibleToFlowQueries(t *testing.T) {
	ctx := context.Background()
	srv := natstest.Start(t)

	st, err := store.Open(ctx, srv.JS, names.New(""))
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	ix := New(st)

	ev := store.Event{ExecID: "m-1", FlowName: "meta", Status: store.StatusRunning, Revision: 1, Time: time.Now().UTC()}
	if err = ix.index(ctx, ev); err != nil {
		t.Fatalf("index: %v", err)
	}

	ids, err := ix.ByFlow(ctx, "meta")
	if err != nil {
		t.Fatalf("by flow: %v", err)
	}

	if len(ids) != 1 || ids[0] != "m-1" {
		t.Fatalf("ByFlow(meta) = %v, want exactly [m-1] (bookkeeping keys must not leak into flow queries)", ids)
	}
}
