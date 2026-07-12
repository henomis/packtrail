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
