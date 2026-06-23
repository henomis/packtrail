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

// Package visibility maintains the eventually-consistent status/flow indexes
// (spec §9). An Indexer durably consumes the packtrail-events stream and projects
// each domain event into the packtrail-idx-status and packtrail-idx-flow KV buckets,
// idempotently and per-revision: an event is applied only if its execution
// revision is newer than the one already indexed, so duplicate or out-of-order
// deliveries — and multiple indexer instances on the same durable — are safe.
//
// A periodic Reconcile authoritatively rebuilds the indexes from the source of
// truth (packtrail-executions), closing any residual drift. The indexes are
// best-effort: use them for dashboards and operational search, never for
// correctness decisions (for those, read packtrail-executions directly).
package visibility

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail/internal/store"
)

const (
	// sep joins the index dimension and the execution id. NATS KV keys cannot
	// contain ':', so the spec's "<status>:<id>" is rendered as "<status>.<id>".
	sep = "."
	// metaPrefix keys, in the status bucket, hold the last-indexed revision and
	// status per execution. They never collide with a "<status>." membership
	// prefix, so status queries skip them.
	metaPrefix = "_rev_" + sep // _rev_.<execID>

	indexerAckWait = 30 * time.Second
)

// allStatuses is the closed set of execution statuses, used by Reconcile to
// purge stale membership entries for an execution under any other status.
var allStatuses = []string{
	store.StatusRunning, store.StatusWaiting, store.StatusCompleted, store.StatusFailed,
}

// meta is the per-execution bookkeeping value stored at metaPrefix+<execID>.
type meta struct {
	Rev    uint64 `json:"rev"`
	Status string `json:"status"`
}

// Indexer projects domain events into the visibility indexes and answers
// lookups by status and by flow.
type Indexer struct {
	store      *store.Store
	js         jetstream.JetStream
	idxStatus  jetstream.KeyValue
	idxFlow    jetstream.KeyValue
	stream     string
	subjPrefix string
	durable    string
	log        *slog.Logger
}

// New returns an Indexer backed by the store's JetStream context, index buckets
// and namespace.
func New(st *store.Store) *Indexer {
	n := st.Names()

	return &Indexer{
		store:      st,
		js:         st.JS(),
		idxStatus:  st.IdxStatus(),
		idxFlow:    st.IdxFlow(),
		stream:     n.StreamEvents,
		subjPrefix: n.SubjEventsPrefix,
		durable:    n.DurIndexer,
		log:        slog.Default().With("component", "indexer"),
	}
}

// Run starts the durable consumer on packtrail-events and projects every event
// until ctx is cancelled. The returned ConsumeContext must be stopped by the
// caller. DeliverAll lets a fresh or restarted indexer catch up from the start
// of the stream; projection is idempotent so replays are harmless.
func (ix *Indexer) Run(ctx context.Context) (jetstream.ConsumeContext, error) {
	cons, err := ix.js.CreateOrUpdateConsumer(ctx, ix.stream, jetstream.ConsumerConfig{
		Durable:       ix.durable,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       indexerAckWait,
		FilterSubject: ix.subjPrefix + ">",
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		return nil, fmt.Errorf("indexer consumer: %w", err)
	}

	return cons.Consume(func(msg jetstream.Msg) {
		var ev store.Event
		if unmarshalErr := json.Unmarshal(msg.Data(), &ev); unmarshalErr != nil {
			ix.log.Error("bad event", "err", unmarshalErr)

			_ = msg.Term() // poison message

			return
		}

		if indexErr := ix.index(ctx, ev); indexErr != nil {
			ix.log.Error("index", "exec", ev.ExecID, "err", indexErr)

			_ = msg.NakWithDelay(time.Second)

			return
		}

		_ = msg.Ack()
	})
}

// index applies a single event idempotently. It writes only if the event's
// revision is newer than the last one indexed for that execution, and removes
// the membership entry under the previous status when the status changes.
func (ix *Indexer) index(ctx context.Context, ev store.Event) error {
	var prevStatus string

	if entry, err := ix.idxStatus.Get(ctx, metaPrefix+ev.ExecID); err == nil {
		var m meta
		if json.Unmarshal(entry.Value(), &m) == nil {
			if ev.Revision <= m.Rev {
				return nil // stale or duplicate: already indexed at >= this revision
			}

			prevStatus = m.Status
		}
	} else if !errors.Is(err, jetstream.ErrKeyNotFound) {
		return err
	}

	val, err := json.Marshal(ev)
	if err != nil {
		return err
	}

	if _, putErr := ix.idxStatus.Put(ctx, ev.Status+sep+ev.ExecID, val); putErr != nil {
		return putErr
	}

	if prevStatus != "" && prevStatus != ev.Status {
		_ = ix.idxStatus.Delete(ctx, prevStatus+sep+ev.ExecID) // best-effort cleanup
	}

	if _, putErr := ix.idxFlow.Put(ctx, ev.FlowName+sep+ev.ExecID, val); putErr != nil {
		return putErr
	}

	m, err := json.Marshal(meta{Rev: ev.Revision, Status: ev.Status})
	if err != nil {
		return err
	}

	_, err = ix.idxStatus.Put(ctx, metaPrefix+ev.ExecID, m)

	return err
}

// Reconcile rebuilds the indexes from the source of truth, closing any drift
// the asynchronous projection may have left behind (spec §9). It scans
// packtrail-executions and authoritatively re-asserts each execution's membership.
//
// Per §13 this full scan is the part that does not scale to high volumes; the
// suggested mitigation is to scope the scan to still-active executions and/or
// archive terminal executions out of the hot bucket.
func (ix *Indexer) Reconcile(ctx context.Context) error {
	keys, err := ix.store.ListExecutionKeys(ctx)
	if err != nil {
		return err
	}

	for _, id := range keys {
		ex, getErr := ix.store.Get(ctx, id)
		if getErr != nil {
			if errors.Is(getErr, store.ErrNotFound) {
				continue
			}

			return getErr
		}

		if reassertErr := ix.reassert(ctx, ex); reassertErr != nil {
			return reassertErr
		}
	}

	return nil
}

// reassert forces the indexes to match an execution's current state: it writes
// the membership entry for the current status, removes entries under every
// other status, refreshes the flow entry, and updates the bookkeeping meta.
func (ix *Indexer) reassert(ctx context.Context, ex *store.Execution) error {
	ev := store.Event{
		ExecID:   ex.ID,
		FlowName: ex.FlowName,
		Status:   ex.Status,
		Node:     ex.CurrentNode,
		Error:    ex.Error,
		Revision: ex.Revision,
		Time:     time.Now().UTC(),
	}

	val, err := json.Marshal(ev)
	if err != nil {
		return err
	}

	if _, putErr := ix.idxStatus.Put(ctx, ex.Status+sep+ex.ID, val); putErr != nil {
		return putErr
	}

	for _, s := range allStatuses {
		if s != ex.Status {
			_ = ix.idxStatus.Delete(ctx, s+sep+ex.ID)
		}
	}

	if _, putErr := ix.idxFlow.Put(ctx, ex.FlowName+sep+ex.ID, val); putErr != nil {
		return putErr
	}

	m, err := json.Marshal(meta{Rev: ex.Revision, Status: ex.Status})
	if err != nil {
		return err
	}

	_, err = ix.idxStatus.Put(ctx, metaPrefix+ex.ID, m)

	return err
}

// ByStatus returns the ids of executions currently indexed under status.
func (ix *Indexer) ByStatus(ctx context.Context, status string) ([]string, error) {
	return listByPrefix(ctx, ix.idxStatus, status+sep)
}

// ByFlow returns the ids of executions belonging to flow.
func (ix *Indexer) ByFlow(ctx context.Context, flow string) ([]string, error) {
	return listByPrefix(ctx, ix.idxFlow, flow+sep)
}

// ByStatusEvents returns the index entries for all executions under status.
// Each entry is the most recent event stored for that execution, so callers
// can build summaries (including the error message) without a per-execution
// round-trip.
func (ix *Indexer) ByStatusEvents(ctx context.Context, status string) ([]store.Event, error) {
	return listEventsByPrefix(ctx, ix.idxStatus, status+sep)
}

// ByFlowEvents returns the index entries for all executions belonging to flow.
func (ix *Indexer) ByFlowEvents(ctx context.Context, flow string) ([]store.Event, error) {
	return listEventsByPrefix(ctx, ix.idxFlow, flow+sep)
}

// listByPrefix returns the execution ids of every key in kv that begins with
// prefix. It uses a server-side Watch filter so only matching keys are
// transferred, avoiding a full-bucket scan.
func listByPrefix(ctx context.Context, kv jetstream.KeyValue, prefix string) ([]string, error) {
	w, err := kv.Watch(ctx, prefix+">", jetstream.IgnoreDeletes(), jetstream.MetaOnly())
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return nil, nil
		}

		return nil, err
	}
	defer func() { _ = w.Stop() }()

	var out []string

	for {
		select {
		case entry, ok := <-w.Updates():
			if !ok || entry == nil {
				return out, nil
			}

			if id, found := strings.CutPrefix(entry.Key(), prefix); found {
				out = append(out, id)
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// listEventsByPrefix returns the stored event values for every key in kv that
// begins with prefix. Unlike listByPrefix it fetches full values so callers
// can build summaries without additional round-trips.
func listEventsByPrefix(ctx context.Context, kv jetstream.KeyValue, prefix string) ([]store.Event, error) {
	w, err := kv.Watch(ctx, prefix+">", jetstream.IgnoreDeletes())
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return nil, nil
		}

		return nil, err
	}
	defer func() { _ = w.Stop() }()

	var out []store.Event

	for {
		select {
		case entry, ok := <-w.Updates():
			if !ok || entry == nil {
				return out, nil
			}

			var ev store.Event
			if json.Unmarshal(entry.Value(), &ev) == nil {
				out = append(out, ev)
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}
