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

	indexerAckWait = 30 * time.Second
)

// allStatuses is the closed set of execution statuses, used by Reconcile to
// purge stale membership entries for an execution under any other status.
var allStatuses = []string{
	store.StatusRunning, store.StatusWaiting, store.StatusCompleted, store.StatusFailed,
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

// index applies a single event idempotently. The flow-index entry doubles as
// the per-execution bookkeeping record: there is exactly one per execution (the
// flow never changes) and its stored event carries the last-indexed revision and
// status. So index reads it to skip stale/duplicate events and to find the
// previous status to clean up, then writes the status membership and refreshes
// the flow entry last (the commit point). This avoids a separate per-execution
// meta key, halving the status-bucket cardinality and dropping one write per
// event.
func (ix *Indexer) index(ctx context.Context, ev store.Event) error {
	prevStatus, fresh, err := ix.lastIndexed(ctx, ev.FlowName, ev.ExecID)
	if err != nil {
		return err
	}

	if fresh >= ev.Revision {
		return nil // stale or duplicate: already indexed at >= this revision
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

	// The flow entry is written last: it is the bookkeeping record a later event
	// reads, so a crash before this point simply reprocesses idempotently.
	_, err = ix.idxFlow.Put(ctx, ev.FlowName+sep+ev.ExecID, val)

	return err
}

// lastIndexed reads the flow-index bookkeeping entry for an execution, returning
// the status and revision last indexed for it. A revision of 0 (with no error)
// means the execution has not been indexed yet.
func (ix *Indexer) lastIndexed(ctx context.Context, flow, execID string) (status string, rev uint64, err error) {
	entry, getErr := ix.idxFlow.Get(ctx, flow+sep+execID)
	if getErr != nil {
		if errors.Is(getErr, jetstream.ErrKeyNotFound) {
			return "", 0, nil
		}

		return "", 0, getErr
	}

	var prev store.Event
	if json.Unmarshal(entry.Value(), &prev) != nil {
		return "", 0, nil // corrupt: treat as not indexed so it gets rewritten
	}

	return prev.Status, prev.Revision, nil
}

// Reconcile rebuilds the indexes from the source of truth, closing any drift
// the asynchronous projection may have left behind (spec §9). It streams every
// execution in packtrail-executions through a single last-per-key watch and
// authoritatively re-asserts each one's membership, so a full pass costs one
// round-trip rather than a key listing plus a Get per execution.
//
// Per §13 this full scan still grows with the total number of executions ever
// created; ReconcileActive is the cheap, frequent counterpart that only touches
// in-flight executions. Run Reconcile infrequently as the deep backstop.
func (ix *Indexer) Reconcile(ctx context.Context) error {
	return ix.store.ForEachExecution(ctx, func(ex *store.Execution) error {
		return ix.reassert(ctx, ex)
	})
}

// ReconcileActive re-asserts the indexes for only the executions the index
// currently lists as in-flight (running or waiting). It costs O(in-flight)
// index reads plus a Get per active execution — independent of how many
// terminal executions have accumulated — so it is cheap enough to run often.
//
// It catches the common drift: an execution that has since completed or failed
// but whose terminal event was dropped, so the index still shows it as
// running/waiting. Reading the source of truth and re-asserting moves it to its
// real status. It cannot resurrect an active execution that is missing from the
// index entirely (its membership key was never written or was lost); the
// periodic full Reconcile remains the backstop for that.
func (ix *Indexer) ReconcileActive(ctx context.Context) error {
	seen := make(map[string]struct{})

	for _, status := range []string{store.StatusRunning, store.StatusWaiting} {
		ids, err := ix.ByStatus(ctx, status)
		if err != nil {
			return err
		}

		for _, id := range ids {
			if _, dup := seen[id]; dup {
				continue
			}

			seen[id] = struct{}{}

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
	}

	return nil
}

// reassert forces the indexes to match an execution's current state: it writes
// the membership entry for the current status, removes entries under every other
// status, and refreshes the flow entry (which doubles as the bookkeeping record)
// last.
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

	_, err = ix.idxFlow.Put(ctx, ex.FlowName+sep+ex.ID, val)

	return err
}

// errStop is returned by a forEachByPrefix callback to end iteration early
// without surfacing an error — the mechanism behind the limited queries.
var errStop = errors.New("stop")

// gcCandidate is a terminal index entry GC will check against the store.
type gcCandidate struct{ flow, status, id string }

// GC prunes index entries whose execution no longer exists in either the hot
// bucket or the cold archive — orphans left behind when an archived terminal
// execution passes its retention and expires. It inspects only terminal
// (completed/failed) entries older than staleAfter: active executions always
// exist in the hot bucket, and recently-terminal ones are still within archive
// retention, so neither needs a source-of-truth read. For each surviving
// candidate it reads the store; if the execution is gone it deletes the flow and
// status membership entries. staleAfter should be the archive retention; a
// non-positive value checks every terminal entry. It returns the count pruned.
func (ix *Indexer) GC(ctx context.Context, staleAfter time.Duration) (int, error) {
	// Collect candidates first, then read-and-delete: mutating idxFlow while its
	// watch is still streaming would disturb the iteration.
	candidates, err := ix.gcCandidates(ctx, staleAfter)
	if err != nil {
		return 0, err
	}

	pruned := 0

	for _, c := range candidates {
		_, getErr := ix.store.Get(ctx, c.id)
		if getErr == nil {
			continue // still present in hot or archive
		}

		if !errors.Is(getErr, store.ErrNotFound) {
			return pruned, getErr
		}

		_ = ix.idxFlow.Delete(ctx, c.flow+sep+c.id)
		_ = ix.idxStatus.Delete(ctx, c.status+sep+c.id)
		pruned++
	}

	return pruned, nil
}

// gcCandidates scans the flow index for terminal entries older than staleAfter
// (a non-positive staleAfter selects every terminal entry).
func (ix *Indexer) gcCandidates(ctx context.Context, staleAfter time.Duration) ([]gcCandidate, error) {
	cutoff := time.Now().Add(-staleAfter)

	var out []gcCandidate

	err := forEachAll(ctx, ix.idxFlow, func(entry jetstream.KeyValueEntry) error {
		var ev store.Event
		if json.Unmarshal(entry.Value(), &ev) != nil {
			return nil
		}

		if !isTerminal(ev.Status) {
			return nil
		}

		if staleAfter > 0 && ev.Time.After(cutoff) {
			return nil
		}

		out = append(out, gcCandidate{ev.FlowName, ev.Status, ev.ExecID})

		return nil
	})

	return out, err
}

// isTerminal reports whether a status is one an execution rests at (and so can
// be archived and eventually expire).
func isTerminal(status string) bool {
	return status == store.StatusCompleted || status == store.StatusFailed
}

// forEachAll streams every entry in kv to fn via a whole-bucket watch.
func forEachAll(ctx context.Context, kv jetstream.KeyValue, fn func(jetstream.KeyValueEntry) error) error {
	w, err := kv.WatchAll(ctx, jetstream.IgnoreDeletes())
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return nil
		}

		return err
	}
	defer func() { _ = w.Stop() }()

	for {
		select {
		case entry, ok := <-w.Updates():
			if !ok || entry == nil {
				return nil
			}

			if cbErr := fn(entry); cbErr != nil {
				return cbErr
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// ByStatus returns the ids of executions currently indexed under status.
func (ix *Indexer) ByStatus(ctx context.Context, status string) ([]string, error) {
	return collectIDs(ctx, ix.idxStatus, status+sep, 0)
}

// ByFlow returns the ids of executions belonging to flow.
func (ix *Indexer) ByFlow(ctx context.Context, flow string) ([]string, error) {
	return collectIDs(ctx, ix.idxFlow, flow+sep, 0)
}

// ByStatusEvents returns the index entries for all executions under status.
// Each entry is the most recent event stored for that execution, so callers
// can build summaries (including the error message) without a per-execution
// round-trip.
func (ix *Indexer) ByStatusEvents(ctx context.Context, status string) ([]store.Event, error) {
	return collectEvents(ctx, ix.idxStatus, status+sep, 0)
}

// ByFlowEvents returns the index entries for all executions belonging to flow.
func (ix *Indexer) ByFlowEvents(ctx context.Context, flow string) ([]store.Event, error) {
	return collectEvents(ctx, ix.idxFlow, flow+sep, 0)
}

// ByStatusEventsLimit is ByStatusEvents capped at limit entries (limit <= 0
// means no cap). Because KV keys have no inherent order, the cap yields an
// arbitrary subset, not an ordered page; it is a guardrail against transferring
// an unbounded result set, not a pagination cursor.
func (ix *Indexer) ByStatusEventsLimit(ctx context.Context, status string, limit int) ([]store.Event, error) {
	return collectEvents(ctx, ix.idxStatus, status+sep, limit)
}

// ByFlowEventsLimit is ByFlowEvents capped at limit entries (limit <= 0 means no
// cap). The same arbitrary-subset caveat as ByStatusEventsLimit applies.
func (ix *Indexer) ByFlowEventsLimit(ctx context.Context, flow string, limit int) ([]store.Event, error) {
	return collectEvents(ctx, ix.idxFlow, flow+sep, limit)
}

// collectIDs gathers the execution ids of keys in kv under prefix, up to limit
// (0 = all).
func collectIDs(ctx context.Context, kv jetstream.KeyValue, prefix string, limit int) ([]string, error) {
	var out []string

	err := forEachByPrefix(ctx, kv, prefix, true, func(entry jetstream.KeyValueEntry) error {
		if id, found := strings.CutPrefix(entry.Key(), prefix); found {
			out = append(out, id)

			if limit > 0 && len(out) >= limit {
				return errStop
			}
		}

		return nil
	})

	return out, err
}

// collectEvents gathers the stored event values of keys in kv under prefix, up
// to limit (0 = all). Corrupt values are skipped.
func collectEvents(ctx context.Context, kv jetstream.KeyValue, prefix string, limit int) ([]store.Event, error) {
	var out []store.Event

	err := forEachByPrefix(ctx, kv, prefix, false, func(entry jetstream.KeyValueEntry) error {
		var ev store.Event
		if json.Unmarshal(entry.Value(), &ev) != nil {
			return nil
		}

		out = append(out, ev)

		if limit > 0 && len(out) >= limit {
			return errStop
		}

		return nil
	})

	return out, err
}

// forEachByPrefix streams every entry in kv whose key begins with prefix to fn
// via a server-side Watch filter, so only matching keys are transferred (no
// full-bucket scan). When metaOnly is set, values are not transferred. fn
// returning errStop ends the scan cleanly; any other error propagates.
func forEachByPrefix(
	ctx context.Context, kv jetstream.KeyValue, prefix string, metaOnly bool,
	fn func(jetstream.KeyValueEntry) error,
) error {
	opts := []jetstream.WatchOpt{jetstream.IgnoreDeletes()}
	if metaOnly {
		opts = append(opts, jetstream.MetaOnly())
	}

	w, err := kv.Watch(ctx, prefix+">", opts...)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return nil
		}

		return err
	}
	defer func() { _ = w.Stop() }()

	for {
		select {
		case entry, ok := <-w.Updates():
			if !ok || entry == nil {
				return nil
			}

			if cbErr := fn(entry); cbErr != nil {
				if errors.Is(cbErr, errStop) {
					return nil
				}

				return cbErr
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
