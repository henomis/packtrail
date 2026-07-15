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
	store.StatusCancelled,
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

// metaPrefix namespaces the per-execution bookkeeping records inside the flow
// bucket: '=' is a legal KV-key character but can never appear in a flow name
// ([A-Za-z0-9_-]), so a bookkeeping key can never collide with a
// "<flow>.<execID>" membership key — even for a flow literally named "meta".
// The trailing '.' keeps the prefix a whole subject token, so prefix watches
// ("meta=.>") work like the membership watches do.
const metaPrefix = "meta=" + sep

func metaKey(execID string) string { return metaPrefix + execID }

// index applies a single event idempotently. The per-execution bookkeeping
// record (meta key) carries the last-indexed revision and status: index reads
// it to skip stale/duplicate events and to find the previous status to clean
// up, writes the status and flow memberships (the read model), and commits the
// meta record last.
//
// The commit is revision-guarded: multiple engine instances run indexers on the
// same durable, so two events for one execution can be projected concurrently.
// Without the guard the read-decide-write races and an older event's writes can
// land after a newer one's, regressing the index until the next reconcile. On a
// CAS conflict the whole projection retries against the fresh record.
func (ix *Indexer) index(ctx context.Context, ev store.Event) error {
	const maxAttempts = 8

	for range maxAttempts {
		applied, err := ix.tryIndex(ctx, ev)
		if err != nil {
			return err
		}

		if applied {
			return nil
		}
	}

	return fmt.Errorf("index %s: too many bookkeeping conflicts", ev.ExecID)
}

// tryIndex runs one guarded projection attempt. It returns (false, nil) when
// the meta-record CAS lost to a concurrent indexer, in which case the caller
// re-reads and retries.
func (ix *Indexer) tryIndex(ctx context.Context, ev store.Event) (bool, error) {
	prev, kvRev, exists, err := ix.lastIndexed(ctx, ev.ExecID)
	if err != nil {
		return false, err
	}

	if exists && prev.Revision >= ev.Revision {
		applied, reassertErr := ix.reassertIndexedEvent(ctx, prev, ev, kvRev)
		if reassertErr != nil {
			return false, reassertErr
		}

		return applied, nil
	}

	val, err := json.Marshal(ev)
	if err != nil {
		return false, err
	}

	// Memberships are the read model: plain writes, re-assertable at any time
	// (by a retry, reassert, or Reconcile). Correctness hangs on the meta CAS
	// below, not on these.
	if _, putErr := ix.idxStatus.Put(ctx, ev.Status+sep+ev.ExecID, val); putErr != nil {
		return false, putErr
	}

	if exists && prev.Status != "" && prev.Status != ev.Status {
		_ = ix.idxStatus.Delete(ctx, prev.Status+sep+ev.ExecID) // best-effort cleanup
	}

	if _, putErr := ix.idxFlow.Put(ctx, ev.FlowName+sep+ev.ExecID, val); putErr != nil {
		return false, putErr
	}

	if exists && prev.FlowName != "" && prev.FlowName != ev.FlowName {
		if deleteErr := ix.deleteFlowMembership(ctx, prev.FlowName, ev.ExecID); deleteErr != nil {
			return false, deleteErr
		}
	}

	// The meta record is committed last: it is what a later event reads, so a
	// crash before this point simply reprocesses idempotently. The CAS
	// (create-if-absent / update-at-revision) fences concurrent indexers.
	if !exists {
		_, err = ix.idxFlow.Create(ctx, metaKey(ev.ExecID), val)
	} else {
		_, err = ix.idxFlow.Update(ctx, metaKey(ev.ExecID), val, kvRev)
	}

	if errors.Is(err, jetstream.ErrKeyExists) || isWrongLastSeq(err) {
		return false, nil // lost to a concurrent indexer: re-read and retry
	}

	return err == nil, err
}

// reassertIndexedEvent handles a stale or duplicate event: the meta record
// already points at prev, but a conflicted attempt for stale may have written
// stale memberships before losing the CAS.
func (ix *Indexer) reassertIndexedEvent(ctx context.Context, prev, stale store.Event, kvRev uint64) (bool, error) {
	if prev.Revision == stale.Revision {
		return true, nil
	}

	val, err := json.Marshal(prev)
	if err != nil {
		return false, err
	}

	if prev.Status != stale.Status {
		_ = ix.idxStatus.Delete(ctx, stale.Status+sep+stale.ExecID)
	}

	if prev.FlowName != stale.FlowName {
		if deleteErr := ix.deleteFlowMembership(ctx, stale.FlowName, stale.ExecID); deleteErr != nil {
			return false, deleteErr
		}
	}

	if _, err = ix.idxStatus.Put(ctx, prev.Status+sep+prev.ExecID, val); err != nil {
		return false, err
	}

	if _, err = ix.idxFlow.Put(ctx, prev.FlowName+sep+prev.ExecID, val); err != nil {
		return false, err
	}

	_, err = ix.idxFlow.Update(ctx, metaKey(prev.ExecID), val, kvRev)
	if errors.Is(err, jetstream.ErrKeyExists) || isWrongLastSeq(err) {
		return false, nil
	}

	return err == nil, err
}

func (ix *Indexer) deleteFlowMembership(ctx context.Context, flow, execID string) error {
	if flow == "" {
		return nil
	}

	err := ix.idxFlow.Delete(ctx, flow+sep+execID)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil
	}

	return err
}

// lastIndexed reads the per-execution bookkeeping record: the stored event
// (whose Revision/Status are the last-indexed state), the KV entry revision for
// CAS, and whether the record exists. A corrupt value is treated as
// not-yet-indexed content but keeps its KV revision, so the rewrite still goes
// through the CAS.
func (ix *Indexer) lastIndexed(
	ctx context.Context, execID string,
) (prev store.Event, kvRev uint64, exists bool, err error) {
	entry, getErr := ix.idxFlow.Get(ctx, metaKey(execID))
	if getErr != nil {
		if errors.Is(getErr, jetstream.ErrKeyNotFound) {
			return store.Event{}, 0, false, nil
		}

		return store.Event{}, 0, false, getErr
	}

	if json.Unmarshal(entry.Value(), &prev) != nil {
		return store.Event{}, entry.Revision(), true, nil // corrupt: rewrite via CAS
	}

	return prev, entry.Revision(), true, nil
}

// isWrongLastSeq reports whether err is the KV revision-conflict error a
// guarded Update returns when the entry moved since it was read.
func isWrongLastSeq(err error) bool {
	var apiErr *jetstream.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence
	}

	return false
}

// Reconcile rebuilds the indexes from the source of truth, closing any drift
// the asynchronous projection may have left behind (spec §9). It streams every
// hot execution in packtrail-executions, then every retained cold execution in
// the archive, through last-per-key watches and authoritatively re-asserts each
// one's membership, so a full pass avoids a key listing plus a Get per execution.
//
// Per §13 this full scan still grows with the hot set plus retained archive set;
// ReconcileActive is the cheap, frequent counterpart that only touches in-flight
// executions. Run Reconcile infrequently as the deep backstop.
func (ix *Indexer) Reconcile(ctx context.Context) error {
	if err := ix.store.ForEachExecution(ctx, func(ex *store.Execution) error {
		return ix.reassert(ctx, ex)
	}); err != nil {
		return err
	}

	return ix.store.ForEachArchivedExecution(ctx, func(ex *store.Execution) error {
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
// the membership entries for the current status and flow, removes entries under
// every other status, and refreshes the bookkeeping record last. It is
// authoritative (reads the source of truth), so its writes are unguarded —
// last-writer-wins against a concurrent event projection, and the periodic
// reconcile re-runs it anyway.
func (ix *Indexer) reassert(ctx context.Context, ex *store.Execution) error {
	prev, _, exists, err := ix.lastIndexed(ctx, ex.ID)
	if err != nil {
		return err
	}

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

	if exists && prev.FlowName != "" && prev.FlowName != ex.FlowName {
		if deleteErr := ix.deleteFlowMembership(ctx, prev.FlowName, ex.ID); deleteErr != nil {
			return deleteErr
		}
	}

	_, err = ix.idxFlow.Put(ctx, metaKey(ex.ID), val)

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

	// Only sweep data-plane entries older than the staleness horizon, revision-
	// guarded, so a re-Start that recreated one of these ids (same id, new
	// generation) between the absence check and the delete keeps its fresh data.
	cutoff := time.Now().Add(-staleAfter)

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
		_ = ix.idxFlow.Delete(ctx, metaKey(c.id))
		_ = ix.store.DeletePayloadsOlderThan(ctx, c.id, cutoff) // sweep stale data-plane orphans the archive sweep missed
		pruned++
	}

	return pruned, nil
}

// gcCandidates scans the bookkeeping records for terminal entries older than
// staleAfter (a non-positive staleAfter selects every terminal entry).
func (ix *Indexer) gcCandidates(ctx context.Context, staleAfter time.Duration) ([]gcCandidate, error) {
	cutoff := time.Now().Add(-staleAfter)

	var out []gcCandidate

	err := forEachByPrefix(ctx, ix.idxFlow, metaPrefix, false, func(entry jetstream.KeyValueEntry) error {
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
	return status == store.StatusCompleted ||
		status == store.StatusFailed ||
		status == store.StatusCancelled
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
