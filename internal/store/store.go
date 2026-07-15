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

package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail/internal/names"
)

const (
	execBucketHistory  = 64
	eventsMaxAge       = 24 * time.Hour
	deadLetterMaxAge   = 30 * 24 * time.Hour // dead-letter records expire after ~30 days
	deadLetterReadWait = 500 * time.Millisecond
	defaultDLQReadCap  = 100
	casBackoffBase     = 250 * time.Microsecond
	casBackoffCap      = 5 * time.Millisecond
	eventDedupWindow   = 2 * time.Minute
	// workDedupWindow is set explicitly (rather than relying on NATS's implicit
	// ~2m default) so the outbox's per-item msg-id dedup — which makes a
	// re-flushed work item idempotent within the window — is self-documenting and
	// robust against a server-default change. Beyond it a duplicate is still
	// state-safe against the guarded transitions.
	workDedupWindow = 2 * time.Minute
)

// DefaultMaxPayloadBytes caps a single data-plane entry (a start input, one
// node's output, one signal payload) by default. It sits well below NATS's
// 1 MiB max message size. Override with SetMaxPayloadBytes; a non-positive
// limit disables the guard.
const DefaultMaxPayloadBytes = 512 << 10 // 512 KiB

// DefaultMaxDocumentBytes caps the serialized execution control document by
// default. Unlike a data-plane payload, the document is small control metadata,
// but a very wide fanout (a BranchState per branch) or a large transient outbox
// can grow it toward NATS's 1 MiB ceiling — where it would otherwise fail as an
// opaque NATS publish error. The guard rejects it first with the typed
// ErrDocumentTooLarge. It sits below 1 MiB with headroom for KV/message
// overhead. Override with SetMaxDocumentBytes; a non-positive limit disables it.
const DefaultMaxDocumentBytes = 768 << 10 // 768 KiB

// ErrConflict is returned when a CAS write loses to a concurrent writer and the
// caller's revision is stale.
var ErrConflict = errors.New("store: revision conflict")

// ErrNotFound is returned when an execution key does not exist.
var ErrNotFound = errors.New("store: not found")

// ErrAlreadyExists is returned by Create when an execution with the same id
// already exists. Callers use it to make Create idempotent (e.g. a caller-supplied
// execution id that dedups a retried Start).
var ErrAlreadyExists = errors.New("store: already exists")

// ErrPayloadTooLarge is returned when a write would persist an execution whose
// payload exceeds the configured max size. The write is rejected before it
// reaches NATS, so the previously persisted (within-limit) state is preserved and
// callers can still record a failure against it.
var ErrPayloadTooLarge = errors.New("store: payload exceeds max size")

// ErrDocumentTooLarge is returned when a control-document write would exceed the
// configured max size. Like ErrPayloadTooLarge it is rejected before it reaches
// NATS, so the last within-limit document stays persisted and a caller can still
// record a (small) failure against it.
var ErrDocumentTooLarge = errors.New("store: control document exceeds max size")

// Store provides access to all Packtrail KV buckets and streams.
type Store struct {
	js              jetstream.JetStream
	names           names.Names
	exec            jetstream.KeyValue
	archive         jetstream.KeyValue // cold store for aged-out terminal execs; nil unless EnableArchive ran
	leases          jetstream.KeyValue
	idxStatus       jetstream.KeyValue
	idxFlow         jetstream.KeyValue
	payloads        jetstream.KeyValue // the data plane: start inputs, node outputs, signal payloads
	maxPayloadBytes int                // per-entry payload-size guard; <= 0 disables it
	maxDocBytes     int                // control-document size guard; <= 0 disables it

	casConflicts atomic.Uint64 // cumulative CAS-conflict retries across all Mutate calls
	deadLetters  atomic.Uint64 // cumulative dead-letters emitted by this instance

	historyEnabled atomic.Bool // set by EnableHistory; EmitEvent mirrors events into the history stream

	leaseObsMu sync.Mutex
	leaseObs   map[string]leaseObservation
}

// Open ensures every bucket and stream exists, under the given namespace, and
// returns a ready Store.
func Open(ctx context.Context, js jetstream.JetStream, n names.Names) (*Store, error) {
	s := &Store{js: js, names: n, maxPayloadBytes: DefaultMaxPayloadBytes, maxDocBytes: DefaultMaxDocumentBytes}

	var err error

	if s.exec, err = js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:  n.BucketExecutions,
		History: execBucketHistory,
	}); err != nil {
		return nil, fmt.Errorf("exec bucket: %w", err)
	}

	if s.leases, err = js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: n.BucketLeases,
		// No bucket-wide TTL: lease validity is governed by the Expires field
		// in each value (see lease.go). A fixed bucket TTL shorter than a
		// configured LeaseTTL would evict live leases early.
		TTL:            0,
		LimitMarkerTTL: time.Minute,
	}); err != nil {
		return nil, fmt.Errorf("leases bucket: %w", err)
	}

	if s.idxStatus, err = js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: n.BucketIdxStatus}); err != nil {
		return nil, fmt.Errorf("idx-status bucket: %w", err)
	}

	if s.idxFlow, err = js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: n.BucketIdxFlow}); err != nil {
		return nil, fmt.Errorf("idx-flow bucket: %w", err)
	}

	if s.payloads, err = js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: n.BucketPayloads}); err != nil {
		return nil, fmt.Errorf("payloads bucket: %w", err)
	}

	if _, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:       n.StreamEvents,
		Subjects:   []string{n.SubjEventsPrefix + ">"},
		MaxAge:     eventsMaxAge,
		Storage:    jetstream.FileStorage,
		Retention:  jetstream.LimitsPolicy,
		Duplicates: eventDedupWindow,
	}); err != nil {
		return nil, fmt.Errorf("events stream: %w", err)
	}

	if _, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:       n.StreamWork,
		Subjects:   []string{n.SubjWorkPrefix + ">"},
		Storage:    jetstream.FileStorage,
		Retention:  jetstream.WorkQueuePolicy,
		Duplicates: workDedupWindow,
	}); err != nil {
		return nil, fmt.Errorf("work stream: %w", err)
	}

	if _, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      n.StreamDeadLetter,
		Subjects:  []string{n.SubjDeadLetterPrefix + ">"},
		MaxAge:    deadLetterMaxAge,
		Storage:   jetstream.FileStorage,
		Retention: jetstream.LimitsPolicy,
	}); err != nil {
		return nil, fmt.Errorf("deadletter stream: %w", err)
	}

	return s, nil
}

// JS exposes the underlying JetStream context for packages that manage their
// own consumers/streams (runtime, scheduler, signal, visibility).
func (s *Store) JS() jetstream.JetStream { return s.js }

// Names returns the resource names this store was opened with, so dependent
// packages share the same namespace.
func (s *Store) Names() names.Names { return s.names }

// IdxStatus exposes the by-status visibility index bucket.
func (s *Store) IdxStatus() jetstream.KeyValue { return s.idxStatus }

// IdxFlow exposes the by-flow visibility index bucket.
func (s *Store) IdxFlow() jetstream.KeyValue { return s.idxFlow }

// CASConflicts returns the cumulative number of CAS-conflict retries observed
// across all Mutate calls. It is a monotonic counter useful for observing
// write-contention pressure (e.g. wide fanout on a single execution key).
func (s *Store) CASConflicts() uint64 { return s.casConflicts.Load() }

// DeadLetters returns the cumulative number of dead-letter records this Store
// instance has emitted since start. It is an in-process counter (resets on
// restart); for a durable, cross-instance total use DeadLetterCount, which reads
// the dead-letter stream's depth.
func (s *Store) DeadLetters() uint64 { return s.deadLetters.Load() }

// EmitDeadLetter appends a dead-letter record to the packtrail-deadletter stream
// and bumps the in-process counter, so a consumer that gives up on poisoned work
// (Term) leaves a durable, queryable trace instead of only a log line.
func (s *Store) EmitDeadLetter(ctx context.Context, dl DeadLetter) error {
	if dl.Time.IsZero() {
		dl.Time = time.Now().UTC()
	}

	data, err := json.Marshal(dl)
	if err != nil {
		return err
	}

	if _, err = s.js.Publish(ctx, s.names.SubjDeadLetterPrefix+dl.Kind, data); err != nil {
		return err
	}

	s.deadLetters.Add(1)

	return nil
}

// DeadLetterCount returns the durable number of dead-letter records currently
// retained in the packtrail-deadletter stream (bounded by its retention). Unlike
// DeadLetters it is cross-instance and survives restarts.
func (s *Store) DeadLetterCount(ctx context.Context) (uint64, error) {
	stream, err := s.js.Stream(ctx, s.names.StreamDeadLetter)
	if err != nil {
		return 0, err
	}

	info, err := stream.Info(ctx)
	if err != nil {
		return 0, err
	}

	return info.State.Msgs, nil
}

// RecentDeadLetters returns up to limit of the most recent dead-letter records,
// oldest-first (limit <= 0 uses a default cap). It reads the tail of the
// dead-letter stream via an ordered consumer, so the cost is bounded by limit, not
// by total volume.
func (s *Store) RecentDeadLetters(ctx context.Context, limit int) ([]DeadLetter, error) {
	if limit <= 0 {
		limit = defaultDLQReadCap
	}

	stream, err := s.js.Stream(ctx, s.names.StreamDeadLetter)
	if err != nil {
		return nil, err
	}

	info, err := stream.Info(ctx)
	if err != nil {
		return nil, err
	}

	if info.State.Msgs == 0 {
		return nil, nil
	}

	start := uint64(1)
	if last := info.State.LastSeq; last > uint64(limit) { //nolint:gosec // limit is a small positive cap
		start = last - uint64(limit) + 1
	}

	cons, err := stream.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{
		DeliverPolicy: jetstream.DeliverByStartSequencePolicy,
		OptStartSeq:   start,
	})
	if err != nil {
		return nil, err
	}

	batch, err := cons.Fetch(limit, jetstream.FetchMaxWait(deadLetterReadWait))
	if err != nil {
		return nil, err
	}

	var out []DeadLetter

	for msg := range batch.Messages() {
		var dl DeadLetter
		if json.Unmarshal(msg.Data(), &dl) == nil {
			out = append(out, dl)
		}
	}

	if batch.Error() != nil {
		return out, batch.Error()
	}

	return out, nil
}

// SetMaxPayloadBytes sets the maximum size, in bytes, of a single data-plane
// entry (a start input, one node's output, one signal payload). A write that
// exceeds it is rejected with ErrPayloadTooLarge before it reaches NATS (see
// PutPayload). A non-positive limit disables the guard. Call before the store
// is used (it is not safe to change concurrently with writes).
func (s *Store) SetMaxPayloadBytes(n int) { s.maxPayloadBytes = n }

// SetMaxDocumentBytes sets the maximum serialized size, in bytes, of an
// execution control document. A write that exceeds it is rejected with
// ErrDocumentTooLarge before it reaches NATS (see update). A non-positive limit
// disables the guard. Call before the store is used (it is not safe to change
// concurrently with writes).
func (s *Store) SetMaxDocumentBytes(n int) { s.maxDocBytes = n }

// Create persists a new execution and returns its initial revision. The id is
// deduped against the cold archive as well as the hot bucket: a retried
// StartWithID whose original execution was already swept into the archive must
// return ErrAlreadyExists, not silently re-run the whole flow under the same
// idempotency key. Because the archive and hot bucket cannot be checked
// atomically, Create checks the archive both before and after the hot Create: if
// an archive sweep moved the id in the gap, the just-created hot key is deleted
// at its create revision and ErrAlreadyExists is returned.
func (s *Store) Create(ctx context.Context, e *Execution) (uint64, error) {
	if _, err := s.getArchived(ctx, e.ID); err == nil {
		return 0, fmt.Errorf("%w: execution %s (archived)", ErrAlreadyExists, e.ID)
	} else if !errors.Is(err, ErrNotFound) {
		return 0, err
	}

	e.UpdatedAt = time.Now().UTC()

	data, err := json.Marshal(e)
	if err != nil {
		return 0, err
	}

	if s.maxDocBytes > 0 && len(data) > s.maxDocBytes {
		return 0, fmt.Errorf("%w: execution %s is %d bytes, limit %d",
			ErrDocumentTooLarge, e.ID, len(data), s.maxDocBytes)
	}

	rev, err := s.exec.Create(ctx, e.ID, data)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyExists) {
			return 0, fmt.Errorf("%w: execution %s", ErrAlreadyExists, e.ID)
		}

		return 0, err
	}

	if err = s.ensureNotArchivedAfterCreate(ctx, e.ID, rev); err != nil {
		return 0, err
	}

	e.Revision = rev

	return rev, nil
}

func (s *Store) ensureNotArchivedAfterCreate(ctx context.Context, id string, rev uint64) error {
	if _, err := s.getArchived(ctx, id); err == nil {
		if delErr := s.deleteCreatedExecution(ctx, id, rev); delErr != nil {
			return fmt.Errorf("discard raced archived execution %s: %w", id, delErr)
		}

		return fmt.Errorf("%w: execution %s (archived)", ErrAlreadyExists, id)
	} else if !errors.Is(err, ErrNotFound) {
		if delErr := s.deleteCreatedExecution(ctx, id, rev); delErr != nil {
			return fmt.Errorf("archive recheck for %s failed: %w (cleanup failed: %v)", id, err, delErr)
		}

		return err
	}

	return nil
}

func (s *Store) deleteCreatedExecution(ctx context.Context, id string, rev uint64) error {
	err := s.exec.Delete(ctx, id, jetstream.LastRevision(rev))
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil
	}

	return err
}

// ArchivedExecution loads id from the cold archive if it is currently retained.
// It intentionally ignores any hot-bucket value: callers use it to avoid
// repairing or recreating transient hot duplicates while an archived execution
// still owns the id.
func (s *Store) ArchivedExecution(ctx context.Context, id string) (*Execution, bool, error) {
	ex, err := s.getArchived(ctx, id)
	if err == nil {
		return ex, true, nil
	} else if !errors.Is(err, ErrNotFound) {
		return nil, false, err
	}

	return nil, false, nil
}

// Get loads an execution and populates its Revision from the KV entry. If an
// archive record exists, it owns the id and wins over any transient hot duplicate
// left by an archive/Create race; otherwise Get reads the hot bucket. Reads of
// aged-out terminal executions still succeed from the archive until retention
// expires. The hot bucket remains the source of truth for mutations; archived
// executions are terminal and never mutated.
func (s *Store) Get(ctx context.Context, id string) (*Execution, error) {
	archived, ok, err := s.ArchivedExecution(ctx, id)
	if err != nil {
		return nil, err
	}

	if ok {
		return archived, nil
	}

	e, err := s.getHot(ctx, id)

	return e, err
}

// getHot loads an execution from the hot bucket only, with no archive fallback.
// Mutate uses it because mutations target the hot bucket exclusively: an archived
// execution is terminal and immutable, and a CAS write against a revision read
// from the archive could never succeed (the hot key does not exist) — it would
// burn the whole Mutate retry budget before failing.
func (s *Store) getHot(ctx context.Context, id string) (*Execution, error) {
	entry, err := s.exec.Get(ctx, id)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, ErrNotFound
		}

		return nil, err
	}

	return decodeExecution(entry)
}

// getArchived loads an execution from the cold archive, or ErrNotFound when the
// archive is disabled or the id is absent (e.g. retention expired).
func (s *Store) getArchived(ctx context.Context, id string) (*Execution, error) {
	if s.archive == nil {
		return nil, ErrNotFound
	}

	entry, err := s.archive.Get(ctx, id)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, ErrNotFound
		}

		return nil, err
	}

	return decodeExecution(entry)
}

// decodeExecution unmarshals a KV entry into an Execution, carrying its revision.
func decodeExecution(entry jetstream.KeyValueEntry) (*Execution, error) {
	var e Execution
	if err := json.Unmarshal(entry.Value(), &e); err != nil {
		return nil, err
	}

	e.Revision = entry.Revision()
	if e.ArchivedRevision != 0 {
		e.Revision = e.ArchivedRevision
	}

	return &e, nil
}

// update performs a single CAS write at e.Revision and returns the new revision.
func (s *Store) update(ctx context.Context, e *Execution) (uint64, error) {
	e.UpdatedAt = time.Now().UTC()

	data, err := json.Marshal(e)
	if err != nil {
		return 0, err
	}

	// Reject an over-limit document before it hits NATS: the last within-limit
	// state stays persisted, so a caller can still record a (small) failure.
	if s.maxDocBytes > 0 && len(data) > s.maxDocBytes {
		return 0, fmt.Errorf("%w: execution %s is %d bytes, limit %d",
			ErrDocumentTooLarge, e.ID, len(data), s.maxDocBytes)
	}

	rev, err := s.exec.Update(ctx, e.ID, data, e.Revision)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyExists) || isWrongLastSeq(err) {
			return 0, ErrConflict
		}

		return 0, err
	}

	e.Revision = rev

	return rev, nil
}

// Mutate runs a read-modify-write CAS loop: it loads the execution, applies fn,
// and writes it back, retrying the whole cycle on a concurrent-write conflict.
// The mutated execution (with its new revision) is returned. Mutate reads the
// hot bucket only (no archive fallback): archived executions are terminal and
// immutable, so mutating one returns ErrNotFound.
func (s *Store) Mutate(ctx context.Context, id string, fn func(*Execution) error) (*Execution, error) {
	const maxAttempts = 64
	for attempt := range maxAttempts {
		if archived, err := s.isArchived(ctx, id); err != nil {
			return nil, err
		} else if archived {
			return nil, ErrNotFound
		}

		e, err := s.getHot(ctx, id)
		if err != nil {
			return nil, err
		}

		err = fn(e)
		if err != nil {
			return nil, err
		}

		_, updateErr := s.update(ctx, e)
		if updateErr != nil {
			if errors.Is(updateErr, ErrConflict) {
				s.casConflicts.Add(1)
				// Back off with jitter to break livelock under contention
				// (e.g. many fanout branches writing the same execution).
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(casBackoff(attempt)):
				}

				continue
			}

			return nil, updateErr
		}

		return e, nil
	}

	return nil, fmt.Errorf("%w: too many retries on %s", ErrConflict, id)
}

func (s *Store) isArchived(ctx context.Context, id string) (bool, error) {
	_, ok, err := s.ArchivedExecution(ctx, id)
	return ok, err
}

// casBackoff returns a small jittered delay growing with the attempt count,
// capped at ~5ms, to de-synchronize concurrent CAS writers.
func casBackoff(attempt int) time.Duration {
	base := time.Duration(attempt+1) * casBackoffBase
	if base > casBackoffCap {
		base = casBackoffCap
	}

	//nolint:gosec,mnd // jitter for CAS backoff: halving is inherent to the algorithm, not a magic number
	return base/2 + time.Duration(rand.Int64N(int64(base/2)+1))
}

// EmitEvent appends a domain event for the execution to the events stream.
func (s *Store) EmitEvent(ctx context.Context, e *Execution) error {
	ev := eventFromExecution(e)

	if err := s.publishEvent(ctx, s.names.SubjEventsPrefix+e.ID, ev, "event."); err != nil {
		return err
	}

	// History mirrors the event, best-effort: a lost trace record must never
	// fail (or retry) the transition that emitted it.
	if s.historyEnabled.Load() {
		if histErr := s.publishEvent(ctx, s.names.SubjHistoryPrefix+e.ID, ev, "history."); histErr != nil {
			slog.Debug("emit history", "exec", e.ID, "err", histErr)
		}
	}

	return nil
}

func eventFromExecution(e *Execution) Event {
	return Event{
		ExecID:   e.ID,
		FlowName: e.FlowName,
		Status:   e.Status,
		Node:     e.CurrentNode,
		Error:    e.Error,
		Revision: e.Revision,
		Time:     time.Now().UTC(),
	}
}

func (s *Store) publishEvent(ctx context.Context, subject string, ev Event, prefix string) error {
	return s.publishEventWithMsgID(ctx, subject, ev, eventMsgID(prefix, ev))
}

func (s *Store) publishEventWithMsgID(ctx context.Context, subject string, ev Event, msgID string) error {
	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}

	_, err = s.js.Publish(ctx, subject, data, jetstream.WithMsgID(msgID))

	return err
}

func eventMsgID(prefix string, ev Event) string {
	return prefix + ev.ExecID + "." + strconv.FormatUint(ev.Revision, 10)
}

// ListExecutionKeys returns all execution ids currently stored. Used by the
// visibility reconciler.
func (s *Store) ListExecutionKeys(ctx context.Context) ([]string, error) {
	keys, err := s.exec.Keys(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return nil, nil
		}

		return nil, err
	}

	return keys, nil
}

// ForEachExecutionKey streams the id of every execution in the hot bucket to fn
// via a metadata-only last-per-key watch, without collecting them into a slice.
// fn returning a non-nil error stops the scan and propagates the error, so a
// caller can cap the number it reads by returning a sentinel after N.
func (s *Store) ForEachExecutionKey(ctx context.Context, fn func(string) error) error {
	w, err := s.exec.WatchAll(ctx, jetstream.IgnoreDeletes(), jetstream.MetaOnly())
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

			if fnErr := fn(entry.Key()); fnErr != nil {
				return fnErr
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// ForEachExecution streams every stored execution to fn via a single
// last-per-key watch over the executions bucket, so the caller reads the whole
// set in one server round-trip instead of a key listing followed by a Get per
// id. The watch delivers each key's latest value; a nil update marks the end of
// the current set. A value that fails to unmarshal is skipped rather than
// aborting the scan. fn must not retain the *Execution past its call.
func (s *Store) ForEachExecution(ctx context.Context, fn func(*Execution) error) error {
	return forEachExecution(ctx, s.exec, fn)
}

// ForEachArchivedExecution streams every execution retained in the cold archive
// to fn. It is a no-op when archival is disabled.
func (s *Store) ForEachArchivedExecution(ctx context.Context, fn func(*Execution) error) error {
	if s.archive == nil {
		return nil
	}

	return forEachExecution(ctx, s.archive, fn)
}

func forEachExecution(ctx context.Context, kv jetstream.KeyValue, fn func(*Execution) error) error {
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

			e, decodeErr := decodeExecution(entry)
			if decodeErr != nil {
				continue
			}

			if fnErr := fn(e); fnErr != nil {
				return fnErr
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// EnableArchive creates (or attaches to) the cold archive bucket with a
// bucket-wide retention TTL, so swept terminal executions are kept queryable for
// roughly retention and then expire automatically. It must be called before the
// archive is used; ArchiveTerminal and Get's fallback are no-ops until it runs.
func (s *Store) EnableArchive(ctx context.Context, retention time.Duration) error {
	archive, err := s.js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:  s.names.BucketExecArchive,
		History: 1,
		TTL:     retention,
	})
	if err != nil {
		return fmt.Errorf("archive bucket: %w", err)
	}

	s.archive = archive

	return nil
}

// ArchiveTerminal moves every archivable execution (terminal and non-resumable:
// completed or cancelled — see Execution.Archivable) out of the hot bucket into
// the cold archive, returning how many it moved. Failed executions remain hot so
// they can still be resumed. Each execution is written to the archive before it
// is deleted from the hot bucket, so a crash mid-sweep can duplicate but never
// lose one; a later sweep re-archives idempotently. It is a no-op when archival
// is disabled.
func (s *Store) ArchiveTerminal(ctx context.Context) (int, error) {
	if s.archive == nil {
		return 0, nil
	}

	pending, err := s.collectArchivable(ctx)
	if err != nil {
		return 0, err
	}

	moved := 0

	for _, candidate := range pending {
		movedOne, archiveErr := s.archiveOne(ctx, candidate)
		if archiveErr != nil {
			return moved, archiveErr
		}

		if movedOne {
			moved++
		}
	}

	return moved, nil
}

type archiveCandidate struct {
	id   string
	data []byte
	rev  uint64
}

func (s *Store) collectArchivable(ctx context.Context) ([]archiveCandidate, error) {
	// Collect first, then move: deleting while the watch is still streaming the
	// hot bucket would mutate what we are iterating.
	var pending []archiveCandidate

	err := s.ForEachExecution(ctx, func(e *Execution) error {
		if !e.Archivable() {
			return nil
		}

		data, marshalErr := s.archivedExecutionData(ctx, e)
		if marshalErr != nil {
			return marshalErr
		}

		pending = append(pending, archiveCandidate{id: e.ID, data: data, rev: e.Revision})

		return nil
	})

	return pending, err
}

func (s *Store) archivedExecutionData(ctx context.Context, e *Execution) ([]byte, error) {
	archived := *e
	archived.ArchivedRevision = e.Revision

	if archived.InputHash == "" {
		input, err := s.GetPayload(ctx, InputKey(e.ID))
		if err != nil && !errors.Is(err, ErrNotFound) {
			return nil, err
		}

		if err == nil {
			archived.InputHash = hashPayload(input)
		}
	}

	return json.Marshal(&archived)
}

func (s *Store) archiveOne(ctx context.Context, candidate archiveCandidate) (bool, error) {
	var e Execution
	if json.Unmarshal(candidate.data, &e) != nil {
		return false, nil
	}

	current, err := s.currentArchiveCandidate(ctx, candidate)
	if err != nil || !current {
		return false, err
	}

	if _, archiveErr := s.getArchived(ctx, e.ID); archiveErr == nil {
		return false, s.deleteExistingArchiveDuplicate(ctx, e.ID, candidate.rev)
	} else if !errors.Is(archiveErr, ErrNotFound) {
		return false, archiveErr
	}

	return s.createArchiveAndDeleteHot(ctx, e.ID, candidate)
}

func (s *Store) currentArchiveCandidate(ctx context.Context, candidate archiveCandidate) (bool, error) {
	current, err := s.getHot(ctx, candidate.id)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	return current.Revision == candidate.rev && current.Archivable(), nil
}

func (s *Store) deleteExistingArchiveDuplicate(ctx context.Context, execID string, rev uint64) error {
	err := s.deleteHotArchived(ctx, execID, rev)
	if errors.Is(err, ErrConflict) {
		return nil
	}

	return err
}

func (s *Store) createArchiveAndDeleteHot(
	ctx context.Context, execID string, candidate archiveCandidate,
) (bool, error) {
	archiveRev, err := s.archive.Create(ctx, execID, candidate.data)
	if errors.Is(err, jetstream.ErrKeyExists) {
		return false, s.deleteExistingArchiveDuplicate(ctx, execID, candidate.rev)
	}

	if err != nil {
		return false, err
	}

	if err = s.deleteHotArchived(ctx, execID, candidate.rev); errors.Is(err, ErrConflict) {
		return false, s.rollbackArchiveCreate(ctx, execID, archiveRev)
	} else if err != nil {
		return false, err
	}

	return true, nil
}

func (s *Store) rollbackArchiveCreate(ctx context.Context, execID string, archiveRev uint64) error {
	err := s.archive.Delete(ctx, execID, jetstream.LastRevision(archiveRev))
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil
	}

	return err
}

func (s *Store) deleteHotArchived(ctx context.Context, execID string, rev uint64) error {
	// An archivable execution is terminal and non-resumable, so only delete the
	// exact revision collected by the sweep. A conflict means a concurrent writer
	// changed the hot key after collection; the caller must not install archive
	// ownership for this stale candidate.
	err := s.exec.Delete(ctx, execID, jetstream.LastRevision(rev))
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil
	}

	if errors.Is(err, jetstream.ErrKeyExists) || isWrongLastSeq(err) {
		return ErrConflict
	}

	if err != nil {
		return err
	}

	// The archive keeps the control metadata; the data plane is dropped — an
	// archived execution's outputs are no longer readable. Best-effort: a failure
	// leaves orphaned entries (visibility GC re-deletes them when the archive
	// record expires) and never blocks the sweep.
	if deleteErr := s.DeletePayloads(ctx, execID); deleteErr != nil {
		slog.Debug("archive: delete payloads", "exec", execID, "err", deleteErr)
	}

	return nil
}

func hashPayload(payload json.RawMessage) string {
	sum := sha256.Sum256(payload)

	return hex.EncodeToString(sum[:])
}

// isWrongLastSeq reports whether err is the server's CAS rejection for KV
// Update (wrong expected last subject sequence).
func isWrongLastSeq(err error) bool {
	var apiErr *jetstream.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence
	}

	return false
}
