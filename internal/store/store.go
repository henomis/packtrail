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
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail/internal/names"
)

const (
	execBucketHistory = 64
	leasesBucketTTL   = 5 * time.Minute
	eventsMaxAge      = 24 * time.Hour
	casBackoffBase    = 250 * time.Microsecond
	casBackoffCap     = 5 * time.Millisecond
)

// ErrConflict is returned when a CAS write loses to a concurrent writer and the
// caller's revision is stale.
var ErrConflict = errors.New("store: revision conflict")

// ErrNotFound is returned when an execution key does not exist.
var ErrNotFound = errors.New("store: not found")

// Store provides access to all Packtrail KV buckets and streams.
type Store struct {
	js        jetstream.JetStream
	names     names.Names
	exec      jetstream.KeyValue
	archive   jetstream.KeyValue // cold store for aged-out terminal execs; nil unless EnableArchive ran
	leases    jetstream.KeyValue
	idxStatus jetstream.KeyValue
	idxFlow   jetstream.KeyValue
}

// Open ensures every bucket and stream exists, under the given namespace, and
// returns a ready Store.
func Open(ctx context.Context, js jetstream.JetStream, n names.Names) (*Store, error) {
	s := &Store{js: js, names: n}

	var err error

	if s.exec, err = js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:  n.BucketExecutions,
		History: execBucketHistory,
	}); err != nil {
		return nil, fmt.Errorf("exec bucket: %w", err)
	}

	if s.leases, err = js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: n.BucketLeases,
		// Bucket-wide TTL is a backstop; correctness relies on the expiry
		// timestamp stored in each lease value (see lease.go).
		TTL:            leasesBucketTTL,
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

	if _, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      n.StreamEvents,
		Subjects:  []string{n.SubjEventsPrefix + ">"},
		MaxAge:    eventsMaxAge,
		Storage:   jetstream.FileStorage,
		Retention: jetstream.LimitsPolicy,
	}); err != nil {
		return nil, fmt.Errorf("events stream: %w", err)
	}

	if _, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      n.StreamWork,
		Subjects:  []string{n.SubjWorkPrefix + ">"},
		Storage:   jetstream.FileStorage,
		Retention: jetstream.WorkQueuePolicy,
	}); err != nil {
		return nil, fmt.Errorf("work stream: %w", err)
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

// Create persists a new execution and returns its initial revision.
func (s *Store) Create(ctx context.Context, e *Execution) (uint64, error) {
	e.UpdatedAt = time.Now().UTC()

	data, err := json.Marshal(e)
	if err != nil {
		return 0, err
	}

	rev, err := s.exec.Create(ctx, e.ID, data)
	if err != nil {
		return 0, err
	}

	e.Revision = rev

	return rev, nil
}

// Get loads an execution and populates its Revision from the KV entry. If the
// id is not in the hot bucket and archival is enabled, it falls back to the
// cold archive, so reads of aged-out terminal executions still succeed until the
// archive's retention expires. The hot bucket remains the source of truth for
// mutations; archived executions are terminal and never mutated.
func (s *Store) Get(ctx context.Context, id string) (*Execution, error) {
	entry, err := s.exec.Get(ctx, id)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return s.getArchived(ctx, id)
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

	return &e, nil
}

// update performs a single CAS write at e.Revision and returns the new revision.
func (s *Store) update(ctx context.Context, e *Execution) (uint64, error) {
	e.UpdatedAt = time.Now().UTC()

	data, err := json.Marshal(e)
	if err != nil {
		return 0, err
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
// The mutated execution (with its new revision) is returned.
func (s *Store) Mutate(ctx context.Context, id string, fn func(*Execution) error) (*Execution, error) {
	const maxAttempts = 64
	for attempt := range maxAttempts {
		e, err := s.Get(ctx, id)
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
	ev := Event{
		ExecID:   e.ID,
		FlowName: e.FlowName,
		Status:   e.Status,
		Node:     e.CurrentNode,
		Error:    e.Error,
		Revision: e.Revision,
		Time:     time.Now().UTC(),
	}

	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}

	_, err = s.js.Publish(ctx, s.names.SubjEventsPrefix+e.ID, data)

	return err
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
	w, err := s.exec.WatchAll(ctx, jetstream.IgnoreDeletes())
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

			var e Execution
			if json.Unmarshal(entry.Value(), &e) != nil {
				continue
			}

			e.Revision = entry.Revision()

			if fnErr := fn(&e); fnErr != nil {
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
// archive is used; ArchiveCompleted and Get's fallback are no-ops until it runs.
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

// ArchiveCompleted moves every completed execution out of the hot bucket into
// the cold archive, returning how many it moved. Only completed executions are
// archived: they are truly terminal, whereas failed executions remain hot so
// they can still be resumed. Each execution is written to the archive before it
// is deleted from the hot bucket, so a crash mid-sweep can duplicate but never
// lose one; a later sweep re-archives idempotently. It is a no-op when archival
// is disabled.
func (s *Store) ArchiveCompleted(ctx context.Context) (int, error) {
	if s.archive == nil {
		return 0, nil
	}

	// Collect first, then move: deleting while the watch is still streaming the
	// hot bucket would mutate what we are iterating.
	var pending [][]byte

	err := s.ForEachExecution(ctx, func(e *Execution) error {
		if e.Status != StatusCompleted {
			return nil
		}

		data, marshalErr := json.Marshal(e)
		if marshalErr != nil {
			return marshalErr
		}

		pending = append(pending, data)

		return nil
	})
	if err != nil {
		return 0, err
	}

	moved := 0

	for _, data := range pending {
		var e Execution
		if json.Unmarshal(data, &e) != nil {
			continue
		}

		if _, putErr := s.archive.Put(ctx, e.ID, data); putErr != nil {
			return moved, putErr
		}

		// Completed is final, so a plain delete cannot race a concurrent
		// transition; tolerate an already-deleted key.
		if delErr := s.exec.Delete(ctx, e.ID); delErr != nil && !errors.Is(delErr, jetstream.ErrKeyNotFound) {
			return moved, delErr
		}

		moved++
	}

	return moved, nil
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
