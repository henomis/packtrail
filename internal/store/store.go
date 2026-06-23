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

// Get loads an execution and populates its Revision from the KV entry.
func (s *Store) Get(ctx context.Context, id string) (*Execution, error) {
	entry, err := s.exec.Get(ctx, id)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, ErrNotFound
		}

		return nil, err
	}

	var e Execution

	err = json.Unmarshal(entry.Value(), &e)
	if err != nil {
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

// isWrongLastSeq reports whether err is the server's CAS rejection for KV
// Update (wrong expected last subject sequence).
func isWrongLastSeq(err error) bool {
	var apiErr *jetstream.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence
	}

	return false
}
