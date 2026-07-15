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
	"strconv"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// The data plane: every payload an execution produces or consumes lives as its
// own entry in the payloads bucket, keyed under the execution id. The control
// plane (the execution document) carries only which entries exist; its guarded
// CAS transitions decide which write is current. Output writers therefore write
// versioned candidate keys first, then commit the selected version in the
// execution document; legacy OutputKey remains readable for old executions.
//
// The "in"/"out"/"sig" sub-tokens keep the key spaces disjoint even though
// node ids and signal names share one alphabet (a node may legally be named
// "in").

// InputKey is the data-plane key of an execution's start input.
func InputKey(execID string) string { return execID + ".in" }

// OutputKey is the data-plane key of a task or branch node's output.
func OutputKey(execID, node string) string { return execID + ".out." + node }

// OutputVersionKey is the data-plane key of a candidate task or branch output.
// The execution document commits exactly one version per output node; uncommitted
// versions are harmless orphans swept with the execution's other payloads.
func OutputVersionKey(execID, node, version string) string {
	return execID + ".outv." + node + "." + version
}

// SignalKey is the data-plane key of a received signal's payload. It is
// versioned by the signal's stream sequence: the control plane commits
// LastSeq[name] via CAS, and the payload for exactly that sequence was written
// first — so two deliveries of the same signal racing across instances can
// never leave the committed sequence pointing at the other delivery's payload.
// Superseded entries are garbage until DeletePayloads sweeps the execution.
func SignalKey(execID, name string, seq uint64) string {
	return execID + ".sig." + name + "." + strconv.FormatUint(seq, 10)
}

// PutPayload stores one data-plane entry, enforcing the per-entry size guard
// (ErrPayloadTooLarge) before the write reaches NATS.
func (s *Store) PutPayload(ctx context.Context, key string, data json.RawMessage) error {
	if s.maxPayloadBytes > 0 && len(data) > s.maxPayloadBytes {
		return fmt.Errorf("%w: payload %s is %d bytes, limit %d",
			ErrPayloadTooLarge, key, len(data), s.maxPayloadBytes)
	}

	_, err := s.payloads.Put(ctx, key, data)

	return err
}

// CreatePayload stores a data-plane entry only if absent — first write wins.
// Used for the start input: an idempotent Start retry carrying a different
// payload must not overwrite the input the original execution runs on. When the
// key already exists it is left untouched and its current value is returned, so
// the caller can detect an id being reused with different data instead of
// silently binding the control plane to another caller's payload; existing is
// nil when this call created the entry.
func (s *Store) CreatePayload(
	ctx context.Context, key string, data json.RawMessage,
) (existing json.RawMessage, err error) {
	if s.maxPayloadBytes > 0 && len(data) > s.maxPayloadBytes {
		return nil, fmt.Errorf("%w: payload %s is %d bytes, limit %d",
			ErrPayloadTooLarge, key, len(data), s.maxPayloadBytes)
	}

	if _, err = s.payloads.Create(ctx, key, data); err != nil {
		if !errors.Is(err, jetstream.ErrKeyExists) {
			return nil, err
		}

		return s.GetPayload(ctx, key)
	}

	return nil, nil
}

// GetPayload loads one data-plane entry, or ErrNotFound.
func (s *Store) GetPayload(ctx context.Context, key string) (json.RawMessage, error) {
	entry, err := s.payloads.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, ErrNotFound
		}

		return nil, err
	}

	return append(json.RawMessage(nil), entry.Value()...), nil
}

// DeletePayloadsOlderThan removes an execution's data-plane entries created
// before cutoff, each via a revision-guarded delete. It is the race-safe
// counterpart to DeletePayloads for the visibility GC, which prunes an id only
// after confirming the execution is gone from both hot and archive: if that id
// was meanwhile *recreated* (a re-Start binds the same id), the new generation's
// entries are young (created after cutoff, so not selected) and/or at a bumped
// revision (so the guarded delete no-ops), and are never wiped. A non-positive
// staleness (cutoff in the future) selects every entry but still revision-guards
// each delete, so a delete that races a recreation still no-ops.
func (s *Store) DeletePayloadsOlderThan(ctx context.Context, execID string, cutoff time.Time) error {
	w, err := s.payloads.Watch(ctx, execID+".>", jetstream.IgnoreDeletes(), jetstream.MetaOnly())
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return nil
		}

		return err
	}
	defer func() { _ = w.Stop() }()

	type staleEntry struct {
		key string
		rev uint64
	}

	var stale []staleEntry

	for {
		select {
		case entry, ok := <-w.Updates():
			if !ok || entry == nil {
				for _, se := range stale {
					// Revision-guarded: a concurrent re-Start that recreated this key
					// bumps its revision, so this delete no-ops rather than wiping the
					// new generation's data.
					_ = s.payloads.Delete(ctx, se.key, jetstream.LastRevision(se.rev))
				}

				return nil
			}

			if entry.Created().Before(cutoff) {
				stale = append(stale, staleEntry{entry.Key(), entry.Revision()})
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// DeletePayloads removes every data-plane entry of one execution. Used by the
// archive sweep: an archived execution keeps its control metadata readable but
// drops its data plane.
func (s *Store) DeletePayloads(ctx context.Context, execID string) error {
	w, err := s.payloads.Watch(ctx, execID+".>", jetstream.IgnoreDeletes(), jetstream.MetaOnly())
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return nil
		}

		return err
	}
	defer func() { _ = w.Stop() }()

	var keys []string

	for {
		select {
		case entry, ok := <-w.Updates():
			if !ok || entry == nil {
				for _, k := range keys {
					_ = s.payloads.Delete(ctx, k)
				}

				return nil
			}

			keys = append(keys, entry.Key())
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
