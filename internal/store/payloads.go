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
	"regexp"
	"strconv"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// tokenPattern bounds the execID/node/name/version components used to build
// data-plane keys below. internal/runtime (and packtrail.go) already validate
// an execution id against this same pattern before any of these functions are
// reached; this is a last-resort defense-in-depth backstop against a future
// caller skipping that step — not a substitute for it — mirroring
// internal/names.New's identical rationale. Every caller of these functions is
// internal to this module and expected to have already validated, so a
// violation panics rather than returning an error.
var tokenPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

func checkToken(kind, s string) {
	if !tokenPattern.MatchString(s) {
		panic(fmt.Sprintf("store: invalid %s %q: must match %s", kind, s, tokenPattern))
	}
}

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
func InputKey(execID string) string {
	checkToken("execution id", execID)

	return execID + ".in"
}

// OutputKey is the data-plane key of a task or branch node's output.
func OutputKey(execID, node string) string {
	checkToken("execution id", execID)
	checkToken("node id", node)

	return execID + ".out." + node
}

// OutputVersionKey is the data-plane key of a candidate task or branch output.
// The execution document commits exactly one version per output node; uncommitted
// versions are harmless orphans swept with the execution's other payloads.
func OutputVersionKey(execID, node, version string) string {
	checkToken("execution id", execID)
	checkToken("node id", node)
	checkToken("output version", version)

	return execID + ".outv." + node + "." + version
}

// SignalKey is the data-plane key of a received signal's payload. It is
// versioned by the signal's stream sequence: the control plane commits
// LastSeq[name] via CAS, and the payload for exactly that sequence was written
// first — so two deliveries of the same signal racing across instances can
// never leave the committed sequence pointing at the other delivery's payload.
// Superseded entries are garbage until DeletePayloads sweeps the execution.
func SignalKey(execID, name string, seq uint64) string {
	checkToken("execution id", execID)
	checkToken("signal name", name)

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

// GetPayloads fetches every data-plane entry belonging to execID — its input,
// every committed and candidate output, every received signal payload — in a
// single round trip, keyed by their full KV key (InputKey/OutputKey/
// OutputVersionKey/SignalKey). Callers that would otherwise call GetPayload
// once per entry (e.g. assembling the full invocation context) can look their
// keys up in the returned map instead: assembling context on every node visit
// of a long linear flow used to do one sequential KV get per prior output,
// compounding to O(depth²) total reads across the execution's lifetime. A
// missing key is simply absent from the map, matching GetPayload's ErrNotFound
// for that key. Returns an empty map (not an error) if the execution has no
// data-plane entries at all.
func (s *Store) GetPayloads(ctx context.Context, execID string) (map[string]json.RawMessage, error) {
	checkToken("execution id", execID)

	w, err := s.payloads.Watch(ctx, execID+".>", jetstream.IgnoreDeletes())
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return map[string]json.RawMessage{}, nil
		}

		return nil, err
	}
	defer func() { _ = w.Stop() }()

	out := map[string]json.RawMessage{}

	for {
		select {
		case entry, ok := <-w.Updates():
			if !ok || entry == nil {
				return out, nil
			}

			out[entry.Key()] = append(json.RawMessage(nil), entry.Value()...)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
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
