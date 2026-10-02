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

// Package snapshot stores periodic snapshots of execution states in KV: a
// cache that lets a state be rebuilt from the snapshot plus the tail of the
// log instead of the whole log. Snapshots are never authoritative; a missing or
// unreadable one just means a longer replay.
package snapshot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail/internal/fold"
	"github.com/henomis/packtrail/internal/infra"
)

// DefaultEvery is the default number of events between snapshots.
const DefaultEvery = 100

// Store reads and writes snapshots.
type Store struct {
	in    *infra.Infra
	every int
}

// New returns a snapshot store writing every n events (0 = DefaultEvery,
// negative = never).
func New(in *infra.Infra, every int) *Store {
	if every == 0 {
		every = DefaultEvery
	}

	return &Store{in: in, every: every}
}

// Due reports whether a state that went from before to after events should
// be snapshotted (it crossed a multiple of the interval).
func (s *Store) Due(before, after int) bool {
	return s.every > 0 && after/s.every > before/s.every
}

// pointer is stored in KV instead of a state too large for one message: the
// state itself lives in the blobs object store.
type pointer struct {
	Blob string `json:"$snapshot_blob"`
}

// Save stores st. A state larger than the claim-check threshold goes to the
// blobs object store with a pointer in KV, so large executions still get
// snapshots instead of replaying their whole log on every cache miss.
func (s *Store) Save(ctx context.Context, st *fold.State) error {
	b, err := json.Marshal(st)
	if err != nil {
		return err
	}

	if len(b) > s.in.BlobThreshold {
		if b, err = s.saveBlob(ctx, st.ExecID, b); err != nil {
			return err
		}
	}

	kv, err := s.in.KV(ctx, s.in.Names.BucketSnapshots)
	if err != nil {
		return err
	}

	_, err = kv.Put(ctx, st.ExecID, b)

	return err
}

func (s *Store) saveBlob(ctx context.Context, execID string, b []byte) ([]byte, error) {
	obs, err := s.in.Object(ctx, s.in.Names.ObjectBlobs)
	if err != nil {
		return nil, err
	}

	// One name per execution: each snapshot replaces the previous one, and
	// blob.DeleteExec removes it with the execution's other blobs.
	name := execID + "/snapshot"
	if _, err = obs.PutBytes(ctx, name, b); err != nil {
		return nil, fmt.Errorf("snapshot %s: %w", execID, err)
	}

	return json.Marshal(pointer{Blob: name})
}

// Load returns the snapshot of execID, or nil when there is none.
func (s *Store) Load(ctx context.Context, execID string) (*fold.State, error) {
	kv, err := s.in.KV(ctx, s.in.Names.BucketSnapshots)
	if err != nil {
		return nil, err
	}

	e, err := kv.Get(ctx, execID)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, nil //nolint:nilnil // no snapshot is not an error.
		}

		return nil, err
	}

	b := e.Value()

	var p pointer
	if json.Unmarshal(b, &p) == nil && p.Blob != "" {
		obs, lerr := s.in.Object(ctx, s.in.Names.ObjectBlobs)
		if lerr != nil {
			return nil, lerr
		}

		if b, lerr = obs.GetBytes(ctx, p.Blob); lerr != nil {
			return nil, fmt.Errorf("snapshot %s: %w", execID, lerr)
		}
	}

	st, err := fold.Unmarshal(b)
	if err != nil {
		return nil, fmt.Errorf("snapshot %s: %w", execID, err)
	}

	return st, nil
}

// Delete removes the snapshot of execID.
func (s *Store) Delete(ctx context.Context, execID string) error {
	kv, err := s.in.KV(ctx, s.in.Names.BucketSnapshots)
	if err != nil {
		return err
	}

	if err = kv.Purge(ctx, execID); err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		return err
	}

	return nil
}
