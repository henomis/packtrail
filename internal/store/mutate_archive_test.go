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
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type maskFirstArchiveGetKV struct {
	jetstream.KeyValue

	masked bool
}

func (m *maskFirstArchiveGetKV) Get(ctx context.Context, key string) (jetstream.KeyValueEntry, error) {
	if !m.masked {
		m.masked = true

		return nil, jetstream.ErrKeyNotFound
	}

	return m.KeyValue.Get(ctx, key)
}

type beforeDeleteKV struct {
	jetstream.KeyValue

	before func()
}

func (b *beforeDeleteKV) Delete(ctx context.Context, key string, opts ...jetstream.KVDeleteOpt) error {
	if b.before != nil {
		before := b.before
		b.before = nil

		before()
	}

	return b.KeyValue.Delete(ctx, key, opts...)
}

// TestMutateArchivedReturnsNotFound verifies Mutate does not read through to the
// cold archive: an archived execution is terminal and immutable, and a mutation
// against it must fail fast with ErrNotFound instead of burning the CAS retry
// budget on a hot-bucket key that does not exist (the pre-fix behaviour, hit by
// e.g. a late signal to an archived execution).
func TestMutateArchivedReturnsNotFound(t *testing.T) {
	ctx := context.Background()
	s := open(t)

	if err := s.EnableArchive(ctx, time.Hour); err != nil {
		t.Fatalf("enable archive: %v", err)
	}

	e := &Execution{ID: "arch-1", FlowName: "f", Status: StatusCompleted}
	if _, err := s.Create(ctx, e); err != nil {
		t.Fatalf("create: %v", err)
	}

	moved, err := s.ArchiveTerminal(ctx)
	if err != nil || moved != 1 {
		t.Fatalf("archive: moved=%d err=%v, want 1/nil", moved, err)
	}

	// Reads still succeed via the archive fallback...
	got, err := s.Get(ctx, "arch-1")
	if err != nil || got.Status != StatusCompleted {
		t.Fatalf("get archived: %v status=%q", err, got.Status)
	}

	// ...but a mutation fails fast with ErrNotFound and zero CAS conflicts.
	conflictsBefore := s.CASConflicts()

	_, err = s.Mutate(ctx, "arch-1", func(e *Execution) error {
		e.Error = "should never be written"
		return nil
	})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("mutate archived: err=%v, want ErrNotFound", err)
	}

	if got := s.CASConflicts() - conflictsBefore; got != 0 {
		t.Fatalf("mutate archived burned %d CAS retries, want 0", got)
	}
}

// TestCreateDedupsAgainstArchive verifies StartWithID idempotency survives the
// archive sweep: once an execution has been moved out of the hot bucket into
// the cold archive, re-creating its id must fail with ErrAlreadyExists — not
// silently mint a fresh execution and re-run the whole flow under the same
// idempotency key.
func TestCreateDedupsAgainstArchive(t *testing.T) {
	ctx := context.Background()
	s := open(t)

	if err := s.EnableArchive(ctx, time.Hour); err != nil {
		t.Fatalf("enable archive: %v", err)
	}

	e := &Execution{ID: "arch-dedup", FlowName: "f", Status: StatusCompleted}
	if _, err := s.Create(ctx, e); err != nil {
		t.Fatalf("create: %v", err)
	}

	moved, err := s.ArchiveTerminal(ctx)
	if err != nil || moved != 1 {
		t.Fatalf("archive: moved=%d err=%v, want 1/nil", moved, err)
	}

	dup := &Execution{ID: "arch-dedup", FlowName: "f", Status: StatusRunning, CurrentNode: "a"}
	if _, err = s.Create(ctx, dup); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("re-create of archived id: err=%v, want ErrAlreadyExists", err)
	}

	// The archived record must be untouched.
	got, err := s.Get(ctx, "arch-dedup")
	if err != nil || got.Status != StatusCompleted {
		t.Fatalf("get after re-create attempt: %v status=%q, want completed", err, got.Status)
	}
}

func TestCreateDedupsArchiveRaceAfterHotCreate(t *testing.T) {
	ctx := context.Background()
	s := open(t)

	if err := s.EnableArchive(ctx, time.Hour); err != nil {
		t.Fatalf("enable archive: %v", err)
	}

	archived := &Execution{ID: "arch-race", FlowName: "f", Status: StatusCompleted}

	data, err := json.Marshal(archived)
	if err != nil {
		t.Fatalf("marshal archive: %v", err)
	}

	if _, err = s.archive.Put(ctx, archived.ID, data); err != nil {
		t.Fatalf("put archive: %v", err)
	}

	s.archive = &maskFirstArchiveGetKV{KeyValue: s.archive}

	dup := &Execution{ID: archived.ID, FlowName: "f", Status: StatusRunning, CurrentNode: "a"}
	if _, err = s.Create(ctx, dup); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("raced re-create: err=%v, want ErrAlreadyExists", err)
	}

	if _, err = s.getHot(ctx, archived.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("raced re-create left hot execution: err=%v, want ErrNotFound", err)
	}

	got, err := s.Get(ctx, archived.ID)
	if err != nil || got.Status != StatusCompleted {
		t.Fatalf("get after raced re-create: %v status=%q, want completed", err, got.Status)
	}
}

func TestArchivePreservesHotRevision(t *testing.T) {
	ctx := context.Background()
	s := open(t)

	if err := s.EnableArchive(ctx, time.Hour); err != nil {
		t.Fatalf("enable archive: %v", err)
	}

	if _, err := s.Create(ctx, &Execution{ID: "arch-rev", FlowName: "f", Status: StatusRunning}); err != nil {
		t.Fatalf("create: %v", err)
	}

	updated, err := s.Mutate(ctx, "arch-rev", func(e *Execution) error {
		e.Status = StatusCompleted

		return nil
	})
	if err != nil {
		t.Fatalf("mutate: %v", err)
	}

	moved, err := s.ArchiveTerminal(ctx)
	if err != nil || moved != 1 {
		t.Fatalf("archive: moved=%d err=%v, want 1/nil", moved, err)
	}

	got, err := s.Get(ctx, "arch-rev")
	if err != nil {
		t.Fatalf("get archived: %v", err)
	}

	if got.Revision != updated.Revision {
		t.Fatalf("archived revision = %d, want original hot revision %d", got.Revision, updated.Revision)
	}
}

func TestArchiveOwnsHotDuplicate(t *testing.T) {
	ctx := context.Background()
	s := open(t)

	if err := s.EnableArchive(ctx, time.Hour); err != nil {
		t.Fatalf("enable archive: %v", err)
	}

	archived := &Execution{ID: "arch-owns", FlowName: "f", Status: StatusCompleted}
	if _, err := s.Create(ctx, archived); err != nil {
		t.Fatalf("create archived source: %v", err)
	}

	moved, err := s.ArchiveTerminal(ctx)
	if err != nil || moved != 1 {
		t.Fatalf("archive: moved=%d err=%v, want 1/nil", moved, err)
	}

	dup := &Execution{ID: archived.ID, FlowName: "f", Status: StatusRunning, CurrentNode: "a"}

	data, err := json.Marshal(dup)
	if err != nil {
		t.Fatalf("marshal duplicate: %v", err)
	}

	if _, err = s.exec.Put(ctx, dup.ID, data); err != nil {
		t.Fatalf("put hot duplicate: %v", err)
	}

	got, err := s.Get(ctx, archived.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if got.Status != StatusCompleted {
		t.Fatalf("Get returned hot duplicate status %q, want archived completed", got.Status)
	}

	called := false

	_, err = s.Mutate(ctx, archived.ID, func(e *Execution) error {
		called = true
		e.Error = "mutated"

		return nil
	})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("mutate archived duplicate: err=%v, want ErrNotFound", err)
	}

	if called {
		t.Fatal("Mutate called fn for archive-owned hot duplicate")
	}

	hot, err := s.getHot(ctx, archived.ID)
	if err != nil {
		t.Fatalf("get hot duplicate: %v", err)
	}

	if hot.Error != "" {
		t.Fatalf("hot duplicate was mutated: error=%q", hot.Error)
	}
}

func TestArchiveBackfillsInputHash(t *testing.T) {
	ctx := context.Background()
	s := open(t)

	if err := s.EnableArchive(ctx, time.Hour); err != nil {
		t.Fatalf("enable archive: %v", err)
	}

	input := json.RawMessage(`{"legacy":true}`)
	if _, err := s.CreatePayload(ctx, InputKey("arch-hash"), input); err != nil {
		t.Fatalf("create input: %v", err)
	}

	if _, err := s.Create(ctx, &Execution{ID: "arch-hash", FlowName: "f", Status: StatusCompleted}); err != nil {
		t.Fatalf("create: %v", err)
	}

	moved, err := s.ArchiveTerminal(ctx)
	if err != nil || moved != 1 {
		t.Fatalf("archive: moved=%d err=%v, want 1/nil", moved, err)
	}

	got, err := s.getArchived(ctx, "arch-hash")
	if err != nil {
		t.Fatalf("get archived: %v", err)
	}

	if got.InputHash != hashPayload(input) {
		t.Fatalf("archived input hash = %q, want %q", got.InputHash, hashPayload(input))
	}
}

func TestArchiveTerminalDoesNotOverwriteArchiveOwner(t *testing.T) {
	ctx := context.Background()
	s := open(t)

	if err := s.EnableArchive(ctx, time.Hour); err != nil {
		t.Fatalf("enable archive: %v", err)
	}

	original := &Execution{
		ID: "arch-owner", FlowName: "original", InputHash: hashPayload(json.RawMessage(`{"original":true}`)),
		Status: StatusCompleted,
	}
	if _, err := s.Create(ctx, original); err != nil {
		t.Fatalf("create original: %v", err)
	}

	moved, err := s.ArchiveTerminal(ctx)
	if err != nil || moved != 1 {
		t.Fatalf("archive original: moved=%d err=%v, want 1/nil", moved, err)
	}

	duplicate := &Execution{
		ID: "arch-owner", FlowName: "duplicate", InputHash: hashPayload(json.RawMessage(`{"duplicate":true}`)),
		Status: StatusCompleted,
	}

	data, err := json.Marshal(duplicate)
	if err != nil {
		t.Fatalf("marshal duplicate: %v", err)
	}

	if _, err = s.exec.Put(ctx, duplicate.ID, data); err != nil {
		t.Fatalf("put hot duplicate: %v", err)
	}

	moved, err = s.ArchiveTerminal(ctx)
	if err != nil {
		t.Fatalf("archive duplicate: moved=%d err=%v", moved, err)
	}

	got, err := s.getArchived(ctx, original.ID)
	if err != nil {
		t.Fatalf("get archived: %v", err)
	}

	if got.FlowName != original.FlowName || got.InputHash != original.InputHash {
		t.Fatalf("archive overwritten by duplicate: %+v, want flow/hash from %+v", got, original)
	}

	if _, err = s.getHot(ctx, duplicate.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("hot duplicate survived archive sweep: err=%v, want ErrNotFound", err)
	}
}

func TestArchiveOneSkipsStaleCollectedRevision(t *testing.T) {
	ctx := context.Background()
	s := open(t)

	if err := s.EnableArchive(ctx, time.Hour); err != nil {
		t.Fatalf("enable archive: %v", err)
	}

	if _, err := s.Create(ctx, &Execution{ID: "arch-stale", FlowName: "f", Status: StatusCompleted}); err != nil {
		t.Fatalf("create: %v", err)
	}

	hot, err := s.getHot(ctx, "arch-stale")
	if err != nil {
		t.Fatalf("get hot: %v", err)
	}

	data, err := s.archivedExecutionData(ctx, hot)
	if err != nil {
		t.Fatalf("candidate data: %v", err)
	}

	if _, err = s.exec.Put(ctx, hot.ID, []byte(`{"id":"arch-stale","flow_name":"f","status":"running"}`)); err != nil {
		t.Fatalf("replace hot: %v", err)
	}

	moved, err := s.archiveOne(ctx, archiveCandidate{id: hot.ID, data: data, rev: hot.Revision})
	if err != nil {
		t.Fatalf("archive one: %v", err)
	}

	if moved {
		t.Fatal("stale collected candidate was archived")
	}

	if _, err = s.getArchived(ctx, hot.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("stale candidate created archive record: err=%v, want ErrNotFound", err)
	}

	got, err := s.getHot(ctx, hot.ID)
	if err != nil {
		t.Fatalf("get recreated hot: %v", err)
	}

	if got.Status != StatusRunning {
		t.Fatalf("recreated hot status = %q, want running", got.Status)
	}
}

func TestArchiveOneRollsBackArchiveWhenHotDeleteConflicts(t *testing.T) {
	ctx := context.Background()
	s := open(t)

	if err := s.EnableArchive(ctx, time.Hour); err != nil {
		t.Fatalf("enable archive: %v", err)
	}

	if _, err := s.Create(ctx, &Execution{ID: "arch-conflict", FlowName: "f", Status: StatusCompleted}); err != nil {
		t.Fatalf("create: %v", err)
	}

	hot, err := s.getHot(ctx, "arch-conflict")
	if err != nil {
		t.Fatalf("get hot: %v", err)
	}

	data, err := s.archivedExecutionData(ctx, hot)
	if err != nil {
		t.Fatalf("candidate data: %v", err)
	}

	realExec := s.exec
	s.exec = &beforeDeleteKV{KeyValue: realExec, before: func() {
		_, putErr := realExec.Put(ctx, hot.ID, []byte(`{"id":"arch-conflict","flow_name":"f","status":"running"}`))
		if putErr != nil {
			t.Errorf("concurrent put: %v", putErr)
		}
	}}

	moved, err := s.archiveOne(ctx, archiveCandidate{id: hot.ID, data: data, rev: hot.Revision})
	if err != nil {
		t.Fatalf("archive one: %v", err)
	}

	if moved {
		t.Fatal("archiveOne reported a move after delete conflict")
	}

	if _, err = s.getArchived(ctx, hot.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("archive record survived rollback: err=%v, want ErrNotFound", err)
	}

	got, err := s.Get(ctx, hot.ID)
	if err != nil {
		t.Fatalf("get hot after rollback: %v", err)
	}

	if got.Status != StatusRunning {
		t.Fatalf("hot update hidden or lost: status=%q, want running", got.Status)
	}
}
