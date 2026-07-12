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
	"errors"
	"testing"
	"time"
)

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
