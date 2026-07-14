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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/henomis/packtrail/internal/names"
	"github.com/henomis/packtrail/internal/natstest"
)

func open(t *testing.T) *Store {
	t.Helper()
	srv := natstest.Start(t)

	s, err := Open(context.Background(), srv.JS, names.New(""))
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	return s
}

func TestCreateGetMutate(t *testing.T) {
	ctx := context.Background()
	s := open(t)

	e := &Execution{ID: "e1", FlowName: "f", Status: StatusRunning, CurrentNode: "a"}
	if _, err := s.Create(ctx, e); err != nil {
		t.Fatalf("create: %v", err)
	}

	got, err := s.Get(ctx, "e1")
	if err != nil || got.CurrentNode != "a" {
		t.Fatalf("get: %v node=%q", err, got.CurrentNode)
	}

	out, err := s.Mutate(ctx, "e1", func(e *Execution) error {
		e.CurrentNode = "b"
		return nil
	})
	if err != nil || out.CurrentNode != "b" {
		t.Fatalf("mutate: %v node=%q", err, out.CurrentNode)
	}

	if out.Revision <= got.Revision {
		t.Fatalf("revision did not advance: %d -> %d", got.Revision, out.Revision)
	}
}

// TestMutateConcurrent verifies the CAS loop serializes concurrent writers
// without losing updates.
func TestMutateConcurrent(t *testing.T) {
	ctx := context.Background()

	s := open(t)
	if _, err := s.Create(ctx, &Execution{ID: "e", Status: StatusRunning, Branches: map[string]BranchState{}}); err != nil {
		t.Fatal(err)
	}

	const n = 20

	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			_, err := s.Mutate(ctx, "e", func(e *Execution) error {
				if e.Branches == nil {
					e.Branches = map[string]BranchState{}
				}

				e.Branches[string(rune('a'+i))] = BranchState{Status: BranchCompleted}

				return nil
			})
			if err != nil {
				t.Errorf("mutate %d: %v", i, err)
			}
		}(i)
	}

	wg.Wait()

	got, _ := s.Get(ctx, "e")
	if len(got.Branches) != n {
		t.Fatalf("lost updates: got %d branches, want %d", len(got.Branches), n)
	}
}

func TestArchiveTerminal(t *testing.T) {
	ctx := context.Background()
	s := open(t)

	if err := s.EnableArchive(ctx, time.Hour); err != nil {
		t.Fatalf("enable archive: %v", err)
	}

	mk := func(id, status string) {
		if _, err := s.Create(ctx, &Execution{ID: id, FlowName: "f", Status: status}); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
	}

	mk("done", StatusCompleted)
	mk("gone", StatusCancelled)
	mk("bust", StatusFailed)
	mk("live", StatusRunning)

	// Completed and cancelled are archivable; failed (resumable) and running stay.
	moved, err := s.ArchiveTerminal(ctx)
	if err != nil || moved != 2 {
		t.Fatalf("ArchiveTerminal = %d, %v; want 2, nil", moved, err)
	}

	keys, err := s.ListExecutionKeys(ctx)
	if err != nil {
		t.Fatalf("keys: %v", err)
	}

	hot := map[string]bool{}
	for _, k := range keys {
		hot[k] = true
	}

	if hot["done"] || hot["gone"] {
		t.Errorf("archivable exec still in hot bucket after archive: hot=%v", keys)
	}

	if !hot["bust"] || !hot["live"] {
		t.Errorf("failed/running execs were archived: hot=%v", keys)
	}

	// Both archived execs are still readable via Get's cold-store fallback.
	if got, gErr := s.Get(ctx, "done"); gErr != nil || got.Status != StatusCompleted {
		t.Fatalf("Get archived completed = %v, %v; want completed", got, gErr)
	}

	if got, gErr := s.Get(ctx, "gone"); gErr != nil || got.Status != StatusCancelled {
		t.Fatalf("Get archived cancelled = %v, %v; want cancelled", got, gErr)
	}
}

func TestLease(t *testing.T) {
	ctx := context.Background()
	s := open(t)

	ok, err := s.AcquireLease(ctx, "e", "inst-A", 30*time.Second)
	if err != nil || !ok {
		t.Fatalf("A acquire: ok=%v err=%v", ok, err)
	}
	// Another instance cannot take a live lease.
	ok, _ = s.AcquireLease(ctx, "e", "inst-B", 30*time.Second)
	if ok {
		t.Fatal("B acquired a live lease held by A")
	}
	// Owner can renew.
	ok, _ = s.AcquireLease(ctx, "e", "inst-A", 30*time.Second)
	if !ok {
		t.Fatal("A could not renew its own lease")
	}
	// After release, B can take over.
	if releaseErr := s.ReleaseLease(ctx, "e", "inst-A"); releaseErr != nil {
		t.Fatalf("release: %v", releaseErr)
	}

	ok, _ = s.AcquireLease(ctx, "e", "inst-B", 30*time.Second)
	if !ok {
		t.Fatal("B could not acquire after release")
	}
}

func TestLeaseExpiry(t *testing.T) {
	ctx := context.Background()
	s := open(t)
	// Short TTL: B should take over once it lapses.
	if ok, _ := s.AcquireLease(ctx, "e", "inst-A", 200*time.Millisecond); !ok {
		t.Fatal("A acquire")
	}

	time.Sleep(300 * time.Millisecond)

	if ok, _ := s.AcquireLease(ctx, "e", "inst-B", 30*time.Second); !ok {
		t.Fatal("B could not take over expired lease")
	}
}

func TestPayloadSizeGuard(t *testing.T) {
	ctx := context.Background()
	s := open(t)
	s.SetMaxPayloadBytes(64)

	big := json.RawMessage(`{"data":"` + strings.Repeat("x", 128) + `"}`)

	// PutPayload rejects an oversized data-plane entry before it reaches NATS.
	if err := s.PutPayload(ctx, OutputKey("e", "n"), big); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("PutPayload oversized: err = %v, want ErrPayloadTooLarge", err)
	}

	// The rejected write left nothing behind; a within-limit entry round-trips.
	if _, err := s.GetPayload(ctx, OutputKey("e", "n")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rejected write left an entry: %v", err)
	}

	if err := s.PutPayload(ctx, OutputKey("e", "n"), json.RawMessage(`{"ok":1}`)); err != nil {
		t.Fatalf("put small: %v", err)
	}

	got, err := s.GetPayload(ctx, OutputKey("e", "n"))
	if err != nil || string(got) != `{"ok":1}` {
		t.Fatalf("get = %s, %v; want {\"ok\":1}", got, err)
	}
}

func TestDocumentSizeGuard(t *testing.T) {
	ctx := context.Background()
	s := open(t)
	s.SetMaxDocumentBytes(256)

	if _, err := s.Create(ctx, &Execution{ID: "e1", FlowName: "f", Status: StatusRunning, CurrentNode: "a"}); err != nil {
		t.Fatalf("create: %v", err)
	}

	// A Mutate that would grow the control document past the limit is rejected
	// before it reaches NATS.
	_, err := s.Mutate(ctx, "e1", func(e *Execution) error {
		e.Error = strings.Repeat("x", 512)

		return nil
	})
	if !errors.Is(err, ErrDocumentTooLarge) {
		t.Fatalf("oversized Mutate: err = %v, want ErrDocumentTooLarge", err)
	}

	// The rejected write left the last within-limit document intact.
	got, err := s.Get(ctx, "e1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if got.Error != "" {
		t.Fatalf("rejected write persisted: Error = %q, want empty", got.Error)
	}

	// A within-limit Mutate still succeeds.
	if _, mErr := s.Mutate(ctx, "e1", func(e *Execution) error {
		e.Error = "small"

		return nil
	}); mErr != nil {
		t.Fatalf("within-limit Mutate: %v", mErr)
	}
}

func TestDocumentSizeGuardOnCreate(t *testing.T) {
	ctx := context.Background()
	s := open(t)
	s.SetMaxDocumentBytes(128)

	_, err := s.Create(ctx, &Execution{
		ID:          "e-create-big",
		FlowName:    "f",
		Status:      StatusRunning,
		CurrentNode: "a",
		Error:       strings.Repeat("x", 512),
	})
	if !errors.Is(err, ErrDocumentTooLarge) {
		t.Fatalf("oversized Create: err = %v, want ErrDocumentTooLarge", err)
	}

	if _, getErr := s.Get(ctx, "e-create-big"); !errors.Is(getErr, ErrNotFound) {
		t.Fatalf("oversized Create persisted an entry: get err = %v, want ErrNotFound", getErr)
	}
}

func TestPayloadGuardDisabled(t *testing.T) {
	ctx := context.Background()
	s := open(t)
	s.SetMaxPayloadBytes(0) // disabled

	big := json.RawMessage(`{"data":"` + strings.Repeat("x", DefaultMaxPayloadBytes+1) + `"}`)
	if err := s.PutPayload(ctx, InputKey("e"), big); err != nil {
		t.Fatalf("put with guard disabled: %v", err)
	}
}

// TestDeletePayloads verifies the per-execution sweep removes every data-plane
// entry of one execution and leaves other executions' entries intact.
func TestDeletePayloads(t *testing.T) {
	ctx := context.Background()
	s := open(t)

	for _, key := range []string{InputKey("a"), OutputKey("a", "n1"), SignalKey("a", "go", 3), InputKey("b")} {
		if err := s.PutPayload(ctx, key, json.RawMessage(`{}`)); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}

	if err := s.DeletePayloads(ctx, "a"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	for _, key := range []string{InputKey("a"), OutputKey("a", "n1"), SignalKey("a", "go", 3)} {
		if _, err := s.GetPayload(ctx, key); !errors.Is(err, ErrNotFound) {
			t.Fatalf("entry %s survived the sweep: %v", key, err)
		}
	}

	if _, err := s.GetPayload(ctx, InputKey("b")); err != nil {
		t.Fatalf("other execution's entry was swept: %v", err)
	}
}

// TestDeletePayloadsOlderThan verifies the age-guarded sweep (F-029): entries
// created before the cutoff are removed, while a fresh entry (as a recreated
// execution generation would write) is preserved, so GC cannot wipe a re-Started
// id's data.
func TestDeletePayloadsOlderThan(t *testing.T) {
	ctx := context.Background()
	s := open(t)

	// Old entry: created before the cutoff.
	if err := s.PutPayload(ctx, InputKey("a"), json.RawMessage(`{"old":1}`)); err != nil {
		t.Fatalf("put old: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	cutoff := time.Now()

	time.Sleep(50 * time.Millisecond)

	// Young entry: created after the cutoff (as a recreated generation would write).
	if err := s.PutPayload(ctx, OutputKey("a", "fresh"), json.RawMessage(`{"new":1}`)); err != nil {
		t.Fatalf("put young: %v", err)
	}

	if err := s.DeletePayloadsOlderThan(ctx, "a", cutoff); err != nil {
		t.Fatalf("delete older than: %v", err)
	}

	if _, err := s.GetPayload(ctx, InputKey("a")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("old entry survived the age-guarded sweep: %v", err)
	}

	if _, err := s.GetPayload(ctx, OutputKey("a", "fresh")); err != nil {
		t.Fatalf("fresh entry was swept (age guard failed): %v", err)
	}
}
