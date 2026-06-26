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

	e := &Execution{ID: "e1", FlowName: "f", Status: StatusRunning, CurrentNode: "a", Payload: json.RawMessage(`{}`)}
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
	if _, err := s.Create(ctx, &Execution{ID: "e", Status: StatusRunning, Payload: json.RawMessage(`{}`), Branches: map[string]BranchState{}}); err != nil {
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

func TestArchiveCompleted(t *testing.T) {
	ctx := context.Background()
	s := open(t)

	if err := s.EnableArchive(ctx, time.Hour); err != nil {
		t.Fatalf("enable archive: %v", err)
	}

	mk := func(id, status string) {
		if _, err := s.Create(ctx, &Execution{ID: id, FlowName: "f", Status: status, Payload: json.RawMessage(`{}`)}); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
	}

	mk("done", StatusCompleted)
	mk("bust", StatusFailed)
	mk("live", StatusRunning)

	moved, err := s.ArchiveCompleted(ctx)
	if err != nil || moved != 1 {
		t.Fatalf("ArchiveCompleted = %d, %v; want 1, nil", moved, err)
	}

	// The completed exec left the hot bucket; failed and running stayed.
	keys, err := s.ListExecutionKeys(ctx)
	if err != nil {
		t.Fatalf("keys: %v", err)
	}

	hot := map[string]bool{}
	for _, k := range keys {
		hot[k] = true
	}

	if hot["done"] {
		t.Error("completed exec still in hot bucket after archive")
	}

	if !hot["bust"] || !hot["live"] {
		t.Errorf("non-completed execs were archived: hot=%v", keys)
	}

	// The archived exec is still readable via Get's cold-store fallback.
	got, err := s.Get(ctx, "done")
	if err != nil || got.Status != StatusCompleted {
		t.Fatalf("Get archived = %v, %v; want completed", got, err)
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
