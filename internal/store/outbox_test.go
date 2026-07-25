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
)

// TestAddOutputIsIdempotent verifies AddOutput records a node once even when
// called more than once for the same node (a Mutate retry must not duplicate
// it in Outputs).
func TestAddOutputIsIdempotent(t *testing.T) {
	e := &Execution{}

	e.AddOutput("a")
	e.AddOutput("b")
	e.AddOutput("a")

	if want := []string{"a", "b"}; !equalStrings(e.Outputs, want) {
		t.Fatalf("Outputs = %v, want %v", e.Outputs, want)
	}
}

// TestSetOutputRecordsVersion verifies SetOutput both appends the node (once)
// and, when version is non-empty, records it for OutputVersion to read back.
// A version-less call must not create the map or a spurious empty entry.
func TestSetOutputRecordsVersion(t *testing.T) {
	e := &Execution{}

	e.SetOutput("a", "v1")
	e.SetOutput("a", "v1") // idempotent re-application must not duplicate Outputs
	e.SetOutput("legacy", "")

	if want := []string{"a", "legacy"}; !equalStrings(e.Outputs, want) {
		t.Fatalf("Outputs = %v, want %v", e.Outputs, want)
	}

	if got := e.OutputVersion("a"); got != "v1" {
		t.Fatalf("OutputVersion(a) = %q, want v1", got)
	}

	if got := e.OutputVersion("legacy"); got != "" {
		t.Fatalf("OutputVersion(legacy) = %q, want \"\" (version-less call)", got)
	}
}

// TestClearOutputRemovesSelection verifies ClearOutput drops node from both
// Outputs and OutputVersions, leaving other nodes untouched, and is a safe
// no-op on a node that was never added.
func TestClearOutputRemovesSelection(t *testing.T) {
	e := &Execution{}

	e.SetOutput("a", "v1")
	e.SetOutput("b", "v2")

	e.ClearOutput("a")

	if want := []string{"b"}; !equalStrings(e.Outputs, want) {
		t.Fatalf("Outputs = %v, want %v", e.Outputs, want)
	}

	if got := e.OutputVersion("a"); got != "" {
		t.Fatalf("OutputVersion(a) = %q, want \"\" after ClearOutput", got)
	}

	if got := e.OutputVersion("b"); got != "v2" {
		t.Fatalf("OutputVersion(b) = %q, want v2 (untouched)", got)
	}

	// No-op on an absent node: must not panic and must leave state unchanged.
	e.ClearOutput("never-added")

	if want := []string{"b"}; !equalStrings(e.Outputs, want) {
		t.Fatalf("Outputs after no-op ClearOutput = %v, want %v", e.Outputs, want)
	}
}

// TestAppendWorkAndAppendSched verify both outbox appenders assign a
// monotonically increasing Seq shared across kinds, and record the right Kind
// (and, for AppendSched, a UTC At).
func TestAppendWorkAndAppendSched(t *testing.T) {
	e := &Execution{}

	e.AppendWork(json.RawMessage(`{"w":1}`))

	at := time.Now().Add(time.Hour)
	e.AppendSched(json.RawMessage(`{"s":1}`), at)

	e.AppendWork(json.RawMessage(`{"w":2}`))

	if len(e.Outbox) != 3 {
		t.Fatalf("Outbox has %d items, want 3", len(e.Outbox))
	}

	for i, want := range []struct {
		kind string
		seq  uint64
	}{
		{OutboxWork, 1},
		{OutboxSched, 2},
		{OutboxWork, 3},
	} {
		if e.Outbox[i].Kind != want.kind || e.Outbox[i].Seq != want.seq {
			t.Fatalf("Outbox[%d] = %+v, want kind=%v seq=%d", i, e.Outbox[i], want.kind, want.seq)
		}
	}

	if !e.Outbox[1].At.Equal(at.UTC()) {
		t.Fatalf("Outbox[1].At = %v, want %v (UTC)", e.Outbox[1].At, at.UTC())
	}

	if e.OutboxSeq != 3 {
		t.Fatalf("OutboxSeq = %d, want 3", e.OutboxSeq)
	}
}

// TestDropOutbox verifies flushed items are removed, items appended
// concurrently (after the flush snapshot was taken) survive, and changed
// correctly reports whether anything was actually dropped.
func TestDropOutbox(t *testing.T) {
	e := &Execution{}

	e.AppendWork(json.RawMessage(`{"w":1}`)) // seq 1
	e.AppendWork(json.RawMessage(`{"w":2}`)) // seq 2
	e.AppendWork(json.RawMessage(`{"w":3}`)) // seq 3, "appended since" a flush of {1,2}

	changed := e.DropOutbox(map[uint64]bool{1: true, 2: true})
	if !changed {
		t.Fatal("DropOutbox reported no change despite dropping seq 1 and 2")
	}

	if len(e.Outbox) != 1 || e.Outbox[0].Seq != 3 {
		t.Fatalf("Outbox = %+v, want only seq 3", e.Outbox)
	}

	// Nothing in flushed matches what's left: no-op, reports unchanged.
	if changed = e.DropOutbox(map[uint64]bool{1: true, 2: true}); changed {
		t.Fatal("DropOutbox reported a change when nothing in flushed matched")
	}

	if len(e.Outbox) != 1 {
		t.Fatalf("Outbox = %+v, want unchanged", e.Outbox)
	}

	// Dropping everything nils the slice rather than leaving an empty one.
	if changed = e.DropOutbox(map[uint64]bool{3: true}); !changed {
		t.Fatal("DropOutbox reported no change despite dropping the last item")
	}

	if e.Outbox != nil {
		t.Fatalf("Outbox = %+v, want nil once empty", e.Outbox)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}

// TestForEachExecutionKey verifies every hot-bucket execution key is visited
// exactly once, and that a callback error stops the walk and propagates.
func TestForEachExecutionKey(t *testing.T) {
	ctx := context.Background()
	s := open(t)

	for _, id := range []string{"e1", "e2", "e3"} {
		if _, err := s.Create(ctx, &Execution{ID: id, FlowName: "f", Status: StatusRunning}); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
	}

	seen := map[string]bool{}

	if err := s.ForEachExecutionKey(ctx, func(key string) error {
		seen[key] = true
		return nil
	}); err != nil {
		t.Fatalf("for each: %v", err)
	}

	for _, id := range []string{"e1", "e2", "e3"} {
		if !seen[id] {
			t.Errorf("key %q not visited; saw %v", id, seen)
		}
	}

	// A callback error stops the walk and is returned to the caller.
	stopErr := errors.New("stop")
	if err := s.ForEachExecutionKey(ctx, func(string) error { return stopErr }); !errors.Is(err, stopErr) {
		t.Fatalf("err = %v, want the callback's own error", err)
	}
}

// TestForEachArchivedExecution verifies it visits only archived executions
// (not the hot bucket) and no-ops without error when archiving isn't enabled.
func TestForEachArchivedExecution(t *testing.T) {
	ctx := context.Background()
	s := open(t)

	// Archiving not enabled yet: a no-op, not an error.
	if err := s.ForEachArchivedExecution(ctx, func(*Execution) error {
		t.Fatal("callback invoked with archiving disabled")
		return nil
	}); err != nil {
		t.Fatalf("for each (disabled): %v", err)
	}

	if err := s.EnableArchive(ctx, time.Hour); err != nil {
		t.Fatalf("enable archive: %v", err)
	}

	if _, err := s.Create(ctx, &Execution{ID: "done", FlowName: "f", Status: StatusCompleted}); err != nil {
		t.Fatalf("create: %v", err)
	}

	if _, err := s.Create(ctx, &Execution{ID: "live", FlowName: "f", Status: StatusRunning}); err != nil {
		t.Fatalf("create: %v", err)
	}

	if _, err := s.ArchiveTerminal(ctx); err != nil {
		t.Fatalf("archive terminal: %v", err)
	}

	var seen []string

	if err := s.ForEachArchivedExecution(ctx, func(e *Execution) error {
		seen = append(seen, e.ID)
		return nil
	}); err != nil {
		t.Fatalf("for each archived: %v", err)
	}

	if len(seen) != 1 || seen[0] != "done" {
		t.Fatalf("visited %v, want exactly [done]", seen)
	}
}
