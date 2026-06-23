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
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail/internal/names"
	"github.com/henomis/packtrail/internal/natstest"
)

func TestAccessors(t *testing.T) {
	s := open(t)

	if s.JS() == nil {
		t.Error("JS() returned nil")
	}

	if s.Names() != names.New("") {
		t.Errorf("Names() = %+v, want default", s.Names())
	}

	if s.IdxStatus() == nil {
		t.Error("IdxStatus() returned nil")
	}

	if s.IdxFlow() == nil {
		t.Error("IdxFlow() returned nil")
	}
}

func TestExecutionActive(t *testing.T) {
	cases := []struct {
		status string
		want   bool
	}{
		{StatusRunning, true},
		{StatusWaiting, true},
		{StatusCompleted, false},
		{StatusFailed, false},
		{"", false},
	}
	for _, c := range cases {
		e := &Execution{Status: c.status}
		if got := e.Active(); got != c.want {
			t.Errorf("Active(%q) = %v, want %v", c.status, got, c.want)
		}
	}
}

func TestGetNotFound(t *testing.T) {
	s := open(t)

	_, err := s.Get(context.Background(), "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(missing) err = %v, want ErrNotFound", err)
	}
}

func TestCreateDuplicate(t *testing.T) {
	ctx := context.Background()
	s := open(t)

	e := &Execution{ID: "dup", Status: StatusRunning, Payload: json.RawMessage(`{}`)}
	if _, err := s.Create(ctx, e); err != nil {
		t.Fatalf("first create: %v", err)
	}

	if _, err := s.Create(ctx, &Execution{ID: "dup", Status: StatusRunning, Payload: json.RawMessage(`{}`)}); err == nil {
		t.Fatal("second create on same id succeeded, want error")
	}
}

func TestMutateGetError(t *testing.T) {
	s := open(t)

	_, err := s.Mutate(context.Background(), "missing", func(*Execution) error {
		t.Fatal("fn should not run when Get fails")
		return nil
	})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Mutate(missing) err = %v, want ErrNotFound", err)
	}
}

func TestMutateFnError(t *testing.T) {
	ctx := context.Background()
	s := open(t)

	if _, err := s.Create(ctx, &Execution{ID: "e", Status: StatusRunning, Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}

	sentinel := errors.New("boom")

	_, err := s.Mutate(ctx, "e", func(*Execution) error { return sentinel })
	if !errors.Is(err, sentinel) {
		t.Fatalf("Mutate err = %v, want sentinel", err)
	}
}

func TestEmitEvent(t *testing.T) {
	ctx := context.Background()
	s := open(t)

	e := &Execution{ID: "ev1", FlowName: "flow", Status: StatusFailed, CurrentNode: "n1", Error: "kaboom", Revision: 7}
	if err := s.EmitEvent(ctx, e); err != nil {
		t.Fatalf("EmitEvent: %v", err)
	}

	stream, err := s.JS().Stream(ctx, s.Names().StreamEvents)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}

	msg, err := stream.GetLastMsgForSubject(ctx, s.Names().SubjEventsPrefix+e.ID)
	if err != nil {
		t.Fatalf("get msg: %v", err)
	}

	var got Event
	if unmarshalErr := json.Unmarshal(msg.Data, &got); unmarshalErr != nil {
		t.Fatalf("unmarshal: %v", unmarshalErr)
	}

	if got.ExecID != e.ID || got.FlowName != "flow" || got.Status != StatusFailed ||
		got.Node != "n1" || got.Error != "kaboom" || got.Revision != 7 {
		t.Fatalf("event mismatch: %+v", got)
	}

	if got.Time.IsZero() {
		t.Error("event Time not set")
	}
}

func TestListExecutionKeys(t *testing.T) {
	ctx := context.Background()
	s := open(t)

	// Empty bucket: ErrNoKeysFound is swallowed into a nil slice.
	keys, err := s.ListExecutionKeys(ctx)
	if err != nil {
		t.Fatalf("empty list: %v", err)
	}

	if len(keys) != 0 {
		t.Fatalf("empty list = %v, want none", keys)
	}

	for _, id := range []string{"a", "b", "c"} {
		if _, createErr := s.Create(ctx, &Execution{ID: id, Status: StatusRunning, Payload: json.RawMessage(`{}`)}); createErr != nil {
			t.Fatal(createErr)
		}
	}

	keys, err = s.ListExecutionKeys(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	sort.Strings(keys)

	if len(keys) != 3 || keys[0] != "a" || keys[1] != "b" || keys[2] != "c" {
		t.Fatalf("keys = %v, want [a b c]", keys)
	}
}

func TestCasBackoff(t *testing.T) {
	// Small attempt stays under the cap; large attempt is clamped to it.
	for _, attempt := range []int{0, 1, 5, 1000} {
		d := casBackoff(attempt)
		if d <= 0 {
			t.Errorf("casBackoff(%d) = %v, want > 0", attempt, d)
		}

		if d > casBackoffCap {
			t.Errorf("casBackoff(%d) = %v, exceeds cap %v", attempt, d, casBackoffCap)
		}
	}
}

func TestIsWrongLastSeq(t *testing.T) {
	if isWrongLastSeq(nil) {
		t.Error("isWrongLastSeq(nil) = true, want false")
	}

	if isWrongLastSeq(errors.New("plain")) {
		t.Error("isWrongLastSeq(plain) = true, want false")
	}

	wrongSeq := &jetstream.APIError{ErrorCode: jetstream.JSErrCodeStreamWrongLastSequence}
	if !isWrongLastSeq(wrongSeq) {
		t.Error("isWrongLastSeq(wrong-last-seq) = false, want true")
	}

	other := &jetstream.APIError{ErrorCode: jetstream.JSErrCodeStreamNotFound}
	if isWrongLastSeq(other) {
		t.Error("isWrongLastSeq(other api err) = true, want false")
	}
}

// TestAcquireLeaseContendedTakeover races several instances to take over a single
// expired lease. Exactly one must win, exercising the CAS-conflict re-read path.
func TestAcquireLeaseContendedTakeover(t *testing.T) {
	ctx := context.Background()
	s := open(t)

	// Seed an already-expired lease so every contender sees a takeover candidate.
	if ok, _ := s.AcquireLease(ctx, "e", "old", time.Millisecond); !ok {
		t.Fatal("seed acquire")
	}

	time.Sleep(20 * time.Millisecond)

	const contenders = 8

	var (
		wins  int
		mu    sync.Mutex
		start = make(chan struct{})
		wg    sync.WaitGroup
	)

	for i := range contenders {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			<-start

			ok, err := s.AcquireLease(ctx, "e", string(rune('A'+i)), 30*time.Second)
			if err != nil {
				t.Errorf("contender %d: %v", i, err)
			}

			if ok {
				mu.Lock()
				wins++
				mu.Unlock()
			}
		}(i)
	}

	close(start)
	wg.Wait()

	if wins != 1 {
		t.Fatalf("expired-lease takeover had %d winners, want exactly 1", wins)
	}
}

// TestOpenContextError drives Open's error-wrapping path: a cancelled context
// makes the first bucket creation fail.
func TestOpenContextError(t *testing.T) {
	srv := natstest.Start(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := Open(ctx, srv.JS, names.New("")); err == nil {
		t.Fatal("Open with cancelled context succeeded, want error")
	}
}

// TestOperationsContextError exercises the NATS-error return paths of each
// operation by cancelling the context before the call.
func TestOperationsContextError(t *testing.T) {
	s := open(t)

	// Seed one execution while the context is still live.
	if _, err := s.Create(context.Background(), &Execution{ID: "x", Status: StatusRunning, Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := s.Get(ctx, "x"); err == nil {
		t.Error("Get with cancelled context: want error")
	}

	if _, err := s.Create(ctx, &Execution{ID: "y", Status: StatusRunning, Payload: json.RawMessage(`{}`)}); err == nil {
		t.Error("Create with cancelled context: want error")
	}

	if _, err := s.Mutate(ctx, "x", func(*Execution) error { return nil }); err == nil {
		t.Error("Mutate with cancelled context: want error")
	}

	if err := s.EmitEvent(ctx, &Execution{ID: "x"}); err == nil {
		t.Error("EmitEvent with cancelled context: want error")
	}

	if _, err := s.ListExecutionKeys(ctx); err == nil {
		t.Error("ListExecutionKeys with cancelled context: want error")
	}

	if _, err := s.AcquireLease(ctx, "x", "inst", time.Second); err == nil {
		t.Error("AcquireLease with cancelled context: want error")
	}

	if err := s.ReleaseLease(ctx, "x", "inst"); err == nil {
		t.Error("ReleaseLease with cancelled context: want error")
	}
}

func TestReleaseLeaseNoLease(t *testing.T) {
	// Releasing a lease that was never acquired is a no-op.
	s := open(t)
	if err := s.ReleaseLease(context.Background(), "never", "inst-A"); err != nil {
		t.Fatalf("release of absent lease: %v", err)
	}
}

func TestReleaseLeaseWrongOwner(t *testing.T) {
	ctx := context.Background()
	s := open(t)

	if ok, _ := s.AcquireLease(ctx, "e", "inst-A", 30*time.Second); !ok {
		t.Fatal("A acquire")
	}

	// B's release must not drop A's lease.
	if err := s.ReleaseLease(ctx, "e", "inst-B"); err != nil {
		t.Fatalf("B release: %v", err)
	}

	// A still holds it: B cannot acquire.
	if ok, _ := s.AcquireLease(ctx, "e", "inst-B", 30*time.Second); ok {
		t.Fatal("B acquired after a no-op release; A's lease was wrongly dropped")
	}
}
