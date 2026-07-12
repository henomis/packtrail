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
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// raceKV wraps the leases bucket and runs a hook once, just before the first
// Update — the window between AcquireLease's read and its CAS write.
type raceKV struct {
	jetstream.KeyValue

	beforeUpdate func()
}

func (r *raceKV) Update(ctx context.Context, key string, value []byte, revision uint64) (uint64, error) {
	if r.beforeUpdate != nil {
		hook := r.beforeUpdate
		r.beforeUpdate = nil

		hook()
	}

	return r.KeyValue.Update(ctx, key, value, revision)
}

// TestAcquireLeaseExpiredRenewalLosesTakeover reproduces the split-brain
// interleaving: instance A holds an *expired* lease and tries to renew it;
// between A's read and A's CAS write, instance B takes the expired lease over.
// A's write then conflicts — and because the lease A observed was expired, the
// conflicting writer may be B, not A's own heartbeat. A must re-read and report
// the lease lost, not shortcut to "still mine". (An unexpired self-owned lease
// may keep the shortcut: nobody else is allowed to write a live lease.)
func TestAcquireLeaseExpiredRenewalLosesTakeover(t *testing.T) {
	ctx := context.Background()
	s := open(t)

	const execID = "lease-race"

	// Seed an expired lease owned by A.
	expired, err := json.Marshal(Lease{Owner: "A", Expires: time.Now().Add(-time.Minute).UTC()})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	if _, err = s.leases.Create(ctx, execID, expired); err != nil {
		t.Fatalf("seed lease: %v", err)
	}

	// Inject B's takeover into A's read→write window.
	real := s.leases
	s.leases = &raceKV{KeyValue: real, beforeUpdate: func() {
		s.leases = real // B (and A's retry re-read) must see the real bucket

		held, acquireErr := s.AcquireLease(ctx, execID, "B", time.Minute)
		if acquireErr != nil || !held {
			t.Fatalf("B takeover: held=%v err=%v", held, acquireErr)
		}

		s.leases = &raceKV{KeyValue: real} // restore wrapper for A's in-flight Update
	}}

	held, err := s.AcquireLease(ctx, execID, "A", time.Minute)
	if err != nil {
		t.Fatalf("A acquire: %v", err)
	}

	if held {
		t.Fatal("A reported the lease held after B's takeover won the CAS (split-brain)")
	}

	// The lease must belong to B.
	entry, err := real.Get(ctx, execID)
	if err != nil {
		t.Fatalf("get lease: %v", err)
	}

	var cur Lease
	if err = json.Unmarshal(entry.Value(), &cur); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if cur.Owner != "B" {
		t.Fatalf("lease owner = %q, want B", cur.Owner)
	}
}

// TestAcquireLeaseLiveRenewalConflictStillHeld verifies the legitimate half of
// the shortcut survives: when the observed lease is self-owned and *unexpired*,
// a CAS conflict can only be our own concurrent renewal, so AcquireLease still
// reports the lease held.
func TestAcquireLeaseLiveRenewalConflictStillHeld(t *testing.T) {
	ctx := context.Background()
	s := open(t)

	const execID = "lease-live"

	live, err := json.Marshal(Lease{Owner: "A", Expires: time.Now().Add(time.Minute).UTC()})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	if _, err = s.leases.Create(ctx, execID, live); err != nil {
		t.Fatalf("seed lease: %v", err)
	}

	// Our own heartbeat renews in the read→write window, bumping the revision.
	real := s.leases
	s.leases = &raceKV{KeyValue: real, beforeUpdate: func() {
		renewed, marshalErr := json.Marshal(Lease{Owner: "A", Expires: time.Now().Add(2 * time.Minute).UTC()})
		if marshalErr != nil {
			t.Fatalf("marshal: %v", marshalErr)
		}

		if _, putErr := real.Put(ctx, execID, renewed); putErr != nil {
			t.Fatalf("concurrent renewal: %v", putErr)
		}
	}}

	held, err := s.AcquireLease(ctx, execID, "A", time.Minute)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	if !held {
		t.Fatal("A lost its own live lease to its own renewal conflict, want still held")
	}
}
