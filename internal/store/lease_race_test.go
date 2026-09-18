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
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// TestLeaseObsPrunesStaleEntries proves leaseObs does not grow unbounded for
// executions this process only ever observes (via LeaseHeld's read-only stall
// check) but never itself acquires/renews/releases — the only paths that call
// clearLeaseObservation. leaseRevisionStale is pure in-memory bookkeeping, so a
// bare Store (no NATS) exercises it directly.
func TestLeaseObsPrunesStaleEntries(t *testing.T) {
	s := &Store{}

	now := time.Now()
	s.leaseObs = map[string]leaseObservation{
		"stale": {revision: 1, at: now.Add(-2 * leaseObsPruneAge)},
		"fresh": {revision: 1, at: now},
	}

	// Drive leaseObsPruneEvery calls for distinct execution ids to trigger the
	// periodic sweep without waiting leaseObsPruneAge of real time.
	for i := range leaseObsPruneEvery {
		s.leaseRevisionStale("driver-"+strconv.Itoa(i), 1, time.Minute)
	}

	s.leaseObsMu.Lock()
	_, staleStillThere := s.leaseObs["stale"]
	_, freshStillThere := s.leaseObs["fresh"]
	s.leaseObsMu.Unlock()

	if staleStillThere {
		t.Error("stale leaseObs entry was not pruned by the periodic sweep")
	}

	if !freshStillThere {
		t.Error("fresh leaseObs entry was incorrectly pruned")
	}
}

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

// TestAcquireLeaseRenewalLosesObservedStaleTakeover reproduces the split-brain
// interleaving: instance A holds a lease and tries to renew it; between A's read
// and A's CAS write, instance B (which has observed A's revision remain stable
// for a full TTL) takes the lease over. A's write then conflicts and must re-read
// and report the lease lost, not shortcut to "still mine".
func TestAcquireLeaseRenewalLosesObservedStaleTakeover(t *testing.T) {
	ctx := context.Background()
	s := open(t)

	const execID = "lease-race"

	const ttl = 20 * time.Millisecond

	lease, err := json.Marshal(Lease{Owner: "A", Expires: time.Now().Add(-time.Minute).UTC()})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	if _, err = s.leases.Create(ctx, execID, lease); err != nil {
		t.Fatalf("seed lease: %v", err)
	}

	if held, acquireErr := s.AcquireLease(ctx, execID, "B", ttl); acquireErr != nil || held {
		t.Fatalf("B first observation: held=%v err=%v, want false/nil", held, acquireErr)
	}

	time.Sleep(ttl + 10*time.Millisecond)

	// Inject B's takeover into A's read→write window.
	origKV := s.leases
	s.leases = &raceKV{KeyValue: origKV, beforeUpdate: func() {
		s.leases = origKV // B (and A's retry re-read) must see the real bucket

		held, acquireErr := s.AcquireLease(ctx, execID, "B", ttl)
		if acquireErr != nil || !held {
			t.Fatalf("B takeover: held=%v err=%v", held, acquireErr)
		}

		s.leases = &raceKV{KeyValue: origKV} // restore wrapper for A's in-flight Update
	}}

	held, err := s.AcquireLease(ctx, execID, "A", time.Minute)
	if err != nil {
		t.Fatalf("A acquire: %v", err)
	}

	if held {
		t.Fatal("A reported the lease held after B's takeover won the CAS (split-brain)")
	}

	// The lease must belong to B.
	entry, err := origKV.Get(ctx, execID)
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
	origKV := s.leases
	s.leases = &raceKV{KeyValue: origKV, beforeUpdate: func() {
		renewed, marshalErr := json.Marshal(Lease{Owner: "A", Expires: time.Now().Add(2 * time.Minute).UTC()})
		if marshalErr != nil {
			t.Fatalf("marshal: %v", marshalErr)
		}

		if _, putErr := origKV.Put(ctx, execID, renewed); putErr != nil {
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
