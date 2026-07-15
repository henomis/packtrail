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
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Lease is the value stored under packtrail-leases/<execID>.
type Lease struct {
	Owner   string    `json:"owner"`
	Expires time.Time `json:"expires"`
}

type leaseObservation struct {
	revision uint64
	at       time.Time
}

// AcquireLease attempts to take or renew ownership of execID for owner with the
// given TTL. It succeeds if the key is absent, already owned by owner, or held
// by another owner whose lease revision has remained unchanged for a full TTL as
// measured by this process's monotonic clock. The CAS guarantees that at most
// one distinct owner wins a single acquisition race. It returns true if the
// lease is now held by owner.
//
// This bounds concurrency but is not a hard lock across time: once a lease
// revision stops advancing for a TTL (its holder paused or crashed) another
// instance can acquire it while the original is still mid-invocation. Callers
// must therefore treat node invocation as at-least-once (the engine renews via
// heartbeat and aborts on a detected loss to narrow, not eliminate, the overlap
// window).
//
//nolint:gocognit,funlen
func (s *Store) AcquireLease(ctx context.Context, execID, owner string, ttl time.Duration) (bool, error) {
	val, err := json.Marshal(Lease{Owner: owner, Expires: time.Now().Add(ttl).UTC()})
	if err != nil {
		return false, err
	}

	// Retry to resolve races (our own heartbeat renewing, or an expired-lease
	// takeover contended by multiple instances).
	for range 8 {
		entry, getErr := s.leases.Get(ctx, execID)
		if errors.Is(getErr, jetstream.ErrKeyNotFound) {
			if _, createErr := s.leases.Create(ctx, execID, val); createErr != nil {
				if errors.Is(createErr, jetstream.ErrKeyExists) {
					continue // someone created it first; re-read
				}

				return false, createErr
			}

			s.clearLeaseObservation(execID)

			return true, nil
		}

		if getErr != nil {
			return false, getErr
		}

		var cur Lease
		if unmarshalErr := json.Unmarshal(entry.Value(), &cur); unmarshalErr != nil {
			return false, unmarshalErr
		}

		if cur.Owner != owner && !s.leaseRevisionStale(execID, entry.Revision(), ttl) {
			return false, nil // held by someone else that is still heartbeating, or not yet proven stale
		}

		// We own it, or the observed foreign lease revision has stayed unchanged
		// for a full TTL: take/renew via CAS at the observed revision.
		if _, updateErr := s.leases.Update(ctx, execID, val, entry.Revision()); updateErr != nil {
			if errors.Is(updateErr, jetstream.ErrKeyExists) || isWrongLastSeq(updateErr) {
				continue // renewal/takeover race: re-read to see who won
			}

			return false, updateErr
		}

		s.clearLeaseObservation(execID)

		return true, nil
	}

	return false, nil
}

// LeaseHeld reports whether execID's ownership lease should be treated as held
// by a live owner — i.e. the lease key exists and its revision has not remained
// unchanged for a full TTL as observed by this process. Used by the stall
// watchdog to avoid re-driving an execution whose work is legitimately in flight.
func (s *Store) LeaseHeld(ctx context.Context, execID string, ttl time.Duration) (bool, error) {
	entry, err := s.leases.Get(ctx, execID)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		s.clearLeaseObservation(execID)

		return false, nil
	}

	if err != nil {
		return false, err
	}

	var cur Lease
	if err = json.Unmarshal(entry.Value(), &cur); err != nil {
		return false, err
	}

	return !s.leaseRevisionStale(execID, entry.Revision(), ttl), nil
}

// ReleaseLease drops ownership of execID if held by owner. Releasing a lease not
// owned by owner is a no-op.
//
// The delete is guarded on the revision read above, so if our own heartbeat
// renewed the lease between the Get and the Delete the delete no-ops and the
// lease lingers until its Expires (≤ LeaseTTL). That is self-healing — the lease
// still frees on expiry — it only briefly delays a legitimate takeover; it never
// deletes a lease a different owner has taken over (the owner check and the
// revision guard both protect that).
func (s *Store) ReleaseLease(ctx context.Context, execID, owner string) error {
	entry, err := s.leases.Get(ctx, execID)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil
	}

	if err != nil {
		return err
	}

	var cur Lease

	err = json.Unmarshal(entry.Value(), &cur)
	if err != nil {
		return err
	}

	if cur.Owner != owner {
		return nil
	}

	err = s.leases.Delete(ctx, execID, jetstream.LastRevision(entry.Revision()))
	if errors.Is(err, jetstream.ErrKeyExists) || isWrongLastSeq(err) {
		return nil // someone else took over; leave it alone
	}

	if err == nil {
		s.clearLeaseObservation(execID)
	}

	return err
}

func (s *Store) leaseRevisionStale(execID string, revision uint64, ttl time.Duration) bool {
	if ttl <= 0 {
		return true
	}

	now := time.Now()

	s.leaseObsMu.Lock()
	defer s.leaseObsMu.Unlock()

	if s.leaseObs == nil {
		s.leaseObs = make(map[string]leaseObservation)
	}

	obs, ok := s.leaseObs[execID]
	if !ok || obs.revision != revision {
		s.leaseObs[execID] = leaseObservation{revision: revision, at: now}

		return false
	}

	return now.Sub(obs.at) >= ttl
}

func (s *Store) clearLeaseObservation(execID string) {
	s.leaseObsMu.Lock()
	defer s.leaseObsMu.Unlock()

	delete(s.leaseObs, execID)
}
