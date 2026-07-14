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

package runtime

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"
)

// TestLeaseRefcountSurvivesConcurrentItems verifies the per-instance lease
// refcount: when two work items for the same execution run concurrently on one
// instance, the first to finish must not release the KV lease out from under
// the second — only the last holder releases it.
func TestLeaseRefcountSurvivesConcurrentItems(t *testing.T) {
	_, eng := newIdleEngine(t)
	ctx := context.Background()

	const execID = "exec-lease"

	// Handler A: 0→1, no in-process lease yet, so it KV-acquires and tracks.
	if eng.retainLease(execID) {
		t.Fatal("retain with no holder succeeded; want KV acquire path")
	}

	held, err := eng.store.AcquireLease(ctx, execID, eng.cfg.OwnerID, eng.cfg.LeaseTTL)
	if err != nil || !held {
		t.Fatalf("acquire: held=%v err=%v", held, err)
	}

	eng.trackLease(execID)

	// Handler B for the same execution: retained in-process, no KV round-trip.
	if !eng.retainLease(execID) {
		t.Fatal("second work item failed to retain the held lease")
	}

	// A finishes first: the KV lease must survive, so a foreign instance still
	// cannot acquire it.
	eng.releaseLease(ctx, execID)

	foreign, err := eng.store.AcquireLease(ctx, execID, "other-owner", time.Second)
	if err != nil {
		t.Fatalf("foreign acquire: %v", err)
	}

	if foreign {
		t.Fatal("foreign owner acquired the lease while a work item was still processing")
	}

	// B finishes: the last holder releases, and the lease becomes acquirable.
	eng.releaseLease(ctx, execID)

	foreign, err = eng.store.AcquireLease(ctx, execID, "other-owner", time.Second)
	if err != nil || !foreign {
		t.Fatalf("foreign acquire after full release: held=%v err=%v", foreign, err)
	}
}

// TestLeaseRefcountConcurrentChurn hammers the acquire/retain → release cycle
// from many goroutines across two executions. Under -race it pins down the
// per-execution locking: the entry unlink racing a goroutine that still holds
// the stale pointer, and concurrent map access from unrelated handlers. Every
// cycle is balanced, so afterwards no KV lease may survive.
func TestLeaseRefcountConcurrentChurn(t *testing.T) {
	_, eng := newIdleEngine(t)
	ctx := context.Background()

	var wg sync.WaitGroup

	for g := range 8 {
		wg.Add(1)

		go func() {
			defer wg.Done()

			execID := "exec-churn-" + strconv.Itoa(g%2)

			for range 50 {
				if !eng.retainLease(execID) {
					// Same owner everywhere: a self-owned acquire always succeeds.
					if _, err := eng.store.AcquireLease(ctx, execID, eng.cfg.OwnerID, eng.cfg.LeaseTTL); err != nil {
						t.Errorf("acquire: %v", err)
						return
					}

					eng.trackLease(execID)
				}

				eng.releaseLease(ctx, execID)
			}
		}()
	}

	wg.Wait()

	// Balanced cycles: the last holder of each execution released its KV lease,
	// so a foreign owner can acquire both.
	for g := range 2 {
		execID := "exec-churn-" + strconv.Itoa(g)

		foreign, err := eng.store.AcquireLease(ctx, execID, "other-owner", time.Second)
		if err != nil || !foreign {
			t.Fatalf("foreign acquire of %s after churn: held=%v err=%v", execID, foreign, err)
		}
	}
}
