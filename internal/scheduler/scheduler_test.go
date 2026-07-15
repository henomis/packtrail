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

package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/henomis/packtrail/internal/names"
	"github.com/henomis/packtrail/internal/natstest"
	"github.com/henomis/packtrail/internal/scheduler"
)

type fired struct {
	key     string
	payload []byte
}

// setup starts an embedded server, a scheduler and a running fired-consumer that
// forwards every firing onto the returned channel.
func setup(t *testing.T) (context.Context, *scheduler.Scheduler, <-chan fired) {
	t.Helper()

	ctx := context.Background()
	srv := natstest.Start(t)

	sched := scheduler.New(srv.JS, names.New(""))
	if err := sched.EnsureStream(ctx); err != nil {
		t.Fatalf("scheduler: %v", err)
	}

	ch := make(chan fired, 4)

	cc, err := sched.ConsumeFired(ctx, "test-fired", 10, nil, func(key string, payload []byte, _ string) error {
		ch <- fired{key: key, payload: append([]byte(nil), payload...)}
		return nil
	})
	if err != nil {
		t.Fatalf("consume fired: %v", err)
	}

	t.Cleanup(cc.Stop)

	return ctx, sched, ch
}

const fireTimeout = 10 * time.Second

// TestAfterFires verifies a one-shot schedule is delivered to ConsumeFired with
// the original key and payload.
func TestAfterFires(t *testing.T) {
	ctx, sched, ch := setup(t)

	if err := sched.After(ctx, "exec-1", time.Second, []byte("hello")); err != nil {
		t.Fatalf("after: %v", err)
	}

	select {
	case f := <-ch:
		if f.key != "exec-1" {
			t.Fatalf("key = %q, want exec-1", f.key)
		}

		if string(f.payload) != "hello" {
			t.Fatalf("payload = %q, want hello", f.payload)
		}
	case <-time.After(fireTimeout):
		t.Fatal("schedule did not fire within timeout")
	}
}

// TestAtFires verifies scheduling at an absolute time delivers the firing.
func TestAtFires(t *testing.T) {
	ctx, sched, ch := setup(t)

	if err := sched.At(ctx, "exec-2", time.Now().Add(time.Second), []byte("payload")); err != nil {
		t.Fatalf("at: %v", err)
	}

	select {
	case f := <-ch:
		if f.key != "exec-2" {
			t.Fatalf("key = %q, want exec-2", f.key)
		}
	case <-time.After(fireTimeout):
		t.Fatal("schedule did not fire within timeout")
	}
}

// TestFireSubject verifies the fire subject embeds the key after the prefix.
func TestFireSubject(t *testing.T) {
	_, sched, _ := setup(t)

	subj := sched.FireSubject("exec-9")
	if subj == "" || subj == "exec-9" {
		t.Fatalf("FireSubject returned %q, want a prefixed subject", subj)
	}
}

// TestReclaimFiredPurgesAcked verifies ReclaimFired removes fired-schedule
// messages below the consumer's ack floor once they are delivered and acked, and
// leaves the stream able to schedule afterwards (F-001/F-013).
func TestReclaimFiredPurgesAcked(t *testing.T) {
	ctx, sched, ch := setup(t)

	const n = 3
	for range n {
		if err := sched.After(ctx, "k", 10*time.Millisecond, []byte(`{}`)); err != nil {
			t.Fatalf("after: %v", err)
		}
	}

	for i := range n {
		select {
		case <-ch:
		case <-time.After(fireTimeout):
			t.Fatalf("firing %d not delivered", i)
		}
	}

	// Poll: the ack floor advances shortly after each firing's handler acks.
	var purged uint64

	deadline := time.Now().Add(fireTimeout)
	for time.Now().Before(deadline) {
		p, err := sched.ReclaimFired(ctx, "test-fired")
		if err != nil {
			t.Fatalf("reclaim: %v", err)
		}

		purged += p
		if purged > 0 {
			break
		}

		time.Sleep(50 * time.Millisecond)
	}

	if purged == 0 {
		t.Fatal("ReclaimFired purged nothing after acked firings")
	}

	// Scheduling still works after a reclaim (definitions/pending timers untouched).
	if err := sched.After(ctx, "k", 10*time.Millisecond, []byte(`{}`)); err != nil {
		t.Fatalf("after (post-reclaim): %v", err)
	}

	select {
	case <-ch:
	case <-time.After(fireTimeout):
		t.Fatal("scheduling broken after reclaim")
	}
}
