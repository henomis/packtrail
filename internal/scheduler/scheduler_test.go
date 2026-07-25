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

	"github.com/nats-io/nats.go/jetstream"

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

// TestConsumeFiredSetsServerSideMaxDeliverBackstop is a regression test: the
// fired consumer used to have no server-side MaxDeliver at all (server default
// -1, unlimited), so a fired message whose metadata read persistently fails
// would Nak forever without ever reaching the client-side exhaustion check.
// ConsumeFired must set a generous backstop above the client-tracked
// maxDeliver.
func TestConsumeFiredSetsServerSideMaxDeliverBackstop(t *testing.T) {
	ctx := context.Background()
	srv := natstest.Start(t)

	n := names.New("")
	sched := scheduler.New(srv.JS, n)

	if err := sched.EnsureStream(ctx); err != nil {
		t.Fatalf("ensure stream: %v", err)
	}

	const maxDeliver = 10

	cc, err := sched.ConsumeFired(ctx, "durable-maxdeliver", maxDeliver, nil,
		func(string, []byte, string) error { return nil })
	if err != nil {
		t.Fatalf("consume fired: %v", err)
	}

	t.Cleanup(cc.Stop)

	stream, err := srv.JS.Stream(ctx, n.StreamSchedule)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}

	cons, err := stream.Consumer(ctx, "durable-maxdeliver")
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}

	info, err := cons.Info(ctx)
	if err != nil {
		t.Fatalf("info: %v", err)
	}

	if info.Config.MaxDeliver <= maxDeliver {
		t.Fatalf("MaxDeliver = %d, want > %d (client-tracked maxDeliver, so the server backstop sits above it)",
			info.Config.MaxDeliver, maxDeliver)
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

// raceJS wraps a jetstream.JetStream so a test can hook the first Info() call
// on one specific stream, letting it inject a state change that lands exactly
// between ReclaimFired's before/after reads (both run synchronously in the
// same goroutine, so no real concurrency is needed to land it there).
type raceJS struct {
	jetstream.JetStream

	streamName  string
	onFirstInfo func()
}

func (r *raceJS) Stream(ctx context.Context, stream string) (jetstream.Stream, error) {
	s, err := r.JetStream.Stream(ctx, stream)
	if err != nil || stream != r.streamName {
		return s, err
	}

	return &raceStream{Stream: s, hook: r.onFirstInfo}, nil
}

type raceStream struct {
	jetstream.Stream

	calls int
	hook  func()
}

func (r *raceStream) Info(ctx context.Context, opts ...jetstream.StreamInfoOpt) (*jetstream.StreamInfo, error) {
	info, err := r.Stream.Info(ctx, opts...)

	r.calls++
	if r.calls == 1 && r.hook != nil {
		r.hook()
	}

	return info, err
}

// TestReclaimFiredPurgeCountIgnoresUnrelatedSubjectGrowth is a regression
// test: ReclaimFired used to compute its purge count from the stream's total
// message count before and after the purge. The same stream also carries
// sched.cron.*/sched.once.* entries, so a new one registered between those two
// reads inflated the "after" count and under-reported how many sched.fire.*
// messages were actually purged. Scoping the count to the fire.> subject (via
// WithSubjectFilter) must make it immune to that unrelated growth.
func TestReclaimFiredPurgeCountIgnoresUnrelatedSubjectGrowth(t *testing.T) {
	ctx := context.Background()
	srv := natstest.Start(t)

	n := names.New("")
	wrapped := &raceJS{JetStream: srv.JS, streamName: n.StreamSchedule}
	sched := scheduler.New(wrapped, n)

	if err := sched.EnsureStream(ctx); err != nil {
		t.Fatalf("ensure stream: %v", err)
	}

	ch := make(chan fired, 4)

	cc, err := sched.ConsumeFired(ctx, "test-fired-race", 10, nil, func(key string, payload []byte, _ string) error {
		ch <- fired{key: key, payload: append([]byte(nil), payload...)}
		return nil
	})
	if err != nil {
		t.Fatalf("consume fired: %v", err)
	}

	t.Cleanup(cc.Stop)

	// ReclaimFired's purge deliberately keeps the boundary message at the ack
	// floor itself (see its doc comment: "the boundary message ... is kept"),
	// so fire one more than we expect purged — the last one becomes the new
	// boundary and stays.
	const want = 3

	const total = want + 1
	for range total {
		if afterErr := sched.After(ctx, "k", 10*time.Millisecond, []byte(`{}`)); afterErr != nil {
			t.Fatalf("after: %v", afterErr)
		}
	}

	for i := range total {
		select {
		case <-ch:
		case <-time.After(fireTimeout):
			t.Fatalf("firing %d not delivered", i)
		}
	}

	injected := false
	wrapped.onFirstInfo = func() {
		if injected {
			return
		}

		injected = true

		// Unrelated concurrent growth on the same stream (a pending, far-future
		// one-shot timer under sched.once.*), landing between ReclaimFired's
		// before/after fire.> reads.
		if afterErr := sched.After(ctx, "unrelated", time.Hour, []byte(`{}`)); afterErr != nil {
			t.Errorf("inject unrelated schedule: %v", afterErr)
		}
	}

	// Sums across polls rather than stopping at the first non-zero purge: the
	// ack floor can advance in more than one step, so a single call may only
	// see part of the 3 acked firings.
	var purged uint64

	deadline := time.Now().Add(fireTimeout)
	for time.Now().Before(deadline) && purged < want {
		p, reclaimErr := sched.ReclaimFired(ctx, "test-fired-race")
		if reclaimErr != nil {
			t.Fatalf("reclaim: %v", reclaimErr)
		}

		purged += p

		if purged < want {
			time.Sleep(50 * time.Millisecond)
		}
	}

	if purged != want {
		t.Fatalf("purged = %d, want %d (unrelated sched.once.* growth should not dilute the fire.> count)", purged, want)
	}
}
