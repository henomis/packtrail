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

package consume

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail/internal/natstest"
)

// TestSlowHandlerIsNotRedelivered: a handler running much longer than the ack
// wait keeps its message (heartbeats), so no second process gets it.
func TestSlowHandlerIsNotRedelivered(t *testing.T) {
	s := natstest.Start(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if _, err := s.JS.CreateStream(ctx, jetstream.StreamConfig{Name: "S", Subjects: []string{"s.>"}}); err != nil {
		t.Fatal(err)
	}

	var calls, acked atomic.Int32

	done := make(chan struct{})

	handler := func(_ context.Context, msg jetstream.Msg) {
		calls.Add(1)
		time.Sleep(3500 * time.Millisecond) // 3.5× the ack wait

		if msg.Ack() == nil {
			acked.Add(1)
		}

		close(done)
	}

	runCtx, stop := context.WithCancel(ctx)
	defer stop()

	// Two competing runners: a redelivery would reach the second one.
	for range 2 {
		go func() {
			_ = Run(runCtx, s.JS, Config{
				Stream:   "S",
				Consumer: jetstream.ConsumerConfig{Durable: "d", AckPolicy: jetstream.AckExplicitPolicy, AckWait: time.Second},
				Handler:  handler, PullExpiry: time.Second,
			})
		}()
	}

	time.Sleep(200 * time.Millisecond)

	if _, err := s.JS.Publish(ctx, "s.x", []byte("x")); err != nil {
		t.Fatal(err)
	}

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("handler never finished")
	}

	time.Sleep(1500 * time.Millisecond) // past another ack wait

	if calls.Load() != 1 || acked.Load() != 1 {
		t.Fatalf("calls = %d, acked = %d: a slow handler was redelivered", calls.Load(), acked.Load())
	}
}

// TestCrashedHandlerIsRedelivered: without a live process heartbeating, the
// message returns after the ack wait.
func TestCrashedHandlerIsRedelivered(t *testing.T) {
	s := natstest.Start(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if _, err := s.JS.CreateStream(ctx, jetstream.StreamConfig{Name: "S", Subjects: []string{"s.>"}}); err != nil {
		t.Fatal(err)
	}

	cons, err := s.JS.CreateOrUpdateConsumer(ctx, "S", jetstream.ConsumerConfig{
		Durable: "d", AckPolicy: jetstream.AckExplicitPolicy, AckWait: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err = s.JS.Publish(ctx, "s.x", []byte("x")); err != nil {
		t.Fatal(err)
	}

	// A "crashed" holder: fetches the message and never acks or heartbeats.
	if _, err = cons.Fetch(1, jetstream.FetchMaxWait(time.Second)); err != nil {
		t.Fatal(err)
	}

	got := make(chan struct{}, 1)

	go func() {
		_ = Run(ctx, s.JS, Config{
			Stream:   "S",
			Consumer: jetstream.ConsumerConfig{Durable: "d", AckPolicy: jetstream.AckExplicitPolicy, AckWait: time.Second},
			Handler: func(_ context.Context, msg jetstream.Msg) {
				_ = msg.Ack()

				got <- struct{}{}
			},
		})
	}()

	select {
	case <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("message held by a dead process was not redelivered")
	}
}

// TestConsumerSetupIsRetried: a consumer that cannot be set up at start (here
// its stream does not exist yet; on a cluster, an API timeout while hundreds
// of replicated consumers are created) is retried, never given up: a runner
// that returned would leave its partition unserved until a restart (G5-08).
func TestConsumerSetupIsRetried(t *testing.T) {
	s := natstest.Start(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	got := make(chan struct{})

	runErr := make(chan error, 1)

	go func() {
		runErr <- Run(ctx, s.JS, Config{
			Stream:   "LATE",
			Consumer: jetstream.ConsumerConfig{Durable: "d", AckPolicy: jetstream.AckExplicitPolicy},
			Handler: func(_ context.Context, msg jetstream.Msg) {
				_ = msg.Ack()

				close(got)
			},
			PullExpiry: time.Second,
		})
	}()

	time.Sleep(1500 * time.Millisecond) // at least one failed setup

	select {
	case err := <-runErr:
		t.Fatalf("Run gave up: %v", err)
	default:
	}

	if _, err := s.JS.CreateStream(ctx, jetstream.StreamConfig{Name: "LATE", Subjects: []string{"late.>"}}); err != nil {
		t.Fatal(err)
	}

	if _, err := s.JS.Publish(ctx, "late.x", []byte("x")); err != nil {
		t.Fatal(err)
	}

	select {
	case <-got:
	case <-ctx.Done():
		t.Fatal("the message was never handled: setup was not retried")
	}
}
