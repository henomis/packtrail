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

package asyncqueue_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/henomis/packtrail/internal/natstest"
	"github.com/henomis/packtrail/invoker"
	"github.com/henomis/packtrail/invoker/asyncqueue"
)

// terminalError is a non-retryable completion error. The worker detects it
// structurally (interface{ Terminal() bool }) — mirroring the runtime engine's
// "unknown flow" completion error — and dead-letters the job instead of looping.
type terminalError struct{}

func (terminalError) Error() string  { return "terminal" }
func (terminalError) Terminal() bool { return true }

// countingCompleter returns err for every CompleteActivity and counts the calls,
// so a test can tell a single dead-letter (Term) from a Nak redelivery loop.
type countingCompleter struct {
	mu    sync.Mutex
	calls int
	err   error
}

func (c *countingCompleter) CompleteActivity(context.Context, string, string, int, invoker.Result) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.calls++

	return c.err
}

func (c *countingCompleter) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.calls
}

func waitForCount(t *testing.T, c *countingCompleter, want int, timeout time.Duration) {
	t.Helper()

	deadline := time.After(timeout)

	for c.count() < want {
		select {
		case <-deadline:
			t.Fatalf("CompleteActivity reached %d calls, want >= %d within %s", c.count(), want, timeout)
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// TestWorkerDeadLettersTerminalCompletion verifies a terminal CompleteActivity
// error (e.g. the engine no longer knows the flow) Terms the job on the first
// delivery instead of Nak-looping forever — so it is invoked exactly once.
func TestWorkerDeadLettersTerminalCompletion(t *testing.T) {
	srv := natstest.Start(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const prefix, kind = "t", "echo"
	if err := asyncqueue.EnsureStream(ctx, srv.JS, prefix, kind); err != nil {
		t.Fatalf("ensure stream: %v", err)
	}

	exec := invoker.Func(func(context.Context, invoker.Request) (invoker.Result, error) {
		return invoker.Result{Status: invoker.StatusOK}, nil
	})
	completer := &countingCompleter{err: terminalError{}}

	w := asyncqueue.NewWorker(srv.JS, prefix, kind, exec, completer)
	go func() { _ = w.Run(ctx) }()

	d := asyncqueue.NewDispatcher(srv.JS, prefix, kind)
	if _, err := d.Invoke(ctx, invoker.Request{ExecutionID: "e1", NodeID: "n1", Target: "a"}); err != nil {
		t.Fatalf("invoke: %v", err)
	}

	waitForCount(t, completer, 1, 5*time.Second)

	time.Sleep(2 * time.Second) // longer than the worker's nakDelay; a Nak would redeliver

	if got := completer.count(); got != 1 {
		t.Fatalf("CompleteActivity called %d times, want 1 (terminal error dead-lettered, not redelivered)", got)
	}
}

// TestWorkerDeadLetterSink verifies the registered sink is invoked with the
// job key, reason and delivery count just before a terminal job is Term'd, so a
// caller (packtrail) can record a durable dead-letter trace.
func TestWorkerDeadLetterSink(t *testing.T) {
	srv := natstest.Start(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const prefix, kind = "t", "echo"
	if err := asyncqueue.EnsureStream(ctx, srv.JS, prefix, kind); err != nil {
		t.Fatalf("ensure stream: %v", err)
	}

	exec := invoker.Func(func(context.Context, invoker.Request) (invoker.Result, error) {
		return invoker.Result{Status: invoker.StatusOK}, nil
	})
	completer := &countingCompleter{err: terminalError{}}

	type record struct {
		key, reason string
	}

	sunk := make(chan record, 1)
	sink := asyncqueue.WithDeadLetterSink(func(_ context.Context, key, reason string, _ uint64) {
		select {
		case sunk <- record{key, reason}:
		default:
		}
	})

	w := asyncqueue.NewWorker(srv.JS, prefix, kind, exec, completer, sink)
	go func() { _ = w.Run(ctx) }()

	d := asyncqueue.NewDispatcher(srv.JS, prefix, kind)
	if _, err := d.Invoke(ctx, invoker.Request{ExecutionID: "e1", NodeID: "n1", Target: "a"}); err != nil {
		t.Fatalf("invoke: %v", err)
	}

	select {
	case got := <-sunk:
		if got.key != "e1/n1" {
			t.Fatalf("sink key = %q, want %q", got.key, "e1/n1")
		}

		if got.reason != "terminal" {
			t.Fatalf("sink reason = %q, want %q", got.reason, "terminal")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("dead-letter sink was not invoked")
	}
}

// TestWorkerRecoversInvokerPanic verifies a panic from the embedder's Invoker is
// recovered and settled as a StatusError completion instead of crashing the
// worker goroutine (which would take down the whole hosting process).
func TestWorkerRecoversInvokerPanic(t *testing.T) {
	srv := natstest.Start(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const prefix, kind = "t", "echo"
	if err := asyncqueue.EnsureStream(ctx, srv.JS, prefix, kind); err != nil {
		t.Fatalf("ensure stream: %v", err)
	}

	exec := invoker.Func(func(context.Context, invoker.Request) (invoker.Result, error) {
		panic("boom in invoker")
	})
	completer := newFakeCompleter()

	w := asyncqueue.NewWorker(srv.JS, prefix, kind, exec, completer)
	go func() { _ = w.Run(ctx) }()

	d := asyncqueue.NewDispatcher(srv.JS, prefix, kind)
	if _, err := d.Invoke(ctx, invoker.Request{ExecutionID: "e1", NodeID: "n1", Target: "a"}); err != nil {
		t.Fatalf("invoke: %v", err)
	}

	select {
	case <-completer.ch:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for CompleteActivity (panic likely crashed the worker)")
	}

	calls := completer.snapshot()
	if len(calls) != 1 {
		t.Fatalf("completions: got %d, want 1", len(calls))
	}

	if calls[0].res.Status != invoker.StatusError {
		t.Fatalf("completion status = %q, want StatusError (panic recovered)", calls[0].res.Status)
	}
}

// TestWorkerDeadLettersExhaustedTransient verifies a persistently-failing
// (non-terminal) completion is dead-lettered after WithMaxDeliver deliveries
// rather than Nak-looping forever.
func TestWorkerDeadLettersExhaustedTransient(t *testing.T) {
	srv := natstest.Start(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const prefix, kind = "t", "echo"
	if err := asyncqueue.EnsureStream(ctx, srv.JS, prefix, kind); err != nil {
		t.Fatalf("ensure stream: %v", err)
	}

	exec := invoker.Func(func(context.Context, invoker.Request) (invoker.Result, error) {
		return invoker.Result{Status: invoker.StatusOK}, nil
	})
	completer := &countingCompleter{err: errors.New("boom")} // non-terminal: retryable

	const maxDeliver = 3

	w := asyncqueue.NewWorker(srv.JS, prefix, kind, exec, completer, asyncqueue.WithMaxDeliver(maxDeliver))
	go func() { _ = w.Run(ctx) }()

	d := asyncqueue.NewDispatcher(srv.JS, prefix, kind)
	if _, err := d.Invoke(ctx, invoker.Request{ExecutionID: "e1", NodeID: "n1", Target: "a"}); err != nil {
		t.Fatalf("invoke: %v", err)
	}

	waitForCount(t, completer, maxDeliver, 20*time.Second)

	time.Sleep(3 * time.Second) // longer than the worker's nakDelay; assert no further redelivery

	if got := completer.count(); got != maxDeliver {
		t.Fatalf("CompleteActivity called %d times, want %d (capped, then dead-lettered)", got, maxDeliver)
	}
}
