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

// failingCompleter is a countingCompleter that also records FailActivity calls,
// so a test can assert the worker settled the execution before dropping the job.
type failingCompleter struct {
	countingCompleter

	mu       sync.Mutex
	fails    []failCall
	failErr  error // returned by FailActivity; nil means the settle succeeded
	failOnce bool  // when set, failErr applies only to the first call
}

type failCall struct {
	execID, node string
	generation   uint64
	attempt      int
	reason       string
}

func (c *failingCompleter) FailActivity(
	_ context.Context, execID, node string, generation uint64, attempt int, reason string,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.fails = append(c.fails, failCall{execID, node, generation, attempt, reason})

	err := c.failErr
	if c.failOnce {
		c.failErr = nil
	}

	return err
}

func (c *failingCompleter) failCalls() []failCall {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]failCall(nil), c.fails...)
}

func waitForFails(t *testing.T, c *failingCompleter, want int, timeout time.Duration) []failCall {
	t.Helper()

	deadline := time.After(timeout)

	for {
		if got := c.failCalls(); len(got) >= want {
			return got
		}

		select {
		case <-deadline:
			t.Fatalf("FailActivity reached %d calls, want >= %d within %s", len(c.failCalls()), want, timeout)
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// startWorker wires a worker over a fresh embedded server and returns a
// dispatcher for it.
func startWorker(
	ctx context.Context, t *testing.T, kind string, exec invoker.Invoker, completer asyncqueue.Completer,
	opts ...asyncqueue.Option,
) *asyncqueue.Dispatcher {
	t.Helper()

	srv := natstest.Start(t)

	const prefix = "t"
	if err := asyncqueue.EnsureStream(ctx, srv.JS, prefix, kind, opts...); err != nil {
		t.Fatalf("ensure stream: %v", err)
	}

	w := asyncqueue.NewWorker(srv.JS, prefix, kind, exec, completer, opts...)
	go func() { _ = w.Run(ctx) }()

	return asyncqueue.NewDispatcher(srv.JS, prefix, kind)
}

// TestDeadLetterFailsTheExecution: a job the worker can never complete must
// settle its execution as failed before being dropped.
//
// Without it the node stays parked on a completion that will never arrive.
// Nothing else settles it — the stall watchdog deliberately excludes async waits,
// since a legitimate one may run arbitrarily long — so the execution never
// completes, never fails, is not redriven, and is not even resumable, because
// Resume applies only to failed executions.
func TestDeadLetterFailsTheExecution(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	exec := invoker.Func(func(context.Context, invoker.Request) (invoker.Result, error) {
		return invoker.Result{Status: invoker.StatusOK}, nil
	})
	completer := &failingCompleter{countingCompleter: countingCompleter{err: terminalError{}}}

	d := startWorker(ctx, t, "echo", exec, completer)

	if _, err := d.Invoke(ctx, invoker.Request{
		ExecutionID: "e1", NodeID: "n1", Target: "a", Generation: 4, Attempt: 2,
	}); err != nil {
		t.Fatalf("invoke: %v", err)
	}

	got := waitForFails(t, completer, 1, 5*time.Second)[0]

	// The guard on the engine side is (node, generation, attempt); all three must
	// survive the round trip through the durable job or a stale drop could fail a
	// later visit of a node the flow legally cycles through.
	want := failCall{execID: "e1", node: "n1", generation: 4, attempt: 2, reason: "terminal"}
	if got.execID != want.execID || got.node != want.node ||
		got.generation != want.generation || got.attempt != want.attempt {
		t.Fatalf("FailActivity(%+v), want %+v", got, want)
	}

	if got.reason == "" {
		t.Error("FailActivity was given no reason; the failed execution would not say why")
	}

	// Settled once, then dropped: a Nak loop would keep calling.
	time.Sleep(2 * time.Second) // longer than nakDelay

	if n := len(completer.failCalls()); n != 1 {
		t.Fatalf("FailActivity called %d times, want 1 (job dropped after the settle, not redelivered)", n)
	}
}

// TestDeadLetterRetriesWhenTheSettleFails: if the execution cannot be failed,
// the job must not be dropped — the redelivery tries again.
//
// This is the case the old code got wrong in the most damaging way: the settle
// was never attempted at all, so an engine that was briefly unreachable cost an
// execution rather than a retry.
func TestDeadLetterRetriesWhenTheSettleFails(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	exec := invoker.Func(func(context.Context, invoker.Request) (invoker.Result, error) {
		return invoker.Result{Status: invoker.StatusOK}, nil
	})

	// The first settle fails (store unreachable); the redelivery's settle works.
	completer := &failingCompleter{
		countingCompleter: countingCompleter{err: terminalError{}},
		failErr:           errors.New("store unreachable"),
		failOnce:          true,
	}

	d := startWorker(ctx, t, "echo", exec, completer)

	if _, err := d.Invoke(ctx, invoker.Request{ExecutionID: "e2", NodeID: "n1", Target: "a"}); err != nil {
		t.Fatalf("invoke: %v", err)
	}

	// Two attempts: the failed one, then the redelivery that succeeds. A drop on
	// the first would leave this at one forever.
	fails := waitForFails(t, completer, 2, 15*time.Second)

	if fails[0].execID != "e2" || fails[1].execID != "e2" {
		t.Fatalf("FailActivity calls = %+v, want both for e2", fails)
	}
}

// TestDeadLetterDropsWithoutAFailer: a completer that does not implement
// activityFailer keeps the previous behaviour — trace, then drop — rather than
// failing to compile or blocking. Hosting a hand-written Completer stays valid.
func TestDeadLetterDropsWithoutAFailer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	exec := invoker.Func(func(context.Context, invoker.Request) (invoker.Result, error) {
		return invoker.Result{Status: invoker.StatusOK}, nil
	})
	completer := &countingCompleter{err: terminalError{}} // no FailActivity

	sunk := make(chan string, 1)
	sink := asyncqueue.WithDeadLetterSink(func(_ context.Context, key, _ string, _ uint64) {
		select {
		case sunk <- key:
		default:
		}
	})

	d := startWorker(ctx, t, "echo", exec, completer, sink)

	if _, err := d.Invoke(ctx, invoker.Request{ExecutionID: "e3", NodeID: "n1", Target: "a"}); err != nil {
		t.Fatalf("invoke: %v", err)
	}

	select {
	case got := <-sunk:
		if got != "e3/n1" {
			t.Fatalf("dead-letter key = %q, want e3/n1", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("job was neither settled nor dropped: a Completer without FailActivity must still make progress")
	}

	waitForCount(t, completer, 1, 5*time.Second)
}
