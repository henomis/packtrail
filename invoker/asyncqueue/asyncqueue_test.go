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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail/internal/natstest"
	"github.com/henomis/packtrail/invoker"
	"github.com/henomis/packtrail/invoker/asyncqueue"
)

type completion struct {
	execID     string
	node       string
	generation uint64
	attempt    int
	res        invoker.Result
}

// fakeCompleter records CompleteActivity calls and signals each on a channel.
type fakeCompleter struct {
	mu    sync.Mutex
	calls []completion
	ch    chan struct{}
}

func newFakeCompleter() *fakeCompleter { return &fakeCompleter{ch: make(chan struct{}, 8)} }

func (f *fakeCompleter) CompleteActivity(_ context.Context, execID, node string, attempt int, res invoker.Result) error {
	f.mu.Lock()
	f.calls = append(f.calls, completion{execID: execID, node: node, attempt: attempt, res: res})
	f.mu.Unlock()

	f.ch <- struct{}{}

	return nil
}

func (f *fakeCompleter) CompleteActivityWithGeneration(
	_ context.Context, execID, node string, generation uint64, attempt int, res invoker.Result,
) error {
	f.mu.Lock()
	f.calls = append(f.calls, completion{
		execID: execID, node: node, generation: generation, attempt: attempt, res: res,
	})
	f.mu.Unlock()

	f.ch <- struct{}{}

	return nil
}

func (f *fakeCompleter) snapshot() []completion {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]completion(nil), f.calls...)
}

func TestDispatcherPublishesPending(t *testing.T) {
	srv := natstest.Start(t)
	ctx := context.Background()

	const prefix, kind = "t", "echo"
	if err := asyncqueue.EnsureStream(ctx, srv.JS, prefix, kind); err != nil {
		t.Fatalf("ensure stream: %v", err)
	}

	d := asyncqueue.NewDispatcher(srv.JS, prefix, kind)

	res, err := d.Invoke(ctx, invoker.Request{
		ExecutionID: "e1", NodeID: "n1", Target: "agentA", Payload: []byte(`"hi"`),
	})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}

	if res.Status != invoker.StatusPending {
		t.Errorf("status: got %q, want pending", res.Status)
	}

	stream, err := srv.JS.Stream(ctx, asyncqueue.StreamName(prefix, kind))
	if err != nil {
		t.Fatalf("stream: %v", err)
	}

	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("info: %v", err)
	}

	if info.State.Msgs != 1 {
		t.Errorf("queued msgs: got %d, want 1", info.State.Msgs)
	}
}

func TestEnsureStreamBoundsQueueByDefault(t *testing.T) {
	srv := natstest.Start(t)
	ctx := context.Background()

	const prefix, kind = "t", "echo"
	if err := asyncqueue.EnsureStream(ctx, srv.JS, prefix, kind); err != nil {
		t.Fatalf("ensure stream: %v", err)
	}

	stream, err := srv.JS.Stream(ctx, asyncqueue.StreamName(prefix, kind))
	if err != nil {
		t.Fatalf("stream: %v", err)
	}

	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("info: %v", err)
	}

	if info.Config.MaxMsgs <= 0 {
		t.Fatalf("MaxMsgs = %d, want a bounded positive default", info.Config.MaxMsgs)
	}

	if info.Config.MaxBytes <= 0 {
		t.Fatalf("MaxBytes = %d, want a bounded positive default", info.Config.MaxBytes)
	}

	if info.Config.Discard != jetstream.DiscardNew {
		t.Fatalf("Discard = %v, want DiscardNew", info.Config.Discard)
	}
}

func TestDispatcherShedsWhenQueueFull(t *testing.T) {
	srv := natstest.Start(t)
	ctx := context.Background()

	const prefix, kind = "t", "echo"
	if err := asyncqueue.EnsureStream(ctx, srv.JS, prefix, kind, asyncqueue.WithMaxQueuedJobs(1)); err != nil {
		t.Fatalf("ensure stream: %v", err)
	}

	d := asyncqueue.NewDispatcher(srv.JS, prefix, kind)
	if _, err := d.Invoke(ctx, invoker.Request{ExecutionID: "e1", NodeID: "n1", Target: "agentA"}); err != nil {
		t.Fatalf("first invoke: %v", err)
	}

	if _, err := d.Invoke(ctx, invoker.Request{ExecutionID: "e2", NodeID: "n2", Target: "agentA"}); err == nil {
		t.Fatal("second invoke succeeded, want publish error while queue is full")
	}

	stream, err := srv.JS.Stream(ctx, asyncqueue.StreamName(prefix, kind))
	if err != nil {
		t.Fatalf("stream: %v", err)
	}

	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("info: %v", err)
	}

	if info.State.Msgs != 1 {
		t.Fatalf("queued msgs = %d, want 1 after shedding the second job", info.State.Msgs)
	}
}

func TestDispatcherDedupsSameAttempt(t *testing.T) {
	srv := natstest.Start(t)
	ctx := context.Background()

	const prefix, kind = "t", "echo"
	if err := asyncqueue.EnsureStream(ctx, srv.JS, prefix, kind); err != nil {
		t.Fatalf("ensure stream: %v", err)
	}

	d := asyncqueue.NewDispatcher(srv.JS, prefix, kind)

	req := invoker.Request{ExecutionID: "e1", NodeID: "n1", Generation: 1, Attempt: 0, Target: "agentA"}
	for range 2 {
		if _, err := d.Invoke(ctx, req); err != nil {
			t.Fatalf("invoke: %v", err)
		}
	}

	req.Generation = 2

	if _, err := d.Invoke(ctx, req); err != nil {
		t.Fatalf("invoke new generation: %v", err)
	}

	stream, err := srv.JS.Stream(ctx, asyncqueue.StreamName(prefix, kind))
	if err != nil {
		t.Fatalf("stream: %v", err)
	}

	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("info: %v", err)
	}

	// Same exec.node.generation.attempt collapses, but a new generation enqueues.
	if info.State.Msgs != 2 {
		t.Errorf("queued msgs after duplicate/new-generation dispatches: got %d, want 2", info.State.Msgs)
	}
}

func TestWorkerRunsExecAndCompletes(t *testing.T) {
	srv := natstest.Start(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const prefix, kind = "t", "echo"
	if err := asyncqueue.EnsureStream(ctx, srv.JS, prefix, kind); err != nil {
		t.Fatalf("ensure stream: %v", err)
	}

	var execCount atomic.Int64

	exec := invoker.Func(func(_ context.Context, req invoker.Request) (invoker.Result, error) {
		execCount.Add(1)
		// Echo the payload back, proving the request is threaded through.
		return invoker.Result{Status: invoker.StatusOK, Payload: req.Payload}, nil
	})

	completer := newFakeCompleter()
	w := asyncqueue.NewWorker(srv.JS, prefix, kind, exec, completer)

	go func() { _ = w.Run(ctx) }()

	d := asyncqueue.NewDispatcher(srv.JS, prefix, kind)
	if _, err := d.Invoke(ctx, invoker.Request{
		ExecutionID: "e1", NodeID: "n1", Generation: 7, Attempt: 0, Target: "agentA", Payload: []byte(`"hello"`),
	}); err != nil {
		t.Fatalf("invoke: %v", err)
	}

	select {
	case <-completer.ch:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for CompleteActivity")
	}

	if got := execCount.Load(); got != 1 {
		t.Errorf("exec invocations: got %d, want 1", got)
	}

	calls := completer.snapshot()
	if len(calls) != 1 {
		t.Fatalf("completions: got %d, want 1", len(calls))
	}

	c := calls[0]
	if c.execID != "e1" || c.node != "n1" || c.generation != 7 || c.attempt != 0 {
		t.Errorf(
			"completion ids: got (%q,%q,%d,%d), want (e1,n1,7,0)",
			c.execID, c.node, c.generation, c.attempt,
		)
	}

	if c.res.Status != invoker.StatusOK || string(c.res.Payload) != `"hello"` {
		t.Errorf("completion result: got (%q,%s), want (ok,\"hello\")", c.res.Status, c.res.Payload)
	}
}

func TestWorkerDoesNotPrefetchBeyondConcurrency(t *testing.T) {
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

	const concurrency = 3

	w := asyncqueue.NewWorker(srv.JS, prefix, kind, exec, newFakeCompleter(), asyncqueue.WithConcurrency(concurrency))

	go func() { _ = w.Run(ctx) }()

	consumer := prefix + "-async-" + kind + "-worker"
	deadline := time.Now().Add(5 * time.Second)

	for {
		cons, err := srv.JS.Consumer(ctx, asyncqueue.StreamName(prefix, kind), consumer)
		if err == nil {
			info, infoErr := cons.Info(ctx)
			if infoErr != nil {
				t.Fatalf("consumer info: %v", infoErr)
			}

			if info.Config.MaxAckPending != concurrency {
				t.Fatalf("MaxAckPending = %d, want %d", info.Config.MaxAckPending, concurrency)
			}

			return
		}

		if time.Now().After(deadline) {
			t.Fatalf("consumer %q was not created: %v", consumer, err)
		}

		time.Sleep(20 * time.Millisecond)
	}
}

func TestWorkerErrorMapsToRetry(t *testing.T) {
	srv := natstest.Start(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const prefix, kind = "t", "echo"
	if err := asyncqueue.EnsureStream(ctx, srv.JS, prefix, kind); err != nil {
		t.Fatalf("ensure stream: %v", err)
	}

	exec := invoker.Func(func(_ context.Context, _ invoker.Request) (invoker.Result, error) {
		return invoker.Result{}, context.DeadlineExceeded // transient transport-style fault
	})

	completer := newFakeCompleter()
	w := asyncqueue.NewWorker(srv.JS, prefix, kind, exec, completer)

	go func() { _ = w.Run(ctx) }()

	d := asyncqueue.NewDispatcher(srv.JS, prefix, kind)
	if _, err := d.Invoke(ctx, invoker.Request{ExecutionID: "e1", NodeID: "n1", Target: "agentA"}); err != nil {
		t.Fatalf("invoke: %v", err)
	}

	select {
	case <-completer.ch:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for CompleteActivity")
	}

	calls := completer.snapshot()
	if calls[0].res.Status != invoker.StatusRetry {
		t.Errorf("status: got %q, want retry", calls[0].res.Status)
	}
}

// observeCallBudget dispatches one job whose node deadline is now+nodeTimeout
// (nodeTimeout==0 means "no node deadline"; negative means an already-expired
// deadline) to a worker configured with the given activity-timeout backstop, and
// returns the call budget (ctx deadline) the embedder's Invoker actually
// received.
func observeCallBudget(t *testing.T, activityTimeout, nodeTimeout time.Duration) time.Duration {
	t.Helper()

	srv := natstest.Start(t)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	const prefix, kind = "t", "echo"
	if err := asyncqueue.EnsureStream(ctx, srv.JS, prefix, kind); err != nil {
		t.Fatalf("ensure stream: %v", err)
	}

	budget := make(chan time.Duration, 1)
	exec := invoker.Func(func(callCtx context.Context, _ invoker.Request) (invoker.Result, error) {
		dl, ok := callCtx.Deadline()
		if !ok {
			budget <- 0
		} else {
			budget <- time.Until(dl)
		}

		return invoker.Result{Status: invoker.StatusOK}, nil
	})

	w := asyncqueue.NewWorker(srv.JS, prefix, kind, exec, newFakeCompleter(),
		asyncqueue.WithActivityTimeout(activityTimeout))

	go func() { _ = w.Run(ctx) }()

	req := invoker.Request{ExecutionID: "e1", NodeID: "n1", Target: "agentA"}
	if nodeTimeout != 0 {
		req.Deadline = time.Now().Add(nodeTimeout)
	}

	if _, err := asyncqueue.NewDispatcher(srv.JS, prefix, kind).Invoke(ctx, req); err != nil {
		t.Fatalf("invoke: %v", err)
	}

	select {
	case d := <-budget:
		return d
	case <-time.After(5 * time.Second):
		t.Fatal("exec never ran")
		return 0
	}
}

// TestWorkerHonorsNodeTimeout is the §4b fix: a per-node timeout shorter than the
// worker's activityTimeout backstop must bound the actual invocation (previously
// it was silently widened to the backstop).
func TestWorkerHonorsNodeTimeout(t *testing.T) {
	// Large backstop, small node timeout: the call must reflect the node timeout.
	got := observeCallBudget(t, time.Hour, 200*time.Millisecond)
	if got <= 0 || got > 30*time.Second {
		t.Fatalf("call budget = %v, want ~200ms (the node timeout, not the 1h backstop)", got)
	}
}

// TestWorkerActivityTimeoutCaps verifies the backstop relationship in both
// directions: a node timeout longer than the backstop is capped at it, and a
// node with no timeout runs at the full backstop.
func TestWorkerActivityTimeoutCaps(t *testing.T) {
	// Node timeout (1h) longer than the 200ms backstop → capped at the backstop.
	if got := observeCallBudget(t, 200*time.Millisecond, time.Hour); got <= 0 || got > 30*time.Second {
		t.Fatalf("capped budget = %v, want ~200ms (the activityTimeout backstop)", got)
	}

	// No node timeout → the full backstop applies.
	if got := observeCallBudget(t, 90*time.Second, 0); got < 30*time.Second {
		t.Fatalf("budget without node timeout = %v, want ~90s (the activityTimeout)", got)
	}
}

// TestWorkerExpiredDeadlineFailsFast: a job dispatched with an already-expired
// node deadline must get a token budget that fails the call immediately — not
// collapse to 0 ("no node timeout") and run at the full activityTimeout
// backstop, which would widen the budget exactly when it is exhausted.
func TestWorkerExpiredDeadlineFailsFast(t *testing.T) {
	if got := observeCallBudget(t, time.Hour, -time.Second); got > time.Second {
		t.Fatalf("budget for expired deadline = %v, want ~0 (fail fast), not the 1h backstop", got)
	}
}
