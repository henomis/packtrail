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

	"github.com/henomis/packtrail/internal/natstest"
	"github.com/henomis/packtrail/invoker"
	"github.com/henomis/packtrail/invoker/asyncqueue"
)

type completion struct {
	execID  string
	node    string
	attempt int
	res     invoker.Result
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
	f.calls = append(f.calls, completion{execID, node, attempt, res})
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

func TestDispatcherDedupsSameAttempt(t *testing.T) {
	srv := natstest.Start(t)
	ctx := context.Background()

	const prefix, kind = "t", "echo"
	if err := asyncqueue.EnsureStream(ctx, srv.JS, prefix, kind); err != nil {
		t.Fatalf("ensure stream: %v", err)
	}

	d := asyncqueue.NewDispatcher(srv.JS, prefix, kind)

	req := invoker.Request{ExecutionID: "e1", NodeID: "n1", Attempt: 0, Target: "agentA"}
	for range 2 {
		if _, err := d.Invoke(ctx, req); err != nil {
			t.Fatalf("invoke: %v", err)
		}
	}

	stream, err := srv.JS.Stream(ctx, asyncqueue.StreamName(prefix, kind))
	if err != nil {
		t.Fatalf("stream: %v", err)
	}

	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("info: %v", err)
	}

	// Same exec.node.attempt collapses to a single enqueued job.
	if info.State.Msgs != 1 {
		t.Errorf("queued msgs after duplicate dispatch: got %d, want 1", info.State.Msgs)
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
		ExecutionID: "e1", NodeID: "n1", Attempt: 0, Target: "agentA", Payload: []byte(`"hello"`),
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
	if c.execID != "e1" || c.node != "n1" || c.attempt != 0 {
		t.Errorf("completion ids: got (%q,%q,%d), want (e1,n1,0)", c.execID, c.node, c.attempt)
	}

	if c.res.Status != invoker.StatusOK || string(c.res.Payload) != `"hello"` {
		t.Errorf("completion result: got (%q,%s), want (ok,\"hello\")", c.res.Status, c.res.Payload)
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
