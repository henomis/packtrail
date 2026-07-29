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

package packtrail_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/internal/natstest"
	"github.com/henomis/packtrail/invoker"
)

const parkedFlow = `
version: "1.0"
name: parked
nodes:
  - {id: a, type: task, invoker: slowkind, target: worker-a}
edges: []
`

// blockingInvoker never returns, so the execution stays parked at its async node
// exactly as one waiting on a long-running activity does.
type blockingInvoker struct{ entered chan struct{} }

func (b *blockingInvoker) Invoke(ctx context.Context, _ invoker.Request) (invoker.Result, error) {
	select {
	case b.entered <- struct{}{}:
	default:
	}

	<-ctx.Done()

	return invoker.Result{}, ctx.Err()
}

// parkedExecution starts a flow whose only node is an async activity that never
// settles, and returns the server and the execution id once it is parked.
func parkedExecution(ctx context.Context, t *testing.T, ns string) (*packtrail.Server, string) {
	t.Helper()

	srv := natstest.Start(t)
	inv := &blockingInvoker{entered: make(chan struct{}, 1)}

	s, err := packtrail.New(srv.NC,
		packtrail.WithNamespace(ns),
		packtrail.WithFlow([]byte(parkedFlow)),
		packtrail.WithAsyncInvoker("slowkind", inv),
	)
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	go func() { _ = s.Run(ctx) }()

	id, err := s.Start(ctx, "parked", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	select {
	case <-inv.entered:
	case <-time.After(15 * time.Second):
		t.Fatal("the async activity was never invoked")
	}

	waitExecStatus(ctx, t, s, id, packtrail.ExecWaiting)

	return s, id
}

func waitExecStatus(ctx context.Context, t *testing.T, s *packtrail.Server, id, want string) {
	t.Helper()

	deadline := time.Now().Add(15 * time.Second)

	for time.Now().Before(deadline) {
		ex, err := s.Get(ctx, id)
		if err != nil {
			t.Fatalf("get %s: %v", id, err)
		}

		if ex.Status == want {
			return
		}

		time.Sleep(50 * time.Millisecond)
	}

	t.Fatalf("execution %s never reached %q", id, want)
}

// TestFailActivitySettlesAParkedNode: an execution parked on an activity that
// will never be completed can be settled, and the result is *failed* — which is
// resumable — rather than cancelled, which is not.
//
// Before this, such an execution had no terminal transition available to it at
// all: the stall watchdog excludes async waits (a legitimate one may run
// arbitrarily long), Resume applies only to failed executions, so Cancel was the
// only way out and it discarded the work.
func TestFailActivitySettlesAParkedNode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, id := parkedExecution(ctx, t, "fa1")

	// Generation 1, attempt 0: the node's first visit.
	if err := s.FailActivity(ctx, id, "a", 1, 0, "worker gave up: job dead-lettered"); err != nil {
		t.Fatalf("fail activity: %v", err)
	}

	waitExecStatus(ctx, t, s, id, packtrail.ExecFailed)

	ex, err := s.Get(ctx, id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if !strings.Contains(ex.Error, "dead-lettered") {
		t.Errorf("failure reason = %q, want it to carry the reason the job was dropped", ex.Error)
	}

	// Failed, therefore resumable — the whole point of failing rather than
	// cancelling.
	if err = s.Resume(ctx, id); err != nil {
		t.Errorf("a failed execution must be resumable: %v", err)
	}
}

// TestFailActivityIgnoresStaleVisits: a drop belonging to an earlier visit of a
// node the flow legally cycles through must not fail the visit in flight.
func TestFailActivityIgnoresStaleVisits(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, id := parkedExecution(ctx, t, "fa2")

	// A generation the execution is not at, and an attempt it is not on.
	for _, stale := range []struct {
		generation uint64
		attempt    int
	}{
		{generation: 99, attempt: 0},
		{generation: 1, attempt: 7},
	} {
		if err := s.FailActivity(ctx, id, "a", stale.generation, stale.attempt, "stale"); err != nil {
			t.Fatalf("stale FailActivity(gen=%d, attempt=%d) returned an error, want a silent no-op: %v",
				stale.generation, stale.attempt, err)
		}
	}

	// Wrong node, right generation.
	if err := s.FailActivity(ctx, id, "not-a-node", 1, 0, "stale"); err != nil {
		t.Fatalf("FailActivity for another node returned an error, want a no-op: %v", err)
	}

	ex, err := s.Get(ctx, id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if ex.Status != packtrail.ExecWaiting {
		t.Fatalf("status = %q after stale FailActivity calls, want it still waiting", ex.Status)
	}
}

// TestFailActivityOnTerminalExecutionIsANoOp: dropping a job for an execution
// that already finished is normal (the completion raced a Cancel), not an error.
func TestFailActivityOnTerminalExecutionIsANoOp(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, id := parkedExecution(ctx, t, "fa3")

	if err := s.Cancel(ctx, id, "operator stopped it"); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	if err := s.FailActivity(ctx, id, "a", 1, 0, "worker gave up"); err != nil {
		t.Fatalf("FailActivity on a cancelled execution: %v", err)
	}

	ex, err := s.Get(ctx, id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if ex.Status != packtrail.ExecCancelled {
		t.Fatalf("status = %q, want the cancellation preserved", ex.Status)
	}
}

// TestFailActivityUnknownExecution: an id that names nothing is not an error —
// the execution may have been archived out from under a slow worker.
func TestFailActivityUnknownExecution(t *testing.T) {
	srv := natstest.Start(t)

	s, err := packtrail.New(srv.NC,
		packtrail.WithNamespace("fa4"),
		packtrail.WithFlow([]byte(parkedFlow)),
		packtrail.WithAsyncInvoker("slowkind", &blockingInvoker{entered: make(chan struct{}, 1)}),
	)
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	err = s.FailActivity(context.Background(), "exec-does-not-exist", "a", 1, 0, "gone")
	if err != nil && !strings.Contains(err.Error(), "not found") {
		t.Fatalf("FailActivity on an unknown execution = %v, want nil or not-found", err)
	}
}
