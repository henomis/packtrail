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
	"encoding/json"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/internal/natstest"
)

const oneTaskFlow = `
version: "1.0"
name: one
nodes:
  - {id: a, type: task, invoker: custom, target: agent-a}
edges: []
`

// okInvoker echoes a fixed payload and reports success.
func okInvoker() packtrail.InvokerFunc {
	return func(_ context.Context, _ packtrail.Request) (packtrail.Result, error) {
		return packtrail.Result{Status: packtrail.StatusOK, Payload: []byte(`{"done":true}`)}, nil
	}
}

// poll runs cond until it returns true or the deadline passes.
func poll(t *testing.T, d time.Duration, cond func() bool) bool {
	t.Helper()

	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}

		time.Sleep(20 * time.Millisecond)
	}

	return false
}

// TestWithFlowsDir loads flow definitions from a directory on disk.
func TestWithFlowsDir(t *testing.T) {
	srv := natstest.Start(t)

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "one.yaml"), []byte(oneTaskFlow), 0o600); err != nil {
		t.Fatalf("write flow: %v", err)
	}

	s, err := packtrail.New(srv.NC, packtrail.WithNamespace("fdir"), packtrail.WithFlowsDir(dir))
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	flows := s.Flows()
	if len(flows) != 1 || flows[0] != "one" {
		t.Fatalf("Flows() = %v, want [one]", flows)
	}
}

// TestObservabilityMethods runs a flow to completion and exercises every
// read-side Server method, plus the engine-tuning options passed to New.
func TestObservabilityMethods(t *testing.T) {
	srv := natstest.Start(t)

	s, err := packtrail.New(srv.NC,
		packtrail.WithNamespace("obs3"),
		packtrail.WithFlow([]byte(oneTaskFlow)),
		packtrail.WithInvoker("custom", okInvoker()),
		packtrail.WithOwnerID("inst-1"),
		packtrail.WithLeaseTTL(15*time.Second),
		packtrail.WithMaxConcurrency(8),
		packtrail.WithDefaultTimeout(5*time.Second),
		packtrail.WithReconcileActive("* * * * * *"),
		packtrail.WithReconcileFull("* * * * * *"),
	)
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = s.Run(ctx) }()

	id, err := s.Start(ctx, "one", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	if ok := poll(t, 10*time.Second, func() bool {
		ex, getErr := s.Get(ctx, id)
		return getErr == nil && ex.Status == packtrail.ExecCompleted
	}); !ok {
		t.Fatal("execution did not complete")
	}

	// List is authoritative and must contain the execution immediately.
	ids, err := s.List(ctx)
	if err != nil || len(ids) != 1 || ids[0] != id {
		t.Fatalf("List = %v, %v; want [%s]", ids, err, id)
	}

	// The visibility indexes are eventually consistent: poll them.
	if ok := poll(t, 10*time.Second, func() bool {
		byStatus, _ := s.ByStatus(ctx, packtrail.ExecCompleted)
		return len(byStatus) == 1 && byStatus[0] == id
	}); !ok {
		t.Fatal("ByStatus did not index the completed execution")
	}

	byFlow, err := s.ByFlow(ctx, "one")
	if err != nil || len(byFlow) != 1 || byFlow[0] != id {
		t.Fatalf("ByFlow = %v, %v; want [%s]", byFlow, err, id)
	}

	statusEvents, err := s.ByStatusEvents(ctx, packtrail.ExecCompleted)
	if err != nil || len(statusEvents) != 1 || statusEvents[0].ExecID != id {
		t.Fatalf("ByStatusEvents = %v, %v; want one event for %s", statusEvents, err, id)
	}

	flowEvents, err := s.ByFlowEvents(ctx, "one")
	if err != nil || len(flowEvents) != 1 || flowEvents[0].ExecID != id {
		t.Fatalf("ByFlowEvents = %v, %v; want one event for %s", flowEvents, err, id)
	}

	// Reconcile rebuilds the indexes from the source of truth without error.
	if reconcileErr := s.Reconcile(ctx); reconcileErr != nil {
		t.Fatalf("Reconcile: %v", reconcileErr)
	}
}

// TestGetNotFound verifies Server.Get surfaces ErrNotFound for a missing id.
func TestGetNotFound(t *testing.T) {
	srv := natstest.Start(t)

	s, err := packtrail.New(srv.NC, packtrail.WithNamespace("gnf"), packtrail.WithFlow([]byte(oneTaskFlow)))
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	if _, getErr := s.Get(context.Background(), "missing"); getErr != packtrail.ErrNotFound {
		t.Fatalf("Get(missing) err = %v, want ErrNotFound", getErr)
	}
}

// TestDuplicateFlowName verifies New rejects two flows sharing a name.
func TestDuplicateFlowName(t *testing.T) {
	srv := natstest.Start(t)

	_, err := packtrail.New(srv.NC,
		packtrail.WithNamespace("dup"),
		packtrail.WithFlow([]byte(oneTaskFlow)),
		packtrail.WithFlow([]byte(oneTaskFlow)),
	)
	if err == nil {
		t.Fatal("New with duplicate flow names succeeded, want error")
	}
}

// TestListFlowsEmpty verifies ListFlows returns nothing when no flows are
// registered (the ErrNoKeysFound path).
func TestListFlowsEmpty(t *testing.T) {
	srv := natstest.Start(t)

	s, err := packtrail.New(srv.NC, packtrail.WithNamespace("empty"))
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	names, err := s.ListFlows(context.Background())
	if err != nil {
		t.Fatalf("ListFlows: %v", err)
	}

	if len(names) != 0 {
		t.Fatalf("ListFlows = %v, want empty", names)
	}
}

// TestServerSignal drives a signal node to resolution via Server.Signal.
func TestServerSignal(t *testing.T) {
	const sigFlow = `
version: "1.0"
name: sigflow
nodes:
  - {id: gate, type: signal, signal_name: approval, timeout: 24h}
  - {id: done, type: task, invoker: custom, target: t}
edges:
  - {from: gate, to: done}
`

	srv := natstest.Start(t)

	s, err := packtrail.New(srv.NC,
		packtrail.WithNamespace("sig"),
		packtrail.WithFlow([]byte(sigFlow)),
		packtrail.WithInvoker("custom", okInvoker()),
	)
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = s.Run(ctx) }()

	id, err := s.Start(ctx, "sigflow", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	// Wait until the execution parks on the signal node.
	if ok := poll(t, 10*time.Second, func() bool {
		ex, getErr := s.Get(ctx, id)
		return getErr == nil && ex.Status == packtrail.ExecWaiting
	}); !ok {
		t.Fatal("execution did not reach waiting state")
	}

	if signalErr := s.Signal(ctx, id, "approval", json.RawMessage(`{"approved":true}`)); signalErr != nil {
		t.Fatalf("signal: %v", signalErr)
	}

	if ok := poll(t, 10*time.Second, func() bool {
		ex, getErr := s.Get(ctx, id)
		return getErr == nil && ex.Status == packtrail.ExecCompleted
	}); !ok {
		t.Fatal("execution did not complete after signal")
	}
}

// TestServerCompleteActivity drives an async (StatusPending) node settled via
// Server.CompleteActivity.
func TestServerCompleteActivity(t *testing.T) {
	srv := natstest.Start(t)

	pending := packtrail.InvokerFunc(func(_ context.Context, _ packtrail.Request) (packtrail.Result, error) {
		return packtrail.Result{Status: packtrail.StatusPending}, nil
	})

	s, err := packtrail.New(srv.NC,
		packtrail.WithNamespace("async"),
		packtrail.WithFlow([]byte(oneTaskFlow)),
		packtrail.WithInvoker("custom", pending),
	)
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = s.Run(ctx) }()

	id, err := s.Start(ctx, "one", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	if ok := poll(t, 10*time.Second, func() bool {
		ex, getErr := s.Get(ctx, id)
		return getErr == nil && ex.Status == packtrail.ExecWaiting
	}); !ok {
		t.Fatal("execution did not park as waiting")
	}

	if completeErr := s.CompleteActivity(ctx, id, "a", 0,
		packtrail.Result{Status: packtrail.StatusOK, Payload: json.RawMessage(`{"settled":true}`)}); completeErr != nil {
		t.Fatalf("complete activity: %v", completeErr)
	}

	if ok := poll(t, 10*time.Second, func() bool {
		ex, getErr := s.Get(ctx, id)
		return getErr == nil && ex.Status == packtrail.ExecCompleted
	}); !ok {
		t.Fatal("execution did not complete after CompleteActivity")
	}
}

// TestServerResume revives a failed execution: the node fails on its first run
// and succeeds after Resume gives it a fresh retry budget.
func TestServerResume(t *testing.T) {
	srv := natstest.Start(t)

	var calls atomic.Int32

	flaky := packtrail.InvokerFunc(func(_ context.Context, _ packtrail.Request) (packtrail.Result, error) {
		if calls.Add(1) == 1 {
			return packtrail.Result{Status: packtrail.StatusError, Error: "boom"}, nil
		}

		return packtrail.Result{Status: packtrail.StatusOK, Payload: []byte(`{"ok":true}`)}, nil
	})

	s, err := packtrail.New(srv.NC,
		packtrail.WithNamespace("resume"),
		packtrail.WithFlow([]byte(oneTaskFlow)),
		packtrail.WithInvoker("custom", flaky),
	)
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = s.Run(ctx) }()

	id, err := s.Start(ctx, "one", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	if ok := poll(t, 10*time.Second, func() bool {
		ex, getErr := s.Get(ctx, id)
		return getErr == nil && ex.Status == packtrail.ExecFailed
	}); !ok {
		t.Fatal("execution did not fail")
	}

	if resumeErr := s.Resume(ctx, id); resumeErr != nil {
		t.Fatalf("resume: %v", resumeErr)
	}

	if ok := poll(t, 10*time.Second, func() bool {
		ex, getErr := s.Get(ctx, id)
		return getErr == nil && ex.Status == packtrail.ExecCompleted
	}); !ok {
		t.Fatal("execution did not complete after Resume")
	}
}

// TestServerScheduleFlow installs a recurring schedule that starts the flow and
// verifies at least one execution is created.
func TestServerScheduleFlow(t *testing.T) {
	srv := natstest.Start(t)

	s, err := packtrail.New(srv.NC,
		packtrail.WithNamespace("cron"),
		packtrail.WithFlow([]byte(oneTaskFlow)),
		packtrail.WithInvoker("custom", okInvoker()),
	)
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = s.Run(ctx) }()

	if scheduleErr := s.ScheduleFlow(ctx, "every-sec", "one", "* * * * * *", nil); scheduleErr != nil {
		t.Fatalf("schedule flow: %v", scheduleErr)
	}

	if ok := poll(t, 15*time.Second, func() bool {
		ids, _ := s.List(ctx)
		return len(ids) >= 1
	}); !ok {
		t.Fatal("scheduled flow did not start any execution")
	}
}
