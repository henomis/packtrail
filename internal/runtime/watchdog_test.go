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
	"testing"
	"time"

	"github.com/henomis/packtrail/internal/dsl"
	"github.com/henomis/packtrail/internal/names"
	"github.com/henomis/packtrail/internal/natstest"
	"github.com/henomis/packtrail/internal/scheduler"
	"github.com/henomis/packtrail/internal/store"
	"github.com/henomis/packtrail/invoker"
)

// watchdogHarness builds a store, engine (running) and the two-flow set the
// watchdog tests manufacture stranded documents against.
func watchdogHarness(t *testing.T, inv invoker.Invoker) (*store.Store, *Engine) {
	t.Helper()

	ctx := context.Background()
	srv := natstest.Start(t)

	st, err := store.Open(ctx, srv.JS, names.New(""))
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	sch := scheduler.New(srv.JS, names.New(""))
	if err = sch.EnsureStream(ctx); err != nil {
		t.Fatalf("scheduler: %v", err)
	}

	flows := map[string]*dsl.Flow{}

	for _, y := range []string{signalRedriveFlow, redriveFlow} {
		flow, parseErr := dsl.Parse([]byte(y))
		if parseErr != nil {
			t.Fatalf("flow: %v", parseErr)
		}

		flows[flow.Name] = flow
	}

	eng, err := New(inv, st, sch, testSignals(t, st), flows, Config{})
	if err != nil {
		t.Fatalf("engine: %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)

	go func() { _ = eng.Run(runCtx) }()

	return st, eng
}

const staleEnough = time.Millisecond

// TestRedriveStalledRunning: a running execution whose driving work item was
// lost (any commit-then-enqueue crash window) must be re-driven to completion
// by the watchdog.
func TestRedriveStalledRunning(t *testing.T) {
	ctx := context.Background()

	inv := invoker.Func(func(context.Context, invoker.Request) (invoker.Result, error) {
		return invoker.Result{Status: invoker.StatusOK}, nil
	})

	st, eng := watchdogHarness(t, inv)

	exec := &store.Execution{
		ID: "stall-run", FlowName: "sig-redrive",
		Status: store.StatusRunning, CurrentNode: "work",
	}
	if _, err := st.Create(ctx, exec); err != nil {
		t.Fatalf("create: %v", err)
	}

	time.Sleep(20 * time.Millisecond) // age past the tiny threshold

	redriven, err := eng.RedriveStalled(ctx, "stall-run", staleEnough)
	if err != nil {
		t.Fatalf("redrive: %v", err)
	}

	if !redriven {
		t.Fatal("stalled running execution was not re-driven")
	}

	waitStatus(t, st, "stall-run", store.StatusCompleted)
}

// TestRedriveStalledFanin: a fanin wait whose eval was lost (all branches
// settled, no fanin_eval in flight) must be re-driven to completion.
func TestRedriveStalledFanin(t *testing.T) {
	ctx := context.Background()

	inv := invoker.Func(func(context.Context, invoker.Request) (invoker.Result, error) {
		t.Error("invoker called: all branches are already settled")

		return invoker.Result{Status: invoker.StatusError, Error: "unexpected"}, nil
	})

	st, eng := watchdogHarness(t, inv)

	exec := &store.Execution{
		ID: "stall-fan", FlowName: "redrive",
		Status: store.StatusWaiting, CurrentNode: "join",

		Branches: map[string]store.BranchState{
			"b1": {NodeID: "b1", Status: store.BranchCompleted},
			"b2": {NodeID: "b2", Status: store.BranchCompleted},
		},
	}
	if _, err := st.Create(ctx, exec); err != nil {
		t.Fatalf("create: %v", err)
	}

	time.Sleep(20 * time.Millisecond)

	redriven, err := eng.RedriveStalled(ctx, "stall-fan", staleEnough)
	if err != nil {
		t.Fatalf("redrive: %v", err)
	}

	if !redriven {
		t.Fatal("stalled fanin wait was not re-driven")
	}

	waitStatus(t, st, "stall-fan", store.StatusCompleted)
}

// TestRedriveStalledSkips: the watchdog must not touch executions that are
// merely quiet for a legitimate reason.
func TestRedriveStalledSkips(t *testing.T) {
	ctx := context.Background()

	inv := invoker.Func(func(context.Context, invoker.Request) (invoker.Result, error) {
		t.Error("invoker called: no skip case may dispatch work")

		return invoker.Result{Status: invoker.StatusError, Error: "unexpected"}, nil
	})

	st, eng := watchdogHarness(t, inv)

	docs := []*store.Execution{
		// Fresh: below the threshold (checked with a huge olderThan).
		{ID: "skip-fresh", FlowName: "sig-redrive", Status: store.StatusRunning, CurrentNode: "work"},
		// Signal wait: the wait timeout owns it.
		{ID: "skip-signal", FlowName: "sig-redrive", Status: store.StatusWaiting, CurrentNode: "gate", WaitSignal: "go"},
		// Async task wait: CompleteActivity owns it (waiting, no WaitSignal, not a fanin).
		{ID: "skip-async", FlowName: "sig-redrive", Status: store.StatusWaiting, CurrentNode: "work"},
		// Scheduled retry backoff still pending: quiet is intentional.
		{
			ID: "skip-backoff", FlowName: "sig-redrive", Status: store.StatusRunning, CurrentNode: "work", Attempt: 1,
			RetryAt: time.Now().Add(time.Hour).UTC(),
		},
		// Terminal: nothing to drive.
		{ID: "skip-done", FlowName: "sig-redrive", Status: store.StatusCompleted},
		// Lease held: an instance is actively processing it right now.
		{ID: "skip-leased", FlowName: "sig-redrive", Status: store.StatusRunning, CurrentNode: "work"},
	}

	for _, d := range docs {
		if _, err := st.Create(ctx, d); err != nil {
			t.Fatalf("create %s: %v", d.ID, err)
		}
	}

	if held, err := st.AcquireLease(ctx, "skip-leased", "another-instance", time.Minute); err != nil || !held {
		t.Fatalf("acquire lease: held=%v err=%v", held, err)
	}

	time.Sleep(20 * time.Millisecond)

	thresholds := map[string]time.Duration{"skip-fresh": time.Hour}

	for _, d := range docs {
		olderThan := staleEnough
		if t2, ok := thresholds[d.ID]; ok {
			olderThan = t2
		}

		redriven, err := eng.RedriveStalled(ctx, d.ID, olderThan)
		if err != nil {
			t.Fatalf("redrive %s: %v", d.ID, err)
		}

		if redriven {
			t.Errorf("%s was re-driven, want skipped", d.ID)
		}
	}

	// Give any wrongly-enqueued work a moment to reach the invoker (which
	// t.Errors) before the test ends.
	time.Sleep(200 * time.Millisecond)
}

// waitStatus polls until the execution reaches the wanted status or fails.
func waitStatus(t *testing.T, st *store.Store, id, want string) {
	t.Helper()

	ctx := context.Background()
	deadline := time.Now().Add(5 * time.Second)

	for time.Now().Before(deadline) {
		ex, err := st.Get(ctx, id)
		if err == nil && ex.Status == want {
			return
		}

		time.Sleep(15 * time.Millisecond)
	}

	ex, _ := st.Get(ctx, id)
	t.Fatalf("execution %s: status=%q node=%q, want %q", id, ex.Status, ex.CurrentNode, want)
}
