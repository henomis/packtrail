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
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail/internal/dsl"
	"github.com/henomis/packtrail/internal/names"
	"github.com/henomis/packtrail/internal/natstest"
	"github.com/henomis/packtrail/internal/scheduler"
	"github.com/henomis/packtrail/internal/signal"
	"github.com/henomis/packtrail/internal/store"
	"github.com/henomis/packtrail/invoker"
)

const signalRedriveFlow = `
version: "1.0"
name: sig-redrive
nodes:
  - {id: gate, type: signal, signal_name: go}
  - {id: work, type: task, subject: "x"}
edges:
  - {from: gate, to: work}
`

// signalHarness builds a store, scheduler, flow and engine for the two
// R2 signal-path tests.
func signalHarness(t *testing.T, inv invoker.Invoker) (*store.Store, *Engine) {
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

	flow, err := dsl.Parse([]byte(signalRedriveFlow))
	if err != nil {
		t.Fatalf("flow: %v", err)
	}

	eng, err := New(inv, st, sch, testSignals(t, st), map[string]*dsl.Flow{flow.Name: flow}, Config{})
	if err != nil {
		t.Fatalf("engine: %v", err)
	}

	return st, eng
}

// TestApplySignalDuplicateRedrivesRunning covers the crash window in
// guardedAdvance on the signal-arrival path: the advance out of the signal wait
// committed — with its work item in the outbox (transactional outbox) — but the
// flush never ran, and the signal delivery Nak'd. Its redelivery hits the
// duplicate-sequence skip, which must re-flush the pending outbox instead of
// acking the execution into a permanent stall.
//
// The test manufactures the post-crash state directly — running at the
// successor with the signal's sequence recorded and the advance committed in
// the outbox, unpublished — and feeds applySignal the duplicate delivery.
func TestApplySignalDuplicateRedrivesRunning(t *testing.T) {
	ctx := context.Background()

	inv := invoker.Func(func(context.Context, invoker.Request) (invoker.Result, error) {
		return invoker.Result{Status: invoker.StatusOK, Payload: json.RawMessage(`{"done":true}`)}, nil
	})

	st, eng := signalHarness(t, inv)

	runCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)

	go func() { _ = eng.Run(runCtx) }()

	exec := &store.Execution{
		ID: "sig-redrive-1", FlowName: "sig-redrive",
		Status: store.StatusRunning, CurrentNode: "work",

		LastSeq: map[string]uint64{"go": 7},
	}

	advItem, err := json.Marshal(workItem{ExecID: "sig-redrive-1", Kind: kindAdvance})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	exec.AppendWork(advItem)

	if _, err = st.Create(ctx, exec); err != nil {
		t.Fatalf("create: %v", err)
	}

	// The redelivered (duplicate) signal.
	if err = eng.applySignal(ctx, signal.Delivery{ExecID: "sig-redrive-1", Name: "go", Seq: 7}); err != nil {
		t.Fatalf("apply signal: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ex, getErr := st.Get(ctx, "sig-redrive-1")
		if getErr == nil && ex.Status == store.StatusCompleted {
			return
		}

		time.Sleep(15 * time.Millisecond)
	}

	ex, _ := st.Get(ctx, "sig-redrive-1")
	t.Fatalf("execution stranded: status=%q node=%q (duplicate signal did not re-drive the running execution)",
		ex.Status, ex.CurrentNode)
}

// failPublishJS wraps a JetStream handle and fails every Publish, simulating a
// broken work-stream publish while KV writes still succeed.
type failPublishJS struct {
	jetstream.JetStream
}

func (f *failPublishJS) Publish(context.Context, string, []byte, ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	return nil, errors.New("injected publish failure")
}

// TestGuardedAdvanceFlushFailureLeavesDurableOutbox verifies the outbox
// contract on the signal/timeout advance path: when the publish path is broken,
// guardedAdvance still commits the transition WITH its follow-on work item in
// the outbox and surfaces the flush error (so the delivery Naks). The advance
// is never lost — once publishing works again, a re-flush completes the flow.
func TestGuardedAdvanceFlushFailureLeavesDurableOutbox(t *testing.T) {
	ctx := context.Background()

	inv := invoker.Func(func(context.Context, invoker.Request) (invoker.Result, error) {
		return invoker.Result{Status: invoker.StatusOK, Payload: json.RawMessage(`{"done":true}`)}, nil
	})

	st, eng := signalHarness(t, inv)

	exec := &store.Execution{
		ID: "sig-flushfail-1", FlowName: "sig-redrive",
		Status: store.StatusWaiting, CurrentNode: "gate", WaitSignal: "go",

		Signals: map[string]bool{"go": true},
	}
	if _, err := st.Create(ctx, exec); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Break the publish path: the commit lands, the flush cannot. (The engine
	// is not consuming yet, so nothing else touches eng.js concurrently.)
	realJS := eng.js
	eng.js = &failPublishJS{JetStream: realJS}

	if err := eng.guardedAdvance(ctx, "sig-flushfail-1", "gate", 0, "go", "work"); err == nil {
		t.Fatal("guardedAdvance with broken publish returned nil, want the flush error surfaced")
	}

	ex, err := st.Get(ctx, "sig-flushfail-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if ex.Status != store.StatusRunning || ex.CurrentNode != "work" {
		t.Fatalf("state = %s@%s, want running@work (the transition must commit regardless)", ex.Status, ex.CurrentNode)
	}

	if len(ex.Outbox) != 1 {
		t.Fatalf("outbox len = %d, want 1 (the advance must survive the failed flush durably)", len(ex.Outbox))
	}

	// Publishing recovers; a re-flush (here via the stall watchdog's primary
	// rule) drives the flow to completion.
	eng.js = realJS

	runCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)

	go func() { _ = eng.Run(runCtx) }()

	redriven, err := eng.RedriveStalled(ctx, "sig-flushfail-1", time.Nanosecond)
	if err != nil || !redriven {
		t.Fatalf("redrive: redriven=%v err=%v, want true/nil", redriven, err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cur, getErr := st.Get(ctx, "sig-flushfail-1"); getErr == nil && cur.Status == store.StatusCompleted {
			return
		}

		time.Sleep(15 * time.Millisecond)
	}

	ex, _ = st.Get(ctx, "sig-flushfail-1")
	t.Fatalf("execution stranded after re-flush: status=%q node=%q", ex.Status, ex.CurrentNode)
}

func TestGuardedAdvanceRejectsStaleGeneration(t *testing.T) {
	ctx := context.Background()
	st, eng := signalHarness(t, invoker.Func(func(_ context.Context, req invoker.Request) (invoker.Result, error) {
		return invoker.Result{Status: invoker.StatusOK, Payload: req.Payload}, nil
	}))

	exec := &store.Execution{
		ID:             "sig-stale-generation",
		FlowName:       "sig-redrive",
		Status:         store.StatusWaiting,
		CurrentNode:    "gate",
		NodeGeneration: 2,
		WaitSignal:     "go",
	}
	if _, err := st.Create(ctx, exec); err != nil {
		t.Fatalf("create: %v", err)
	}

	if err := eng.guardedAdvance(ctx, exec.ID, "gate", 1, "go", "work"); err != nil {
		t.Fatalf("stale guardedAdvance: %v", err)
	}

	afterStale, err := st.Get(ctx, exec.ID)
	if err != nil {
		t.Fatalf("get stale: %v", err)
	}

	if afterStale.Status != store.StatusWaiting ||
		afterStale.CurrentNode != "gate" ||
		afterStale.NodeGeneration != 2 ||
		afterStale.WaitSignal != "go" {
		t.Fatalf("after stale guardedAdvance = %+v, want unchanged signal wait", afterStale)
	}

	if err = eng.guardedAdvance(ctx, exec.ID, "gate", 2, "go", "work"); err != nil {
		t.Fatalf("fresh guardedAdvance: %v", err)
	}

	afterFresh, err := st.Get(ctx, exec.ID)
	if err != nil {
		t.Fatalf("get fresh: %v", err)
	}

	if afterFresh.Status != store.StatusRunning ||
		afterFresh.CurrentNode != "work" ||
		afterFresh.NodeGeneration != 3 ||
		afterFresh.WaitSignal != "" {
		t.Fatalf("after fresh guardedAdvance = %+v, want running work generation 3", afterFresh)
	}
}
