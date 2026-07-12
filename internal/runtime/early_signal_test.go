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
	"testing"
	"time"

	"github.com/henomis/packtrail/internal/store"
	"github.com/henomis/packtrail/invoker"
)

const earlySigFlow = `
version: "1.0"
name: early-sig
nodes:
  - {id: gate, type: signal, signal_name: go}
edges: []
`

// TestEarlySignalWaitsForExecution: a signal published just before its
// execution's StartWithID lands is Nak'd and redelivered until the execution
// exists, instead of being silently dropped (the pre-fix behaviour, which
// stranded the wait forever).
func TestEarlySignalWaitsForExecution(t *testing.T) {
	h := newAsyncHarness(t, earlySigFlow)
	ctx := context.Background()

	const id = "order-early"

	// The signal races ahead of the Start.
	if err := h.engine.Signal(ctx, id, "go", json.RawMessage(`{"ok":true}`)); err != nil {
		t.Fatalf("early signal: %v", err)
	}

	if _, err := h.engine.StartWithID(ctx, id, "early-sig", nil); err != nil {
		t.Fatalf("start: %v", err)
	}

	// The redelivered signal finds the parked wait and completes the flow
	// (redelivery cadence is ~2s, so allow a generous window).
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		ex, err := h.store.Get(ctx, id)
		if err == nil && ex.Status == store.StatusCompleted {
			return
		}

		time.Sleep(50 * time.Millisecond)
	}

	ex, _ := h.store.Get(ctx, id)
	t.Fatalf("early signal never applied: status=%q wait=%q", ex.Status, ex.WaitSignal)
}

const preSigFlow = `
version: "1.0"
name: pre-sig
nodes:
  - {id: a, type: task, subject: "x"}
  - {id: gate, type: signal, signal_name: go}
edges:
  - {from: a, to: gate}
`

// TestSignalBeforeNodeIsConsumedOnArrival: a signal applied while the execution
// is still at an earlier node must be consumed when the execution reaches the
// signal node. Previously the early-consume path routed through
// guardedAdvance, whose waiting-only guard silently no-opped for a running
// execution — stranding it at the gate with the signal already stored.
func TestSignalBeforeNodeIsConsumedOnArrival(t *testing.T) {
	h := newAsyncHarness(t, preSigFlow)
	ctx := context.Background()

	id, err := h.engine.Start(ctx, "pre-sig", json.RawMessage(`{"n":1}`))
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	// Node a parks as an async activity; the execution is nowhere near the gate.
	_ = h.nextReq(t)
	h.waitStatus(t, id, store.StatusWaiting)

	// The signal arrives early and is stored on the document.
	if sigErr := h.engine.Signal(ctx, id, "go", json.RawMessage(`{"approved":true}`)); sigErr != nil {
		t.Fatalf("signal: %v", sigErr)
	}

	deadline := time.Now().Add(5 * time.Second)

	for {
		if ex := h.get(t, id); ex.Signals["go"] {
			break
		}

		if time.Now().After(deadline) {
			t.Fatal("early signal never stored")
		}

		time.Sleep(20 * time.Millisecond)
	}

	// Node a completes; the execution reaches the gate and must consume the
	// stored signal immediately instead of parking.
	if completeErr := h.engine.CompleteActivity(ctx, id, "a", 0,
		invoker.Result{Status: invoker.StatusOK, Payload: json.RawMessage(`{"n":2}`)}); completeErr != nil {
		t.Fatalf("complete a: %v", completeErr)
	}

	ex := h.waitStatus(t, id, store.StatusCompleted)

	if got := h.results(t, id); string(got.Signals["go"]) != `{"approved":true}` {
		t.Fatalf("signal payload missing from the assembled context on early consumption: %s", got.Signals["go"])
	}

	if len(ex.Signals) != 0 {
		t.Fatalf("consumed signal still stored: %v", ex.Signals)
	}
}

// TestOrphanSignalDeadLetters: a signal for an execution that never existed
// exhausts the delivery cap and leaves a durable dead-letter record instead of
// vanishing into a log line.
func TestOrphanSignalDeadLetters(t *testing.T) {
	h := newAsyncHarnessCfg(t, earlySigFlow, Config{MaxDeliver: 2})
	ctx := context.Background()

	if err := h.engine.Signal(ctx, "no-such-exec", "go", nil); err != nil {
		t.Fatalf("signal: %v", err)
	}

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if h.store.DeadLetters() > 0 {
			dls, err := h.store.RecentDeadLetters(ctx, 10)
			if err != nil {
				t.Fatalf("recent dead letters: %v", err)
			}

			for _, dl := range dls {
				if dl.Kind == store.DeadLetterSignal && dl.Key == "no-such-exec/go" {
					return
				}
			}

			t.Fatalf("dead letters recorded but none for the orphan signal: %+v", dls)
		}

		time.Sleep(100 * time.Millisecond)
	}

	t.Fatal("orphan signal never dead-lettered")
}
