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

	"github.com/henomis/packtrail/internal/signal"
	"github.com/henomis/packtrail/internal/store"
	"github.com/henomis/packtrail/pkg/protocol"
)

func signalFlow(timeout string) string {
	return `
name: sig
nodes:
  - {id: start, type: task, subject: "tasks.start.{execution_id}"}
  - id: wait
    type: signal
    signal_name: approval
    timeout: ` + timeout + `
    on_timeout: fallback
  - {id: after, type: task, subject: "tasks.after.{execution_id}"}
  - {id: fallback, type: task, subject: "tasks.fallback.{execution_id}"}
edges:
  - {from: start, to: wait}
  - {from: wait, to: after}
`
}

func TestSignalResume(t *testing.T) {
	h := newHarness(t, signalFlow("24h"), Config{})
	h.serve(t, "tasks.start.*", passthrough)

	reached := make(chan string, 2)

	h.serve(t, "tasks.after.*", func(_ context.Context, req protocol.TaskRequest) (protocol.TaskResponse, error) {
		reached <- "after"
		return protocol.TaskResponse{Status: protocol.StatusOK, Payload: req.Payload}, nil
	})
	h.serve(t, "tasks.fallback.*", func(_ context.Context, req protocol.TaskRequest) (protocol.TaskResponse, error) {
		reached <- "fallback"
		return protocol.TaskResponse{Status: protocol.StatusOK, Payload: req.Payload}, nil
	})

	id, err := h.engine.Start(context.Background(), "sig", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	h.waitStatus(t, id, store.StatusWaiting, 5*time.Second)

	if signalErr := h.engine.Signal(context.Background(), id, "approval", json.RawMessage(`{"approved":true}`)); signalErr != nil {
		t.Fatalf("signal: %v", signalErr)
	}

	ex := h.waitStatus(t, id, store.StatusCompleted, 5*time.Second)

	select {
	case got := <-reached:
		if got != "after" {
			t.Fatalf("reached %q, want after", got)
		}
	case <-time.After(time.Second):
		t.Fatal("after not reached")
	}

	_ = ex

	doc, err := h.engine.Results(context.Background(), id)
	if err != nil {
		t.Fatalf("results: %v", err)
	}

	if got := parseCtx(t, doc); string(got.Signals["approval"]) != `{"approved":true}` {
		t.Errorf("signal payload missing from the assembled context: %s", doc)
	}
}

// TestSignalIdempotent verifies a redelivered signal (same stream sequence) does
// not alter state twice (acceptance §12).
func TestSignalIdempotent(t *testing.T) {
	h := newHarness(t, signalFlow("24h"), Config{})
	h.serve(t, "tasks.start.*", passthrough)
	h.serve(t, "tasks.after.*", passthrough)
	h.serve(t, "tasks.fallback.*", passthrough)

	id, err := h.engine.Start(context.Background(), "sig", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	h.waitStatus(t, id, store.StatusWaiting, 5*time.Second)

	d := signal.Delivery{ExecID: id, Name: "approval", Seq: 42, Payload: json.RawMessage(`{"approved":true}`)}
	if applyErr := h.engine.applySignal(context.Background(), d); applyErr != nil {
		t.Fatalf("apply 1: %v", applyErr)
	}

	ex := h.waitStatus(t, id, store.StatusCompleted, 5*time.Second)
	if ex.LastSeq["approval"] != 42 {
		t.Fatalf("LastSeq = %d, want 42", ex.LastSeq["approval"])
	}

	// Redelivery of the same sequence: must be a no-op.
	if applyErr := h.engine.applySignal(context.Background(), d); applyErr != nil {
		t.Fatalf("apply 2 (dup): %v", applyErr)
	}

	ex2, _ := h.store.Get(context.Background(), id)
	if ex2.Revision != ex.Revision {
		t.Errorf("duplicate signal changed state: revision %d -> %d", ex.Revision, ex2.Revision)
	}
}

func TestSignalTimeout(t *testing.T) {
	h := newHarness(t, signalFlow("250ms"), Config{})
	h.serve(t, "tasks.start.*", passthrough)

	reached := make(chan string, 2)

	h.serve(t, "tasks.after.*", func(_ context.Context, req protocol.TaskRequest) (protocol.TaskResponse, error) {
		reached <- "after"
		return protocol.TaskResponse{Status: protocol.StatusOK, Payload: req.Payload}, nil
	})
	h.serve(t, "tasks.fallback.*", func(_ context.Context, req protocol.TaskRequest) (protocol.TaskResponse, error) {
		reached <- "fallback"
		return protocol.TaskResponse{Status: protocol.StatusOK, Payload: req.Payload}, nil
	})

	id, err := h.engine.Start(context.Background(), "sig", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	h.waitStatus(t, id, store.StatusCompleted, 10*time.Second)

	select {
	case got := <-reached:
		if got != "fallback" {
			t.Fatalf("reached %q, want fallback", got)
		}
	case <-time.After(time.Second):
		t.Fatal("fallback not reached")
	}
}
