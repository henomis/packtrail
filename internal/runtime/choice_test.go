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
	"strings"
	"testing"
	"time"

	"github.com/henomis/packtrail/internal/store"
	"github.com/henomis/packtrail/pkg/protocol"
)

const choiceFlow = `
name: choice
nodes:
  - {id: triage, type: task, subject: "tasks.triage.{execution_id}"}
  - id: route
    type: choice
    rules:
      - when: "results.triage.risk_score > 80"
        to: escalation
      - default: true
        to: synthesis
  - {id: escalation, type: task, subject: "tasks.escalation.{execution_id}"}
  - {id: synthesis, type: task, subject: "tasks.synthesis.{execution_id}"}
edges:
  - {from: triage, to: route}
`

func runChoice(t *testing.T, riskScore int) string {
	t.Helper()
	h := newHarness(t, choiceFlow, Config{})
	h.serve(t, "tasks.triage.*", func(_ context.Context, _ protocol.TaskRequest) (protocol.TaskResponse, error) {
		return protocol.TaskResponse{Status: protocol.StatusOK, Payload: setField(json.RawMessage(`{}`), "risk_score", riskScore)}, nil
	})

	reached := make(chan string, 2)

	h.serve(t, "tasks.escalation.*", func(_ context.Context, req protocol.TaskRequest) (protocol.TaskResponse, error) {
		reached <- "escalation"
		return protocol.TaskResponse{Status: protocol.StatusOK, Payload: req.Payload}, nil
	})
	h.serve(t, "tasks.synthesis.*", func(_ context.Context, req protocol.TaskRequest) (protocol.TaskResponse, error) {
		reached <- "synthesis"
		return protocol.TaskResponse{Status: protocol.StatusOK, Payload: req.Payload}, nil
	})

	id, err := h.engine.Start(context.Background(), "choice", json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	h.waitStatus(t, id, store.StatusCompleted, 5*time.Second)

	select {
	case got := <-reached:
		return got
	case <-time.After(time.Second):
		t.Fatal("no terminal task reached")
		return ""
	}
}

func TestChoiceRouting(t *testing.T) {
	if got := runChoice(t, 90); got != "escalation" {
		t.Errorf("risk 90 routed to %q, want escalation", got)
	}

	if got := runChoice(t, 10); got != "synthesis" {
		t.Errorf("risk 10 routed to %q, want synthesis", got)
	}
}

// A choice rule that errors at runtime: input.n is a number and input.s a
// string, so the comparison is a type mismatch that expr evaluates to an error.
const choiceErrOnErrorFailFlow = `
name: choice-onerror-fail
nodes:
  - {id: seed, type: task, subject: "tasks.seed.{execution_id}"}
  - id: route
    type: choice
    on_error: fail
    rules:
      - when: "input.n > input.s"
        to: hi
      - default: true
        to: lo
  - {id: hi, type: task, subject: "tasks.hi.{execution_id}"}
  - {id: lo, type: task, subject: "tasks.lo.{execution_id}"}
edges:
  - {from: seed, to: route}
`

const choiceErrDefaultFlow = `
name: choice-onerror-default
nodes:
  - {id: seed, type: task, subject: "tasks.seed.{execution_id}"}
  - id: route
    type: choice
    rules:
      - when: "input.n > input.s"
        to: hi
      - default: true
        to: lo
  - {id: hi, type: task, subject: "tasks.hi.{execution_id}"}
  - {id: lo, type: task, subject: "tasks.lo.{execution_id}"}
edges:
  - {from: seed, to: route}
`

// TestChoiceOnErrorFailsExecution: with on_error: fail, a choice rule that errors
// at runtime fails the execution instead of routing to the default (F-033).
func TestChoiceOnErrorFailsExecution(t *testing.T) {
	h := newHarness(t, choiceErrOnErrorFailFlow, Config{})
	h.serve(t, "tasks.seed.*", passthrough)
	h.serve(t, "tasks.hi.*", passthrough)
	h.serve(t, "tasks.lo.*", passthrough)

	id, err := h.engine.Start(context.Background(), "choice-onerror-fail", json.RawMessage(`{"n":1,"s":"a"}`))
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	ex := h.waitStatus(t, id, store.StatusFailed, 5*time.Second)
	if !strings.Contains(ex.Error, "evaluation error") {
		t.Fatalf("error = %q, want choice evaluation error", ex.Error)
	}
}

// TestChoiceEvalErrorFallsToDefault: without on_error, the same erring rule is
// treated as no-match and the default route is taken (unchanged behavior) (F-033).
func TestChoiceEvalErrorFallsToDefault(t *testing.T) {
	h := newHarness(t, choiceErrDefaultFlow, Config{})
	h.serve(t, "tasks.seed.*", passthrough)

	reached := make(chan string, 2)

	h.serve(t, "tasks.hi.*", func(_ context.Context, req protocol.TaskRequest) (protocol.TaskResponse, error) {
		reached <- "hi"
		return protocol.TaskResponse{Status: protocol.StatusOK, Payload: req.Payload}, nil
	})
	h.serve(t, "tasks.lo.*", func(_ context.Context, req protocol.TaskRequest) (protocol.TaskResponse, error) {
		reached <- "lo"
		return protocol.TaskResponse{Status: protocol.StatusOK, Payload: req.Payload}, nil
	})

	id, err := h.engine.Start(context.Background(), "choice-onerror-default", json.RawMessage(`{"n":1,"s":"a"}`))
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	h.waitStatus(t, id, store.StatusCompleted, 5*time.Second)

	select {
	case got := <-reached:
		if got != "lo" {
			t.Fatalf("routed to %q, want lo (default on eval error)", got)
		}
	case <-time.After(time.Second):
		t.Fatal("no terminal task reached")
	}
}
