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
