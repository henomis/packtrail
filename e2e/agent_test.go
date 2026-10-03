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

package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/event"
	"github.com/henomis/packtrail/worker"
)

// agentFlow is an agent loop: a planner routes itself, by dynamic edges, to
// a tool, to a human (an interrupt resumed from outside) or to the end.
// Usage feeds a token budget.
const agentFlow = `
name: agent
channels:
  scratch: {reducer: append}
budget: {tokens: %d}
max_steps: 1000
start: plan
nodes:
  - {id: plan, type: task, kind: planner, dynamic: [tool, ask, finish], next: finish}
  - {id: tool, type: task, kind: tool, next: plan}
  - {id: ask, type: task, kind: asker, next: plan}
  - {id: finish, type: task, kind: finisher}
`

// agentTask: the planner calls a tool on each visit up to Steps, asks the
// human on visit AskAt, then finishes.
type agentTask struct {
	Steps int `json:"steps"`
	AskAt int `json:"ask_at"`
}

const (
	planTokens = 10
	toolTokens = 5
)

func agentHandlers() map[string]worker.Handler {
	h := map[string]worker.Handler{}

	h["planner"] = func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		var in agentTask
		if err := j.Input(&in); err != nil {
			return nil, worker.Permanent(err)
		}

		v := j.Context.Visits["plan"]
		next := "tool"

		switch {
		case v == in.AskAt:
			next = "ask"
		case v > in.Steps:
			next = "finish"
		}

		return &worker.Result{
			Output: map[string]any{"visit": v, "next": next}, Next: next,
			Usage: map[string]float64{"tokens": planTokens},
		}, nil
	}

	h["tool"] = func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		n := j.Context.Visits["tool"]

		return &worker.Result{
			Output: map[string]any{"call": n}, Writes: map[string]any{"scratch": fmt.Sprintf("tool-%d", n)},
			Usage: map[string]float64{"tokens": toolTokens},
		}, nil
	}

	h["asker"] = func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		var answer string

		resumed, err := j.Resumed(&answer)
		if err != nil {
			return nil, worker.Permanent(err)
		}

		if !resumed {
			return nil, worker.Interrupt(map[string]any{"question": "may I continue?"})
		}

		return &worker.Result{
			Output: map[string]any{"answer": answer}, Writes: map[string]any{"scratch": "human:" + answer},
		}, nil
	}

	h["finisher"] = func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		var notes []string
		if err := j.Channel("scratch", &notes); err != nil {
			return nil, worker.Permanent(err)
		}

		return &worker.Result{Output: map[string]any{"notes": len(notes)}}, nil
	}

	return h
}

// agentWorkers starts one worker process per kind of the agent flow.
func agentWorkers(cl *cluster) {
	for kind, h := range agentHandlers() {
		cl.worker(kind, h, worker.WithConcurrency(8))
	}
}

// TestAgentLoop: a long loop with a human in the middle, continued as new
// several times on the way, ends with exact visits, counters and outputs.
func TestAgentLoop(t *testing.T) {
	cl := newCluster(t, []string{fmt.Sprintf(agentFlow, 1_000_000)}, packtrail.WithHistoryLimit(60))
	agentWorkers(cl)

	const steps, askAt = 40, 7

	id := cl.start("agent", agentTask{Steps: steps, AskAt: askAt})

	st := cl.waitUntil(id, "waiting", func(st *packtrail.State) bool { return st.Status == packtrail.StatusWaiting })

	ask := st.Tasks["ask"]
	if ask == nil || !jsonEqual(ask.Interrupt, mustJSON(map[string]any{"question": "may I continue?"})) {
		t.Fatalf("waiting without the ask interrupt: %+v", st.Tasks)
	}

	// Resume is idempotent per interrupt: the second one is stale.
	for range 2 {
		if err := cl.c.Resume(cl.ctx, id, "ask", "yes"); err != nil {
			t.Fatal(err)
		}
	}

	st = cl.wait(id)
	f := cl.check(id)

	if st.Status != packtrail.StatusCompleted {
		t.Fatalf("status %s: %s %s", st.Status, st.Reason, st.Error)
	}

	// Visits 1..steps of plan route out (one to ask, the rest to tools);
	// visit steps+1 finishes.
	tools := steps - 1
	if st.Visits["plan"] != steps+1 || st.Visits["tool"] != tools || st.Visits["ask"] != 1 || st.Visits["finish"] != 1 {
		t.Fatalf("visits %v", st.Visits)
	}

	if want := float64(planTokens*(steps+1) + toolTokens*tools); st.Counters["tokens"] != want {
		t.Fatalf("tokens %g, want %g", st.Counters["tokens"], want)
	}

	var notes []string
	if err := st.Channel("scratch", &notes); err != nil || len(notes) != tools+1 {
		t.Fatalf("scratch %s", st.Channels["scratch"])
	}

	if notes[askAt-1] != "human:yes" || notes[0] != "tool-1" || notes[len(notes)-1] != fmt.Sprintf("tool-%d", tools) {
		t.Fatalf("scratch out of order: %v", notes)
	}

	var fin struct{ Notes int }
	if err := st.Result("finish", &fin); err != nil || fin.Notes != tools+1 {
		t.Fatalf("finish %s", st.Results["finish"])
	}

	if f.count[event.ExecutionContinued] < 2 {
		t.Fatalf("%d continuations, want several (history limit)", f.count[event.ExecutionContinued])
	}

	if f.count[event.NodeInterrupted] != 1 {
		t.Fatalf("%d interrupts", f.count[event.NodeInterrupted])
	}

	// Every planner output is kept, one per visit, in order.
	outs, err := cl.c.OutputHistory(cl.ctx, id, "plan")
	if err != nil || len(outs) != steps+1 {
		t.Fatalf("plan outputs %d %v", len(outs), err)
	}

	for i, o := range outs {
		var p struct{ Visit int }
		if json.Unmarshal(o, &p) != nil || p.Visit != i+1 {
			t.Fatalf("plan output %d is %s", i, o)
		}
	}
}

// TestAgentBudget: a loop that spends past its token budget fails with
// budget_exceeded, and the failure does not route to any handler.
func TestAgentBudget(t *testing.T) {
	cl := newCluster(t, []string{fmt.Sprintf(agentFlow, 200)})
	agentWorkers(cl)

	id := cl.start("agent", agentTask{Steps: 100, AskAt: -1})
	st := cl.wait(id)
	cl.check(id)

	if st.Status != packtrail.StatusFailed || st.Reason != event.ReasonBudget {
		t.Fatalf("status %s reason %s: %s", st.Status, st.Reason, st.Error)
	}

	if st.Counters["tokens"] <= 200 || st.Counters["tokens"] > 200+planTokens {
		t.Fatalf("tokens %g: the budget must stop the first step past it", st.Counters["tokens"])
	}
}
