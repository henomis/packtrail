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

// Example agentloop: the shape of a tool-using agent, built from generic
// parts — packtrail knows nothing about LLMs.
//
// A planner task decides what to do next and routes there itself with a
// dynamic edge (worker.Result.Next, LangGraph's Command(goto)); the possible
// targets are declared in the flow. Tools append to a `messages` channel and
// loop back to the planner. max_steps is the recursion limit, and the
// `calls` budget caps how many tool calls the loop may make. Here the planner
// is a few lines of rules; in a real agent it would call a model.
//
//	go run ./examples/agentloop
package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/examples/internal/devserver"
	"github.com/henomis/packtrail/examples/internal/exutil"
	"github.com/henomis/packtrail/worker"
)

const ns = "example-agentloop"

const flowYAML = `
name: agent
max_steps: 30
budget: {calls: 5}
channels:
  messages: {reducer: append}
nodes:
  - id: plan
    type: task
    kind: planner
    dynamic: [search, calculate, answer]
  - {id: search, type: task, kind: tool, next: plan}
  - {id: calculate, type: task, kind: tool, next: plan}
  - {id: answer, type: task, kind: tool}
start: plan
`

func main() {
	nc, closeAll := devserver.Connect()
	defer closeAll()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	g := exutil.NewGroup(ctx)
	defer g.Stop()

	eng := g.Engine(nc, packtrail.WithNamespace(ns), packtrail.WithFlowYAML([]byte(flowYAML)))

	// The planner looks at what happened so far and picks the next tool.
	g.Serve(nc, ns, "planner", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		var msgs []string

		_ = j.Channel("messages", &msgs)

		next := "answer"

		switch {
		case !contains(msgs, "search:"):
			next = "search"
		case !contains(msgs, "calculate:"):
			next = "calculate"
		}

		return &worker.Result{
			Output: map[string]any{"decision": next},
			Writes: map[string]any{"messages": "plan: " + next},
			Next:   next,
		}, nil
	})

	// One worker kind serves every tool node.
	g.Serve(nc, ns, "tool", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		var q struct{ Question string }

		_ = j.Input(&q)

		var msg string

		switch j.Node {
		case "search":
			msg = "search: the Eiffel tower is 330 m tall"
		case "calculate":
			msg = "calculate: 330 m / 3 m per floor = 110 floors"
		default:
			msg = "answer: about 110 floors high"
		}

		return &worker.Result{
			Output: map[string]any{"text": msg},
			Writes: map[string]any{"messages": msg},
			Usage:  map[string]float64{"calls": 1},
		}, nil
	})

	st := exutil.Run(ctx, eng.Client(), "agent", map[string]any{"question": "how many floors tall is the Eiffel tower?"})

	fmt.Printf("status: %s after %d steps, %v tool calls\n", st.Status, st.Steps, st.Counters["calls"])
	fmt.Printf("planner ran %d times\n", st.Visits["plan"])
	fmt.Printf("messages: %s\n", st.Channels["messages"])
}

func contains(msgs []string, prefix string) bool {
	for _, m := range msgs {
		if strings.HasPrefix(m, prefix) {
			return true
		}
	}

	return false
}
