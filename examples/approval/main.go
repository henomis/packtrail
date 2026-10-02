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

// Example approval: human in the loop, two ways.
//
//   - An await node parks the execution until a named signal arrives (or a
//     timeout routes it elsewhere). Nothing runs while it waits: the deadline
//     is a durable JetStream schedule.
//
//   - A task can interrupt itself (worker.Interrupt) with a question; the
//     execution waits until Client.Resume answers, then the node runs again
//     with the answer (LangGraph's interrupt()).
//
//     go run ./examples/approval
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/examples/internal/devserver"
	"github.com/henomis/packtrail/examples/internal/exutil"
	"github.com/henomis/packtrail/worker"
)

const ns = "example-approval"

const flowYAML = `
name: expense
nodes:
  - id: check
    type: task
    kind: policy
    next: approval
  - id: approval
    type: await
    signal: manager-decision
    timeout: 72h
    on_timeout: escalate
    next: route
  - id: route
    type: choice
    rules:
      - {when: "signals['manager-decision'].approved", to: pay}
      - {default: true, to: reject}
  - {id: pay, type: task, kind: payments}
  - {id: reject, type: task, kind: notify}
  - {id: escalate, type: task, kind: notify}
`

func main() {
	nc, closeAll := devserver.Connect()
	defer closeAll()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	g := exutil.NewGroup(ctx)
	defer g.Stop()

	eng := g.Engine(nc, packtrail.WithNamespace(ns), packtrail.WithFlowYAML([]byte(flowYAML)))

	// policy interrupts when the amount needs an explanation, then continues
	// with the explanation it was given.
	g.Serve(nc, ns, "policy", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		var in struct{ Amount float64 }

		_ = j.Input(&in)

		var reason string

		resumed, err := j.Resumed(&reason)
		if err != nil {
			return nil, worker.Permanent(err)
		}

		if in.Amount > 1000 && !resumed {
			return nil, worker.Interrupt(map[string]any{"question": "why is this expense above 1000?"})
		}

		return &worker.Result{Output: map[string]any{"amount": in.Amount, "justification": reason}}, nil
	})
	g.Serve(nc, ns, "payments", func(context.Context, *worker.Job) (*worker.Result, error) {
		return &worker.Result{Output: map[string]any{"paid": true}}, nil
	})
	g.Serve(nc, ns, "notify", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		return &worker.Result{Output: map[string]any{"notified": j.Node}}, nil
	})

	c := eng.Client()

	id, err := c.Start(ctx, "expense", map[string]any{"amount": 2500})
	if err != nil {
		log.Fatal(err)
	}

	// 1. The policy task interrupts with a question.
	st := exutil.WaitFor(ctx, c, id, packtrail.StatusWaiting)
	fmt.Printf("interrupted at %q: %s\n", "check", st.Tasks["check"].Interrupt)

	if err := c.Resume(ctx, id, "check", "team offsite, 10 people"); err != nil {
		log.Fatal(err)
	}

	// 2. Now it waits at the await node for the manager.
	exutil.WaitFor(ctx, c, id, packtrail.StatusWaiting, "approval")
	fmt.Println("waiting for signal manager-decision …")

	if err := c.Signal(ctx, id, "manager-decision", map[string]any{"approved": true, "by": "ana"}); err != nil {
		log.Fatal(err)
	}

	st, err = c.Wait(ctx, id)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%s via %s, check=%s\n", st.Status, st.LastNode, st.Results["check"])
}
