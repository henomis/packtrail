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

// Example subflow: composing flows.
//
// The `onboarding` flow runs the `verify` flow as a child execution: the
// child gets its own id, log and state, its output becomes
// results.identity in the parent, and its usage counters are added to the
// parent's. With on_parent_close: cancel (the default), cancelling the parent
// cancels a running child too.
//
//	go run ./examples/subflow
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

const ns = "example-subflow"

const parentYAML = `
name: onboarding
nodes:
  - {id: identity, type: subflow, flow: verify, input: "input.person", next: welcome}
  - {id: welcome, type: task, kind: mailer}
`

const childYAML = `
name: verify
nodes:
  - {id: document, type: task, kind: checker, next: review}
  - {id: review, type: await, signal: reviewed, timeout: 24h, on_timeout: reject, next: done}
  - {id: done, type: task, kind: checker}
  - {id: reject, type: task, kind: checker}
`

func main() {
	nc, closeAll := devserver.Connect()
	defer closeAll()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	g := exutil.NewGroup(ctx)
	defer g.Stop()

	eng := g.Engine(nc, packtrail.WithNamespace(ns),
		packtrail.WithFlowYAML([]byte(parentYAML)), packtrail.WithFlowYAML([]byte(childYAML)))

	g.Serve(nc, ns, "checker", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		var p struct{ Name string }

		_ = j.Input(&p)

		return &worker.Result{
			Output: map[string]any{"verified": j.Node == "done", "name": p.Name},
			Usage:  map[string]float64{"checks": 1},
		}, nil
	})
	g.Serve(nc, ns, "mailer", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		var id struct{ Name string }

		_ = j.Result("identity", &id)

		return &worker.Result{Output: map[string]any{"mail": "welcome, " + id.Name}}, nil
	})

	c := eng.Client()

	// 1. Parent and child run to completion; the child waits for a reviewer.
	parent, err := c.Start(ctx, "onboarding", map[string]any{"person": map[string]any{"name": "Ada"}})
	if err != nil {
		log.Fatal(err)
	}

	child := childOf(ctx, c, parent)
	exutil.WaitFor(ctx, c, child, packtrail.StatusWaiting, "review")
	fmt.Printf("child %s is waiting for a reviewer\n", child)

	if err := c.Signal(ctx, child, "reviewed", map[string]any{"ok": true}); err != nil {
		log.Fatal(err)
	}

	st, err := c.Wait(ctx, parent)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("parent: %s, identity=%s, welcome=%s, checks=%v\n\n",
		st.Status, st.Results["identity"], st.Results["welcome"], st.Counters["checks"])

	// 2. Cancelling a parent cancels its running child.
	parent2, err := c.Start(ctx, "onboarding", map[string]any{"person": map[string]any{"name": "Bob"}})
	if err != nil {
		log.Fatal(err)
	}

	child2 := childOf(ctx, c, parent2)
	exutil.WaitFor(ctx, c, child2, packtrail.StatusWaiting, "review")

	if err := c.Cancel(ctx, parent2, "applicant withdrew"); err != nil {
		log.Fatal(err)
	}

	cst := exutil.WaitFor(ctx, c, child2, packtrail.StatusCancelled)
	fmt.Printf("cancelled parent %s → child %s is %s (%s)\n", parent2, child2, cst.Status, cst.Reason)
}

// childOf waits until the parent started its child and returns the child id.
func childOf(ctx context.Context, c *packtrail.Client, parent string) string {
	for {
		st, err := c.Get(ctx, parent)
		if err == nil {
			for _, ch := range st.Children {
				return ch.ChildID
			}
		}

		select {
		case <-ctx.Done():
			log.Fatal("no child started")
		case <-time.After(20 * time.Millisecond):
		}
	}
}
