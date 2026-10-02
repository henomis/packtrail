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

// Example timetravel: the event log as a debugging tool.
//
//  1. Run a three-step flow whose last step fails.
//
//  2. Print the history: every event with its stream sequence.
//
//  3. Read the state as it was right after step two (StateAt).
//
//  4. Fix the worker and Rerun the failed node: a new execution forked from
//     the point where that node was scheduled; steps one and two are not run
//     again, their results are inherited.
//
//  5. Fork from right after step one: the fork runs steps two and three again
//     and can diverge from the original.
//
//     go run ./examples/timetravel
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/event"
	"github.com/henomis/packtrail/examples/internal/devserver"
	"github.com/henomis/packtrail/examples/internal/exutil"
	"github.com/henomis/packtrail/worker"
)

const ns = "example-timetravel"

const flowYAML = `
name: story
channels: {log: {reducer: append}}
nodes:
  - {id: one, type: task, kind: step, next: two}
  - {id: two, type: task, kind: step, next: three}
  - {id: three, type: task, kind: step}
`

func main() {
	nc, closeAll := devserver.Connect()
	defer closeAll()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	g := exutil.NewGroup(ctx)
	defer g.Stop()

	eng := g.Engine(nc, packtrail.WithNamespace(ns), packtrail.WithFlowYAML([]byte(flowYAML)))

	var broken atomic.Bool

	broken.Store(true)

	var runs atomic.Int32

	g.Serve(nc, ns, "step", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		n := runs.Add(1)

		if j.Node == "three" && broken.Load() {
			return nil, worker.Permanent(errors.New("step three is broken"))
		}

		entry := fmt.Sprintf("%s(run %d)", j.Node, n)

		return &worker.Result{Output: map[string]any{"entry": entry}, Writes: map[string]any{"log": entry}}, nil
	})

	c := eng.Client()

	// 1. Run: fails at step three.
	st := exutil.Run(ctx, c, "story", nil)
	fmt.Printf("original %s: %s at %q\n\n", st.ExecID, st.Status, st.FailedNode)

	// 2. History.
	evs, err := c.History(ctx, st.ExecID)
	if err != nil {
		log.Fatal(err)
	}

	var afterOne, afterTwo uint64

	for _, ev := range evs {
		fmt.Printf("  seq %3d  %-18s %s\n", ev.Seq, ev.Type, describe(ev))

		if d, ok := ev.Data.(*event.NodeDone); ok {
			switch d.Node {
			case "one":
				afterOne = ev.Seq
			case "two":
				afterTwo = ev.Seq
			}
		}
	}

	// 3. State at a point in the past.
	past, err := c.StateAt(ctx, st.ExecID, afterTwo)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("\nstate at seq %d: status=%s log=%s\n", afterTwo, past.Status, past.Channels["log"])

	// 4. Fix the bug and rerun the failed node.
	broken.Store(false)

	rerun, err := c.Rerun(ctx, st.ExecID, "three")
	if err != nil {
		log.Fatal(err)
	}

	rst, err := c.Wait(ctx, rerun)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("rerun    %s: %s, log=%s\n", rerun, rst.Status, rst.Channels["log"])

	// 5. Fork after step one.
	fork, err := c.Fork(ctx, st.ExecID, afterOne)
	if err != nil {
		log.Fatal(err)
	}

	fst, err := c.Wait(ctx, fork)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("fork     %s: %s, log=%s (forked from %s@%d)\n", fork, fst.Status, fst.Channels["log"],
		fst.ForkedFrom, fst.ForkSeq)
}

func describe(ev event.Event) string {
	switch d := ev.Data.(type) {
	case *event.Entered:
		return d.Node
	case *event.Scheduled:
		return fmt.Sprintf("%s attempt %d", d.Node, d.Attempt)
	case *event.NodeDone:
		return fmt.Sprintf("%s → %s", d.Node, d.Output)
	case *event.NodeFail:
		return fmt.Sprintf("%s: %s", d.Node, d.Error)
	case *event.Failed:
		return d.Reason
	default:
		return ""
	}
}
