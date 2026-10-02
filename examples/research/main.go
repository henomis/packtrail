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

// Example research: parallel branches writing to shared state.
//
// Three sources are queried in parallel (fanout); each branch appends its
// findings to the `notes` channel and adds its cost to `cost`. Channels fold
// concurrent writes with reducers — no branch overwrites another — and the
// join waits for a quorum of 2 sources. A budget on the `cost` counter would
// stop the execution if the branches overspent. The writer then reads the
// merged state.
//
//	go run ./examples/research
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand/v2"
	"time"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/examples/internal/devserver"
	"github.com/henomis/packtrail/examples/internal/exutil"
	"github.com/henomis/packtrail/worker"
)

const ns = "example-research"

const flowYAML = `
name: research
channels:
  notes:   {reducer: append}
  sources: {reducer: merge}
  cost:    {reducer: sum}
budget: {cost: 10}
nodes:
  - {id: gather, type: fanout, branches: [archive, journals, forums], next: enough}
  - {id: archive,  type: task, kind: source, timeout: 5s}
  - {id: journals, type: task, kind: source, timeout: 5s}
  - {id: forums,   type: task, kind: source, timeout: 5s}
  - {id: enough, type: join, policy: "quorum:2", next: write}
  - {id: write, type: task, kind: writer}
`

func main() {
	nc, closeAll := devserver.Connect()
	defer closeAll()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	g := exutil.NewGroup(ctx)
	defer g.Stop()

	eng := g.Engine(nc, packtrail.WithNamespace(ns), packtrail.WithFlowYAML([]byte(flowYAML)))

	// Each source takes a random time; the join proceeds after the first two
	// and the slowest branch is cancelled (its late result is ignored).
	g.Serve(nc, ns, "source", func(ctx context.Context, j *worker.Job) (*worker.Result, error) {
		var in struct{ Topic string }

		_ = j.Input(&in)

		select {
		case <-time.After(time.Duration(50+rand.IntN(400)) * time.Millisecond): //nolint:gosec // demo jitter.
		case <-ctx.Done():
			return nil, ctx.Err()
		}

		return &worker.Result{
			Output: map[string]any{"source": j.Node},
			Writes: map[string]any{
				"notes":   fmt.Sprintf("%s says something about %s", j.Node, in.Topic),
				"sources": map[string]bool{j.Node: true},
				"cost":    2.5,
			},
			Usage: map[string]float64{"cost": 2.5},
		}, nil
	}, worker.WithConcurrency(3))

	g.Serve(nc, ns, "writer", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		var notes []string
		if err := j.Channel("notes", &notes); err != nil {
			return nil, worker.Permanent(err)
		}

		return &worker.Result{Output: map[string]any{"report": fmt.Sprintf("%d findings", len(notes))}}, nil
	})

	st := exutil.Run(ctx, eng.Client(), "research", map[string]any{"topic": "event sourcing"})

	var pretty bytes.Buffer
	if err := json.Indent(&pretty, st.Output, "", "  "); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("status: %s\nbranches: %v\nchannels: %s\n", st.Status, st.Branches, pretty.String())
	fmt.Printf("report: %s\n", st.Results["write"])
}
