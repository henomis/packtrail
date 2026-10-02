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

// Example hello: the smallest packtrail program. A two-step flow, one worker
// kind, start an execution and wait for its result.
//
//	go run ./examples/hello
package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/examples/internal/devserver"
	"github.com/henomis/packtrail/examples/internal/exutil"
	"github.com/henomis/packtrail/worker"
)

const ns = "example-hello"

const flowYAML = `
name: hello
nodes:
  - {id: greet, type: task, kind: text, next: shout}
  - {id: shout, type: task, kind: text}
`

func main() {
	nc, closeAll := devserver.Connect()
	defer closeAll()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// g.Stop cancels the engine and the worker and waits for them to drain,
	// before closeAll closes the connection.
	g := exutil.NewGroup(ctx)
	defer g.Stop()

	// The engine provisions the namespace, registers the flow and processes
	// commands (packtrail.New + Init + Run).
	eng := g.Engine(nc, packtrail.WithNamespace(ns), packtrail.WithFlowYAML([]byte(flowYAML)))

	// A worker serves one kind; the node id tells it what to do.
	g.Serve(nc, ns, "text", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		switch j.Node {
		case "greet":
			var in struct{ Name string }
			if err := j.Input(&in); err != nil {
				return nil, worker.Permanent(err)
			}

			return &worker.Result{Output: map[string]any{"text": "hello, " + in.Name}}, nil
		default: // shout: read the previous node's output from the context
			var prev struct{ Text string }
			if err := j.Result("greet", &prev); err != nil {
				return nil, worker.Permanent(err)
			}

			return &worker.Result{Output: map[string]any{"text": strings.ToUpper(prev.Text) + "!"}}, nil
		}
	})

	client := eng.Client()

	id, err := client.Start(ctx, "hello", map[string]any{"name": "packtrail"})
	if err != nil {
		log.Fatal(err)
	}

	st, err := client.Wait(ctx, id)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("execution %s: %s\n", id, st.Status)
	fmt.Printf("output: %s\n", st.Output)
}
