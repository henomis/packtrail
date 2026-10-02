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

// Example resilience: what happens when workers misbehave.
//
//   - flaky fails twice with an ordinary error: the retry policy (exponential
//     backoff, durable timers) runs it again until attempt 3 succeeds.
//
//   - slow hangs on its first attempt: the per-attempt timeout fires, the late
//     result is ignored, and attempt 2 succeeds.
//
//   - typed returns an output that violates output_schema once: the
//     violation counts as a failed attempt and is retried.
//
//   - broken returns worker.Permanent: no retry, the execution fails with the
//     node and the reason recorded.
//
//     go run ./examples/resilience
package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/examples/internal/devserver"
	"github.com/henomis/packtrail/examples/internal/exutil"
	"github.com/henomis/packtrail/worker"
)

const ns = "example-resilience"

const flowYAML = `
name: pipeline
nodes:
  - id: flaky
    type: task
    kind: flaky
    retry: {max_attempts: 5, backoff: exponential, delay: 100ms, max_delay: 2s}
    next: slow
  - id: slow
    type: task
    kind: slow
    timeout: 1s
    retry: {max_attempts: 2, backoff: fixed, delay: 100ms}
    next: typed
  - id: typed
    type: task
    kind: typed
    retry: {max_attempts: 2, backoff: fixed, delay: 100ms}
    output_schema:
      type: object
      required: [total]
      properties: {total: {type: number}}
    next: broken
  - {id: broken, type: task, kind: broken, retry: {max_attempts: 3}}
`

func main() {
	nc, closeAll := devserver.Connect()
	defer closeAll()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	g := exutil.NewGroup(ctx)
	defer g.Stop()

	eng := g.Engine(nc, packtrail.WithNamespace(ns), packtrail.WithFlowYAML([]byte(flowYAML)))

	g.Serve(nc, ns, "flaky", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		fmt.Printf("  flaky  attempt %d\n", j.Attempt)

		if j.Attempt < 3 {
			return nil, errors.New("connection reset") // retryable
		}

		return &worker.Result{Output: map[string]any{"attempts": j.Attempt}}, nil
	})
	g.Serve(nc, ns, "slow", func(ctx context.Context, j *worker.Job) (*worker.Result, error) {
		fmt.Printf("  slow   attempt %d\n", j.Attempt)

		if j.Attempt == 1 {
			select { // longer than the 1s timeout
			case <-time.After(3 * time.Second):
			case <-ctx.Done():
			}
		}

		return &worker.Result{Output: map[string]any{"attempt": j.Attempt}}, nil
	}, worker.WithConcurrency(2))
	g.Serve(nc, ns, "typed", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		fmt.Printf("  typed  attempt %d\n", j.Attempt)

		if j.Attempt == 1 {
			return &worker.Result{Output: map[string]any{"total": "forty-two"}}, nil // wrong type
		}

		return &worker.Result{Output: map[string]any{"total": 42}}, nil
	})
	g.Serve(nc, ns, "broken", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		fmt.Printf("  broken attempt %d\n", j.Attempt)

		return nil, worker.Permanent(errors.New("invalid account number"))
	})

	c := eng.Client()

	st := exutil.Run(ctx, c, "pipeline", nil)
	fmt.Printf("status %s at node %q: %s (%s)\n", st.Status, st.FailedNode, st.Error, st.Reason)
	fmt.Printf("results: flaky=%s slow=%s typed=%s\n", st.Results["flaky"], st.Results["slow"], st.Results["typed"])
}
