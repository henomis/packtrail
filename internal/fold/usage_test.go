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

package fold

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/henomis/packtrail/event"
	"github.com/henomis/packtrail/internal/cmd"
)

func tokens(n float64) map[string]float64 { return map[string]float64{"tokens": n} }

func (x *h) failU(key string, retryable bool, usage map[string]float64) {
	x.t.Helper()

	t := x.task(key)
	x.do(cmd.Fail, cmd.FailData{
		TaskRef: cmd.TaskRef{Key: key, Generation: t.Generation, Attempt: t.Attempt},
		Error:   "boom", Retryable: retryable, Usage: usage,
	})
}

func (x *h) interruptU(key string, usage map[string]float64) {
	x.t.Helper()

	t := x.task(key)
	x.do(cmd.Interrupt, cmd.InterruptData{
		TaskRef: cmd.TaskRef{Key: key, Generation: t.Generation, Attempt: t.Attempt},
		Payload: json.RawMessage(`{"q":1}`), Usage: usage,
	})
}

func (x *h) tokens(want float64) {
	x.t.Helper()

	if got := x.st.Counters["tokens"]; got != want {
		x.t.Fatalf("tokens = %g, want %g", got, want)
	}
}

func usageFlow(budget int) string {
	return fmt.Sprintf(`
name: u
budget: {tokens: %d}
nodes:
  - id: a
    type: task
    kind: k
    retry: {max_attempts: 5}
    output_schema: {type: object, required: [text]}
    on_failure: h
  - {id: h, type: task, kind: k}
`, budget)
}

// Every attempt's usage counts, whether it failed, returned an invalid output
// or interrupted; a duplicate or stale result counts nothing.
func TestUsageOfEveryAttemptCounts(t *testing.T) {
	x := newH(t, usageFlow(100))
	x.start(`{}`)

	stale := *x.task("a")

	x.failU("a", true, tokens(10))
	x.tokens(10)
	x.fire(event.TimerRetry)

	if evs := x.do(cmd.Fail, cmd.FailData{
		TaskRef: cmd.TaskRef{Key: "a", Generation: stale.Generation, Attempt: stale.Attempt},
		Error:   "boom", Retryable: true, Usage: tokens(10),
	}); len(evs) != 0 {
		t.Fatal("stale failure accepted")
	}

	x.okWith("a", cmd.CompleteData{Output: json.RawMessage(`{"nope":1}`), Usage: tokens(5)})
	x.tokens(15)
	x.fire(event.TimerRetry)

	x.interruptU("a", tokens(20))
	x.tokens(35)
	x.status(StatusWaiting)
	x.do(cmd.Resume, cmd.ResumeData{Node: "a", Value: json.RawMessage(`"go"`)})

	x.okWith("a", cmd.CompleteData{Output: json.RawMessage(`{"text":"ok"}`), Usage: tokens(1)})
	x.tokens(36)
	x.status(StatusCompleted)
}

// Usage past the budget fails the execution at that node, with reason budget:
// an interrupt does not pause, a failure is neither retried nor routed to
// on_failure.
func TestUsageOverBudgetFailsExecution(t *testing.T) {
	cases := map[string]func(x *h){
		"interrupt":         func(x *h) { x.interruptU("a", tokens(11)) },
		"retryable failure": func(x *h) { x.failU("a", true, tokens(11)) },
		"permanent failure": func(x *h) { x.failU("a", false, tokens(11)) },
		"invalid output":    func(x *h) { x.okWith("a", cmd.CompleteData{Output: json.RawMessage(`{}`), Usage: tokens(11)}) },
		"valid output": func(x *h) {
			x.okWith("a", cmd.CompleteData{Output: json.RawMessage(`{"text":"ok"}`), Usage: tokens(11)})
		},
	}

	for name, step := range cases {
		x := newH(t, usageFlow(10))
		x.start(`{}`)
		step(x)

		if x.st.Status != StatusFailed || x.st.Reason != event.ReasonBudget || x.st.FailedNode != "a" {
			t.Fatalf("%s: status %s reason %s node %q", name, x.st.Status, x.st.Reason, x.st.FailedNode)
		}

		x.tokens(11)

		for _, ev := range x.last {
			if ev.Type == event.NodeEntered || ev.Type == event.TimerScheduled {
				t.Fatalf("%s: %s after the budget ran out", name, ev.Type)
			}
		}
	}

	// Reaching the budget exactly is not exceeding it.
	x := newH(t, usageFlow(10))
	x.start(`{}`)
	x.interruptU("a", tokens(10))
	x.status(StatusWaiting)
}

// A failed or interrupted map item or fan-out branch adds its usage to the
// execution's counters.
func TestUsageOfItemsAndBranches(t *testing.T) {
	x := newH(t, `
name: m
nodes:
  - {id: m, type: map, kind: k, over: input.items, max_parallel: 2}
`)
	x.start(`{"items":[1,2]}`)
	x.interruptU("m#0", tokens(2))
	x.failU("m#1", false, tokens(3))
	x.tokens(5)

	y := newH(t, `
name: f
nodes:
  - {id: split, type: fanout, branches: [l, r], next: join}
  - {id: l, type: task, kind: k}
  - {id: r, type: task, kind: k}
  - {id: join, type: join, policy: any}
`)
	y.start(`{}`)
	y.failU("l", false, tokens(4))
	y.okWith("r", cmd.CompleteData{Usage: tokens(1)})
	y.tokens(5)
}
