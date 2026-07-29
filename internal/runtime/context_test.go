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
	"slices"
	"testing"
	"time"

	"github.com/henomis/packtrail/internal/store"
	"github.com/henomis/packtrail/pkg/protocol"
)

// capture records the invocation context a task handler was given, so a test can
// assert on what the node actually saw rather than on the final results document
// (which is assembled after the fact and cannot show per-node scoping).
type capture struct {
	got  chan ctxDoc
	name string
}

func newCapture(name string) *capture {
	return &capture{got: make(chan ctxDoc, 4), name: name}
}

func (c *capture) handler(t *testing.T) protocol.Handler {
	t.Helper()

	return func(_ context.Context, req protocol.TaskRequest) (protocol.TaskResponse, error) {
		c.got <- parseCtx(t, req.Payload)

		return protocol.TaskResponse{
			Status:  protocol.StatusOK,
			Payload: json.RawMessage(`{"node":"` + c.name + `"}`),
		}, nil
	}
}

func (c *capture) wait(t *testing.T) ctxDoc {
	t.Helper()

	select {
	case got := <-c.got:
		return got
	case <-time.After(5 * time.Second):
		t.Fatalf("node %q was never invoked", c.name)
		return ctxDoc{}
	}
}

// twoFanFlow runs two fan-outs back to back. The branch ids differ per fan, so
// the second join's successor can be checked for contamination by the first.
const twoFanFlow = `
name: twofan
nodes:
  - {id: fo1, type: fanout, branches: [a1, a2]}
  - {id: a1, type: task, subject: "tasks.a1.{execution_id}"}
  - {id: a2, type: task, subject: "tasks.a2.{execution_id}"}
  - {id: join1, type: fanin, wait_for: [a1, a2], join_policy: all}
  - {id: fo2, type: fanout, branches: [b1, b2]}
  - {id: b1, type: task, subject: "tasks.b1.{execution_id}"}
  - {id: b2, type: task, subject: "tasks.b2.{execution_id}"}
  - {id: join2, type: fanin, wait_for: [b1, b2], join_policy: all}
  - {id: done, type: task, subject: "tasks.done.{execution_id}"}
edges:
  - {from: fo1, to: join1}
  - {from: join1, to: fo2}
  - {from: fo2, to: join2}
  - {from: join2, to: done}
`

// TestBranchesScopedToCurrentFan: the node after the second join sees only the
// second fan's branch outputs.
//
// ex.Branches accumulates across fan-outs — nothing removes a settled fan's
// entries — so copying it wholesale gave `done` all four branch outputs with no
// way to tell this fan's replies from the previous fan's. An invoker that joins
// `branches` into one document (the natural reading) silently included stale
// work from a fan two steps back.
func TestBranchesScopedToCurrentFan(t *testing.T) {
	h := newHarness(t, twoFanFlow, Config{})

	done := newCapture("done")

	for _, b := range []string{"a1", "a2", "b1", "b2"} {
		h.serve(t, "tasks."+b+".*", okBranch(b))
	}

	h.serve(t, "tasks.done.*", done.handler(t))

	id, err := h.engine.Start(context.Background(), "twofan", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	got := done.wait(t)

	branches := make([]string, 0, len(got.Branches))
	for b := range got.Branches {
		branches = append(branches, b)
	}

	slices.Sort(branches)

	if want := []string{"b1", "b2"}; !slices.Equal(branches, want) {
		t.Errorf("done saw branches %v, want %v (the first fan's outputs leaked into the second join)",
			branches, want)
	}

	// The earlier fan is not lost — it is still addressable by node id, which is
	// where a flow that genuinely wants it should look.
	for _, b := range []string{"a1", "a2", "b1", "b2"} {
		if len(got.Results[b]) == 0 {
			t.Errorf("results.%s missing: scoping branches must not hide earlier outputs", b)
		}
	}

	h.waitStatus(t, id, store.StatusCompleted, 5*time.Second)
}

// TestReleasedByNamesTheSignal: the node an `await` released knows which signal
// released it, and a later node does not inherit that.
//
// A signal node contributes no output, so it can never be last_node — before
// released_by an invoker could see the signal's payload sitting in `signals` but
// could not tell whether it had just arrived or had been received ten nodes ago.
func TestReleasedByNamesTheSignal(t *testing.T) {
	h := newHarness(t, signalFlow("24h"), Config{})
	h.serve(t, "tasks.start.*", passthrough)

	after := newCapture("after")
	h.serve(t, "tasks.after.*", after.handler(t))
	h.serve(t, "tasks.fallback.*", passthrough)

	id, err := h.engine.Start(context.Background(), "sig", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	h.waitStatus(t, id, store.StatusWaiting, 5*time.Second)

	if signalErr := h.engine.Signal(
		context.Background(), id, "approval", json.RawMessage(`{"approved":true}`),
	); signalErr != nil {
		t.Fatalf("signal: %v", signalErr)
	}

	got := after.wait(t)
	if got.ReleasedBy != "approval" {
		t.Fatalf("released_by = %q, want approval", got.ReleasedBy)
	}

	// The whole point: the payload is reachable from the name.
	if string(got.Signals[got.ReleasedBy]) != `{"approved":true}` {
		t.Errorf("signals[released_by] = %s, want the signal payload", got.Signals[got.ReleasedBy])
	}

	h.waitStatus(t, id, store.StatusCompleted, 5*time.Second)
}

// TestReleasedByEmptyOnTimeout: an await that timed out routes to on_timeout
// carrying no payload, so that node must not be told a signal released it.
func TestReleasedByEmptyOnTimeout(t *testing.T) {
	h := newHarness(t, signalFlow("50ms"), Config{})
	h.serve(t, "tasks.start.*", passthrough)

	fallback := newCapture("fallback")

	h.serve(t, "tasks.after.*", passthrough)
	h.serve(t, "tasks.fallback.*", fallback.handler(t))

	if _, err := h.engine.Start(context.Background(), "sig", nil); err != nil {
		t.Fatalf("start: %v", err)
	}

	if got := fallback.wait(t); got.ReleasedBy != "" {
		t.Errorf("released_by = %q on the timeout route, want empty: no signal arrived", got.ReleasedBy)
	}
}

func TestCurrentFanBranches(t *testing.T) {
	results := map[string]json.RawMessage{
		"a1": json.RawMessage(`1`), "a2": json.RawMessage(`2`),
		"b1": json.RawMessage(`3`), "b2": json.RawMessage(`4`),
	}

	ex := &store.Execution{Branches: map[string]store.BranchState{
		"a1": {NodeID: "a1", Generation: 2},
		"a2": {NodeID: "a2", Generation: 2},
		"b1": {NodeID: "b1", Generation: 5},
		"b2": {NodeID: "b2", Generation: 5},
	}}

	got := currentFanBranches(ex, results)
	if len(got) != 2 || got["b1"] == nil || got["b2"] == nil {
		t.Fatalf("currentFanBranches = %v, want only the generation-5 fan", got)
	}

	// A branch whose output has not settled is absent rather than null: the map
	// says "this fan's results so far", and a nil entry would read as an output.
	delete(results, "b2")

	if got = currentFanBranches(ex, results); len(got) != 1 || got["b1"] == nil {
		t.Fatalf("currentFanBranches with an unsettled branch = %v, want only b1", got)
	}

	// Documents written before per-branch generations carry 0 and must stay
	// visible, or an execution mid-flight across an upgrade loses its join.
	legacy := &store.Execution{Branches: map[string]store.BranchState{"a1": {NodeID: "a1"}}}
	if got = currentFanBranches(legacy, results); len(got) != 1 {
		t.Fatalf("currentFanBranches(legacy) = %v, want the generation-0 fan included", got)
	}

	if got = currentFanBranches(&store.Execution{}, results); len(got) != 0 {
		t.Fatalf("currentFanBranches(no fan) = %v, want empty", got)
	}
}

func TestReleasedByScopedToGeneration(t *testing.T) {
	ex := &store.Execution{NodeGeneration: 7, ReleasedBy: "approval", ReleasedGeneration: 7}
	if got := releasedBy(ex); got != "approval" {
		t.Errorf("releasedBy at the released node = %q, want approval", got)
	}

	// One node further on: the release is history, not this node's input.
	ex.NodeGeneration = 8
	if got := releasedBy(ex); got != "" {
		t.Errorf("releasedBy at a later node = %q, want empty", got)
	}

	if got := releasedBy(&store.Execution{NodeGeneration: 1}); got != "" {
		t.Errorf("releasedBy with no release = %q, want empty", got)
	}
}
