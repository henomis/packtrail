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
	"slices"
	"testing"

	"github.com/henomis/packtrail/event"
	"github.com/henomis/packtrail/internal/cmd"
)

// child returns the running child instance key.
func (x *h) child(key string) *ChildState {
	x.t.Helper()

	c := x.st.Children[key]
	if c == nil {
		x.t.Fatalf("child %q not running; children=%v status=%s", key, keys(x.st.Children), x.st.Status)
	}

	return c
}

// childEnds reports the end of child instance key to the parent.
func (x *h) childEnds(key, status, output string) []event.Event {
	x.t.Helper()

	c := x.child(key)

	return x.do(cmd.ChildDone, cmd.ChildDoneData{
		ChildID: c.ChildID, Status: status, Output: json.RawMessage(output), Error: "child " + status,
	})
}

func lastOf[T any](x *h, typ event.Type) *T {
	x.t.Helper()

	for i := len(x.log) - 1; i >= 0; i-- {
		if x.log[i].Type == typ {
			return x.log[i].Data.(*T) //nolint:forcetypeassert // the type matches the event type.
		}
	}

	x.t.Fatalf("no %s event", typ)

	return nil
}

// childFan: a task branch and two subflow branches, one abandoned.
const childFan = `
name: cf
nodes:
  - {id: f, type: fanout, branches: [a, b, c], next: j}
  - {id: a, type: task, kind: k}
  - {id: b, type: subflow, flow: child, input: "{'who': 'b'}"}
  - {id: c, type: subflow, flow: child, on_parent_close: abandon}
  - {id: j, type: join, policy: %s, next: z}
  - {id: z, type: task, kind: k}
`

func TestSubflowBranchesAll(t *testing.T) {
	x := newH(t, fmt.Sprintf(childFan, "all"))
	x.start(`{"q":1}`)

	b, c := x.child("b"), x.child("c")
	if b.Owner != "f" || c.Owner != "f" || b.Node != "b" || c.Policy != "abandon" {
		t.Fatalf("branch children %+v %+v", b, c)
	}

	if in := lastOf[event.Child](x, event.ChildStarted); in.Node != "c" || string(in.Input) != `{"q":1}` {
		t.Fatalf("c started with %s", in.Input)
	}

	x.ok("a", `{"a":1}`)
	x.childEnds("b", "completed", `{"b":1}`)

	if _, open := x.st.Tasks["z"]; open {
		t.Fatal("join settled with a branch still running")
	}

	x.childEnds("c", "completed", `{"c":1}`)
	x.task("z")

	join := lastOf[event.Join](x, event.JoinCompleted)
	if !join.OK || len(join.CancelChildren) != 0 || !slices.Equal(join.Succeeded, []string{"a", "b", "c"}) {
		t.Fatalf("join %+v", join)
	}

	if string(x.st.Results["b"]) != `{"b":1}` || string(x.st.Results["j"]) != `{"a":{"a":1},"b":{"b":1},"c":{"c":1}}` {
		t.Fatalf("results %v", x.st.Results)
	}
}

func TestSubflowBranchesAnyCancelsTheRest(t *testing.T) {
	x := newH(t, fmt.Sprintf(childFan, "any"))
	x.start(`{}`)

	c := x.child("c").ChildID

	evs := x.childEnds("b", "completed", `{"b":1}`)
	if got := types(evs); !slices.Equal(got, []event.Type{
		event.ChildCompleted, event.NodeCancelled, event.NodeEntered, event.JoinCompleted, event.NodeEntered,
		event.NodeScheduled,
	}) {
		t.Fatalf("events %v", got)
	}

	// The task branch is cancelled, the cancel-policy child is cancelled, the
	// abandoned one is left running but forgotten.
	if join := lastOf[event.Join](x, event.JoinCompleted); len(join.CancelChildren) != 0 {
		t.Fatalf("join cancels %v", join.CancelChildren)
	}

	if len(x.st.Children) != 0 || x.st.Branches["c"] != BranchCancelled || x.st.Branches["b"] != BranchCompleted {
		t.Fatalf("children %v branches %v", keys(x.st.Children), x.st.Branches)
	}

	// The abandoned child's late end changes nothing.
	if evs = x.do(cmd.ChildDone, cmd.ChildDoneData{ChildID: c, Status: "completed"}); len(evs) != 0 {
		t.Fatalf("late child end produced %v", types(evs))
	}

	// With the cancel policy on the losing child, the join cancels it.
	y := newH(t, `
name: cf
nodes:
  - {id: f, type: fanout, branches: [b, c], next: j}
  - {id: b, type: subflow, flow: child}
  - {id: c, type: subflow, flow: child}
  - {id: j, type: join, policy: any}
`)
	y.start(`{}`)

	loser := y.child("c").ChildID

	y.childEnds("b", "completed", `{}`)
	y.status(StatusCompleted)

	if join := lastOf[event.Join](y, event.JoinCompleted); !slices.Equal(join.CancelChildren, []string{loser}) {
		t.Fatalf("join cancels %v, want %s", join.CancelChildren, loser)
	}
}

func TestSubflowBranchFailure(t *testing.T) {
	x := newH(t, fmt.Sprintf(childFan, "all"))
	x.start(`{}`)

	running := x.child("c").ChildID

	x.childEnds("b", "failed", ``)
	x.status(StatusFailed)

	if x.st.Reason != event.ReasonJoin || x.st.Errors["b"].Reason != event.ReasonChild {
		t.Fatalf("reason %s, errors %v", x.st.Reason, x.st.Errors)
	}

	// c is abandoned: neither the join nor the failure cancels it.
	if f := lastOf[event.Failed](x, event.ExecutionFailed); slices.Contains(f.CancelChildren, running) {
		t.Fatalf("abandoned child cancelled: %v", f.CancelChildren)
	}

	// quorum:2 of three survives one failed child.
	y := newH(t, fmt.Sprintf(childFan, "'quorum:2'"))
	y.start(`{}`)
	y.childEnds("b", "failed", ``)
	y.ok("a", `{}`)
	y.childEnds("c", "completed", `{}`)
	y.task("z")
}

const childMap = `
name: cm
start: m
nodes:
  - {id: m, type: map, flow: child, over: input.items, max_parallel: 2, %s next: z}
  - {id: z, type: task, kind: k}
%s`

// mapHandler is the on_failure target of childMap.
const mapHandler = "  - {id: h, type: task, kind: k}\n"

func TestMapOfSubflows(t *testing.T) {
	x := newH(t, fmt.Sprintf(childMap, `input: "{'n': item, 'i': index}",`, ""))
	x.start(`{"items":[10,20,30]}`)

	if keys(x.st.Children)[0] != "m#0" || len(x.st.Children) != 2 {
		t.Fatalf("children %v", keys(x.st.Children))
	}

	if c := x.child("m#1"); c.Owner != "m" || c.Node != "m" || c.Index != 1 {
		t.Fatalf("item child %+v", c)
	}

	started := lastOf[event.Child](x, event.ChildStarted)
	if started.Key != "m#1" || string(started.Input) != `{"i":1,"n":20}` {
		t.Fatalf("item 1 started as %+v", started)
	}

	// Out of order: item 1 first frees a slot for item 2.
	x.childEnds("m#1", "completed", `{"r":20}`)
	x.child("m#2")
	x.childEnds("m#2", "completed", `{"r":30}`)
	x.childEnds("m#0", "completed", `{"r":10}`)
	x.task("z")

	if string(x.st.Results["m"]) != `[{"r":10},{"r":20},{"r":30}]` || len(x.st.Children) != 0 {
		t.Fatalf("map result %s", x.st.Results["m"])
	}
}

func TestMapOfSubflowsInput(t *testing.T) {
	// Object items are the input as they are.
	x := newH(t, fmt.Sprintf(childMap, "", ""))
	x.start(`{"items":[{"a":1}]}`)

	if in := lastOf[event.Child](x, event.ChildStarted); string(in.Input) != `{"a":1}` {
		t.Fatalf("input %s", in.Input)
	}

	// Other items need an input expression.
	y := newH(t, fmt.Sprintf(childMap, "", ""))
	y.start(`{"items":[1]}`)
	y.status(StatusFailed)

	if y.st.Reason != event.ReasonExpression {
		t.Fatalf("reason %s: %s", y.st.Reason, y.st.Error)
	}
}

func TestMapOfSubflowsFailure(t *testing.T) {
	// With on_failure: the running items are cancelled, the map routes.
	x := newH(t, fmt.Sprintf(childMap, `input: "{'n': item}", on_failure: h,`, mapHandler))
	x.start(`{"items":[1,2,3]}`)

	other := x.child("m#1").ChildID

	evs := x.childEnds("m#0", "failed", ``)
	if got := types(evs); !slices.Equal(got, []event.Type{
		event.ChildCompleted, event.MapAborted, event.NodeEntered, event.NodeScheduled,
	}) {
		t.Fatalf("events %v", got)
	}

	if a := lastOf[event.MapAbort](x, event.MapAborted); a.Index != 0 || !slices.Equal(a.CancelChildren, []string{other}) {
		t.Fatalf("abort %+v", a)
	}

	if ne := x.st.Errors["m"]; ne.Index == nil || *ne.Index != 0 || ne.Reason != event.ReasonChild {
		t.Fatalf("errors.m = %+v", ne)
	}

	if evs = x.do(cmd.ChildDone, cmd.ChildDoneData{ChildID: other, Status: "completed"}); len(evs) != 0 {
		t.Fatalf("cancelled item's end produced %v", types(evs))
	}

	x.task("h")

	// Without: the execution fails and cancels the items still running.
	y := newH(t, fmt.Sprintf(childMap, `input: "{'n': item}",`, ""))
	y.start(`{"items":[1,2,3]}`)

	other = y.child("m#1").ChildID

	y.childEnds("m#0", "failed", ``)
	y.status(StatusFailed)

	if f := lastOf[event.Failed](y, event.ExecutionFailed); y.st.Reason != event.ReasonChild ||
		!slices.Equal(f.CancelChildren, []string{other}) {
		t.Fatalf("reason %s, cancels %v", y.st.Reason, f.CancelChildren)
	}
}

func TestForkRestartsBranchAndItemChildren(t *testing.T) {
	for name, yaml := range map[string]string{
		"branch": fmt.Sprintf(childFan, "all"),
		"item":   fmt.Sprintf(childMap, `input: "{'n': item}",`, ""),
	} {
		t.Run(name, func(t *testing.T) {
			x := newH(t, yaml)
			x.start(`{"items":[1,2]}`)

			src, _ := x.st.Clone()
			fork := New("e2")

			evs, err := DecideFork(x.def, fork, src, "e1", 9, "fork-1", nil, t0)
			if err != nil {
				t.Fatal(err)
			}

			if len(fork.Children) != len(x.st.Children) {
				t.Fatalf("fork children %v, source %v", keys(fork.Children), keys(x.st.Children))
			}

			for key, c := range fork.Children {
				was := x.st.Children[key]
				if c.ChildID == was.ChildID || c.Owner != was.Owner || c.Index != was.Index || c.Node != was.Node {
					t.Fatalf("%s: fork child %+v, source %+v", key, c, was)
				}
			}

			re, err := Fold(x.def, "e2", evs)
			if err != nil {
				t.Fatal(err)
			}

			a, _ := json.Marshal(re)
			b, _ := json.Marshal(fork)

			if string(a) != string(b) {
				t.Fatal("fold(fork events) != fork state")
			}
		})
	}
}

// a fork whose writes make a re-started branch child's input
// invalid must fail the fork with expression_error, not panic in the fold.
const forkFan = `
name: ff
channels:
  cfg: {default: {a: 1}}
nodes:
  - {id: f, type: fanout, branches: [b1, b2], next: j}
  - {id: b1, type: subflow, flow: child, input: "%s"}
  - {id: b2, type: subflow, flow: child, input: "%s"}
  - {id: j, type: join}
`

func forkWith(t *testing.T, yaml string, writes map[string]json.RawMessage) (*h, *State, []event.Event, error) {
	t.Helper()

	x := newH(t, yaml)
	x.start(`{}`)

	src, _ := x.st.Clone()
	fork := New("e2")

	var (
		evs []event.Event
		err error
	)

	func() {
		defer func() {
			if p := recover(); p != nil {
				t.Fatalf("DecideFork panicked: %v", p)
			}
		}()

		evs, err = DecideFork(x.def, fork, src, "e1", 9, "fork-1", writes, t0)
	}()

	return x, fork, evs, err
}

func TestForkFailingBranchChildFailsTheFork(t *testing.T) {
	y := fmt.Sprintf(forkFan, "channels.cfg", "channels.cfg")
	_, fork, _, err := forkWith(t, y, map[string]json.RawMessage{"cfg": json.RawMessage(`5`)})

	if err != nil || fork.Status != StatusFailed || fork.Reason != event.ReasonExpression {
		t.Fatalf("fork: err %v status %s reason %s", err, fork.Status, fork.Reason)
	}
}

// the failure of a fork must not cancel the SOURCE's children.
func TestFailedForkKeepsSourceChildren(t *testing.T) {
	y := `
name: ff
channels:
  cfg: {default: {a: 1}}
nodes:
  - {id: f, type: fanout, branches: [b1, b2], next: j}
  - {id: b1, type: subflow, flow: child, input: "{'x': 1}"}
  - {id: b2, type: subflow, flow: child, input: "channels.cfg"}
  - {id: j, type: join}
`

	x, fork, evs, err := forkWith(t, y, map[string]json.RawMessage{"cfg": json.RawMessage(`5`)})
	if err != nil || fork.Status != StatusFailed {
		t.Fatalf("fork: err %v status %s", err, fork.Status)
	}

	failed := evs[len(evs)-1].Data.(*event.Failed) //nolint:forcetypeassert // last event of a failed fork.

	for _, c := range x.st.Children { // the source's (e1) running children
		for _, id := range failed.CancelChildren {
			if id == c.ChildID {
				t.Fatalf("fork e2's ExecutionFailed cancels the source's child %s (cancel list %v)",
					id, failed.CancelChildren)
			}
		}
	}
}
