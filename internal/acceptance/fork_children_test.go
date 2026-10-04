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

package acceptance

import (
	"context"
	"testing"
	"time"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/event"
)

// forking a fan-out of two subflow branches with writes that make
// a branch's input invalid must yield a failed fork (expression_error), not
// a Fork call that never returns.
func TestForkWithBadBranchInputFails(t *testing.T) {
	e := NewEnv(t, []string{`
name: ff
channels:
  cfg: {default: {a: 1}}
nodes:
  - {id: f, type: fanout, branches: [b1, b2], next: j}
  - {id: b1, type: subflow, flow: kid, input: "channels.cfg"}
  - {id: b2, type: subflow, flow: kid, input: "channels.cfg"}
  - {id: j, type: join}
`, `
name: kid
nodes:
  - {id: w, type: await, signal: never, timeout: 1h}
`})

	id := e.Start("ff", nil)
	e.Eventually(func() bool {
		st, err := e.Client.Get(e.Ctx, id)

		return err == nil && len(st.Children) == 2
	}, func() string { return "children never started" })

	evs, err := e.Client.History(e.Ctx, id)
	if err != nil {
		t.Fatal(err)
	}

	var seq uint64

	for _, ev := range evs {
		if ev.Type == event.ChildStarted {
			seq = ev.Seq
		}
	}

	ctx, cancel := context.WithTimeout(e.Ctx, 10*time.Second)
	defer cancel()

	fork, err := e.Client.Fork(ctx, id, seq, packtrail.WithForkWrites(map[string]any{"cfg": 5}))
	if err != nil {
		t.Fatalf("Fork: %v", err)
	}

	st := e.Wait(fork)
	if st.Status != packtrail.StatusFailed || st.Reason != event.ReasonExpression {
		t.Fatalf("fork %s: %s %s", fork, st.Status, st.Reason)
	}
}

// a fork that fails while re-starting its children must leave
// the source execution alone.
func TestFailedForkLeavesSourceChildren(t *testing.T) {
	e := NewEnv(t, []string{`
name: ff
channels:
  cfg: {default: {a: 1}}
nodes:
  - {id: f, type: fanout, branches: [b1, b2], next: j}
  - {id: b1, type: subflow, flow: kid, input: "{'x': 1}"}
  - {id: b2, type: subflow, flow: kid, input: "channels.cfg"}
  - {id: j, type: join}
`, `
name: kid
nodes:
  - {id: w, type: await, signal: never, timeout: 1h}
`})

	id := e.Start("ff", nil)

	var src *packtrail.State

	e.Eventually(func() bool {
		var err error

		src, err = e.Client.Get(e.Ctx, id)

		return err == nil && len(src.Children) == 2
	}, func() string { return "children never started" })

	evs, err := e.Client.History(e.Ctx, id)
	if err != nil {
		t.Fatal(err)
	}

	var seq uint64

	for _, ev := range evs {
		if ev.Type == event.ChildStarted {
			seq = ev.Seq
		}
	}

	fork, err := e.Client.Fork(e.Ctx, id, seq, packtrail.WithForkWrites(map[string]any{"cfg": 5}))
	if err != nil {
		t.Fatalf("Fork: %v", err)
	}

	if st := e.Wait(fork); st.Status != packtrail.StatusFailed {
		t.Fatalf("fork %s", st.Status)
	}

	b2 := src.Children["b2"].ChildID

	time.Sleep(time.Second)

	kid, err := e.Client.Get(e.Ctx, b2)
	if err != nil {
		t.Fatal(err)
	}

	srcNow, err := e.Client.Get(e.Ctx, id)
	if err != nil {
		t.Fatal(err)
	}

	if kid.Status != packtrail.StatusWaiting || srcNow.Status.Terminal() {
		t.Fatalf("source %s's child %s is %s (%s); source is now %s (%s: %s)", id, b2, kid.Status, kid.Reason,
			srcNow.Status, srcNow.Reason, srcNow.Error)
	}
}
