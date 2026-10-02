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
	"testing"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/event"
)

const g507Loop = `
name: loop
retention: 2s
start: step
nodes:
  - {id: step, type: task, kind: echo, next: again}
  - {id: again, type: choice, rules: [{when: "visits.step < 30", to: step}, {default: true, to: done}]}
  - {id: done, type: await, signal: finish, timeout: 1h}
`

// TestG507ContinueAsNewBoundsTheLog: a looping execution's live log stays
// bounded by the history limit, while its id, behaviour and full history are
// unchanged: History, OutputHistory, StateAt and Fork before the live log,
// a signal after several continuations, and the archive (G5-07).
func TestG507ContinueAsNewBoundsTheLog(t *testing.T) {
	e := NewEnv(t, []string{g507Loop}, packtrail.WithHistoryLimit(20))
	e.Worker("echo", Echo)

	id := e.Start("loop", nil, packtrail.WithExecutionID("loop-1"))
	e.WaitStatus(id, packtrail.StatusWaiting)

	// The live log is bounded.
	events, err := e.S.JS.Stream(e.Ctx, "packtrail-events")
	if err != nil {
		t.Fatal(err)
	}

	subject := attach(t, e).EventSubject(id)

	info, err := events.Info(e.Ctx, jetstream.WithSubjectFilter(subject))
	if err != nil {
		t.Fatal(err)
	}

	if live := info.State.Subjects[subject]; live == 0 || live > 10 {
		t.Fatalf("live log holds %d decisions", live)
	}

	// The full history is still there, across the segments.
	evs, err := e.Client.History(e.Ctx, id)
	if err != nil {
		t.Fatal(err)
	}

	if n := countType(evs, event.ExecutionContinued); n < 3 {
		t.Fatalf("%d continuations, want several", n)
	}

	if c := countEvents(t, e, id, event.NodeCompleted, "step"); c != 30 {
		t.Fatalf("history holds %d step completions, want 30", c)
	}

	if outs, oerr := e.Client.OutputHistory(e.Ctx, id, "step"); oerr != nil || len(outs) != 30 {
		t.Fatalf("output history: %d %v", len(outs), oerr)
	}

	// Time travel and fork before the live log.
	var first uint64

	for _, ev := range evs {
		if d, ok := ev.Data.(*event.NodeDone); ok && d.Node == "step" && first == 0 {
			first = ev.Seq
		}
	}

	// The decision that completed step the first time also entered it again.
	st, err := e.Client.StateAt(e.Ctx, id, first)
	if err != nil || st.Visits["step"] != 2 || st.Results["step"] == nil || st.Status.Terminal() {
		t.Fatalf("state at %d: %+v %v", first, st, err)
	}

	fork, err := e.Client.Fork(e.Ctx, id, first, packtrail.WithForkID("loop-fork"))
	if err != nil {
		t.Fatal(err)
	}

	if fst := e.WaitStatus(fork, packtrail.StatusWaiting); fst.Visits["step"] != 30 || fst.ForkedFrom != id {
		t.Fatalf("fork visits %d from %q", fst.Visits["step"], fst.ForkedFrom)
	}

	// A signal reaches the execution after several continuations.
	if err = e.Client.Signal(e.Ctx, id, "finish", nil); err != nil {
		t.Fatal(err)
	}

	final := e.Completed(id)
	if final.Visits["step"] != 30 || final.ExecID != id {
		t.Fatalf("final %+v", final)
	}

	// Archived, the history still includes the segments.
	e.Eventually(func() bool {
		s, gerr := e.Client.Get(e.Ctx, id)

		return gerr == nil && s.Archived
	}, func() string { return "never archived" })

	if c := countEvents(t, e, id, event.NodeCompleted, "step"); c != 30 {
		t.Fatalf("archived history holds %d step completions, want 30", c)
	}
}

func countType(evs []event.Event, typ event.Type) int {
	n := 0

	for _, ev := range evs {
		if ev.Type == typ {
			n++
		}
	}

	return n
}

// TestG507ContinueAcrossEngineFailover: continuations under concurrent and
// restarted engines neither lose nor duplicate history (G5-07).
func TestG507ContinueAcrossEngineFailover(t *testing.T) {
	e := NewEnv(t, []string{g507Loop}, packtrail.WithHistoryLimit(15))
	e.StartEngine()
	e.Worker("echo", Echo)

	id := e.Start("loop", nil)

	e.Eventually(func() bool {
		st, err := e.Client.Get(e.Ctx, id)

		return err == nil && st.Visits["step"] >= 10
	}, func() string { return "loop did not progress" })

	e.RestartEngine()
	e.StartEngine()

	e.WaitStatus(id, packtrail.StatusWaiting)

	if err := e.Client.Signal(e.Ctx, id, "finish", nil); err != nil {
		t.Fatal(err)
	}

	e.Completed(id)

	evs, err := e.Client.History(e.Ctx, id)
	if err != nil {
		t.Fatal(err)
	}

	for i := 1; i < len(evs); i++ {
		if evs[i].Index != evs[i-1].Index+1 {
			t.Fatalf("history index %d after %d (%s): lost or duplicated", evs[i].Index, evs[i-1].Index, evs[i].Type)
		}
	}

	if c := countEvents(t, e, id, event.NodeCompleted, "step"); c != 30 {
		t.Fatalf("history holds %d step completions, want 30", c)
	}
}
