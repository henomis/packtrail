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
	"testing"

	"github.com/henomis/packtrail/event"
	"github.com/henomis/packtrail/internal/cmd"
)

func (x *h) interrupt(key, payload string) { //nolint:unparam // the key reads better at call sites.
	x.t.Helper()

	t := x.task(key)
	x.do(cmd.Interrupt, cmd.InterruptData{
		TaskRef: cmd.TaskRef{Key: key, Generation: t.Generation, Attempt: t.Attempt},
		Payload: json.RawMessage(payload),
	})
}

// seen is the interrupt payload the job of task "a" would carry.
func (x *h) seen() string {
	x.t.Helper()

	return string(x.st.ContextView(x.task("a")).Interrupt)
}

// A resumed job sees the payload it interrupted with, on every attempt; a
// second interrupt replaces it; a job never interrupted carries none.
func TestInterruptPayloadOnResume(t *testing.T) {
	x := newH(t, retrying)
	x.start(`{}`)

	if x.seen() != "" {
		t.Fatal("fresh job carries an interrupt payload")
	}

	x.interrupt("a", `{"q":1}`)
	x.do(cmd.Resume, cmd.ResumeData{Node: "a", Value: json.RawMessage(`"r1"`)})

	if got := x.seen(); got != `{"q":1}` {
		t.Fatalf("resumed job interrupt = %q", got)
	}

	// Retried after a failure, then after a timeout.
	x.failT("a", true)
	x.fire(event.TimerRetry)

	for id, tm := range x.st.Timers {
		if tm.Purpose == event.TimerTimeout && tm.Attempt == 2 {
			x.do(cmd.Timer, cmd.TimerData{TimerID: id})
		}
	}

	x.fire(event.TimerRetry)

	if a := x.task("a"); a.Attempt != 3 || x.seen() != `{"q":1}` {
		t.Fatalf("retry of a resumed job: attempt %d, interrupt %q", a.Attempt, x.seen())
	}

	x.interrupt("a", `{"q":2}`)
	x.do(cmd.Resume, cmd.ResumeData{Node: "a", Value: json.RawMessage(`"r2"`)})

	if got := x.seen(); got != `{"q":2}` {
		t.Fatalf("second resume interrupt = %q", got)
	}

	x.ok("a", `{}`)
	x.status(StatusCompleted)
}

// A fork taken while paused, then resumed, and a fork of a resumed run both
// hand the payload to the job; a fork before the interrupt hands none.
func TestInterruptPayloadAcrossFork(t *testing.T) {
	x := newH(t, linear)
	x.start(`{}`)

	before, _ := x.st.Clone()

	x.interrupt("a", `{"q":1}`)
	paused, _ := x.st.Clone()

	x.do(cmd.Resume, cmd.ResumeData{Node: "a", Value: json.RawMessage(`"yes"`)})
	resumed, _ := x.st.Clone()

	// The fork is driven as e1 (the harness's id), forked from "src".
	fork := func(src *State) *h {
		t.Helper()

		st := New("e1")

		evs, err := DecideFork(x.def, st, src, "src", 9, "fork-"+string(src.Status), nil, t0)
		if err != nil {
			t.Fatal(err)
		}

		y := &h{t: t, def: x.def, st: st, log: evs, now: t0, n: 100}
		y.checkFold()

		return y
	}

	if got := fork(before).seen(); got != "" {
		t.Fatalf("fork before the interrupt: %q", got)
	}

	if got := fork(resumed).seen(); got != `{"q":1}` {
		t.Fatalf("fork of a resumed run: interrupt = %q", got)
	}

	y := fork(paused)
	y.do(cmd.Resume, cmd.ResumeData{Node: "a", Value: json.RawMessage(`"yes"`)})

	if got := y.seen(); got != `{"q":1}` {
		t.Fatalf("fork resumed: interrupt = %q", got)
	}
}

// A resume without a value (Client.Resume with nil, the dashboard's empty
// resume) is still a resume: the job sees the interrupt payload and a null
// resume value.
func TestResumeWithoutValueKeepsInterruptPayload(t *testing.T) {
	x := newH(t, linear)
	x.start(`{}`)
	x.interrupt("a", `{"q":1}`)
	x.do(cmd.Resume, cmd.ResumeData{Node: "a"})

	if a := x.task("a"); a.Status != TaskScheduled {
		t.Fatalf("not resumed: %s", a.Status)
	}

	if got := x.seen(); got != `{"q":1}` {
		t.Fatalf("resumed job interrupt = %q, want {\"q\":1}", got)
	}

	if got := string(x.st.ContextView(x.task("a")).Resume); got != "null" {
		t.Fatalf("resumed job resume = %q, want null", got)
	}
}
