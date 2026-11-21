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

// recordOf returns the record carried by an attempt-ending event.
func recordOf(ev event.Event) (json.RawMessage, bool) {
	switch d := ev.Data.(type) {
	case *event.NodeDone:
		return d.Record, true
	case *event.NodeFail:
		return d.Record, true
	case *event.Interrupted:
		return d.Record, true
	default:
		return nil, false
	}
}

// A record rides on the event of the attempt it belongs to (a failure, an
// invalid output, an interrupt, a completion) and is never folded: the state
// (and so the snapshot) is the same with and without records. A stale result
// keeps nothing.
func TestRecordIsCarriedNotFolded(t *testing.T) {
	run := func(withRecord bool) (*h, []json.RawMessage) {
		x := newH(t, usageFlow(100))
		x.start(`{}`)

		n := 0
		rec := func() json.RawMessage {
			if !withRecord {
				return nil
			}

			n++

			return json.RawMessage(fmt.Sprintf(`{"call":%d}`, n))
		}

		ref := func() cmd.TaskRef {
			t := x.task("a")

			return cmd.TaskRef{Key: "a", Generation: t.Generation, Attempt: t.Attempt}
		}

		stale := ref()

		x.do(cmd.Fail, cmd.FailData{TaskRef: ref(), Error: "boom", Retryable: true, Record: rec()})
		x.fire(event.TimerRetry)

		if evs := x.do(cmd.Fail, cmd.FailData{TaskRef: stale, Error: "late", Retryable: true, Record: rec()}); len(evs) != 0 {
			t.Fatalf("stale failure accepted: %v", types(evs))
		}

		x.okWith("a", cmd.CompleteData{Output: json.RawMessage(`{"nope":1}`), Record: rec()}) // invalid output
		x.fire(event.TimerRetry)
		x.do(cmd.Interrupt, cmd.InterruptData{TaskRef: ref(), Payload: json.RawMessage(`{"q":1}`), Record: rec()})
		x.do(cmd.Resume, cmd.ResumeData{Node: "a", Value: json.RawMessage(`"go"`)})
		x.okWith("a", cmd.CompleteData{Output: json.RawMessage(`{"text":"ok"}`), Record: rec()})
		x.status(StatusCompleted)

		var got []json.RawMessage

		for _, ev := range x.log {
			if r, ok := recordOf(ev); ok {
				got = append(got, r)
			}
		}

		return x, got
	}

	with, recs := run(true)
	without, none := run(false)

	a, _ := json.Marshal(with.st)
	b, _ := json.Marshal(without.st)

	if string(a) != string(b) {
		t.Fatalf("records changed the state\nwith:    %s\nwithout: %s", a, b)
	}

	// The stale failure (call 2) left nothing.
	want := []string{`{"call":1}`, `{"call":3}`, `{"call":4}`, `{"call":5}`}
	if len(recs) != len(want) {
		t.Fatalf("records %s, want %v", recs, want)
	}

	for i, r := range recs {
		if string(r) != want[i] {
			t.Fatalf("record %d = %s, want %s", i, r, want[i])
		}
	}

	for i, r := range none {
		if r != nil {
			t.Fatalf("event %d has record %s without one set", i, r)
		}
	}
}

// A cached completion carries no record (no worker ran), and a null record is
// no record.
func TestRecordNotOnCachedOrNull(t *testing.T) {
	for name, data := range map[string]cmd.CompleteData{
		"cached": {Output: json.RawMessage(`{"text":"ok"}`), Cached: true, Record: json.RawMessage(`{"x":1}`)},
		"null":   {Output: json.RawMessage(`{"text":"ok"}`), Record: json.RawMessage(`null`)},
	} {
		t.Run(name, func(t *testing.T) {
			x := newH(t, usageFlow(100))
			x.start(`{}`)

			for _, ev := range x.okWith("a", data) {
				if r, ok := recordOf(ev); ok && r != nil {
					t.Fatalf("%s carries record %s", ev.Type, r)
				}
			}
		})
	}
}
