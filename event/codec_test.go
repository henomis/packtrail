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

package event

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"
)

func samples() []Event {
	raw := json.RawMessage(`{"a":1}`)
	at := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)

	return []Event{
		New(ExecutionStarted, &Started{
			ExecID: "e", Flow: "f", FlowHash: "h", Input: raw,
			Parent: &ParentRef{ExecID: "p", Node: "n", ChildN: 2}, Attrs: map[string]string{"k": "v"},
		}),
		New(ExecutionForked, &Forked{ExecID: "e2", From: "e", Seq: 9, State: raw}),
		New(ExecutionCompleted, &Completed{Output: raw}),
		New(ExecutionFailed, &Failed{Error: "x", Reason: ReasonError, Node: "n", CancelChildren: []string{"c"}}),
		New(ExecutionCancelled, &Cancelled{Reason: "r", CancelChildren: []string{"c"}}),
		New(NodeEntered, &Entered{Node: "n"}),
		New(NodeScheduled, &Scheduled{
			Key: "m#1", Node: "m", Kind: "k", Index: 1, Generation: 3, Attempt: 2,
			Owner: "m", Item: raw, Resume: raw,
		}),
		New(NodeCompleted, &NodeDone{
			Key: "n", Node: "n", Generation: 1, Attempt: 1, Output: raw,
			Writes: map[string]json.RawMessage{"c": raw}, Next: "x", Usage: map[string]float64{"u": 1}, Cached: true,
			CacheKey: "ck",
		}),
		New(NodeFailed, &NodeFail{Key: "n", Node: "n", Generation: 1, Attempt: 1, Error: "e", Reason: "r", WillRetry: true}),
		New(NodeInterrupted, &Interrupted{Key: "n", Node: "n", Generation: 1, Payload: raw}),
		New(NodeCancelled, &NodeCancel{Key: "n", Node: "n", Reason: "r"}),
		New(ChoiceEvaluated, &Choice{Node: "c", Rule: 1, To: "x", Error: "e"}),
		New(FanoutStarted, &Fanout{Node: "f", Join: "j", Branches: []string{"a"}}),
		New(JoinCompleted, &Join{
			Node: "j", Fanout: "f", Succeeded: []string{"a"}, Failed: []string{"b"}, OK: true,
			Output: raw,
		}),
		New(AwaitStarted, &Await{Node: "w", Signal: "s", TimerID: "t1"}),
		New(AwaitTimedOut, &AwaitTimeout{Node: "w", To: "x"}),
		New(SignalReceived, &Signal{Name: "s", ID: "i", Payload: raw}),
		New(SignalConsumed, &SignalUse{Node: "w", Name: "s", ID: "i"}),
		New(MapStarted, &Map{Node: "m", Items: []json.RawMessage{raw}, MaxParallel: 2}),
		New(MapCompleted, &MapDone{Node: "m"}),
		New(MapAborted, &MapAbort{Node: "m", Index: 2}),
		New(ChannelsUpdated, &Update{ID: "u1", Writes: map[string]json.RawMessage{"notes": json.RawMessage(`"x"`)}}),
		New(ExecutionContinued, &Continued{
			State:   json.RawMessage(`{"exec_id":"e"}`),
			Segment: Segment{Object: "e.seg.1", FirstIndex: 1, LastIndex: 9, FirstSeq: 1, LastSeq: 4},
		}),
		New(ChildStarted, &Child{Node: "s", ChildID: "c", ChildN: 1, Flow: "f", Input: raw, Policy: "cancel"}),
		New(ChildCompleted, &ChildDone{
			Node: "s", ChildID: "c", Status: "completed", Output: raw, Error: "e",
			Counters: map[string]float64{"u": 2},
		}),
		New(TimerScheduled, &Timer{
			ID: "t1", N: 1, At: at, Purpose: TimerRetry, Key: "n", Node: "n", Generation: 1,
			Attempt: 2,
		}),
		New(TimerFired, &Fired{ID: "t1"}),
	}
}

func TestRoundTripEveryType(t *testing.T) {
	seen := map[Type]bool{}

	for _, ev := range samples() {
		ev.Time = time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
		ev.CmdID = "cmd-1"
		ev.Index = 7

		body, err := Encode(ev)
		if err != nil {
			t.Fatal(err)
		}

		got, err := Decode(body)
		if err != nil {
			t.Fatalf("%s: %v", ev.Type, err)
		}

		if !reflect.DeepEqual(got, ev) {
			t.Fatalf("%s: round trip\n got %#v\nwant %#v", ev.Type, got, ev)
		}

		seen[ev.Type] = true
	}

	for _, ty := range []Type{
		ExecutionStarted, ExecutionForked, ExecutionContinued, ExecutionCompleted, ExecutionFailed,
		ExecutionCancelled, NodeEntered, NodeScheduled, NodeCompleted, NodeFailed, NodeInterrupted, NodeCancelled,
		ChoiceEvaluated, FanoutStarted, JoinCompleted, AwaitStarted, AwaitTimedOut, SignalReceived, SignalConsumed,
		MapStarted, MapCompleted, MapAborted, ChannelsUpdated, ChildStarted, ChildCompleted, TimerScheduled, TimerFired,
	} {
		if !seen[ty] {
			t.Errorf("no round-trip sample for %s", ty)
		}
	}
}

func TestDecodeRejectsUnknown(t *testing.T) {
	for _, body := range []string{
		`{"type":"Nope","v":1,"data":{}}`,
		`{"type":"NodeEntered","v":2,"data":{}}`,
		`not json`,
	} {
		if _, err := Decode([]byte(body)); err == nil {
			t.Errorf("Decode(%s) accepted", body)
		}
	}

	if _, err := Decode([]byte(`{"type":"Nope","v":1}`)); !errors.Is(err, ErrUnknown) {
		t.Fatalf("err = %v, want ErrUnknown", err)
	}
}

// TestDecisionRoundTrip: a decision is one message holding its events in
// order; the headers list the types and carry the command id.
func TestDecisionRoundTrip(t *testing.T) {
	evs := samples()[:3]
	for i := range evs {
		evs[i].Time = time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
		evs[i].CmdID = "cmd-1"
		evs[i].Index = i + 1
	}

	body, h, err := EncodeDecision(evs)
	if err != nil {
		t.Fatal(err)
	}

	want := []Type{evs[0].Type, evs[1].Type, evs[2].Type}
	if got := Types(h); !reflect.DeepEqual(got, want) || h.Get(HeaderCmdID) != "cmd-1" {
		t.Fatalf("headers %v", h)
	}

	got, err := DecodeDecision(body)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(got, evs) {
		t.Fatalf("round trip\n got %#v\nwant %#v", got, evs)
	}

	if _, _, err = EncodeDecision(nil); err == nil {
		t.Fatal("an empty decision must not encode")
	}

	for _, bad := range []string{`[]`, `{"type":"NodeEntered"}`, `[{"type":"Nope","v":1}]`} {
		if _, err = DecodeDecision([]byte(bad)); err == nil {
			t.Fatalf("%s: decoded", bad)
		}
	}
}
