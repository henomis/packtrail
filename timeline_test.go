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

package packtrail_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/event"
)

func TestBuildTimeline(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	at := func(s int) time.Time { return t0.Add(time.Duration(s) * time.Second) }
	ev := func(s int, typ event.Type, data any) event.Event { return event.Event{Type: typ, Time: at(s), Data: data} }

	evs := []event.Event{
		{Type: event.ExecutionStarted, Time: at(0), Trace: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
			Data: &event.Started{ExecID: "x", Flow: "f"}},
		// a: fails once, then completes; the worker reported when it started.
		ev(0, event.NodeScheduled, &event.Scheduled{Key: "a", Node: "a", Kind: "k", Generation: 1, Attempt: 1}),
		ev(2, event.NodeFailed, &event.NodeFail{Key: "a", Node: "a", Generation: 1, Attempt: 1, Error: "boom",
			Reason: "error", WillRetry: true, Started: at(1)}),
		ev(3, event.NodeScheduled, &event.Scheduled{Key: "a", Node: "a", Kind: "k", Generation: 1, Attempt: 2}),
		ev(6, event.NodeCompleted, &event.NodeDone{Key: "a", Node: "a", Generation: 1, Attempt: 2,
			Started: at(4), Usage: map[string]float64{"tokens": 7}, Summary: json.RawMessage(`{"tools":1}`)}),
		// h: interrupts, then its resume completes from the cache.
		ev(6, event.NodeScheduled, &event.Scheduled{Key: "h", Node: "h", Kind: "k", Generation: 2, Attempt: 1}),
		ev(7, event.NodeInterrupted, &event.Interrupted{Key: "h", Node: "h", Generation: 2}),
		ev(9, event.NodeScheduled, &event.Scheduled{Key: "h", Node: "h", Kind: "k", Generation: 3, Attempt: 1}),
		ev(9, event.NodeCompleted, &event.NodeDone{Key: "h", Node: "h", Generation: 3, Attempt: 1, Cached: true}),
		// w waits for a signal; s runs a subflow that fails.
		ev(10, event.AwaitStarted, &event.Await{Node: "w", Signal: "go"}),
		ev(12, event.SignalConsumed, &event.SignalUse{Node: "w", Name: "go"}),
		ev(12, event.ChildStarted, &event.Child{Node: "s", ChildID: "x-s"}),
		ev(15, event.ChildCompleted, &event.ChildDone{Node: "s", ChildID: "x-s", Status: "failed", Error: "no"}),
		// c is still running when the execution is cancelled.
		ev(15, event.NodeScheduled, &event.Scheduled{Key: "c", Node: "c", Kind: "k", Generation: 4, Attempt: 1}),
		ev(16, event.ExecutionCancelled, &event.Cancelled{Reason: "stop"}),
	}

	tl := packtrail.BuildTimeline(evs)
	if tl.ExecID != "x" || tl.Flow != "f" || tl.Status != packtrail.StatusCancelled || !tl.Ended.Equal(at(16)) ||
		tl.TraceID != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Fatalf("timeline %+v", tl)
	}

	type want struct {
		node, outcome string
		queue, run    time.Duration
	}

	wants := []want{
		{"a", packtrail.OutcomeRetrying, time.Second, time.Second},
		{"a", packtrail.OutcomeCompleted, time.Second, 2 * time.Second},
		{"h", packtrail.OutcomeInterrupted, 0, time.Second},
		{"h", packtrail.OutcomeCompleted, 0, 0},
		{"w", packtrail.OutcomeSignalled, 0, 2 * time.Second},
		{"s", "failed", 0, 3 * time.Second},
		{"c", packtrail.OutcomeAbandoned, 0, 0},
	}

	if len(tl.Steps) != len(wants) {
		t.Fatalf("%d steps: %+v", len(tl.Steps), tl.Steps)
	}

	for i, w := range wants {
		s := tl.Steps[i]
		if s.Node != w.node || s.Outcome != w.outcome || s.Queue != w.queue || s.Run != w.run {
			t.Errorf("step %d = %s %s queue %v run %v, want %+v", i, s.Node, s.Outcome, s.Queue, s.Run, w)
		}
	}

	if a := tl.Steps[1]; a.Usage["tokens"] != 7 || string(a.Summary) != `{"tools":1}` || a.Attempt != 2 {
		t.Errorf("a's second attempt: %+v", a)
	}

	if !tl.Steps[3].Cached || tl.Steps[5].Error != "no" || tl.Steps[4].Type != packtrail.StepAwait {
		t.Errorf("steps %+v", tl.Steps)
	}

	b, err := json.Marshal(tl.Steps[1])
	if err != nil || !strings.Contains(string(b), `"queue_ms":1000`) || !strings.Contains(string(b), `"run_ms":2000`) {
		t.Errorf("json %s (%v)", b, err)
	}
}

func TestBuildTimelineClockSkew(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tl := packtrail.BuildTimeline([]event.Event{
		{Type: event.ExecutionStarted, Time: t0, Data: &event.Started{ExecID: "x", Flow: "f"}},
		{Type: event.NodeScheduled, Time: t0, Data: &event.Scheduled{Key: "a", Node: "a", Generation: 1, Attempt: 1}},
		// The worker's clock runs ahead of the engine's.
		{Type: event.NodeCompleted, Time: t0.Add(time.Second),
			Data: &event.NodeDone{Key: "a", Node: "a", Generation: 1, Attempt: 1, Started: t0.Add(5 * time.Second)}},
	})

	if s := tl.Steps[0]; s.Queue != 5*time.Second || s.Run != 0 {
		t.Fatalf("skewed step: queue %v run %v", s.Queue, s.Run)
	}
}

// TestBuildTimelineCancelledChildrenAndForks checks subflows a join gave up
// on are cancelled, not left running, and a fork's timeline names its flow.
func TestBuildTimelineCancelledChildrenAndForks(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ev := func(s int, typ event.Type, data any) event.Event {
		return event.Event{Type: typ, Time: t0.Add(time.Duration(s) * time.Second), Data: data}
	}

	tl := packtrail.BuildTimeline([]event.Event{
		ev(0, event.ExecutionForked, &event.Forked{ExecID: "f", From: "x", State: json.RawMessage(`{"flow":"fl"}`)}),
		ev(1, event.ChildStarted, &event.Child{Node: "b1", ChildID: "c1"}),
		ev(1, event.ChildStarted, &event.Child{Node: "b2", ChildID: "c2"}),
		ev(2, event.ChildCompleted, &event.ChildDone{Node: "b1", ChildID: "c1", Status: "completed"}),
		ev(3, event.JoinCompleted, &event.Join{Node: "j", CancelChildren: []string{"c2"}}),
	})

	if tl.Flow != "fl" || tl.ExecID != "f" {
		t.Fatalf("fork timeline %+v", tl)
	}

	if s := tl.Steps[1]; s.ChildID != "c2" || s.Outcome != packtrail.OutcomeCancelled || s.Run != 2*time.Second {
		t.Fatalf("cancelled child %+v", s)
	}
}
