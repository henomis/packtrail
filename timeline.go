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

package packtrail

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/henomis/packtrail/event"
	"github.com/henomis/packtrail/internal/obs"
)

// Timeline is an execution's history as steps: each task attempt, await and
// subflow, with when it was scheduled, started and ended. It is derived from
// the event log alone, so it is available for any execution, live or
// archived, without any telemetry backend.
type Timeline struct {
	ExecID string `json:"exec_id"`
	Flow   string `json:"flow"`
	Status Status `json:"status"`
	// TraceID is the execution's trace, when it was started with one.
	TraceID string         `json:"trace_id,omitempty"`
	Started time.Time      `json:"started"`
	Ended   time.Time      `json:"ended,omitzero"`
	Steps   []TimelineStep `json:"steps"`
}

// Step types of a timeline.
const (
	StepTask    = "task"
	StepAwait   = "await"
	StepSubflow = "subflow"
)

// Step outcomes of a timeline.
const (
	OutcomeCompleted   = "completed"
	OutcomeFailed      = "failed"
	OutcomeRetrying    = "retrying"
	OutcomeInterrupted = "interrupted"
	OutcomeCancelled   = "cancelled"
	OutcomeSignalled   = "signalled"
	OutcomeTimedOut    = "timed_out"
	OutcomeRunning     = "running"
	OutcomeAbandoned   = "abandoned"
)

// TimelineStep is one task attempt, await or subflow of a timeline.
type TimelineStep struct {
	Type       string `json:"type"`
	Node       string `json:"node"`
	Key        string `json:"key,omitempty"`
	Kind       string `json:"kind,omitempty"`
	Index      int    `json:"index,omitempty"`
	Generation int    `json:"generation,omitempty"`
	Attempt    int    `json:"attempt,omitempty"`
	// Scheduled is when the step began: the attempt was scheduled, the await
	// started, the subflow was started.
	Scheduled time.Time `json:"scheduled"`
	// Started is when the worker's handler started; zero when the worker did
	// not report it (and for awaits, subflows and cached results).
	Started time.Time `json:"started,omitzero"`
	Ended   time.Time `json:"ended,omitzero"`
	// Queue is the time between Scheduled and Started, Run the time between
	// Started (else Scheduled) and Ended. Worker and engine clocks may
	// differ: neither is ever negative.
	Queue   time.Duration `json:"-"`
	Run     time.Duration `json:"-"`
	Outcome string        `json:"outcome"`
	Error   string        `json:"error,omitempty"`
	Reason  string        `json:"reason,omitempty"`
	// Signal is the signal an await waits for; ChildID the subflow's
	// execution.
	Signal  string             `json:"signal,omitempty"`
	ChildID string             `json:"child_id,omitempty"`
	Cached  bool               `json:"cached,omitempty"`
	Usage   map[string]float64 `json:"usage,omitempty"`
	// Summary is what the worker reported doing (Result.Summary).
	Summary json.RawMessage `json:"summary,omitempty"`
	// Seq is the sequence of the event that began the step.
	Seq uint64 `json:"seq,omitempty"`
}

// MarshalJSON writes Queue and Run in milliseconds.
func (s TimelineStep) MarshalJSON() ([]byte, error) {
	type plain TimelineStep

	return json.Marshal(struct {
		plain

		QueueMS float64 `json:"queue_ms,omitempty"`
		RunMS   float64 `json:"run_ms,omitempty"`
	}{plain(s), ms(s.Queue), ms(s.Run)})
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 } //nolint:mnd // µs to ms.

// Timeline returns the timeline of an execution (see BuildTimeline).
func (c *Client) Timeline(ctx context.Context, execID string) (*Timeline, error) {
	evs, err := c.History(ctx, execID)
	if err != nil {
		return nil, err
	}

	return BuildTimeline(evs), nil
}

// BuildTimeline derives a timeline from an execution's events, in order.
func BuildTimeline(evs []event.Event) *Timeline {
	b := &timelineBuilder{tl: &Timeline{Status: StatusRunning, Steps: []TimelineStep{}}, open: map[string]int{}}

	for _, ev := range evs {
		b.add(ev)
	}

	for i := range b.tl.Steps {
		s := &b.tl.Steps[i]
		if s.Ended.IsZero() {
			s.Outcome = OutcomeRunning
			if b.tl.Status.Terminal() {
				s.Outcome = OutcomeAbandoned
			}
		}

		s.durations()
	}

	return b.tl
}

type timelineBuilder struct {
	tl *Timeline
	// open maps a step's identity (type, key, generation; the attempt for
	// tasks) to its index in Steps while it has not ended.
	open map[string]int
}

func (b *timelineBuilder) add(ev event.Event) {
	switch d := ev.Data.(type) {
	case *event.Started:
		b.begin(ev, d.ExecID, d.Flow)
	case *event.Forked:
		var src struct {
			Flow string `json:"flow"`
		}

		_ = json.Unmarshal(d.State, &src) // the flow name only; an undecodable state leaves it empty

		b.begin(ev, d.ExecID, src.Flow)
	case *event.Completed:
		b.end(ev, StatusCompleted)
	case *event.Failed:
		b.end(ev, StatusFailed)
	case *event.Cancelled:
		b.end(ev, StatusCancelled)
	default:
		b.addStep(ev)
	}
}

func (b *timelineBuilder) begin(ev event.Event, execID, flow string) {
	b.tl.ExecID, b.tl.Started = execID, ev.Time
	if flow != "" {
		b.tl.Flow = flow
	}

	if sc := obs.SpanContext(ev.Trace); sc.IsValid() {
		b.tl.TraceID = sc.TraceID().String()
	}
}

func (b *timelineBuilder) end(ev event.Event, st Status) {
	b.tl.Status, b.tl.Ended = st, ev.Time
}

//nolint:gocyclo // one case per step event.
func (b *timelineBuilder) addStep(ev event.Event) {
	switch d := ev.Data.(type) {
	case *event.Scheduled:
		b.push(taskID(d.Key, d.Generation, d.Attempt), TimelineStep{
			Type: StepTask, Node: d.Node, Key: d.Key, Kind: d.Kind, Index: d.Index, Generation: d.Generation,
			Attempt: d.Attempt, Scheduled: ev.Time, Seq: ev.Seq,
		})
	case *event.NodeDone:
		b.settle(taskID(d.Key, d.Generation, d.Attempt), ev, OutcomeCompleted, func(s *TimelineStep) {
			s.Started, s.Usage, s.Summary, s.Cached = d.Started, d.Usage, d.Summary, d.Cached
		})
	case *event.NodeFail:
		outcome := OutcomeFailed
		if d.WillRetry {
			outcome = OutcomeRetrying
		}

		b.settle(taskID(d.Key, d.Generation, d.Attempt), ev, outcome, func(s *TimelineStep) {
			s.Started, s.Usage, s.Summary, s.Error, s.Reason = d.Started, d.Usage, d.Summary, d.Error, d.Reason
		})
	case *event.Interrupted:
		b.settle(b.latest(taskPrefix(d.Key, d.Generation)), ev, OutcomeInterrupted, func(s *TimelineStep) {
			s.Started, s.Usage, s.Summary = d.Started, d.Usage, d.Summary
		})
	case *event.NodeCancel:
		b.settle(b.latest(StepTask+"/"+d.Key+"/"), ev, OutcomeCancelled, func(s *TimelineStep) {
			s.Reason = d.Reason
		})
	case *event.Await:
		b.push(StepAwait+"/"+d.Node, TimelineStep{
			Type: StepAwait, Node: d.Node, Signal: d.Signal, Scheduled: ev.Time, Seq: ev.Seq,
		})
	case *event.SignalUse:
		b.settle(StepAwait+"/"+d.Node, ev, OutcomeSignalled, nil)
	case *event.AwaitTimeout:
		b.settle(StepAwait+"/"+d.Node, ev, OutcomeTimedOut, nil)
	case *event.Child:
		b.push(childID(d.Key, d.Node), TimelineStep{
			Type: StepSubflow, Node: d.Node, Key: d.Key, Index: d.Index, ChildID: d.ChildID, Scheduled: ev.Time,
			Seq: ev.Seq,
		})
	case *event.Join:
		b.cancelChildren(ev, d.CancelChildren)
	case *event.MapAbort:
		b.cancelChildren(ev, d.CancelChildren)
	case *event.ChildDone:
		outcome := OutcomeCompleted
		if d.Status != string(StatusCompleted) {
			outcome = d.Status
		}

		b.settle(childID(d.Key, d.Node), ev, outcome, func(s *TimelineStep) { s.Error = d.Error })
	}
}

func taskID(key string, generation, attempt int) string {
	return taskPrefix(key, generation) + itoa(attempt)
}

func taskPrefix(key string, generation int) string {
	return StepTask + "/" + key + "/" + itoa(generation) + "/"
}

func childID(key, node string) string {
	if key == "" {
		key = node
	}

	return StepSubflow + "/" + key
}

func itoa(n int) string { return strconv.Itoa(n) }

func (b *timelineBuilder) push(id string, s TimelineStep) {
	b.tl.Steps = append(b.tl.Steps, s)
	b.open[id] = len(b.tl.Steps) - 1
}

// latest is the id of the open step with this prefix that began last.
func (b *timelineBuilder) latest(prefix string) string {
	best, at := "", -1

	for id, i := range b.open {
		if strings.HasPrefix(id, prefix) && i > at {
			best, at = id, i
		}
	}

	return best
}

func (b *timelineBuilder) settle(id string, ev event.Event, outcome string, fill func(*TimelineStep)) {
	i, ok := b.open[id]
	if !ok {
		return
	}

	delete(b.open, id)

	s := &b.tl.Steps[i]
	s.Ended, s.Outcome = ev.Time, outcome

	if fill != nil {
		fill(s)
	}
}

// cancelChildren settles the subflow steps of children a join or an
// aborted map gave up on: their completion is never recorded.
func (b *timelineBuilder) cancelChildren(ev event.Event, children []string) {
	for _, child := range children {
		for id, i := range b.open {
			if s := b.tl.Steps[i]; s.Type == StepSubflow && s.ChildID == child {
				b.settle(id, ev, OutcomeCancelled, nil)
			}
		}
	}
}

func (s *TimelineStep) durations() {
	from := s.Scheduled

	if !s.Started.IsZero() {
		s.Queue = max(s.Started.Sub(s.Scheduled), 0)
		from = s.Started
	}

	if !s.Ended.IsZero() {
		s.Run = max(s.Ended.Sub(from), 0)
	}
}
