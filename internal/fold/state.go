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

// Package fold is the pure heart of packtrail: State is the projection of an
// execution's events, Apply folds one event into it, and Decide turns a
// command into the events it produces. Nothing here touches NATS or the clock
// (time enters as data), so the whole state machine is tested with tables.
package fold

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/henomis/packtrail/event"
	"github.com/henomis/packtrail/flow"
)

// Status is the lifecycle status of an execution.
type Status string

// Execution statuses.
const (
	StatusRunning   Status = "running"
	StatusWaiting   Status = "waiting"
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
	StatusCancelled Status = "cancelled"
)

// Terminal reports whether s is a final status.
func (s Status) Terminal() bool {
	return s == StatusCompleted || s == StatusFailed || s == StatusCancelled
}

// Task statuses.
const (
	TaskScheduled   = "scheduled"
	TaskRetrying    = "retrying"
	TaskInterrupted = "interrupted"
)

// Branch / map item statuses.
const (
	BranchPending   = "pending"
	BranchCompleted = "completed"
	BranchFailed    = "failed"
	BranchCancelled = "cancelled"
)

// Task is an active task instance (a plain task, a fan-out branch or a map
// item). Settled instances are removed.
type Task struct {
	Key        string          `json:"key"`
	Node       string          `json:"node"`
	Kind       string          `json:"kind"`
	Index      int             `json:"index,omitempty"`
	Owner      string          `json:"owner,omitempty"`
	Generation int             `json:"generation"`
	Attempt    int             `json:"attempt"`
	Status     string          `json:"status"`
	Item       json.RawMessage `json:"item,omitempty"`
	Resume     json.RawMessage `json:"resume,omitempty"`
	Interrupt  json.RawMessage `json:"interrupt,omitempty"`
}

// Fan is an open fan-out.
type Fan struct {
	Join     string            `json:"join"`
	Branches map[string]string `json:"branches"`
}

// MapRun is an open map node.
type MapRun struct {
	Items       []json.RawMessage `json:"items"`
	Outputs     []json.RawMessage `json:"outputs"`
	Done        int               `json:"done"`
	Next        int               `json:"next"`
	MaxParallel int               `json:"max_parallel"`
}

// AwaitState is an open await node.
type AwaitState struct {
	Signal  string `json:"signal"`
	TimerID string `json:"timer_id"`
}

// ChildState is a running subflow.
type ChildState struct {
	ChildID string `json:"child_id"`
	Flow    string `json:"flow"`
	Policy  string `json:"policy"`
}

// Buffered is a received signal not consumed yet.
type Buffered struct {
	Name    string          `json:"name"`
	ID      string          `json:"id"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// NodeError is how a node last failed permanently: what an on_failure
// handler (and any expression) sees as errors.<node>.
type NodeError struct {
	Error  string `json:"error"`
	Reason string `json:"reason"`
	// Attempt is the attempt that failed (tasks and map items).
	Attempt int `json:"attempt,omitempty"`
	// Index is the failed item of a map.
	Index *int `json:"index,omitempty"`
}

// State is the fold of an execution's events.
type State struct {
	ExecID   string            `json:"exec_id"`
	Flow     string            `json:"flow"`
	FlowHash string            `json:"flow_hash"`
	Status   Status            `json:"status"`
	Input    json.RawMessage   `json:"input,omitempty"`
	Parent   *event.ParentRef  `json:"parent,omitempty"`
	Attrs    map[string]string `json:"attrs,omitempty"`

	Channels map[string]json.RawMessage `json:"channels"`
	Results  map[string]json.RawMessage `json:"results"`
	LastNode string                     `json:"last_node,omitempty"`
	Visits   map[string]int             `json:"visits"`
	Steps    int                        `json:"steps"`
	Signals  map[string]json.RawMessage `json:"signals"`
	Buffered []Buffered                 `json:"buffered,omitempty"`
	// SignalIDs remembers recent signal ids (deduplication) with the time
	// they arrived; bounded by signalIDWindow and maxSignalIDs.
	SignalIDs map[string]time.Time `json:"signal_ids,omitempty"`
	// UpdateIDs remembers recent update ids, bounded the same way.
	UpdateIDs map[string]time.Time `json:"update_ids,omitempty"`
	Counters  map[string]float64   `json:"counters"`
	Branches  map[string]string    `json:"branches"`
	// Errors holds the last permanent failure of each node; a later success
	// of the node clears it.
	Errors map[string]NodeError `json:"errors"`

	Tasks    map[string]*Task        `json:"tasks"`
	Fans     map[string]*Fan         `json:"fans"`
	Maps     map[string]*MapRun      `json:"maps"`
	Awaits   map[string]*AwaitState  `json:"awaits"`
	Children map[string]*ChildState  `json:"children"`
	Timers   map[string]*event.Timer `json:"timers"`
	N        int                     `json:"n"`

	Output     json.RawMessage `json:"output,omitempty"`
	Error      string          `json:"error,omitempty"`
	Reason     string          `json:"reason,omitempty"`
	FailedNode string          `json:"failed_node,omitempty"`

	Created    time.Time `json:"created"`
	Updated    time.Time `json:"updated"`
	ForkedFrom string    `json:"forked_from,omitempty"`
	ForkSeq    uint64    `json:"fork_seq,omitempty"`

	// Base is the index of the last ExecutionContinued (0: none): the live log
	// starts there. Segments are the archived stretches before it, in order.
	Base     int             `json:"base,omitempty"`
	Segments []event.Segment `json:"segments,omitempty"`

	// LastSeq is the stream sequence of the last applied event: the expected
	// sequence of the next append (optimistic concurrency).
	LastSeq uint64 `json:"last_seq"`
	// Events counts applied events.
	Events int `json:"events"`
	// Archived is set by readers of an archived execution; the fold never
	// sets it.
	Archived bool `json:"archived,omitempty"`
}

// New returns the empty state of an execution that has no events yet.
func New(execID string) *State {
	s := &State{ExecID: execID}
	s.init()

	return s
}

func (s *State) init() {
	if s.Channels == nil {
		s.Channels = map[string]json.RawMessage{}
	}

	if s.Results == nil {
		s.Results = map[string]json.RawMessage{}
	}

	if s.Visits == nil {
		s.Visits = map[string]int{}
	}

	if s.Signals == nil {
		s.Signals = map[string]json.RawMessage{}
	}

	if s.SignalIDs == nil {
		s.SignalIDs = map[string]time.Time{}
	}

	if s.UpdateIDs == nil {
		s.UpdateIDs = map[string]time.Time{}
	}

	if s.Counters == nil {
		s.Counters = map[string]float64{}
	}

	if s.Branches == nil {
		s.Branches = map[string]string{}
	}

	if s.Errors == nil {
		s.Errors = map[string]NodeError{}
	}

	s.initActive()
}

func (s *State) initActive() {
	if s.Tasks == nil {
		s.Tasks = map[string]*Task{}
	}

	if s.Fans == nil {
		s.Fans = map[string]*Fan{}
	}

	if s.Maps == nil {
		s.Maps = map[string]*MapRun{}
	}

	if s.Awaits == nil {
		s.Awaits = map[string]*AwaitState{}
	}

	if s.Children == nil {
		s.Children = map[string]*ChildState{}
	}

	if s.Timers == nil {
		s.Timers = map[string]*event.Timer{}
	}
}

// Exists reports whether the execution has been started.
func (s *State) Exists() bool { return s.Events > 0 }

// Clone returns a deep copy (via JSON, the snapshot encoding).
func (s *State) Clone() (*State, error) {
	b, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}

	return Unmarshal(b)
}

// Unmarshal decodes an encoded state (snapshot, fork).
func Unmarshal(b []byte) (*State, error) {
	var s State
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, fmt.Errorf("fold: decode state: %w", err)
	}

	s.init()

	return &s, nil
}

// ErrApply reports an event that cannot be folded into the state: a corrupt
// log or a bug, never a normal condition.
var ErrApply = errors.New("fold: cannot apply event")

// Fold folds events into a fresh state of execution execID. def is the flow
// definition the execution is bound to (needed for channel reducers); it may
// be nil only for logs without channel writes.
func Fold(def *flow.Flow, execID string, events []event.Event) (*State, error) {
	s := New(execID)

	for _, ev := range events {
		if err := s.Apply(def, ev); err != nil {
			return nil, err
		}

		if ev.Seq > 0 {
			s.LastSeq = ev.Seq
		}
	}

	return s, nil
}

// Apply folds one event into the state.
func (s *State) Apply(def *flow.Flow, ev event.Event) error {
	// ExecutionForked and ExecutionContinued replace the state, so their index
	// is checked against the state they bring.
	expected := s.Events + 1

	switch d := ev.Data.(type) {
	case *event.Forked:
		expected = 1
	case *event.Continued:
		src, err := Unmarshal(d.State)
		if err != nil {
			return fmt.Errorf("%w %s: %w", ErrApply, ev.Type, err)
		}

		expected = src.Events + 1
	}

	if ev.Index != 0 && ev.Index != expected {
		return fmt.Errorf("%w %s: event index %d, expected %d (gap in the log)", ErrApply, ev.Type, ev.Index, expected)
	}

	if err := s.apply(def, ev); err != nil {
		return fmt.Errorf("%w %s: %w", ErrApply, ev.Type, err)
	}

	s.Events++

	if !ev.Time.IsZero() {
		s.Updated = ev.Time
	}

	if !s.Status.Terminal() && s.Status != "" {
		s.recomputeStatus()
	}

	return nil
}

func (s *State) apply(def *flow.Flow, ev event.Event) error {
	switch d := ev.Data.(type) {
	case *event.Started:
		s.applyStarted(def, ev, d)
	case *event.Forked:
		return s.applyForked(d)
	case *event.Continued:
		src, err := Unmarshal(d.State)
		if err != nil {
			return err
		}

		*s = *src
		s.Base = ev.Index
	case *event.Completed:
		s.terminate(StatusCompleted)
		s.Output = d.Output
	case *event.Failed:
		s.terminate(StatusFailed)
		s.Error, s.Reason, s.FailedNode = d.Error, d.Reason, d.Node
	case *event.Cancelled:
		s.terminate(StatusCancelled)
		s.Reason = d.Reason
	case *event.Entered:
		s.Visits[d.Node]++
		s.Steps++
	default:
		return s.applyNode(def, ev)
	}

	return nil
}

func (s *State) applyStarted(def *flow.Flow, ev event.Event, d *event.Started) {
	s.ExecID, s.Flow, s.FlowHash = d.ExecID, d.Flow, d.FlowHash
	s.Input, s.Parent, s.Attrs = d.Input, d.Parent, d.Attrs
	s.Status = StatusRunning
	s.Created = ev.Time

	if def != nil {
		for name, c := range def.Channels {
			if c.Default != nil {
				b, _ := json.Marshal(c.Default) //nolint:errchkjson // defaults are decoded JSON values
				s.Channels[name] = b
			}
		}
	}
}

func (s *State) applyForked(d *event.Forked) error {
	src, err := Unmarshal(d.State)
	if err != nil {
		return err
	}

	*s = *src
	s.ExecID = d.ExecID
	s.ForkedFrom, s.ForkSeq = d.From, d.Seq
	s.Base, s.Segments = 0, nil // the fork's history starts at the fork
	s.Timers = map[string]*event.Timer{}
	s.Events, s.LastSeq = 0, 0

	return nil
}

// terminate closes every open structure: nothing is in flight after a final
// status, and late completions / timers are ignored as stale.
func (s *State) terminate(st Status) {
	s.Status = st
	s.Tasks = nil
	s.Fans = nil
	s.Maps = nil
	s.Awaits = nil
	s.Children = nil
	s.Timers = nil
	s.initActive()
}

func (s *State) applyNode(def *flow.Flow, ev event.Event) error {
	switch d := ev.Data.(type) {
	case *event.Scheduled:
		s.applyScheduled(d)
	case *event.NodeDone:
		return s.applyNodeDone(def, d)
	case *event.NodeFail:
		s.applyNodeFail(d)
	case *event.Interrupted:
		t := s.Tasks[d.Key]
		if t == nil {
			return fmt.Errorf("interrupt of unknown task %q", d.Key)
		}

		t.Status = TaskInterrupted
		t.Interrupt = d.Payload
	case *event.NodeCancel:
		s.settleTask(d.Key, BranchCancelled)
	case *event.Choice:
		// Routing decision: recorded for audit, no state of its own.
	case *event.Update:
		return s.applyUpdate(def, d, ev.Time)
	default:
		return s.applyStructure(ev)
	}

	return nil
}

func (s *State) applyScheduled(d *event.Scheduled) {
	s.Tasks[d.Key] = &Task{
		Key: d.Key, Node: d.Node, Kind: d.Kind, Index: d.Index, Owner: d.Owner,
		Generation: d.Generation, Attempt: d.Attempt, Status: TaskScheduled, Item: d.Item, Resume: d.Resume,
	}
	s.N = max(s.N, d.Generation)

	if m := s.Maps[d.Owner]; m != nil && d.Node == d.Owner {
		m.Next = max(m.Next, d.Index+1)
	}
}

func (s *State) applyNodeDone(def *flow.Flow, d *event.NodeDone) error {
	t := s.Tasks[d.Key]
	if t == nil {
		return fmt.Errorf("completion of unknown task %q", d.Key)
	}

	for _, name := range slices.Sorted(maps.Keys(d.Writes)) {
		reducer := flow.ReducerReplace
		if def != nil {
			reducer = def.Channels[name].ReducerOrDefault()
		}

		v, err := Reduce(reducer, s.Channels[name], d.Writes[name])
		if err != nil {
			return err
		}

		s.Channels[name] = v
	}

	for k, v := range d.Usage {
		s.Counters[k] += v
	}

	if m := s.Maps[t.Owner]; m != nil && t.Node == t.Owner {
		m.Outputs[t.Index] = d.Output
		m.Done++
	} else {
		s.Results[t.Node] = d.Output
		s.LastNode = t.Node
		delete(s.Errors, t.Node)
	}

	s.settleTask(d.Key, BranchCompleted)

	return nil
}

func (s *State) applyNodeFail(d *event.NodeFail) {
	t := s.Tasks[d.Key]
	if t == nil {
		return
	}

	if d.WillRetry {
		t.Status = TaskRetrying

		return
	}

	ne := NodeError{Error: d.Error, Reason: d.Reason, Attempt: d.Attempt}

	if m := s.Maps[t.Owner]; m != nil && t.Node == t.Owner {
		idx := t.Index
		ne.Index = &idx
	}

	s.Errors[t.Node] = ne
	s.LastNode = t.Node
	s.settleTask(d.Key, BranchFailed)
}

// settleTask removes an instance and records its outcome on its fan-out.
func (s *State) settleTask(key, outcome string) {
	t := s.Tasks[key]
	if t == nil {
		return
	}

	delete(s.Tasks, key)

	if f := s.Fans[t.Owner]; f != nil {
		f.Branches[t.Node] = outcome
	}
}

func (s *State) applyStructure(ev event.Event) error {
	switch d := ev.Data.(type) {
	case *event.Fanout:
		f := &Fan{Join: d.Join, Branches: map[string]string{}}
		for _, b := range d.Branches {
			f.Branches[b] = BranchPending
		}

		s.Fans[d.Node] = f
	case *event.Join:
		if f := s.Fans[d.Fanout]; f != nil {
			s.Branches = f.Branches
		}

		delete(s.Fans, d.Fanout)
		s.Results[d.Node] = d.Output
		s.LastNode = d.Node

		if d.OK {
			delete(s.Errors, d.Node)
		} else {
			s.Errors[d.Node] = NodeError{
				Error: fmt.Sprintf("branches %v did not succeed", d.Failed), Reason: event.ReasonJoin,
			}
		}
	case *event.Map:
		s.Maps[d.Node] = &MapRun{
			Items: d.Items, Outputs: make([]json.RawMessage, len(d.Items)), MaxParallel: d.MaxParallel,
		}
	case *event.MapDone:
		m := s.Maps[d.Node]
		if m == nil {
			return fmt.Errorf("completion of unknown map %q", d.Node)
		}

		out, err := json.Marshal(m.Outputs)
		if err != nil {
			return err
		}

		s.Results[d.Node] = out
		s.LastNode = d.Node
		delete(s.Maps, d.Node)
		delete(s.Errors, d.Node)
	case *event.MapAbort:
		delete(s.Maps, d.Node)
	default:
		return s.applyWait(ev)
	}

	return nil
}

func (s *State) applyWait(ev event.Event) error {
	switch d := ev.Data.(type) {
	case *event.Await:
		s.Awaits[d.Node] = &AwaitState{Signal: d.Signal, TimerID: d.TimerID}
	case *event.AwaitTimeout:
		if a := s.Awaits[d.Node]; a != nil {
			delete(s.Timers, a.TimerID)
		}

		delete(s.Awaits, d.Node)
	case *event.Signal:
		s.Buffered = append(s.Buffered, Buffered{Name: d.Name, ID: d.ID, Payload: d.Payload})
		rememberID(s.SignalIDs, d.ID, ev.Time)
	case *event.SignalUse:
		return s.applySignalUse(d)
	case *event.Child:
		s.Children[d.Node] = &ChildState{ChildID: d.ChildID, Flow: d.Flow, Policy: d.Policy}
		s.N = max(s.N, d.ChildN)
	case *event.ChildDone:
		delete(s.Children, d.Node)
		s.Results[d.Node] = d.Output
		s.LastNode = d.Node

		if d.Status == string(StatusCompleted) {
			delete(s.Errors, d.Node)
		} else {
			s.Errors[d.Node] = NodeError{Error: d.Error, Reason: event.ReasonChild}
		}

		for k, v := range d.Counters {
			s.Counters[k] += v
		}
	case *event.Timer:
		t := *d
		s.Timers[d.ID] = &t
		s.N = max(s.N, d.N)
	case *event.Fired:
		delete(s.Timers, d.ID)
	default:
		return fmt.Errorf("unhandled event %T", ev.Data)
	}

	return nil
}

func (s *State) applyUpdate(def *flow.Flow, d *event.Update, at time.Time) error {
	for _, name := range slices.Sorted(maps.Keys(d.Writes)) {
		reducer := flow.ReducerReplace
		if def != nil {
			reducer = def.Channels[name].ReducerOrDefault()
		}

		v, err := Reduce(reducer, s.Channels[name], d.Writes[name])
		if err != nil {
			return err
		}

		s.Channels[name] = v
	}

	rememberID(s.UpdateIDs, d.ID, at)

	return nil
}

func (s *State) applySignalUse(d *event.SignalUse) error {
	i := slices.IndexFunc(s.Buffered, func(b Buffered) bool { return b.ID == d.ID })
	if i < 0 {
		return fmt.Errorf("consumed signal %q is not buffered", d.ID)
	}

	b := s.Buffered[i]
	s.Buffered = slices.Delete(s.Buffered, i, i+1)
	s.Signals[d.Name] = b.Payload
	s.Results[d.Node] = b.Payload
	s.LastNode = d.Node

	if a := s.Awaits[d.Node]; a != nil {
		delete(s.Timers, a.TimerID)
	}

	delete(s.Awaits, d.Node)

	return nil
}

// Bounds of the signal deduplication memory: the command stream already
// deduplicates for 10 minutes; ids are kept for a day, at most maxSignalIDs.
const (
	signalIDWindow = 24 * time.Hour
	maxSignalIDs   = 10000
)

// rememberID records id at time at in a deduplication memory and forgets ids
// older than the window, then the oldest beyond the cap. Event times only:
// the fold stays deterministic.
func rememberID(m map[string]time.Time, id string, at time.Time) {
	m[id] = at

	for k, t := range m {
		if at.Sub(t) > signalIDWindow {
			delete(m, k)
		}
	}

	if len(m) <= maxSignalIDs {
		return
	}

	ids := slices.Collect(maps.Keys(m))
	slices.SortFunc(ids, func(a, b string) int {
		if c := m[a].Compare(m[b]); c != 0 {
			return c
		}

		return strings.Compare(a, b)
	})

	for _, k := range ids[:len(ids)-maxSignalIDs] {
		delete(m, k)
	}
}

// recomputeStatus derives running/waiting: an execution is waiting when
// nothing is in flight and it only waits for a signal or a resume.
func (s *State) recomputeStatus() {
	for _, t := range s.Tasks {
		if t.Status != TaskInterrupted {
			s.Status = StatusRunning
			return
		}
	}

	if len(s.Children) > 0 || len(s.Maps) > 0 {
		s.Status = StatusRunning
		return
	}

	if len(s.Awaits) > 0 || len(s.Tasks) > 0 {
		s.Status = StatusWaiting
		return
	}

	s.Status = StatusRunning
}
