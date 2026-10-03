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

// Package event defines packtrail's event types. The ordered stream of events
// of an execution is its source of truth: state, snapshots, indexes, jobs and
// timers are all derived from it. Every event is versioned; the codec rejects
// unknown types and versions instead of silently folding them wrong.
package event

import (
	"encoding/json"
	"time"
)

// Type names an event.
type Type string

// Event types.
const (
	ExecutionStarted   Type = "ExecutionStarted"
	ExecutionForked    Type = "ExecutionForked"
	ExecutionContinued Type = "ExecutionContinued"
	ExecutionCompleted Type = "ExecutionCompleted"
	ExecutionFailed    Type = "ExecutionFailed"
	ExecutionCancelled Type = "ExecutionCancelled"
	NodeEntered        Type = "NodeEntered"
	NodeScheduled      Type = "NodeScheduled"
	NodeCompleted      Type = "NodeCompleted"
	NodeFailed         Type = "NodeFailed"
	NodeInterrupted    Type = "NodeInterrupted"
	NodeCancelled      Type = "NodeCancelled"
	ChoiceEvaluated    Type = "ChoiceEvaluated"
	FanoutStarted      Type = "FanoutStarted"
	JoinCompleted      Type = "JoinCompleted"
	AwaitStarted       Type = "AwaitStarted"
	AwaitTimedOut      Type = "AwaitTimedOut"
	SignalReceived     Type = "SignalReceived"
	SignalConsumed     Type = "SignalConsumed"
	ChannelsUpdated    Type = "ChannelsUpdated"
	MapStarted         Type = "MapStarted"
	MapCompleted       Type = "MapCompleted"
	MapAborted         Type = "MapAborted"
	ChildStarted       Type = "ChildStarted"
	ChildCompleted     Type = "ChildCompleted"
	TimerScheduled     Type = "TimerScheduled"
	TimerFired         Type = "TimerFired"
)

// Terminal reports whether t ends an execution: completed, failed or
// cancelled. ExecutionContinued does not; the execution goes on.
func (t Type) Terminal() bool {
	switch t { //nolint:exhaustive // only terminal types matter.
	case ExecutionCompleted, ExecutionFailed, ExecutionCancelled:
		return true
	default:
		return false
	}
}

// Version is the current version of every event type.
const Version = 1

// Event is one entry of an execution's log.
type Event struct {
	Type    Type      `json:"type"`
	Version int       `json:"v"`
	Time    time.Time `json:"time"`
	// Index is the 1-based position of the event in its execution's log. The
	// fold rejects a gap, and readers use it to tell whether a cached state is
	// exactly one event behind.
	Index int `json:"i"`
	// CmdID is the id of the command whose decision produced the event.
	CmdID string `json:"cmd_id,omitempty"`
	// Trace is the W3C traceparent of the command that caused the event,
	// propagated to the jobs and commands derived from it.
	Trace string `json:"trace,omitempty"`
	// Data is a pointer to the type-specific payload struct.
	Data any `json:"data"`

	// Seq is the stream sequence of the message the event was stored in (set
	// on read). A decision is one message, so its events share a sequence.
	Seq uint64 `json:"-"`
	// DecisionEnd marks the last event of its decision (set on read): forks
	// are cut on these boundaries so a decision is never split.
	DecisionEnd bool `json:"-"`
}

// ParentRef identifies the parent of a subflow execution.
type ParentRef struct {
	ExecID string `json:"exec_id"`
	Node   string `json:"node"`
	ChildN int    `json:"child_n"`
}

// Started is the payload of ExecutionStarted.
type Started struct {
	ExecID   string            `json:"exec_id"`
	Flow     string            `json:"flow"`
	FlowHash string            `json:"flow_hash"`
	Input    json.RawMessage   `json:"input"`
	Parent   *ParentRef        `json:"parent,omitempty"`
	Attrs    map[string]string `json:"attrs,omitempty"`
}

// Forked is the payload of ExecutionForked: the first event of an execution
// forked from another one at a given sequence. State is the encoded fold state
// of the source at that point.
type Forked struct {
	ExecID string          `json:"exec_id"`
	From   string          `json:"from"`
	Seq    uint64          `json:"seq"`
	State  json.RawMessage `json:"state"`
}

// Continued is the payload of ExecutionContinued (continue-as-new under the
// same id): the log grew past the history limit, so the events before this
// one were archived as Segment and purged from the live log. State is the
// folded state right before this event; it becomes the base the rest of the
// live log folds onto.
type Continued struct {
	State   json.RawMessage `json:"state"`
	Segment Segment         `json:"segment"`
}

// Segment is a stretch of an execution's history archived by a continuation:
// object Object in the archive store holds the events with index
// FirstIndex…LastIndex (stream sequences FirstSeq…LastSeq). Every segment but
// the first starts with the ExecutionContinued event that based it.
type Segment struct {
	Object     string `json:"object"`
	FirstIndex int    `json:"first_index"`
	LastIndex  int    `json:"last_index"`
	FirstSeq   uint64 `json:"first_seq"`
	LastSeq    uint64 `json:"last_seq"`
}

// Completed is the payload of ExecutionCompleted.
type Completed struct {
	Output json.RawMessage `json:"output,omitempty"`
}

// Failure reasons.
const (
	ReasonError            = "error"
	ReasonTimeout          = "timeout"
	ReasonInvalidOutput    = "invalid_output"
	ReasonBudget           = "budget_exceeded"
	ReasonMaxSteps         = "max_steps"
	ReasonAwaitTimeout     = "await_timeout"
	ReasonJoin             = "join_failed"
	ReasonChild            = "child_failed"
	ReasonExpression       = "expression_error"
	ReasonDeliveryFailed   = "delivery_failed"
	ReasonUnknownFlow      = "unknown_flow"
	ReasonDecisionTooLarge = "decision_too_large"
)

// Failed is the payload of ExecutionFailed.
type Failed struct {
	Error  string `json:"error"`
	Reason string `json:"reason"`
	Node   string `json:"node,omitempty"`
	// CancelChildren lists child executions to cancel (on_parent_close).
	CancelChildren []string `json:"cancel_children,omitempty"`
}

// Cancelled is the payload of ExecutionCancelled.
type Cancelled struct {
	Reason         string   `json:"reason,omitempty"`
	CancelChildren []string `json:"cancel_children,omitempty"`
}

// Entered is the payload of NodeEntered: one visit of a node. It drives
// visits, steps and the recursion limit.
type Entered struct {
	Node string `json:"node"`
}

// Scheduled is the payload of NodeScheduled: one attempt of a task instance
// is ready for a worker. Key identifies the instance ("node", or "node#i" for
// a map item); Generation is unique within the execution and changes on every
// new visit or resume, Attempt counts retries within a generation.
type Scheduled struct {
	Key        string          `json:"key"`
	Node       string          `json:"node"`
	Kind       string          `json:"kind"`
	Index      int             `json:"index,omitempty"`
	Generation int             `json:"generation"`
	Attempt    int             `json:"attempt"`
	Owner      string          `json:"owner,omitempty"`
	Item       json.RawMessage `json:"item,omitempty"`
	Resume     json.RawMessage `json:"resume,omitempty"`
}

// NodeDone is the payload of NodeCompleted.
type NodeDone struct {
	Key        string                     `json:"key"`
	Node       string                     `json:"node"`
	Generation int                        `json:"generation"`
	Attempt    int                        `json:"attempt"`
	Output     json.RawMessage            `json:"output,omitempty"`
	Writes     map[string]json.RawMessage `json:"writes,omitempty"`
	Next       string                     `json:"next,omitempty"`
	Usage      map[string]float64         `json:"usage,omitempty"`
	Cached     bool                       `json:"cached,omitempty"`
	CacheKey   string                     `json:"cache_key,omitempty"`
}

// NodeFail is the payload of NodeFailed. WillRetry says whether another
// attempt follows (immediately or after a retry timer).
type NodeFail struct {
	Key        string             `json:"key"`
	Node       string             `json:"node"`
	Generation int                `json:"generation"`
	Attempt    int                `json:"attempt"`
	Error      string             `json:"error"`
	Reason     string             `json:"reason"`
	WillRetry  bool               `json:"will_retry"`
	Usage      map[string]float64 `json:"usage,omitempty"`
}

// Interrupted is the payload of NodeInterrupted.
type Interrupted struct {
	Key        string             `json:"key"`
	Node       string             `json:"node"`
	Generation int                `json:"generation"`
	Payload    json.RawMessage    `json:"payload,omitempty"`
	Usage      map[string]float64 `json:"usage,omitempty"`
}

// NodeCancel is the payload of NodeCancelled: an in-flight instance abandoned
// (a join that no longer needs it). Its late completion is ignored.
type NodeCancel struct {
	Key    string `json:"key"`
	Node   string `json:"node"`
	Reason string `json:"reason"`
}

// Choice is the payload of ChoiceEvaluated.
type Choice struct {
	Node  string `json:"node"`
	Rule  int    `json:"rule"`
	To    string `json:"to"`
	Error string `json:"error,omitempty"`
}

// Fanout is the payload of FanoutStarted.
type Fanout struct {
	Node     string   `json:"node"`
	Join     string   `json:"join"`
	Branches []string `json:"branches"`
}

// Join is the payload of JoinCompleted.
type Join struct {
	Node      string          `json:"node"`
	Fanout    string          `json:"fanout"`
	Succeeded []string        `json:"succeeded,omitempty"`
	Failed    []string        `json:"failed,omitempty"`
	OK        bool            `json:"ok"`
	Output    json.RawMessage `json:"output,omitempty"`
	// CancelChildren lists the child executions of branches still running
	// when the join settled (unless on_parent_close: abandon).
	CancelChildren []string `json:"cancel_children,omitempty"`
}

// Await is the payload of AwaitStarted.
type Await struct {
	Node    string `json:"node"`
	Signal  string `json:"signal"`
	TimerID string `json:"timer_id"`
}

// AwaitTimeout is the payload of AwaitTimedOut.
type AwaitTimeout struct {
	Node string `json:"node"`
	To   string `json:"to,omitempty"`
}

// Signal is the payload of SignalReceived.
type Signal struct {
	Name    string          `json:"name"`
	ID      string          `json:"id"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// SignalUse is the payload of SignalConsumed: the oldest buffered signal of
// that name is consumed by the await node.
type SignalUse struct {
	Node string `json:"node"`
	Name string `json:"name"`
	ID   string `json:"id"`
}

// Map is the payload of MapStarted.
type Map struct {
	Node        string            `json:"node"`
	Items       []json.RawMessage `json:"items"`
	MaxParallel int               `json:"max_parallel"`
}

// MapDone is the payload of MapCompleted.
type MapDone struct {
	Node string `json:"node"`
}

// Update is the payload of ChannelsUpdated: channel writes from outside the
// graph (Client.Update, or a fork with edits), folded by each channel's
// reducer. ID is the update id (deduplication).
type Update struct {
	ID     string                     `json:"id"`
	Writes map[string]json.RawMessage `json:"writes"`
}

// MapAbort is the payload of MapAborted: an item failed permanently and the
// map routes to its on_failure; the items still in flight are cancelled.
type MapAbort struct {
	Node  string `json:"node"`
	Index int    `json:"index"`
	// CancelChildren lists the child executions of items still running
	// (unless on_parent_close: abandon).
	CancelChildren []string `json:"cancel_children,omitempty"`
}

// Child is the payload of ChildStarted.
type Child struct {
	Node    string          `json:"node"`
	ChildID string          `json:"child_id"`
	ChildN  int             `json:"child_n"`
	Flow    string          `json:"flow"`
	Input   json.RawMessage `json:"input"`
	Policy  string          `json:"policy"`
	// Key identifies the instance: "node#i" for a map item; empty means Node.
	Key string `json:"key,omitempty"`
	// Owner is the fanout (for a branch) or map (for an item) the child
	// settles into; empty for a plain subflow node.
	Owner string `json:"owner,omitempty"`
	// Index is the map item the child runs.
	Index int `json:"index,omitempty"`
}

// InstanceKey returns the key of the child instance.
func (c *Child) InstanceKey() string {
	if c.Key == "" {
		return c.Node
	}

	return c.Key
}

// ChildDone is the payload of ChildCompleted.
type ChildDone struct {
	Node string `json:"node"`
	// Key is the instance (see Child.Key); empty means Node.
	Key      string             `json:"key,omitempty"`
	ChildID  string             `json:"child_id"`
	Status   string             `json:"status"`
	Output   json.RawMessage    `json:"output,omitempty"`
	Error    string             `json:"error,omitempty"`
	Counters map[string]float64 `json:"counters,omitempty"`
}

// Timer purposes.
const (
	TimerRetry   = "retry"
	TimerTimeout = "timeout"
	TimerAwait   = "await"
)

// Timer is the payload of TimerScheduled.
type Timer struct {
	ID         string    `json:"id"`
	N          int       `json:"n"`
	At         time.Time `json:"at"`
	Purpose    string    `json:"purpose"`
	Key        string    `json:"key,omitempty"`
	Node       string    `json:"node,omitempty"`
	Generation int       `json:"generation,omitempty"`
	Attempt    int       `json:"attempt,omitempty"`
}

// Fired is the payload of TimerFired.
type Fired struct {
	ID string `json:"id"`
}
