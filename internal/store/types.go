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

// Package store wraps the NATS JetStream KV buckets and streams that hold all
// Packtrail state: executions, ownership leases, visibility indexes and the domain
// event stream. Every state transition is a CAS (optimistic concurrency) write.
package store

import (
	"encoding/json"
	"time"
)

// Execution status values.
const (
	StatusRunning   = "running"
	StatusWaiting   = "waiting"
	StatusCompleted = "completed"
	StatusFailed    = "failed"
	StatusCancelled = "cancelled"
)

// Branch status values.
const (
	BranchPending   = "pending"
	BranchCompleted = "completed"
	BranchFailed    = "failed"
)

// Outbox item kinds: how a committed follow-on message reaches NATS.
const (
	OutboxWork  = "work"  // publish to the execution's work subject now
	OutboxSched = "sched" // schedule via the Message Scheduler to fire at At
)

// OutboxItem is a follow-on message committed atomically with the state
// transition that requires it (the transactional-outbox pattern). The item is
// published *after* the CAS write and then cleared; a crash in between leaves
// it durably on the document, where any later touch (work redelivery, a
// completion, the stall watchdog) re-publishes it. Publishes carry a
// per-execution msg-id ("<execID>.<Seq>") so a re-flush inside the stream's
// dedup window is dropped; beyond the window a duplicate is state-safe against
// the guarded transitions.
type OutboxItem struct {
	Kind string          `json:"kind"`
	Item json.RawMessage `json:"item"`        // fully-marshaled work item
	At   time.Time       `json:"at,omitzero"` // sched only: absolute fire time (late flushes keep the original deadline)
	Seq  uint64          `json:"seq"`         // per-execution monotonic id
}

// Execution is the runtime instance of a Flow: the control plane. It is the
// single source of truth for an execution's progress and is persisted in the
// packtrail-executions KV bucket. Payloads (start input, node outputs, signal
// payloads) live in the separate payloads bucket — the data plane — keyed per
// entry (see payloads.go); the document carries only which entries exist, so
// its size grows with nodes visited, never with payload bytes.
type Execution struct {
	ID          string `json:"id"`
	FlowName    string `json:"flow_name"`
	CurrentNode string `json:"current_node"`
	Status      string `json:"status"`
	// execution-scoped visit generation for CurrentNode; increments on node entry
	NodeGeneration uint64 `json:"node_generation,omitempty"`
	// attempts spent on CurrentNode (task retries)
	Attempt int `json:"attempt"`
	// node ids with a stored output, in settle order
	Outputs        []string               `json:"outputs,omitempty"`
	OutputVersions map[string]string      `json:"output_versions,omitempty"` // committed versioned output keys, per node
	Branches       map[string]BranchState `json:"branches,omitempty"`        // active fanout/fanin branches
	LastSeq        map[string]uint64      `json:"last_seq,omitempty"`        // last applied JetStream seq, per signal_name
	// received-but-unconsumed markers, per signal_name
	Signals    map[string]bool `json:"signals,omitempty"`
	WaitSignal string          `json:"wait_signal,omitempty"` // signal_name currently awaited
	Activity   *ActivityResult `json:"activity,omitempty"`    //nolint:lll // async completion that arrived before the task parked
	Error      string          `json:"error,omitempty"`
	RetryAt    time.Time       `json:"retry_at,omitzero"` //nolint:lll // when the scheduled retry of CurrentNode fires (running + Attempt > 0)
	Outbox     []OutboxItem    `json:"outbox,omitempty"`  //nolint:lll // follow-on messages committed with the last transition, pending publish
	OutboxSeq  uint64          `json:"outbox_seq,omitempty"`
	Revision   uint64          `json:"-"` // current KV revision, for CAS (not persisted in value)
	UpdatedAt  time.Time       `json:"updated_at"`
}

// AddOutput records that node's legacy output exists in the data plane. Call
// inside the Mutate callback that commits the settle; idempotent per node.
func (e *Execution) AddOutput(node string) {
	e.appendOutput(node)
}

// SetOutput records that node's versioned output exists in the data plane. The
// version is committed with the guarded control-plane CAS so stale attempts can
// leave orphan payload candidates without changing which output Results reads.
func (e *Execution) SetOutput(node, version string) {
	e.appendOutput(node)

	if version == "" {
		return
	}

	if e.OutputVersions == nil {
		e.OutputVersions = make(map[string]string, 1)
	}

	e.OutputVersions[node] = version
}

// ClearOutput removes node's selected output from the control plane. The payload
// object itself may remain in the data plane until execution cleanup; without the
// selection Results will not read it.
func (e *Execution) ClearOutput(node string) {
	if len(e.Outputs) > 0 {
		outputs := e.Outputs[:0]
		for _, out := range e.Outputs {
			if out != node {
				outputs = append(outputs, out)
			}
		}

		e.Outputs = outputs
	}

	if e.OutputVersions != nil {
		delete(e.OutputVersions, node)
	}
}

// OutputVersion returns the committed version for node, or "" for legacy output
// entries written before output versioning was introduced.
func (e *Execution) OutputVersion(node string) string {
	if e.OutputVersions == nil {
		return ""
	}

	return e.OutputVersions[node]
}

func (e *Execution) appendOutput(node string) {
	for _, n := range e.Outputs {
		if n == node {
			return
		}
	}

	e.Outputs = append(e.Outputs, node)
}

// AppendWork adds a work-stream publish to the execution's outbox. Call inside
// the Mutate callback that commits the transition requiring it.
func (e *Execution) AppendWork(item json.RawMessage) {
	e.OutboxSeq++
	e.Outbox = append(e.Outbox, OutboxItem{Kind: OutboxWork, Item: item, Seq: e.OutboxSeq})
}

// AppendSched adds a scheduled delivery (firing at the absolute time at) to the
// execution's outbox. Call inside the Mutate callback that commits the
// transition requiring it.
func (e *Execution) AppendSched(item json.RawMessage, at time.Time) {
	e.OutboxSeq++
	e.Outbox = append(e.Outbox, OutboxItem{Kind: OutboxSched, Item: item, At: at.UTC(), Seq: e.OutboxSeq})
}

// DropOutbox removes the outbox items whose Seq is in flushed, keeping any
// appended since (a concurrent transition may have added more).
func (e *Execution) DropOutbox(flushed map[uint64]bool) (changed bool) {
	kept := e.Outbox[:0]

	for _, it := range e.Outbox {
		if flushed[it.Seq] {
			changed = true
			continue
		}

		kept = append(kept, it)
	}

	if len(kept) == 0 {
		e.Outbox = nil
		return changed
	}

	e.Outbox = kept

	return changed
}

// ActivityResult is an async activity completion stored on the execution when it
// arrives before the dispatching task has persisted its waiting state (the
// "completion before wait" race). The parking task consumes it instead of
// waiting. Status mirrors the invoker status string ("ok"/"error"/"retry").
type ActivityResult struct {
	Node       string          `json:"node"`
	Generation uint64          `json:"generation,omitempty"`
	Attempt    int             `json:"attempt"`
	Status     string          `json:"status"`
	Payload    json.RawMessage `json:"payload,omitempty"`
	Error      string          `json:"error,omitempty"`
}

// BranchState is the persisted control state of a single fanout branch; a
// completed branch's result lives in the data plane and is selected by
// Execution.OutputVersions when versioned.
type BranchState struct {
	NodeID     string `json:"node_id"`
	Status     string `json:"status"`
	Generation uint64 `json:"generation,omitempty"` // fanout visit generation that dispatched this branch
	Attempt    int    `json:"attempt,omitempty"`    // attempts spent on this branch (async retries)
	Error      string `json:"error,omitempty"`
}

// Active reports whether the execution is still in progress.
func (e *Execution) Active() bool {
	return e.Status == StatusRunning || e.Status == StatusWaiting
}

// Archivable reports whether the execution is terminal and will never be mutated
// again, so it can be swept from the hot bucket into the cold archive. Completed
// and cancelled qualify; failed does not, because Resume can revive a failed
// execution — it must stay hot and mutable. Keeping cancelled (terminal,
// non-resumable) hot would otherwise accumulate forever, bloating the hot bucket
// and every full Reconcile scan.
func (e *Execution) Archivable() bool {
	return e.Status == StatusCompleted || e.Status == StatusCancelled
}

// Dead-letter source kinds — which durable consumer dropped the poisoned work.
const (
	DeadLetterWork     = "work"     // the execution work consumer
	DeadLetterSchedule = "schedule" // the fired-schedule consumer (e.g. a removed-flow cron tick)
	DeadLetterSignal   = "signal"   // the external-signal consumer
	DeadLetterAsync    = "async"    // an async-invoker worker completion
)

// DeadLetter is a durable record of a poisoned work item that a consumer gave up
// on (Term'd) — a terminal error or an exhausted delivery cap. It is appended to
// the packtrail-deadletter stream so dropped work is observable (queryable and
// alertable) rather than vanishing into a log line. Kind identifies the source
// consumer; Key is its routing token (an execution id, a schedule key, or
// "<exec>/<node>" for an async completion).
type DeadLetter struct {
	Kind       string    `json:"kind"`
	Key        string    `json:"key"`
	Reason     string    `json:"reason"`
	Deliveries uint64    `json:"deliveries,omitempty"`
	Time       time.Time `json:"time"`
}

// Event is a domain event appended to the packtrail-events stream and consumed by
// the visibility indexer. Revision is the KV revision of the execution at the
// time the event was emitted, used for idempotent, per-revision indexing.
type Event struct {
	ExecID   string    `json:"exec_id"`
	FlowName string    `json:"flow_name"`
	Status   string    `json:"status"`
	Node     string    `json:"node"`
	Error    string    `json:"error,omitempty"`
	Revision uint64    `json:"revision"`
	Time     time.Time `json:"time"`
}
