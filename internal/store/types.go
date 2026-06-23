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
)

// Branch status values.
const (
	BranchPending   = "pending"
	BranchCompleted = "completed"
	BranchFailed    = "failed"
)

// Execution is the runtime instance of a Flow. It is the single source of truth
// for an execution and is persisted in the packtrail-executions KV bucket.
type Execution struct {
	ID          string                     `json:"id"`
	FlowName    string                     `json:"flow_name"`
	CurrentNode string                     `json:"current_node"`
	Status      string                     `json:"status"`
	Payload     json.RawMessage            `json:"payload"`
	Attempt     int                        `json:"attempt"`               // attempts spent on CurrentNode (task retries)
	Branches    map[string]BranchState     `json:"branches,omitempty"`    // active fanout/fanin branches
	LastSeq     map[string]uint64          `json:"last_seq,omitempty"`    // last applied JetStream seq, per signal_name
	Signals     map[string]json.RawMessage `json:"signals,omitempty"`     // latest received payload, per signal_name
	WaitSignal  string                     `json:"wait_signal,omitempty"` // signal_name currently awaited
	Activity    *ActivityResult            `json:"activity,omitempty"`    //nolint:lll // async completion that arrived before the task parked
	Error       string                     `json:"error,omitempty"`
	Revision    uint64                     `json:"-"` // current KV revision, for CAS (not persisted in value)
	UpdatedAt   time.Time                  `json:"updated_at"`
}

// ActivityResult is an async activity completion stored on the execution when it
// arrives before the dispatching task has persisted its waiting state (the
// "completion before wait" race). The parking task consumes it instead of
// waiting. Status mirrors the invoker status string ("ok"/"error"/"retry").
type ActivityResult struct {
	Node    string          `json:"node"`
	Attempt int             `json:"attempt"`
	Status  string          `json:"status"`
	Payload json.RawMessage `json:"payload,omitempty"`
	Error   string          `json:"error,omitempty"`
}

// BranchState is the persisted state of a single fanout branch.
type BranchState struct {
	NodeID  string          `json:"node_id"`
	Status  string          `json:"status"`
	Attempt int             `json:"attempt,omitempty"` // attempts spent on this branch (async retries)
	Result  json.RawMessage `json:"result,omitempty"`
	Error   string          `json:"error,omitempty"`
}

// Active reports whether the execution is still in progress.
func (e *Execution) Active() bool {
	return e.Status == StatusRunning || e.Status == StatusWaiting
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
