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

// Package cmd defines the commands that drive an execution. A command is a
// request (start, complete a task, deliver a signal, fire a timer, …) published
// on the execution's partition of the command stream; the engine decides which
// events it produces. Every command carries an id: the stream deduplicates on
// it (Nats-Msg-Id) and the decision is idempotent, so redelivery is harmless.
//
// The JSON encoding is the wire protocol shared by every SDK (docs/protocol.md).
package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/henomis/packtrail/event"
	"github.com/henomis/packtrail/internal/names"
)

// Type names a command.
type Type string

// Command types.
const (
	Start     Type = "start"
	Fork      Type = "fork"
	Complete  Type = "complete"
	Fail      Type = "fail"
	Interrupt Type = "interrupt"
	Resume    Type = "resume"
	Signal    Type = "signal"
	Cancel    Type = "cancel"
	// Update writes channels of a running execution (synchronous: the
	// command carries a reply subject).
	Update    Type = "update"
	Timer     Type = "timer"
	ChildDone Type = "child_done"
	Archive   Type = "archive"
	// Redispatch lifts the quarantine of an execution (dispatcher replay).
	Redispatch Type = "redispatch"
)

// Headers.
const (
	HeaderMsgID       = "Nats-Msg-Id"
	HeaderType        = "Pt-Cmd-Type"
	HeaderTraceparent = "traceparent"
)

// Command is the envelope of every command.
type Command struct {
	ID     string          `json:"id"`
	Type   Type            `json:"type"`
	ExecID string          `json:"exec_id"`
	Time   time.Time       `json:"time,omitzero"`
	Data   json.RawMessage `json:"data,omitempty"`
	// Reply, when set, is a subject the engine answers on once the command is
	// decided (stored, a no-op, or rejected): wire.Reply.
	Reply string `json:"reply,omitempty"`
}

// StartData is the payload of Start.
type StartData struct {
	Flow string `json:"flow"`
	// Version pins a flow hash; empty means the latest registered version.
	Version string           `json:"version,omitempty"`
	Input   json.RawMessage  `json:"input,omitempty"`
	Parent  *event.ParentRef `json:"parent,omitempty"`
}

// ForkData is the payload of Fork: create ExecID from the state of From at
// sequence Seq (the end of a decision).
type ForkData struct {
	From string `json:"from"`
	Seq  uint64 `json:"seq"`
	// Writes edit the fork's channels (through their reducers) before it
	// resumes: LangGraph's update_state then continue.
	Writes map[string]json.RawMessage `json:"writes,omitempty"`
}

// UpdateData is the payload of Update. The command id is the update id: an
// update is applied once, however often it is delivered.
type UpdateData struct {
	Writes map[string]json.RawMessage `json:"writes"`
}

// TaskRef identifies one attempt of a task instance; a completion for any
// other attempt is stale and ignored.
type TaskRef struct {
	Key        string `json:"key"`
	Generation int    `json:"generation"`
	Attempt    int    `json:"attempt"`
}

// CompleteData is the payload of Complete.
type CompleteData struct {
	TaskRef

	Output   json.RawMessage            `json:"output,omitempty"`
	Writes   map[string]json.RawMessage `json:"writes,omitempty"`
	Next     string                     `json:"next,omitempty"`
	Usage    map[string]float64         `json:"usage,omitempty"`
	CacheKey string                     `json:"cache_key,omitempty"`
	Cached   bool                       `json:"cached,omitempty"`
}

// FailData is the payload of Fail.
type FailData struct {
	TaskRef

	Error     string             `json:"error"`
	Retryable bool               `json:"retryable"`
	Reason    string             `json:"reason,omitempty"`
	Usage     map[string]float64 `json:"usage,omitempty"`
}

// InterruptData is the payload of Interrupt.
type InterruptData struct {
	TaskRef

	Payload json.RawMessage    `json:"payload,omitempty"`
	Usage   map[string]float64 `json:"usage,omitempty"`
}

// ResumeData is the payload of Resume: re-run the interrupted node with Value
// available to the worker as `resume`.
type ResumeData struct {
	Node  string          `json:"node"`
	Key   string          `json:"key,omitempty"`
	Value json.RawMessage `json:"value,omitempty"`
}

// SignalData is the payload of Signal. The command id is the signal id.
type SignalData struct {
	Name    string          `json:"name"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// CancelData is the payload of Cancel.
type CancelData struct {
	Reason string `json:"reason,omitempty"`
}

// TimerData is the payload of Timer (published by a JetStream schedule).
type TimerData struct {
	TimerID string `json:"timer_id"`
}

// ChildDoneData is the payload of ChildDone, sent to the parent.
type ChildDoneData struct {
	ChildID  string             `json:"child_id"`
	Status   string             `json:"status"`
	Output   json.RawMessage    `json:"output,omitempty"`
	Error    string             `json:"error,omitempty"`
	Counters map[string]float64 `json:"counters,omitempty"`
}

// New builds a command with payload data.
func New(id string, t Type, execID string, data any) (Command, error) {
	c := Command{ID: id, Type: t, ExecID: execID}

	if data != nil {
		b, err := json.Marshal(data)
		if err != nil {
			return Command{}, fmt.Errorf("cmd: encode %s: %w", t, err)
		}

		c.Data = b
	}

	return c, nil
}

// Validate checks the envelope.
func (c Command) Validate() error {
	if c.ID == "" {
		return errors.New("cmd: missing id")
	}

	if len(c.ID) > maxIDLen {
		return fmt.Errorf("cmd: id longer than %d", maxIDLen)
	}

	if err := names.CheckToken("execution id", c.ExecID); err != nil {
		return fmt.Errorf("cmd: %w", err)
	}

	switch c.Type {
	case Start, Fork, Complete, Fail, Interrupt, Resume, Signal, Cancel, Timer, ChildDone, Archive, Redispatch,
		Update:
		return nil
	default:
		return fmt.Errorf("cmd: unknown type %q", c.Type)
	}
}

const maxIDLen = 256

// Decode parses a command body and validates the envelope.
func Decode(body []byte) (Command, error) {
	var c Command
	if err := json.Unmarshal(body, &c); err != nil {
		return Command{}, fmt.Errorf("cmd: decode: %w", err)
	}

	return c, c.Validate()
}

// Payload decodes the command payload into v.
func (c Command) Payload(v any) error {
	if len(c.Data) == 0 {
		return nil
	}

	if err := json.Unmarshal(c.Data, v); err != nil {
		return fmt.Errorf("cmd: decode %s payload: %w", c.Type, err)
	}

	return nil
}
