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

// Package wire holds the message formats shared by the engine and the SDKs
// beyond commands and events: worker jobs, dead letters and cache entries,
// plus the publish helpers that apply the claim-check and deduplication ids.
package wire

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/henomis/packtrail/internal/blob"
	"github.com/henomis/packtrail/internal/cmd"
	"github.com/henomis/packtrail/internal/infra"
	"github.com/henomis/packtrail/internal/sched"
)

// Header names.
const (
	HeaderMsgID       = "Nats-Msg-Id"
	HeaderTraceparent = "traceparent"
	HeaderProtocol    = "Pt-Protocol-Version"
)

// Reply is the engine's answer to a command that carries a reply subject
// (cmd.Command.Reply), sent once the command is decided: OK with the sequence
// of the decision it produced (or of the last one, for a no-op), or a Code:
// "invalid" (malformed or does not fit the flow), "rejected" (refused in the
// execution's current state), "not_found", "failed" (dead-lettered).
type Reply struct {
	OK  bool   `json:"ok"`
	Seq uint64 `json:"seq,omitempty"`
	// Joined marks a start that found its execution already there.
	Joined bool   `json:"joined,omitempty"`
	Code   string `json:"code,omitempty"`
	Error  string `json:"error,omitempty"`
}

// Reply codes.
const (
	ReplyInvalid  = "invalid"
	ReplyRejected = "rejected"
	ReplyNotFound = "not_found"
	ReplyFailed   = "failed"
	ReplyConflict = "conflict"
)

// Progress is an intermediate result a worker publishes while it runs a job:
// core NATS, never written to the log, received only by clients already
// listening (G5-04). Seq counts the messages of one job attempt from 1.
type Progress struct {
	ExecID     string          `json:"exec_id"`
	Node       string          `json:"node"`
	Key        string          `json:"key"`
	Generation int             `json:"generation"`
	Attempt    int             `json:"attempt"`
	Seq        int64           `json:"seq"`
	Time       time.Time       `json:"time"`
	Data       json.RawMessage `json:"data,omitempty"`
}

// Stop tells workers that running jobs of an execution are no longer wanted:
// every job of the execution (All: it ended) or the listed attempts (a task
// was cancelled, an attempt timed out). Published on core NATS by the
// dispatcher, best effort: a worker that misses it learns from the log head
// that the execution ended, and any late result is stale anyway (G5-02).
type Stop struct {
	ExecID string     `json:"exec_id"`
	All    bool       `json:"all,omitempty"`
	Tasks  []StopTask `json:"tasks,omitempty"`
}

// StopTask names one attempt to stop.
type StopTask struct {
	Key        string `json:"key"`
	Generation int    `json:"generation"`
	Attempt    int    `json:"attempt"`
}

// Matches reports whether s stops the attempt of key.
func (s Stop) Matches(key string, generation, attempt int) bool {
	if s.All {
		return true
	}

	for _, t := range s.Tasks {
		if t.Key == key && t.Generation == generation && t.Attempt == attempt {
			return true
		}
	}

	return false
}

// Job is a unit of work for a worker of Kind.
type Job struct {
	ExecID     string          `json:"exec_id"`
	Flow       string          `json:"flow"`
	FlowHash   string          `json:"flow_hash"`
	Node       string          `json:"node"`
	Kind       string          `json:"kind"`
	Key        string          `json:"key"`
	Index      int             `json:"index,omitempty"`
	Generation int             `json:"generation"`
	Attempt    int             `json:"attempt"`
	Context    json.RawMessage `json:"context"`
	// Meta is the node's meta from the execution's flow version.
	Meta json.RawMessage `json:"meta,omitempty"`
	// Reply is the command subject the worker publishes its result to.
	Reply       string       `json:"reply"`
	CacheKey    string       `json:"cache_key,omitempty"`
	Concurrency *Concurrency `json:"concurrency,omitempty"`
	Traceparent string       `json:"traceparent,omitempty"`
}

// Concurrency is a per-key concurrency limit a worker must honour.
type Concurrency struct {
	Key string `json:"key"`
	Max int    `json:"max"`
}

// JobMsgID is the deduplication id of one attempt of a task instance.
func JobMsgID(execID, key string, gen, attempt int) string {
	return "job." + execID + "." + key + "." + strconv.Itoa(gen) + "." + strconv.Itoa(attempt)
}

// CmdID derives the id of the command a worker sends for one attempt: a
// redelivered job produces the same id, so the stream deduplicates it.
func CmdID(execID, key string, gen, attempt int) string {
	return "w." + execID + "." + key + "." + strconv.Itoa(gen) + "." + strconv.Itoa(attempt)
}

// DeadLetter is a message the system gave up on.
type DeadLetter struct {
	Kind string `json:"kind"`
	Key  string `json:"key"`
	// Flow is the flow a trigger dead letter was meant to start.
	Flow       string              `json:"flow,omitempty"`
	Reason     string              `json:"reason"`
	Deliveries uint64              `json:"deliveries,omitempty"`
	Time       time.Time           `json:"time"`
	Subject    string              `json:"subject"`
	Header     map[string][]string `json:"header,omitempty"`
	Body       []byte              `json:"body,omitempty"`
	// Seq is the dead-letter stream sequence (set on read).
	Seq uint64 `json:"-"`
}

// Dead-letter kinds.
const (
	DLQCommand = "cmd"
	DLQJob     = "job"
	DLQTrigger = "trigger"
	// DLQDispatch records an event the dispatcher gave up on (its execution
	// is quarantined).
	DLQDispatch = "dispatch"
)

// PublishDLQ records a dead letter. It is deduplicated per (kind, key,
// original subject sequence) so a crash between recording and terminating the
// original message does not record it twice.
func PublishDLQ(ctx context.Context, in *infra.Infra, d DeadLetter, dedup string) error {
	if d.Time.IsZero() {
		d.Time = time.Now().UTC()
	}

	b, err := json.Marshal(d)
	if err != nil {
		return err
	}

	m := nats.NewMsg(in.Names.DLQSubject(d.Kind, tokenOr(d.Key)))
	m.Data = b

	if dedup != "" {
		m.Header.Set(HeaderMsgID, "dlq."+dedup)
	}

	if _, err = in.JS.PublishMsg(ctx, m); err != nil {
		return fmt.Errorf("wire: dead letter: %w", err)
	}

	return nil
}

func tokenOr(s string) string {
	for _, r := range s {
		if r == '.' || r == '*' || r == '>' || r == ' ' {
			return "_"
		}
	}

	if s == "" {
		return "_"
	}

	return s
}

// msgID is the stream deduplication id of a command: its id, unless it asks
// for a reply. A caller retrying such a command (same id, new inbox) must reach
// the engine to get an answer; the fold still applies it once by its id.
func msgID(c cmd.Command) string {
	if c.Reply != "" {
		return c.ID + "@" + c.Reply
	}

	return c.ID
}

// PublishCmd publishes a command to its execution's partition, applying the
// claim-check and using the command id as the deduplication id (see msgID).
func PublishCmd(ctx context.Context, in *infra.Infra, c cmd.Command, traceparent string) error {
	if err := c.Validate(); err != nil {
		return err
	}

	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	m := nats.NewMsg(in.CmdSubject(c.ExecID))
	m.Header.Set(HeaderMsgID, msgID(c))
	m.Header.Set(cmd.HeaderType, string(c.Type))
	m.Header.Set(HeaderProtocol, infra.ProtocolVersion)

	if traceparent != "" {
		m.Header.Set(HeaderTraceparent, traceparent)
	}

	if m.Data, err = blob.Offload(ctx, in, c.ExecID, b, m.Header); err != nil {
		return err
	}

	if _, err = in.JS.PublishMsg(ctx, m); err != nil {
		return fmt.Errorf("wire: publish %s command: %w", c.Type, err)
	}

	return nil
}

// Timer ids of the expiry of an execution. They are not flow timers: no
// timer command fires for them.
const (
	// TimerExpire archives or deletes a terminal execution after its flow's
	// retention.
	TimerExpire = "archive"
	// TimerDelete deletes an execution: an archived one after its flow's
	// archive retention, or one whose deletion had to wait for the
	// deduplication window.
	TimerDelete = "delete"
)

// ScheduleCmd installs a one-shot JetStream schedule on the timer subject
// that publishes command c to the execution's command subject at time at.
// The schedule subject is a rollup, so re-installing it is idempotent.
func ScheduleCmd(ctx context.Context, in *infra.Infra, execID, timerID string, at time.Time, c cmd.Command) error {
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	m := nats.NewMsg(in.Names.TimerSubject(execID, timerID))
	m.Header.Set(sched.HeaderSchedule, sched.At(at))
	m.Header.Set(sched.HeaderScheduleTarget, in.CmdSubject(execID))
	m.Header.Set(cmd.HeaderType, string(c.Type))
	m.Data = b

	if _, err = in.JS.PublishMsg(ctx, m); err != nil {
		return fmt.Errorf("wire: schedule %s command: %w", c.Type, err)
	}

	return nil
}

// triggerDataKey wraps a trigger message that is not a JSON object.
const triggerDataKey = "data"

// TriggerStart is the start command a trigger of flow publishes for a message
// with body data, starting execID. A body that is not a JSON object is wrapped
// as {"data": …}.
func TriggerStart(flow, execID string, data []byte) (cmd.Command, error) {
	input := json.RawMessage(data)
	if len(data) == 0 || !json.Valid(data) {
		b, _ := json.Marshal(map[string]string{triggerDataKey: string(data)}) //nolint:errchkjson // strings always encode
		input = b
	} else if data[0] != '{' {
		b, _ := json.Marshal(map[string]json.RawMessage{triggerDataKey: data}) //nolint:errchkjson // data is valid JSON
		input = b
	}

	return cmd.New("trigger."+execID, cmd.Start, execID, cmd.StartData{Flow: flow, Input: input})
}

// CacheEntry is a cached task result.
type CacheEntry struct {
	Output  json.RawMessage            `json:"output,omitempty"`
	Writes  map[string]json.RawMessage `json:"writes,omitempty"`
	Next    string                     `json:"next,omitempty"`
	Usage   map[string]float64         `json:"usage,omitempty"`
	Expires time.Time                  `json:"expires"`
}
