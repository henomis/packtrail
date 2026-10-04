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

// Package worker is the Go SDK for packtrail workers. A worker serves one
// kind: it pulls jobs from <ns>.work.<kind>, runs a handler, and reports the
// outcome as a command (complete, fail, interrupt) to the job's reply
// subject. The protocol is plain NATS + JSON; SDKs in other
// languages implement the same steps.
//
//	w, _ := worker.New(nc, "summarize", func(ctx context.Context, j *worker.Job) (*worker.Result, error) {
//		var in struct{ Text string }
//		if err := j.Input(&in); err != nil {
//			return nil, worker.Permanent(err)
//		}
//		return &worker.Result{Output: map[string]any{"summary": in.Text[:10]}}, nil
//	})
//	_ = w.Run(ctx)
package worker

import (
	"encoding/json"
	"errors"
	"fmt"
)

// Job is one attempt of a task instance.
type Job struct {
	ExecID     string
	Flow       string
	FlowHash   string
	Node       string
	Kind       string
	Key        string
	Index      int
	Generation int
	Attempt    int
	// Deliveries counts deliveries of this job message (redelivery after a
	// crash or nak).
	Deliveries  uint64
	Traceparent string
	// Context is the execution view: input, channels, results, last_node,
	// visits, signals, branches, counters, errors, and item/index/resume when
	// set.
	Context Context
	// Meta is the node's meta (flow.Node.Meta) as defined in the flow version
	// the execution runs, so a redeploy never changes it under a running
	// execution, a fork or a rerun. Nil when the node has none.
	Meta json.RawMessage

	progress *progressSink
	store    *Store
}

// NodeError is how a node last failed permanently.
type NodeError struct {
	Error   string `json:"error"`
	Reason  string `json:"reason"`
	Attempt int    `json:"attempt,omitempty"`
	// Index is the failed item of a map.
	Index *int `json:"index,omitempty"`
}

// Context is the execution view handed to a worker.
type Context struct {
	Input    json.RawMessage            `json:"input"`
	Channels map[string]json.RawMessage `json:"channels"`
	Results  map[string]json.RawMessage `json:"results"`
	LastNode string                     `json:"last_node"`
	Visits   map[string]int             `json:"visits"`
	Signals  map[string]json.RawMessage `json:"signals"`
	Branches map[string]string          `json:"branches"`
	Counters map[string]float64         `json:"counters"`
	// Errors holds the last permanent failure of each node: what an
	// on_failure handler reacts to (errors[last_node] for the node that
	// routed here).
	Errors map[string]NodeError `json:"errors"`
	Item   json.RawMessage      `json:"item,omitempty"`
	Index  *int                 `json:"index,omitempty"`
	Resume json.RawMessage      `json:"resume,omitempty"`
	// Interrupt is the payload the instance interrupted with, on every
	// attempt of its resumed run; absent otherwise.
	Interrupt json.RawMessage `json:"interrupt,omitempty"`
}

func decodeInto(raw json.RawMessage, v any) error {
	if len(raw) == 0 {
		return errors.New("worker: value not present")
	}

	return json.Unmarshal(raw, v)
}

// DecodeMeta decodes the node's meta into v.
func (j *Job) DecodeMeta(v any) error { return decodeInto(j.Meta, v) }

// Input decodes the execution input.
func (j *Job) Input(v any) error { return decodeInto(j.Context.Input, v) }

// Channel decodes the current value of a state channel.
func (j *Job) Channel(name string, v any) error { return decodeInto(j.Context.Channels[name], v) }

// Result decodes the latest output of a node.
func (j *Job) Result(node string, v any) error { return decodeInto(j.Context.Results[node], v) }

// Signal decodes a consumed signal payload.
func (j *Job) Signal(name string, v any) error { return decodeInto(j.Context.Signals[name], v) }

// Item decodes the map item of this instance.
func (j *Job) Item(v any) error { return decodeInto(j.Context.Item, v) }

// Resumed reports whether the job is the resumption of an interrupt, and
// decodes the resume value into v (when v is non-nil).
func (j *Job) Resumed(v any) (bool, error) {
	if len(j.Context.Resume) == 0 {
		return false, nil
	}

	if v == nil {
		return true, nil
	}

	return true, json.Unmarshal(j.Context.Resume, v)
}

// Interrupted reports whether the job is a resumed run carrying the payload
// its instance interrupted with (Interrupt(payload)), and decodes it into v
// (when v is non-nil). An interrupt without payload resumes with false.
func (j *Job) Interrupted(v any) (bool, error) {
	if len(j.Context.Interrupt) == 0 {
		return false, nil
	}

	if v == nil {
		return true, nil
	}

	return true, json.Unmarshal(j.Context.Interrupt, v)
}

// Result is the outcome of a successful job.
type Result struct {
	// Output is the node output: a JSON object (a map, a struct, or a
	// json.RawMessage holding an object), or nil.
	Output any
	// Writes are deltas for state channels, folded by each channel's reducer.
	Writes map[string]any
	// Next routes to a declared dynamic successor (Command(goto)).
	Next string
	// Usage reports generic counters (budgets).
	Usage map[string]float64
}

// Error classes.
type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// Permanent marks err as terminal: the attempt fails without retry (I-11).
func Permanent(err error) error {
	if err == nil {
		return nil
	}

	return &permanentError{err: err}
}

// IsPermanent reports whether err was marked Permanent.
func IsPermanent(err error) bool {
	var p *permanentError

	return errors.As(err, &p)
}

// UsageError carries the usage of a run that ended with an error (see
// WithUsage). It unwraps to Err, so the error is classified as Err is.
type UsageError struct {
	Err   error
	Usage map[string]float64
}

func (e *UsageError) Error() string { return e.Err.Error() }
func (e *UsageError) Unwrap() error { return e.Err }

// WithUsage attaches usage counters to the error a handler returns, so a run
// that interrupts or fails still adds what it spent to the execution's
// counters (budgets), as Result.Usage does for a run that completes. It
// composes with Interrupt, Permanent and %w wrapping in any order; when
// several are nested, the outermost counts. Every attempt's usage is added,
// once. WithUsage(nil, …) is nil: a successful run reports Result.Usage.
func WithUsage(err error, usage map[string]float64) error {
	if err == nil {
		return nil
	}

	return &UsageError{Err: err, Usage: usage}
}

// usageOf returns the usage attached to err, if any.
func usageOf(err error) map[string]float64 {
	var u *UsageError
	if errors.As(err, &u) {
		return u.Usage
	}

	return nil
}

// InterruptError pauses the execution at this node until Client.Resume.
type InterruptError struct {
	Payload any
}

func (e *InterruptError) Error() string { return "worker: interrupted" }

// Interrupt returns an error that interrupts the node with payload (shown to
// whoever resumes it). On resume the node runs again with Job.Resumed true
// and the payload in Job.Interrupted.
func Interrupt(payload any) error { return &InterruptError{Payload: payload} }

func marshalOutput(v any) (json.RawMessage, error) {
	if v == nil {
		return nil, nil
	}

	var b []byte

	switch x := v.(type) {
	case json.RawMessage:
		b = x
	case []byte:
		b = x
	default:
		var err error
		if b, err = json.Marshal(v); err != nil {
			return nil, fmt.Errorf("worker: encode output: %w", err)
		}
	}

	return b, nil
}
