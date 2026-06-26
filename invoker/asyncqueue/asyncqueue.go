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

// Package asyncqueue is packtrail's durable asynchronous Invoker: it makes any
// ordinary synchronous Invoker run off the engine's critical path with
// at-least-once delivery. The Invoker contract's StatusPending +
// Server.CompleteActivity is the seam — asyncqueue supplies the machinery the
// engine deliberately leaves out, so embedding apps with slow nodes (an agent
// call, an HTTP request, a shell-exec) do not block an engine slot.
//
// It has two halves:
//
//   - Dispatcher is the parking side. It implements invoker.Invoker: instead of
//     running the node inline it publishes a durable job to a JetStream
//     work-queue and returns StatusPending, so the engine parks the execution
//     (waiting) and frees its slot.
//   - Worker is the execution side. It consumes those jobs, runs the embedder's
//     own synchronous Invoker, and settles the result via Completer
//     (CompleteActivity). A job is acked only after the completion is handed
//     back, so a crashed worker's job is redelivered (at-least-once).
//
// It is ecosystem-agnostic: the executor is a plain invoker.Invoker, so
// asyncqueue knows nothing about what the slow work actually is. Like
// invoker/natstask it depends on nothing beyond NATS.
package asyncqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail/invoker"
)

// Completer settles an asynchronous activity back on the packtrail engine.
// *packtrail.Server satisfies it.
type Completer interface {
	CompleteActivity(ctx context.Context, execID, node string, attempt int, res invoker.Result) error
}

// job is the durable work item a Dispatcher publishes and a Worker consumes. It
// carries only generic invoker.Request fields — nothing about the kind of work.
type job struct {
	ExecID  string          `json:"exec_id"`
	Node    string          `json:"node"`
	Attempt int             `json:"attempt"`
	Target  string          `json:"target"`
	Payload json.RawMessage `json:"payload"`
}

// StreamName derives the work-queue stream name for an async invoker kind.
func StreamName(prefix, kind string) string { return prefix + "-async-" + kind }

// Subject derives the job subject for an async invoker kind.
func Subject(prefix, kind string) string { return prefix + ".async." + kind }

// durable derives the worker's durable consumer name for an async invoker kind.
func durable(prefix, kind string) string { return prefix + "-async-" + kind + "-worker" }

// EnsureStream creates (idempotently) the JetStream work-queue stream that
// carries jobs for kind, with a dedup window so a redelivered dispatch does not
// double-enqueue. Call it once before publishing or consuming.
func EnsureStream(ctx context.Context, js jetstream.JetStream, prefix, kind string, opts ...Option) error {
	c := newConfig(opts)

	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:       StreamName(prefix, kind),
		Subjects:   []string{Subject(prefix, kind)},
		Retention:  jetstream.WorkQueuePolicy,
		Duplicates: c.dedupWindow,
	})
	if err != nil {
		return fmt.Errorf("asyncqueue: ensure stream %q: %w", kind, err)
	}

	return nil
}

// Dispatcher implements invoker.Invoker by publishing a durable job and reporting
// it as pending, so the engine parks the execution and frees its slot for the
// node's whole runtime. The job's dedup id is exec.node.attempt, so a redelivered
// dispatch of the same attempt is collapsed; a genuine retry (new attempt) is not.
type Dispatcher struct {
	js      jetstream.JetStream
	subject string
}

// NewDispatcher returns a Dispatcher publishing to the job subject for kind.
func NewDispatcher(js jetstream.JetStream, prefix, kind string) *Dispatcher {
	return &Dispatcher{js: js, subject: Subject(prefix, kind)}
}

// Invoke publishes the job and reports it as pending.
func (d *Dispatcher) Invoke(ctx context.Context, req invoker.Request) (invoker.Result, error) {
	data, err := json.Marshal(job{
		ExecID:  req.ExecutionID,
		Node:    req.NodeID,
		Attempt: req.Attempt,
		Target:  req.Target,
		Payload: req.Payload,
	})
	if err != nil {
		return invoker.Result{}, err
	}

	msgID := req.ExecutionID + "." + req.NodeID + "." + strconv.Itoa(req.Attempt)

	_, err = d.js.Publish(ctx, d.subject, data, jetstream.WithMsgID(msgID))
	if err != nil {
		// Dispatch failed: let the engine retry the dispatch per the node policy.
		return invoker.Result{}, fmt.Errorf("asyncqueue: publish job: %w", err)
	}

	return invoker.Result{Status: invoker.StatusPending}, nil
}
