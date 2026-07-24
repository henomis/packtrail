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

// Package protocol defines the public NATS request/reply contract between the
// Packtrail engine and the remote services that implement workflow tasks.
//
// This is the only package in the module intended to be imported by external
// projects. Everything under internal/ is private to the engine.
package protocol

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/nats-io/nats.go"
)

// Task response status values.
const (
	StatusOK    = "ok"    // task succeeded; Payload is the new shared context
	StatusError = "error" // task failed permanently (no retry requested)
	StatusRetry = "retry" // task asks the engine to retry per the node policy
)

// TaskRequest is the envelope sent by the engine to a task subject via
// request/reply.
type TaskRequest struct {
	ExecutionID string          `json:"execution_id"`
	NodeID      string          `json:"node_id"`
	Payload     json.RawMessage `json:"payload"`
	Generation  uint64          `json:"generation,omitempty"`
	Attempt     int             `json:"attempt"`
	Deadline    time.Time       `json:"deadline"`
}

// TaskResponse is the envelope a task service returns to the engine.
type TaskResponse struct {
	Status  string          `json:"status"`
	Payload json.RawMessage `json:"payload,omitempty"`
	Error   string          `json:"error,omitempty"`
}

// Handler implements the business logic of a single task. It receives the
// decoded request and returns a response. Returning a non-nil error is treated
// as a transient failure and reported to the engine as StatusRetry.
type Handler func(ctx context.Context, req TaskRequest) (TaskResponse, error)

// Serve subscribes a Handler to subject as a queue subscriber (queue group
// "packtrail-workers"), decoding TaskRequest and replying with TaskResponse. It is
// a convenience for task services and tests; the engine itself never calls it.
//
// ctx is the worker's lifetime: every handler invocation derives its context
// from it (tightened by the request's per-call deadline), so cancelling ctx at
// shutdown cancels in-flight handlers. Cancelling ctx does not unsubscribe —
// the returned subscription should still be drained/unsubscribed by the caller
// when done.
//
// Each request runs h on its own goroutine, so concurrent requests run h
// concurrently with each other — h must be safe for concurrent use. A
// handler panic is recovered and reported to the caller as StatusError; it
// does not take down the worker process.
//
// subject may contain NATS wildcards (e.g. "tasks.triage.*") so a single worker
// can serve every execution of a task.
func Serve(ctx context.Context, nc *nats.Conn, subject string, h Handler) (*nats.Subscription, error) {
	return nc.QueueSubscribe(subject, "packtrail-workers", func(msg *nats.Msg) {
		// Every NATS async callback for a *nats.Conn — every subscription, every
		// subject — is dispatched one at a time through one shared per-connection
		// goroutine. Handling the request inline here would block that goroutine
		// (and therefore every other subscription sharing nc, including unrelated
		// task kinds) for as long as h takes to return. Handing off to its own
		// goroutine per request restores per-request concurrency and keeps one
		// slow or wedged handler from stalling the rest of the process's traffic.
		go serveOne(ctx, msg, h)
	})
}

func serveOne(ctx context.Context, msg *nats.Msg, h Handler) {
	// A handler panic must still produce a reply (so the engine sees a failure
	// instead of waiting out a request timeout) and must not escape this
	// goroutine — an unrecovered panic here would crash the whole process,
	// taking down every other task kind sharing this connection, not just the
	// one execution that triggered it.
	defer func() {
		if r := recover(); r != nil {
			slog.Error("protocol: handler panic",
				"subject", msg.Subject, "panic", r, "stack", string(debug.Stack()))

			reply(msg, TaskResponse{Status: StatusError, Error: fmt.Sprintf("handler panic: %v", r)})
		}
	}()

	var req TaskRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		reply(msg, TaskResponse{Status: StatusError, Error: "bad request: " + err.Error()})
		return
	}

	callCtx := ctx

	if !req.Deadline.IsZero() {
		var cancel context.CancelFunc

		callCtx, cancel = context.WithDeadline(ctx, req.Deadline)
		defer cancel()
	}

	resp, err := h(callCtx, req)
	if err != nil {
		resp = TaskResponse{Status: StatusRetry, Error: err.Error()}
	} else if !validStatus(resp.Status) {
		resp = TaskResponse{
			Status: StatusError,
			Error: fmt.Sprintf(
				"handler returned invalid task status %q; want %q, %q, or %q",
				resp.Status, StatusOK, StatusError, StatusRetry),
		}
	}

	reply(msg, resp)
}

func validStatus(status string) bool {
	return status == StatusOK || status == StatusError || status == StatusRetry
}

// ServeNamespaced is like Serve but prepends namespace to subject, matching the
// convention used by the engine's built-in nats-task invoker. Use it for
// out-of-process workers so they subscribe to the right namespaced subject
// without having to construct it manually.
//
//	protocol.ServeNamespaced(ctx, nc, "packtrail", "tasks.triage.*", handler)
//	// subscribes to "packtrail.tasks.triage.*"
func ServeNamespaced(
	ctx context.Context, nc *nats.Conn, namespace, subject string, h Handler,
) (*nats.Subscription, error) {
	return Serve(ctx, nc, namespace+"."+subject, h)
}

func reply(msg *nats.Msg, resp TaskResponse) {
	data, err := json.Marshal(resp)
	if err != nil {
		if data2, marshalErr := json.Marshal(TaskResponse{Status: StatusError, Error: err.Error()}); marshalErr == nil {
			data = data2
		}
	}

	_ = msg.Respond(data)
}
