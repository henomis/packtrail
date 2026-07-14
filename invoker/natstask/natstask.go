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

// Package natstask is packtrail's built-in Invoker: a NATS request/reply caller that
// speaks the pkg/protocol envelope. Any task worker that serves the protocol
// (protocol.Serve on "tasks.<x>.*") works with it unchanged. It pulls in no
// dependency beyond NATS, keeping the packtrail core ecosystem-agnostic.
package natstask

import (
	"context"
	"encoding/json"

	"github.com/nats-io/nats.go"

	"github.com/henomis/packtrail/invoker"
	"github.com/henomis/packtrail/pkg/protocol"
)

// Kind is the invoker kind under which this Invoker is conventionally registered.
const Kind = "nats-task"

// Invoker performs request/reply against the node's target subject.
type Invoker struct {
	nc     *nats.Conn
	prefix string
}

// New returns a nats-task Invoker over nc. prefix is the namespace (e.g.
// "packtrail" or "acme") prepended to every task subject so workers are
// isolated per deployment. An empty prefix defaults to "packtrail".
func New(nc *nats.Conn, prefix string) *Invoker {
	if prefix == "" {
		prefix = "packtrail"
	}

	return &Invoker{nc: nc, prefix: prefix}
}

// Invoke marshals req into a protocol.TaskRequest, performs a NATS request to
// req.Target (the resolved subject), and maps the protocol.TaskResponse back to
// an invoker.Result. The request honours ctx's deadline (set by the engine from
// the node timeout).
func (i *Invoker) Invoke(ctx context.Context, req invoker.Request) (invoker.Result, error) {
	treq := protocol.TaskRequest{
		ExecutionID: req.ExecutionID,
		NodeID:      req.NodeID,
		Payload:     req.Payload,
		Generation:  req.Generation,
		Attempt:     req.Attempt,
		Deadline:    req.Deadline,
	}

	data, err := json.Marshal(treq)
	if err != nil {
		return invoker.Result{}, err
	}

	callCtx := ctx

	if !req.Deadline.IsZero() {
		if ctxDeadline, ok := ctx.Deadline(); !ok || req.Deadline.Before(ctxDeadline) {
			var cancel context.CancelFunc

			callCtx, cancel = context.WithDeadline(ctx, req.Deadline)
			defer cancel()
		}
	}

	msg, err := i.nc.RequestWithContext(callCtx, i.prefix+"."+req.Target, data)
	if err != nil {
		return invoker.Result{}, err
	}

	var tresp protocol.TaskResponse

	err = json.Unmarshal(msg.Data, &tresp)
	if err != nil {
		return invoker.Result{}, err
	}

	// The request/reply protocol defines only ok/error/retry. "pending" is the
	// engine's async-park sentinel: passed through, it would park the execution
	// waiting for a CompleteActivity that no request/reply worker ever sends —
	// a permanent, watchdog-excluded stall. Fail the node with an actionable
	// reason instead; any other out-of-contract status flows through to the
	// engine's unknown-status fail-fast.
	if tresp.Status == string(invoker.StatusPending) {
		return invoker.Result{
			Status: invoker.StatusError,
			Error: "nats-task worker replied status \"pending\", which the request/reply protocol " +
				"does not support; use an asynchronous Invoker (StatusPending + CompleteActivity) for async work",
		}, nil
	}

	return invoker.Result{
		Status:  invoker.Status(tresp.Status),
		Payload: tresp.Payload,
		Error:   tresp.Error,
	}, nil
}
