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

package asyncqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail/invoker"
)

const (
	maxAckPendingFactor = 2
	nakDelay            = 2 * time.Second
	heartbeatDivisor    = 3
)

// Worker consumes jobs for one async invoker kind and runs the embedder's
// synchronous Invoker off the engine's critical path, reporting completion via
// Completer. It is the durability boundary: a job is acked only after the
// completion is handed back, so a crashed worker's job is redelivered
// (at-least-once). Many Workers (in or out of process) can share a kind's
// work-queue stream to scale horizontally.
type Worker struct {
	js        jetstream.JetStream
	prefix    string
	kind      string
	exec      invoker.Invoker
	completer Completer
	cfg       config
	sem       chan struct{}
	log       *slog.Logger
}

// NewWorker builds a Worker that runs exec for jobs of kind and settles them via
// completer. prefix and kind must match the Dispatcher's. EnsureStream must have
// been called for the same kind (packtrail's WithAsyncInvoker does both).
func NewWorker(
	js jetstream.JetStream, prefix, kind string,
	exec invoker.Invoker, completer Completer, opts ...Option,
) *Worker {
	c := newConfig(opts)

	return &Worker{
		js:        js,
		prefix:    prefix,
		kind:      kind,
		exec:      exec,
		completer: completer,
		cfg:       c,
		sem:       make(chan struct{}, c.concurrency),
		log:       slog.Default().With("component", "asyncqueue-worker", "kind", kind),
	}
}

// Run consumes jobs until ctx is cancelled.
func (w *Worker) Run(ctx context.Context) error {
	cons, err := w.js.CreateOrUpdateConsumer(ctx, StreamName(w.prefix, w.kind), jetstream.ConsumerConfig{
		Durable:       durable(w.prefix, w.kind),
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       w.cfg.ackWait,
		MaxAckPending: w.cfg.concurrency * maxAckPendingFactor,
		FilterSubject: Subject(w.prefix, w.kind),
	})
	if err != nil {
		return fmt.Errorf("asyncqueue: job consumer: %w", err)
	}

	cc, err := cons.Consume(func(msg jetstream.Msg) {
		w.sem <- struct{}{}

		go func() {
			defer func() { <-w.sem }()

			w.handle(ctx, msg)
		}()
	})
	if err != nil {
		return fmt.Errorf("asyncqueue: consume: %w", err)
	}
	defer cc.Stop()

	<-ctx.Done()

	return nil
}

// handle runs one job and settles it, extending the ack window while the
// invocation is in flight.
func (w *Worker) handle(ctx context.Context, msg jetstream.Msg) {
	var j job
	if err := json.Unmarshal(msg.Data(), &j); err != nil {
		w.log.Error("bad job", "err", err)

		_ = msg.Term() // poison: do not redeliver

		return
	}

	hb, cancelHB := context.WithCancel(ctx)
	go w.heartbeat(hb, msg)

	defer cancelHB()

	res := w.invoke(ctx, j)
	if err := w.completer.CompleteActivity(ctx, j.ExecID, j.Node, j.Attempt, res); err != nil {
		w.log.Error("complete activity", "exec", j.ExecID, "node", j.Node, "err", err)

		if nakErr := msg.NakWithDelay(nakDelay); nakErr != nil {
			w.log.Warn("nak job", "exec", j.ExecID, "node", j.Node, "err", nakErr)
		}

		return
	}

	if ackErr := msg.Ack(); ackErr != nil {
		w.log.Warn("ack job", "exec", j.ExecID, "node", j.Node, "err", ackErr)
	}
}

// invoke reconstructs the node invocation and runs the embedder's Invoker under
// the activity timeout. A non-nil error is a transient transport fault and maps
// to StatusRetry so the engine re-dispatches per the node's retry policy.
func (w *Worker) invoke(ctx context.Context, j job) invoker.Result {
	callCtx, cancel := context.WithTimeout(ctx, w.cfg.activityTimeout)
	defer cancel()

	res, err := w.exec.Invoke(callCtx, invoker.Request{
		Invoker:     w.kind,
		Target:      j.Target,
		ExecutionID: j.ExecID,
		NodeID:      j.Node,
		Payload:     j.Payload,
		Attempt:     j.Attempt,
		Deadline:    time.Now().Add(w.cfg.activityTimeout),
	})
	if err != nil {
		return invoker.Result{Status: invoker.StatusRetry, Error: err.Error()}
	}

	return res
}

func (w *Worker) heartbeat(ctx context.Context, msg jetstream.Msg) {
	t := time.NewTicker(w.cfg.ackWait / heartbeatDivisor)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := msg.InProgress(); err != nil {
				w.log.Warn("heartbeat InProgress", "err", err)
			}
		}
	}
}
