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
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
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

	// In-flight jobs run under procParent, detached from ctx's cancellation, so a
	// graceful shutdown drains running jobs within drainTimeout instead of aborting
	// their invocation (and the CompleteActivity that settles it) mid-flight. A hard
	// crash still relies on redelivery (at-least-once); a clean restart no longer
	// re-runs in-flight jobs for non-idempotent targets. stopProc aborts stragglers
	// past the drain deadline.
	procParent, stopProc := context.WithCancel(context.WithoutCancel(ctx))
	defer stopProc()

	var inflight sync.WaitGroup

	cc, err := cons.Consume(func(msg jetstream.Msg) {
		if ctx.Err() != nil {
			_ = msg.NakWithDelay(nakDelay)
			return
		}

		w.sem <- struct{}{}

		inflight.Add(1)

		go func() {
			defer inflight.Done()
			defer func() { <-w.sem }()

			w.handle(procParent, msg)
		}()
	})
	if err != nil {
		return fmt.Errorf("asyncqueue: consume: %w", err)
	}

	defer func() {
		cc.Stop()
		w.drain(&inflight, stopProc)
	}()

	<-ctx.Done()

	return nil
}

// drain waits for in-flight jobs to settle, bounded by drainTimeout; past the
// window it cancels the stragglers (their jobs redeliver) and waits for them to
// unwind so no job goroutine outlives Run.
func (w *Worker) drain(inflight *sync.WaitGroup, stopProc context.CancelFunc) {
	done := make(chan struct{})

	go func() {
		inflight.Wait()
		close(done)
	}()

	select {
	case <-done:
		return
	case <-time.After(w.cfg.drainTimeout):
		w.log.Warn("drain timeout: aborting in-flight jobs", "timeout", w.cfg.drainTimeout)
		stopProc()
		inflight.Wait()
	}
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
		// A terminal completion error (e.g. the engine no longer knows the flow)
		// can never succeed; an exhausted transient one would Nak forever. Either
		// way, dead-letter the job (Term) instead of redelivering indefinitely.
		if isTerminal(err) || deliveriesExhausted(msg, w.cfg.maxDeliver) {
			w.log.Warn("dead-lettering job", "exec", j.ExecID, "node", j.Node, "err", err)

			if w.cfg.deadLetterSink != nil {
				w.cfg.deadLetterSink(ctx, j.ExecID+"/"+j.Node, err.Error(), numDelivered(msg))
			}

			_ = msg.Term()

			return
		}

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
// the effective timeout: the per-node budget when set, capped by the worker's
// activityTimeout backstop. A non-nil error is a transient transport fault and
// maps to StatusRetry so the engine re-dispatches per the node's retry policy.
//
// A panic from the embedder's Invoker is recovered and converted into a
// StatusError result: this runs on the worker's own goroutine, which has no
// recovery above it, so an unrecovered panic would crash the entire hosting
// process (the engine and every other in-flight job). A panic is treated as a
// permanent failure rather than a retry — a redelivery would likely re-panic —
// with a logged stack for diagnosis; the activity is settled (failed) via
// CompleteActivity like any other outcome. Embedders should still not panic; this
// is a backstop so one buggy invocation fails just its execution, not the fleet.
func (w *Worker) invoke(ctx context.Context, j job) (res invoker.Result) {
	timeout := w.effectiveTimeout(j)

	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	defer func() {
		if r := recover(); r != nil {
			w.log.Error("invoker panic",
				"exec", j.ExecID, "node", j.Node, "attempt", j.Attempt,
				"panic", r, "stack", string(debug.Stack()))

			res = invoker.Result{
				Status: invoker.StatusError,
				Error:  fmt.Sprintf("invoker panic: %v", r),
			}
		}
	}()

	var err error

	res, err = w.exec.Invoke(callCtx, invoker.Request{
		Invoker:     w.kind,
		Target:      j.Target,
		ExecutionID: j.ExecID,
		NodeID:      j.Node,
		Payload:     j.Payload,
		Attempt:     j.Attempt,
		Deadline:    time.Now().Add(timeout),
	})
	if err != nil {
		return invoker.Result{Status: invoker.StatusRetry, Error: err.Error()}
	}

	return res
}

// effectiveTimeout bounds an invocation by min(node timeout, activityTimeout):
// the worker's activityTimeout is the ceiling/backstop, and a node that sets a
// shorter timeout tightens it. A node with no timeout (j.Timeout == 0) runs at
// the full activityTimeout. A node timeout longer than the backstop is capped at
// it — raise WithActivityTimeout if longer calls are required.
func (w *Worker) effectiveTimeout(j job) time.Duration {
	timeout := w.cfg.activityTimeout
	if j.Timeout > 0 && j.Timeout < timeout {
		timeout = j.Timeout
	}

	return timeout
}

// isTerminal reports whether err (or one it wraps) declares itself non-retryable
// via interface{ Terminal() bool }. The check is structural so asyncqueue need
// not import the runtime package that defines the terminal completion error.
func isTerminal(err error) bool {
	var t interface{ Terminal() bool }

	return errors.As(err, &t) && t.Terminal()
}

// deliveriesExhausted reports whether a job has been delivered at least
// maxDeliver times. A non-positive maxDeliver disables the cap; a metadata read
// failure is treated as not-exhausted so a transient fault keeps retrying rather
// than prematurely dead-lettering.
func deliveriesExhausted(msg jetstream.Msg, maxDeliver int) bool {
	if maxDeliver <= 0 {
		return false
	}

	meta, err := msg.Metadata()
	if err != nil {
		return false
	}

	return meta.NumDelivered >= uint64(maxDeliver) //nolint:gosec // maxDeliver is a small positive config value
}

// numDelivered returns a message's delivery count, or 0 if unavailable.
func numDelivered(msg jetstream.Msg) uint64 {
	if meta, err := msg.Metadata(); err == nil {
		return meta.NumDelivered
	}

	return 0
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
