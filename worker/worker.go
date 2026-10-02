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

package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail/internal/blob"
	"github.com/henomis/packtrail/internal/cmd"
	"github.com/henomis/packtrail/internal/consume"
	"github.com/henomis/packtrail/internal/infra"
	"github.com/henomis/packtrail/internal/names"
	"github.com/henomis/packtrail/internal/wire"
)

// Handler runs one job. Return a Result on success; an error fails the
// attempt (retried by the node's policy unless Permanent); Interrupt pauses
// the execution. A panic is recovered and treated as a retryable failure: it
// never takes the worker process down (I-21).
type Handler func(ctx context.Context, job *Job) (*Result, error)

// Option configures a Worker.
type Option func(*Worker) error

// Defaults.
const (
	DefaultConcurrency = 4
	DefaultAckWait     = 30 * time.Second
	DefaultMaxDeliver  = 5
	minAckWait         = time.Second
	// DefaultMaxAckPending bounds the jobs of one kind in flight across all
	// worker processes. Each process takes only as many as its concurrency.
	DefaultMaxAckPending = 10000
	semRetryFirst        = 50 * time.Millisecond
	semRetryCap          = time.Second
)

// WithNamespace sets the namespace (default "packtrail").
func WithNamespace(ns string) Option {
	return func(w *Worker) error {
		if !names.ValidPrefix(ns) {
			return fmt.Errorf("worker: invalid namespace %q", ns)
		}

		w.ns = ns

		return nil
	}
}

// WithConcurrency bounds parallel jobs; the consumer never prefetches more
// than this: prefetched jobs would expire while waiting for a slot.
func WithConcurrency(n int) Option {
	return func(w *Worker) error {
		if n > 0 {
			w.concurrency = n
		}

		return nil
	}
}

// WithAckWait sets how long a job may go without a heartbeat before it is
// redelivered. Values under one second are raised to one second (a
// nanosecond ack wait would break the heartbeat ticker).
func WithAckWait(d time.Duration) Option {
	return func(w *Worker) error {
		w.ackWait = max(d, minAckWait)

		return nil
	}
}

// WithMaxDeliver sets how many deliveries a job gets before it is
// dead-lettered and its node failed (redelivery happens on crash or when the
// command cannot be published).
func WithMaxDeliver(n int) Option {
	return func(w *Worker) error {
		if n > 0 {
			w.maxDeliver = n
		}

		return nil
	}
}

// WithMaxAckPending bounds the jobs of this kind in flight across every
// worker process (default 10000). It is a kind-wide setting: see
// WithReconfigure.
func WithMaxAckPending(n int) Option {
	return func(w *Worker) error {
		if n > 0 {
			w.maxAckPending = n
		}

		return nil
	}
}

// WithReconfigure makes this worker apply its kind-wide settings (ack wait,
// max deliver, max ack pending) to the shared consumer. Without it the
// consumer is created once by the first worker and then used as is, so
// processes started with different options do not overwrite each other.
func WithReconfigure() Option {
	return func(w *Worker) error {
		w.reconfigure = true

		return nil
	}
}

// WithDrainTimeout bounds the graceful drain on shutdown.
func WithDrainTimeout(d time.Duration) Option {
	return func(w *Worker) error {
		if d > 0 {
			w.drain = d
		}

		return nil
	}
}

// WithBlobTimeout bounds one claim-check transfer (large job contexts and
// results) when the handler context has no deadline (default 2m).
func WithBlobTimeout(d time.Duration) Option {
	return func(w *Worker) error {
		if d > 0 {
			w.blobTimeout = d
		}

		return nil
	}
}

// WithLogger sets the logger.
func WithLogger(l *slog.Logger) Option {
	return func(w *Worker) error {
		w.logger = l

		return nil
	}
}

// Worker serves one kind.
type Worker struct {
	nc          *nats.Conn
	kind        string
	handler     Handler
	ns          string
	concurrency int
	ackWait     time.Duration
	maxDeliver  int
	// effective kind-wide settings, read from the shared consumer.
	effMaxDeliver atomic.Int64
	effAckWait    atomic.Int64
	maxAckPending int
	reconfigure   bool

	waitMu      sync.Mutex
	waiting     map[string]bool
	drain       time.Duration
	logger      *slog.Logger
	blobTimeout time.Duration

	in *infra.Infra

	// running jobs, matched by stop notices; liveCheck is how often a running
	// job checks its execution's log head (G5-02).
	running   jobs
	liveCheck time.Duration
	streamMu  sync.Mutex
	events    jetstream.Stream
}

// New returns a worker for kind. It performs no I/O.
func New(nc *nats.Conn, kind string, handler Handler, opts ...Option) (*Worker, error) {
	if nc == nil || handler == nil {
		return nil, errors.New("worker: nil connection or handler")
	}

	if err := names.CheckToken("worker kind", kind); err != nil {
		return nil, fmt.Errorf("worker: %w", err)
	}

	w := &Worker{
		nc: nc, kind: kind, handler: handler, ns: names.Default, concurrency: DefaultConcurrency,
		ackWait: DefaultAckWait, maxDeliver: DefaultMaxDeliver, logger: slog.Default(),
		maxAckPending: DefaultMaxAckPending, liveCheck: DefaultLiveCheck,
	}

	for _, o := range opts {
		if err := o(w); err != nil {
			return nil, err
		}
	}

	return w, nil
}

// Run serves jobs until ctx is done, then drains in-flight jobs.
func (w *Worker) Run(ctx context.Context) error {
	in, err := infra.New(w.nc, names.New(w.ns), w.logger)
	if err != nil {
		return err
	}

	if err = in.Attach(ctx); err != nil {
		return err
	}

	if w.blobTimeout > 0 {
		in.BlobTimeout = w.blobTimeout
	}

	w.in = in

	sub, err := w.subscribeStops()
	if err != nil {
		return err
	}

	defer func() { _ = sub.Unsubscribe() }()

	return consume.Run(ctx, in.JS, consume.Config{
		Stream: in.Names.StreamWork,
		Consumer: jetstream.ConsumerConfig{
			Durable: in.Names.DurWorker(w.kind), FilterSubject: in.Names.WorkSubject(w.kind),
			AckPolicy: jetstream.AckExplicitPolicy, AckWait: w.ackWait, MaxAckPending: w.maxAckPending,
			// Unlimited on the server: busy-key hand-backs are naks too, and
			// only the worker can tell them from real redeliveries (F2-02).
			MaxDeliver: -1,
		},
		Concurrency:  w.concurrency,
		KeepExisting: !w.reconfigure,
		OnReady: func(info *jetstream.ConsumerInfo) {
			md := w.maxDeliver
			if info.Config.MaxDeliver > 1 {
				md = info.Config.MaxDeliver - 1 // consumer created before F2-02
			}

			w.effMaxDeliver.Store(int64(md))
			w.effAckWait.Store(int64(info.Config.AckWait))
		},
		Drain:   w.drain,
		Logger:  w.logger,
		Handler: w.handle,
	})
}

func (w *Worker) handle(ctx context.Context, msg jetstream.Msg) {
	md, err := msg.Metadata()
	if err != nil {
		_ = msg.Nak()

		return
	}

	// A job from a newer protocol cannot be interpreted safely (5.1).
	if v := msg.Headers().Get(wire.HeaderProtocol); v != "" && v != infra.ProtocolVersion {
		w.deadLetter(ctx, msg, md, "", "unsupported protocol version "+v)

		return
	}

	body, ok, err := w.resolve(ctx, msg)
	if err != nil {
		w.deadLetter(ctx, msg, md, "", "job body: "+err.Error())

		return
	}

	if !ok {
		_ = msg.Nak() // shutting down

		return
	}

	var wj wire.Job
	if err = json.Unmarshal(body, &wj); err != nil {
		w.deadLetter(ctx, msg, md, "", "undecodable job: "+err.Error())

		return
	}

	// Real redeliveries are a crash or a failed publish; hand-backs because a
	// key was busy are not attempts and are subtracted (F-01, F2-02).
	// Deliveries only grow when a process died holding the job or could not
	// publish its result: a busy key never naks, it hands a fresh copy back.
	if md.NumDelivered > uint64(w.effMaxDeliver.Load()) { //nolint:gosec // positive.
		w.exhausted(ctx, msg, md, wj)

		return
	}

	release, ok := w.slot(ctx, msg, wj)
	if !ok {
		return // handed back
	}
	defer release()

	jctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	rj := &runningJob{key: wj.Key, generation: wj.Generation, attempt: wj.Attempt, cancel: cancel}
	w.running.add(wj.ExecID, rj)

	defer w.running.remove(wj.ExecID, rj)

	stopLive := w.watchLive(jctx, wj, cancel)
	c, err := w.run(jctx, wj, md.NumDelivered)

	stopLive()

	if errors.Is(context.Cause(jctx), ErrCancelled) {
		// The work is no longer wanted: its result would be stale (G5-02).
		w.logger.Debug("worker: job cancelled", "exec", wj.ExecID, "node", wj.Node, "key", wj.Key)

		_ = msg.Ack()

		return
	}

	if err != nil {
		w.logger.Warn("worker: cannot build result", "exec", wj.ExecID, "node", wj.Node, "err", err)
	}

	if err = w.publish(ctx, wj, c); err != nil {
		w.logger.Warn("worker: cannot publish result, job will be redelivered", "exec", wj.ExecID, "err", err)

		_ = msg.NakWithDelay(time.Second)

		return
	}

	_ = msg.Ack()
}

// resolve reads a claim-checked job body. A read failure is retried while the
// job is kept (heartbeated), so an object-store blip never spends a delivery
// attempt; only a blob still missing after a few tries (not a lagging
// replica) is permanent (err != nil). ok=false means shutdown.
func (w *Worker) resolve(ctx context.Context, msg jetstream.Msg) (body []byte, ok bool, err error) {
	delay := semRetryFirst
	stopping := consume.ShuttingDown(ctx)
	missing := 0

	for {
		b, rerr := blob.Resolve(ctx, w.in, msg.Data(), msg.Headers())
		if rerr == nil {
			return b, true, nil
		}

		if errors.Is(rerr, jetstream.ErrObjectNotFound) {
			if missing++; missing >= blobNotFoundTries {
				return nil, false, rerr
			}
		}

		w.logger.Warn("worker: cannot read job body, will retry", "subject", msg.Subject(), "err", rerr)

		select {
		case <-ctx.Done():
			return nil, false, nil
		case <-stopping:
			return nil, false, nil
		case <-time.After(delay):
		}

		delay = min(delay*2, semRetryCap) //nolint:mnd // exponential backoff.
	}
}

// blobNotFoundTries is how many reads a missing job body gets before it is
// treated as gone rather than not replicated yet.
const blobNotFoundTries = 3

// run invokes the handler and turns its outcome into a command.
func (w *Worker) run(ctx context.Context, wj wire.Job, deliveries uint64) (cmd.Command, error) {
	job := &Job{
		ExecID: wj.ExecID, Flow: wj.Flow, FlowHash: wj.FlowHash, Node: wj.Node, Kind: wj.Kind, Key: wj.Key,
		Index: wj.Index, Generation: wj.Generation, Attempt: wj.Attempt, Deliveries: deliveries,
		Traceparent: wj.Traceparent, progress: w.progressSink(wj),
	}

	ref := cmd.TaskRef{Key: wj.Key, Generation: wj.Generation, Attempt: wj.Attempt}
	id := wire.CmdID(wj.ExecID, wj.Key, wj.Generation, wj.Attempt)

	if err := json.Unmarshal(wj.Context, &job.Context); err != nil {
		return failCmd(id, wj.ExecID, ref, "undecodable context: "+err.Error(), false)
	}

	res, err := w.call(ctx, job)

	var intr *InterruptError

	switch {
	case errors.As(err, &intr):
		p, merr := marshalOutput(intr.Payload)
		if merr != nil {
			return failCmd(id, wj.ExecID, ref, merr.Error(), false)
		}

		return cmd.New(id, cmd.Interrupt, wj.ExecID, cmd.InterruptData{TaskRef: ref, Payload: p})
	case err != nil:
		return failCmd(id, wj.ExecID, ref, err.Error(), !IsPermanent(err))
	}

	return completeCmd(id, wj, ref, res)
}

func (w *Worker) call(ctx context.Context, job *Job) (res *Result, err error) {
	defer func() {
		if p := recover(); p != nil {
			w.logger.Error("worker: handler panic", "exec", job.ExecID, "node", job.Node, "panic", p,
				"stack", string(debug.Stack()))

			res, err = nil, fmt.Errorf("handler panic: %v", p)
		}
	}()

	return w.handler(ctx, job)
}

func failCmd(id, execID string, ref cmd.TaskRef, msg string, retryable bool) (cmd.Command, error) {
	return cmd.New(id, cmd.Fail, execID, cmd.FailData{TaskRef: ref, Error: msg, Retryable: retryable})
}

func completeCmd(id string, wj wire.Job, ref cmd.TaskRef, res *Result) (cmd.Command, error) {
	if res == nil {
		res = &Result{}
	}

	out, err := marshalOutput(res.Output)
	if err != nil {
		return failCmd(id, wj.ExecID, ref, err.Error(), false)
	}

	var writes map[string]json.RawMessage

	if len(res.Writes) > 0 {
		writes = make(map[string]json.RawMessage, len(res.Writes))

		for k, v := range res.Writes {
			b, lerr := marshalOutput(v)
			if lerr != nil {
				return failCmd(id, wj.ExecID, ref, lerr.Error(), false)
			}

			if b == nil {
				b = json.RawMessage("null")
			}

			writes[k] = b
		}
	}

	return cmd.New(id, cmd.Complete, wj.ExecID, cmd.CompleteData{
		TaskRef: ref, Output: out, Writes: writes, Next: res.Next, Usage: res.Usage, CacheKey: wj.CacheKey,
	})
}

func (w *Worker) publish(ctx context.Context, wj wire.Job, c cmd.Command) error {
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	m := nats.NewMsg(wj.Reply)
	m.Header.Set(wire.HeaderMsgID, c.ID)
	m.Header.Set(cmd.HeaderType, string(c.Type))
	m.Header.Set(wire.HeaderProtocol, infra.ProtocolVersion)

	if wj.Traceparent != "" {
		m.Header.Set(wire.HeaderTraceparent, wj.Traceparent)
	}

	if m.Data, err = blob.Offload(ctx, w.in, wj.ExecID, b, m.Header); err != nil {
		return err
	}

	_, err = w.in.JS.PublishMsg(ctx, m)

	return err
}

// exhausted fails the node and dead-letters a job that was delivered too
// many times (the worker kept dying on it, or its result could not be
// published).
func (w *Worker) exhausted(ctx context.Context, msg jetstream.Msg, md *jetstream.MsgMetadata, wj wire.Job) {
	ref := cmd.TaskRef{Key: wj.Key, Generation: wj.Generation, Attempt: wj.Attempt}

	c, err := cmd.New(wire.CmdID(wj.ExecID, wj.Key, wj.Generation, wj.Attempt), cmd.Fail, wj.ExecID,
		cmd.FailData{
			TaskRef: ref, Error: "job delivery attempts exhausted", Retryable: false,
			Reason: "delivery_failed",
		})
	if err == nil {
		err = w.publish(ctx, wj, c)
	}

	if err != nil {
		_ = msg.NakWithDelay(time.Second)

		return
	}

	w.deadLetter(ctx, msg, md, wj.ExecID, "job delivery attempts exhausted")
}

func (w *Worker) deadLetter(ctx context.Context, msg jetstream.Msg, md *jetstream.MsgMetadata, execID, reason string) {
	d := wire.DeadLetter{
		Kind: wire.DLQJob, Key: execID, Reason: reason, Deliveries: md.NumDelivered, Subject: msg.Subject(),
		Header: msg.Headers(), Body: msg.Data(),
	}

	if err := wire.PublishDLQ(ctx, w.in, d, "job."+strconv.FormatUint(md.Sequence.Stream, 10)); err != nil {
		_ = msg.NakWithDelay(time.Second)

		return
	}

	if err := msg.Term(); err != nil {
		w.logger.Warn("worker: term failed", "err", err)
	}
}
