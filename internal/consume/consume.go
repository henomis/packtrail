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

// Package consume runs a JetStream pull consumer with packtrail's lifecycle:
// optional pinned priority group (one active consumer per partition with
// managed failover), bounded concurrency, panic isolation and a graceful
// drain on shutdown — in-flight handlers keep running under a context detached
// from the shutdown signal and are only cancelled when the drain budget is
// exhausted (I-15).
package consume

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// DefaultDrain is the default drain budget.
const DefaultDrain = 30 * time.Second

// Handler processes one message and must ack/nak/term it.
type Handler func(ctx context.Context, msg jetstream.Msg)

// Config configures Run.
type Config struct {
	Stream   string
	Consumer jetstream.ConsumerConfig
	// Group, when set, makes the consumer a pinned priority group: only one
	// client receives messages at a time, another takes over when it stops
	// pulling.
	Group string
	// Concurrency bounds parallel handlers (<= 1: strictly serial).
	Concurrency int
	Drain       time.Duration
	// PullExpiry bounds how long a pull request lives on the server: a
	// request left behind by a stopped client can absorb a message until the
	// ack wait expires (0 = DefaultPullExpiry).
	PullExpiry time.Duration
	// KeepExisting uses an existing consumer as configured instead of
	// updating it (consumers shared by many processes, e.g. workers).
	KeepExisting bool
	// OnReady receives the consumer's effective configuration.
	OnReady func(*jetstream.ConsumerInfo)
	// Pulling is called once, when the consumer starts pulling messages.
	Pulling func()
	Logger  *slog.Logger
	Handler Handler
}

const (
	pinnedTTL    = 2 * time.Second
	unpinTimeout = 2 * time.Second
	// DefaultPullExpiry is the default lifetime of a pull request.
	DefaultPullExpiry = 5 * time.Second
	// DefaultAckWait is the default ack wait. Handlers send InProgress while
	// they run, so the ack wait only bounds how long a message held by a dead
	// process waits before redelivery — not how long a handler may take.
	DefaultAckWait = 5 * time.Second
	minHeartbeat   = 100 * time.Millisecond
	fetchRetry     = 100 * time.Millisecond
	// setupRetryFirst and setupRetryCap bound the backoff of consumer setup.
	setupRetryFirst = time.Second
	setupRetryCap   = 30 * time.Second
)

// Run creates (or updates) the consumer and processes messages until ctx is
// done, then drains.
func Run(ctx context.Context, js jetstream.JetStream, cfg Config) error {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	if cfg.Drain <= 0 {
		cfg.Drain = DefaultDrain
	}

	cc := cfg.Consumer
	if cc.AckWait <= 0 {
		cc.AckWait = DefaultAckWait
	}

	if cfg.PullExpiry <= 0 {
		cfg.PullExpiry = DefaultPullExpiry
	}

	if cfg.Group != "" {
		cc.PriorityPolicy = jetstream.PriorityPolicyPinned
		cc.PriorityGroups = []string{cfg.Group}
		cc.PinnedTTL = pinnedTTL
	}

	cons, err := ensureConsumerRetry(ctx, js, cfg, cc)
	if err != nil {
		return err
	}

	// The consumer may be shared and configured by another process: heartbeat
	// against the ack wait it really has.
	info := cons.CachedInfo()
	if cfg.OnReady != nil {
		cfg.OnReady(info)
	}

	ackWait := info.Config.AckWait
	if ackWait <= 0 {
		ackWait = cc.AckWait
	}

	r := &runner{cfg: cfg, heartbeat: max(ackWait/3, minHeartbeat)} //nolint:mnd // a third of the ack wait.
	r.stopCh = make(chan struct{})
	pctx, procCancel := context.WithCancel(context.WithoutCancel(ctx))
	r.procCtx, r.procCancel = context.WithValue(pctx, stopKey{}, (<-chan struct{})(r.stopCh)), procCancel

	defer r.procCancel()

	if cfg.Concurrency > 1 {
		r.sem = make(chan struct{}, cfg.Concurrency)
		r.fetchLoop(ctx, cons)
	} else if err = r.consumeSerial(ctx, cons); err != nil {
		return err
	}

	r.drain()

	if cfg.Group != "" {
		// Hand the partition over now instead of after the pin TTL: the next
		// engine pulling takes it (correctness never depends on the pin —
		// appends are guarded by expected sequences).
		uctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), unpinTimeout)
		defer cancel()

		if s, ierr := js.Stream(uctx, cfg.Stream); ierr == nil {
			_ = s.UnpinConsumer(uctx, cc.Durable, cfg.Group)
		}
	}

	return nil
}

// ensureConsumerRetry sets the consumer up, retrying with backoff until it
// succeeds or ctx ends. A failure here is transient by nature (an API timeout
// while a cluster creates hundreds of replicated consumers, a leader
// election, a server restart, a stream not provisioned yet); giving up would
// leave a partition with no engine or dispatcher until the process restarts.
func ensureConsumerRetry(ctx context.Context, js jetstream.JetStream, cfg Config,
	cc jetstream.ConsumerConfig,
) (jetstream.Consumer, error) {
	delay := setupRetryFirst

	for {
		cons, err := ensureConsumer(ctx, js, cfg.Stream, cc, cfg.KeepExisting)
		if err == nil {
			return cons, nil
		}

		if ctx.Err() != nil {
			return nil, fmt.Errorf("consume: consumer %s on %s: %w", cc.Durable, cfg.Stream, err)
		}

		cfg.Logger.Warn("packtrail: cannot set up consumer, will retry", "consumer", cc.Durable,
			"stream", cfg.Stream, "in", delay, "err", err)

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("consume: consumer %s on %s: %w", cc.Durable, cfg.Stream, err)
		case <-time.After(delay):
		}

		delay = min(delay*2, setupRetryCap) //nolint:mnd // exponential backoff.
	}
}

// ensureConsumer creates the consumer, or updates it — unless keepExisting,
// in which case an existing consumer is used as configured: a consumer shared
// by many processes must not be reconfigured by whichever started last.
func ensureConsumer(ctx context.Context, js jetstream.JetStream, stream string, cc jetstream.ConsumerConfig,
	keepExisting bool,
) (jetstream.Consumer, error) {
	if !keepExisting {
		return js.CreateOrUpdateConsumer(ctx, stream, cc)
	}

	cons, err := js.Consumer(ctx, stream, cc.Durable)
	if err == nil {
		return cons, nil
	}

	if !errors.Is(err, jetstream.ErrConsumerNotFound) {
		return nil, err
	}

	cons, err = js.CreateConsumer(ctx, stream, cc)
	if errors.Is(err, jetstream.ErrConsumerExists) {
		return js.Consumer(ctx, stream, cc.Durable)
	}

	return cons, err
}

// consumeSerial processes one message at a time with a push-style consume
// loop (engine partitions, dispatcher, cron): strictly ordered.
func (r *runner) consumeSerial(ctx context.Context, cons jetstream.Consumer) error {
	opts := []jetstream.PullConsumeOpt{jetstream.PullExpiry(r.cfg.PullExpiry), jetstream.PullMaxMessages(1)}
	if r.cfg.Group != "" {
		opts = append(opts, jetstream.PullPriorityGroup(r.cfg.Group))
	}

	sub, err := cons.Consume(r.deliver, opts...)
	if err != nil {
		return fmt.Errorf("consume: %s: %w", r.cfg.Consumer.Durable, err)
	}

	r.pulling()

	<-ctx.Done()
	r.fence()
	sub.Stop()

	return nil
}

// fence stops new deliveries from joining the in-flight group, so Wait never
// races an Add.
// pulling reports, the first time, that the consumer has asked for messages.
func (r *runner) pulling() {
	if r.pulled {
		return
	}

	r.pulled = true

	if r.cfg.Pulling != nil {
		r.cfg.Pulling()
	}
}

func (r *runner) fence() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.stopping {
		r.stopping = true
		close(r.stopCh)
	}
}

type stopKey struct{}

// ShuttingDown returns a channel closed when the runner that called the
// handler starts shutting down. Handler contexts are detached from shutdown
// (in-flight work drains); a handler that is only *waiting* (not working)
// uses this to give its message back at once (F2-06).
func ShuttingDown(ctx context.Context) <-chan struct{} {
	if ch, ok := ctx.Value(stopKey{}).(<-chan struct{}); ok {
		return ch
	}

	return nil
}

// fetchLoop pulls exactly as many messages as this process has free slots:
// concurrency is per process (more processes = more capacity), and nothing is
// buffered client-side where it could expire unheartbeated.
func (r *runner) fetchLoop(ctx context.Context, cons jetstream.Consumer) {
	defer r.fence()

	for {
		select {
		case <-ctx.Done():
			return
		case r.sem <- struct{}{}:
		}

		want := 1

	more:
		for want < cap(r.sem) {
			select {
			case r.sem <- struct{}{}:
				want++
			default:
				break more
			}
		}

		got := r.fetchBatch(ctx, cons, want)

		for range want - got {
			<-r.sem
		}
	}
}

func (r *runner) fetchBatch(ctx context.Context, cons jetstream.Consumer, want int) int {
	// The fetch ends at the pull expiry or on shutdown, whichever comes first.
	fctx, cancel := context.WithTimeout(ctx, r.cfg.PullExpiry)
	defer cancel()

	opts := []jetstream.FetchOpt{jetstream.FetchContext(fctx)}
	if r.cfg.Group != "" {
		opts = append(opts, jetstream.FetchPriorityGroup(r.cfg.Group))
	}

	batch, err := cons.Fetch(want, opts...)
	if err != nil {
		if ctx.Err() == nil {
			r.cfg.Logger.Debug("packtrail: fetch", "consumer", r.cfg.Consumer.Durable, "err", err)
			time.Sleep(fetchRetry)
		}

		return 0
	}

	r.pulling()

	got := 0

	for msg := range batch.Messages() {
		got++

		r.mu.Lock()
		if r.stopping {
			r.mu.Unlock()

			_ = msg.Nak()

			<-r.sem

			continue
		}

		r.inflight.Add(1)
		r.mu.Unlock()

		go func() {
			defer func() { <-r.sem }()

			r.handle(msg)
		}()
	}

	return got
}

type runner struct {
	cfg        Config
	heartbeat  time.Duration
	sem        chan struct{}
	inflight   sync.WaitGroup
	mu         sync.Mutex
	stopping   bool
	stopCh     chan struct{}
	procCtx    context.Context //nolint:containedctx // detached handler context, lives with the runner.
	procCancel context.CancelFunc

	// pulled is set once Pulling was called (read and written only by the
	// goroutine that pulls).
	pulled bool
}

func (r *runner) deliver(msg jetstream.Msg) {
	r.mu.Lock()
	if r.stopping {
		r.mu.Unlock()

		_ = msg.Nak()

		return
	}

	r.inflight.Add(1)
	r.mu.Unlock()

	r.handle(msg)
}

func (r *runner) handle(msg jetstream.Msg) {
	defer r.inflight.Done()

	defer func() {
		if p := recover(); p != nil {
			r.cfg.Logger.Error("packtrail: handler panic", "consumer", r.cfg.Consumer.Durable, "panic", p,
				"stack", string(debug.Stack()))

			_ = msg.Nak()
		}
	}()

	stop := r.keepAlive(msg)
	defer stop()

	r.cfg.Handler(r.procCtx, msg)
}

// keepAlive sends InProgress while a handler runs, so a slow handler (a large
// replay, an archive, a slow blob) is never redelivered to another process
// while it is still working.
func (r *runner) keepAlive(msg jetstream.Msg) func() {
	done := make(chan struct{})

	go func() {
		defer func() {
			if p := recover(); p != nil {
				r.cfg.Logger.Error("packtrail: heartbeat panic", "consumer", r.cfg.Consumer.Durable, "panic", p)
			}
		}()

		t := time.NewTicker(r.heartbeat)
		defer t.Stop()

		for {
			select {
			case <-done:
				return
			case <-t.C:
				_ = msg.InProgress()
			}
		}
	}()

	return func() { close(done) }
}

// drain waits for in-flight handlers within the budget, then cancels them.
func (r *runner) drain() {
	done := make(chan struct{})

	go func() {
		r.inflight.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(r.cfg.Drain):
		r.cfg.Logger.Warn("packtrail: drain budget exhausted, cancelling in-flight work",
			"consumer", r.cfg.Consumer.Durable)
		r.procCancel()
		<-done
	}
}
