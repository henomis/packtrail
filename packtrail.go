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

// Package packtrail is a durable workflow engine built only on NATS
// JetStream. It is event-sourced: every execution is an ordered log of events
// on its own subject, the single source of truth from which state, snapshots,
// indexes, jobs and timers are derived. It interprets a declarative graph
// (tasks, choices, fan-out/join, awaits, dynamic maps, subflows) with typed
// state channels and reducers, durable timers, interrupts, time travel and
// forks — and it is agnostic: a task is work done by a worker of some kind, in
// any language, over a versioned NATS protocol.
//
// An Engine processes commands and dispatches work; a Client starts and
// drives executions; workers (package worker) execute tasks.
package packtrail

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail/flow"
	"github.com/henomis/packtrail/internal/dispatch"
	"github.com/henomis/packtrail/internal/engine"
	"github.com/henomis/packtrail/internal/eventlog"
	"github.com/henomis/packtrail/internal/infra"
	"github.com/henomis/packtrail/internal/loader"
	"github.com/henomis/packtrail/internal/metrics"
	"github.com/henomis/packtrail/internal/names"
	"github.com/henomis/packtrail/internal/projection"
	"github.com/henomis/packtrail/internal/registry"
	"github.com/henomis/packtrail/internal/snapshot"
	"github.com/henomis/packtrail/internal/statecache"
)

// Metrics is a snapshot of an engine's counters.
type Metrics = metrics.Snapshot

// Engine runs packtrail: command processing per partition, the dispatcher,
// cron and triggers.
type Engine struct {
	nc  *nats.Conn
	cfg config

	initMu sync.Mutex
	ready  bool

	in      *infra.Infra
	ld      *loader.Loader
	eng     *engine.Engine
	disp    *dispatch.Dispatcher
	metrics *metrics.M
	client  *Client
}

// New validates the configuration and returns an Engine. It performs no I/O
// (I-10): resources are provisioned by Init (or lazily by Run).
func New(nc *nats.Conn, opts ...Option) (*Engine, error) {
	if nc == nil {
		return nil, fmt.Errorf("%w: nil connection", ErrInvalidArgument)
	}

	cfg, err := buildConfig(opts)
	if err != nil {
		return nil, err
	}

	if cfg.logger == nil {
		cfg.logger = slog.Default()
	}

	return &Engine{nc: nc, cfg: cfg, metrics: &metrics.M{}}, nil
}

// buildConfig applies opts and checks the result. It is every validation New
// does besides the connection, shared with ValidateOptions so the two cannot
// disagree.
func buildConfig(opts []Option) (config, error) {
	cfg := config{namespace: names.Default}

	for _, o := range opts {
		if err := o(&cfg); err != nil {
			return config{}, err
		}
	}

	seen := map[string]bool{}

	for _, f := range cfg.flows {
		if seen[f.Name] {
			return config{}, fmt.Errorf("%w: flow %q registered twice", ErrInvalidArgument, f.Name)
		}

		seen[f.Name] = true
	}

	return cfg, nil
}

// Init provisions the namespace (streams, buckets, object stores), checks the
// server, registers the configured flows and installs the configured
// schedules. It is idempotent.
func (e *Engine) Init(ctx context.Context) error {
	e.initMu.Lock()
	defer e.initMu.Unlock()

	if e.ready {
		return nil
	}

	in, err := infra.New(e.nc, names.New(e.cfg.namespace), e.cfg.logger)
	if err != nil {
		return err
	}

	if e.cfg.blobThreshold > 0 {
		in.BlobThreshold = e.cfg.blobThreshold
	}

	e.cfg.timeouts.apply(in)
	in.Replicas = e.cfg.replicas

	if err = in.Provision(ctx, e.cfg.partitions); err != nil {
		return err
	}

	e.in = in
	e.ld = loader.New(in, eventlog.New(in), snapshot.New(in, e.cfg.snapshotEvery), registry.New(in))
	e.eng = &engine.Engine{
		In: in, Loader: e.ld, Cache: statecache.New(e.cfg.cacheSize), Metrics: e.metrics, Drain: e.cfg.drain,
		Now: e.cfg.clock, HistoryLimit: historyLimit(e.cfg.historyLimit),
	}
	e.disp = &dispatch.Dispatcher{
		In: in, Loader: e.ld, Cache: statecache.New(e.cfg.cacheSize), Metrics: e.metrics, Drain: e.cfg.drain,
	}
	e.eng.Redispatch = e.disp.Replay
	e.client = newClientFromInfra(in, e.ld)

	for _, f := range e.cfg.flows {
		if _, err = e.ld.Flows.Register(ctx, f); err != nil {
			return fmt.Errorf("packtrail: register %s: %w", f.Name, err)
		}
	}

	for _, s := range e.cfg.schedules {
		if err = e.client.Schedule(ctx, s.name, s.flow, s.cron, s.input, ScheduleTimeZone(s.tz)); err != nil {
			return err
		}
	}

	e.ready = true

	return nil
}

// Run processes commands and dispatches effects until ctx is done, then
// drains in-flight work within the drain budget. It calls Init if needed.
func (e *Engine) Run(ctx context.Context) error {
	if err := e.Init(ctx); err != nil {
		return err
	}

	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)

	run := func(fn func(context.Context) error) {
		wg.Go(func() {
			if err := fn(ctx); err != nil && !errors.Is(err, context.Canceled) {
				mu.Lock()

				errs = append(errs, err)
				mu.Unlock()
			}
		})
	}

	for _, p := range e.partitions() {
		if !e.cfg.noCommands {
			run(func(ctx context.Context) error { return e.eng.RunPartition(ctx, p) })
		}

		if !e.cfg.noDispatch {
			run(func(ctx context.Context) error { return e.disp.Run(ctx, p) })
		}
	}

	if !e.cfg.noCommands {
		run(e.eng.RunCron)

		for _, f := range e.cfg.flows {
			for i, tr := range f.Triggers {
				run(func(ctx context.Context) error { return e.eng.RunTrigger(ctx, f, i, tr) })
			}
		}
	}

	wg.Wait()

	return errors.Join(errs...)
}

func (e *Engine) partitions() []int {
	if len(e.cfg.owned) > 0 {
		var out []int

		for _, p := range e.cfg.owned {
			if p < e.in.Partitions {
				out = append(out, p)
			}
		}

		return out
	}

	out := make([]int, e.in.Partitions)
	for i := range out {
		out[i] = i
	}

	return out
}

// Client returns a client bound to this engine's namespace. Valid after Init.
func (e *Engine) Client() *Client { return e.client }

// Register stores a flow definition (a new version when it changed) and
// returns its version hash. Valid after Init.
func (e *Engine) Register(ctx context.Context, def *flow.Flow) (string, error) {
	return e.client.Register(ctx, def)
}

// Partitions returns the deployed partition count. Valid after Init.
func (e *Engine) Partitions() int { return e.in.Partitions }

// Metrics returns the engine counters and the dispatcher lag.
func (e *Engine) Metrics(ctx context.Context) Metrics {
	m := e.metrics.Snapshot()

	if e.in == nil {
		return m
	}

	for _, p := range e.partitions() {
		c, err := e.in.JS.Consumer(ctx, e.in.Names.StreamEvents, e.in.Names.DurDispatch(p))
		if err != nil {
			continue
		}

		info, ierr := c.Info(ctx)
		if ierr != nil {
			continue
		}

		pending := info.NumPending + uint64(info.NumAckPending) //nolint:gosec // non-negative.
		m.ProjectionLag += pending

		if stall := stalledFor(info, pending); stall > 0 {
			if m.DispatchStalls == nil {
				m.DispatchStalls = map[int]time.Duration{}
			}

			m.DispatchStalls[p] = stall
			m.DispatchStall = max(m.DispatchStall, stall)
		}
	}

	return m
}

// stalledFor is how long a consumer with pending messages has gone without an
// acknowledgement (0 when nothing is pending).
func stalledFor(info *jetstream.ConsumerInfo, pending uint64) time.Duration {
	if pending == 0 {
		return 0
	}

	since := info.Created
	if info.AckFloor.Last != nil && info.AckFloor.Last.After(since) {
		since = *info.AckFloor.Last
	}

	return time.Since(since)
}

// Archive archives a terminal execution now, regardless of retention.
func (e *Engine) Archive(ctx context.Context, execID string) error {
	if err := checkExecID(execID); err != nil {
		return err
	}

	return e.eng.Archive(ctx, execID)
}

// RebuildIndex rebuilds the visibility index from the event log: every
// execution in the events stream is folded and its summary rewritten. The
// index is a projection, so it can always be recovered this way.
func (e *Engine) RebuildIndex(ctx context.Context) (int, error) {
	if err := e.Init(ctx); err != nil {
		return 0, err
	}

	s, err := e.in.JS.Stream(ctx, e.in.Names.StreamEvents)
	if err != nil {
		return 0, err
	}

	info, err := s.Info(ctx, jetstream.WithSubjectFilter(e.in.Names.EventsSubjects()))
	if err != nil {
		return 0, err
	}

	n := 0

	for subj := range info.State.Subjects {
		execID := subj[strings.LastIndexByte(subj, '.')+1:]

		st, _, lerr := e.ld.Load(ctx, execID, 0)
		if lerr != nil || !st.Exists() {
			continue
		}

		if lerr = projection.Index(ctx, e.in, st, true, true); lerr != nil {
			return n, lerr
		}

		n++
	}

	return n, nil
}

func historyLimit(n int) int {
	switch {
	case n == 0:
		return DefaultHistoryLimit
	case n < 0:
		return 0
	default:
		return n
	}
}
