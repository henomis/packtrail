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

// Package packtrail is the public, embeddable entry point to the packtrail durable
// workflow engine. packtrail orchestrates declarative YAML flow graphs — task,
// fanout, fanin, choice and signal nodes — with crash-durable state backed only
// by NATS (Core + JetStream + KV + Message Scheduler).
//
// packtrail is ecosystem-agnostic: nodes are executed through a pluggable Invoker,
// so any project can drive its own services (an agent caller, an HTTP client,
// a NATS request/reply worker) while inheriting durability, retries,
// fan-in policies, conditional routing, signals and timers. A built-in
// "nats-task" Invoker (pkg/protocol request/reply) is always registered.
//
//	nc, _ := nats.Connect(nats.DefaultURL)
//	srv, _ := packtrail.New(nc,
//	    packtrail.WithFlowsDir("flows"),
//	    packtrail.WithInvoker("agent", myInvoker),  // your ecosystem's transport
//	    packtrail.WithResultCache(),                // idempotent retries
//	)
//	id, _ := srv.Start(ctx, "research-pipeline", nil)
//	srv.Run(ctx) // blocks: engine + indexer
//
// The Server does not own the *nats.Conn it is given; the caller connects and
// closes it.
package packtrail

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail/internal/dsl"
	"github.com/henomis/packtrail/internal/names"
	"github.com/henomis/packtrail/internal/rules"
	"github.com/henomis/packtrail/internal/runtime"
	"github.com/henomis/packtrail/internal/scheduler"
	"github.com/henomis/packtrail/internal/signal"
	"github.com/henomis/packtrail/internal/store"
	"github.com/henomis/packtrail/internal/visibility"
	"github.com/henomis/packtrail/invoker"
	"github.com/henomis/packtrail/invoker/asyncqueue"
	"github.com/henomis/packtrail/invoker/natstask"
	"github.com/henomis/packtrail/pkg/protocol"
)

// Invoker contract, re-exported so embedding apps depend only on this package
// when plugging in a custom transport.
type (
	// Invoker executes a single node invocation. Implement it to plug in a
	// transport for your ecosystem.
	Invoker = invoker.Invoker
	// InvokerFunc adapts a plain function to Invoker.
	InvokerFunc = invoker.Func
	// Request is the invocation passed to an Invoker.
	Request = invoker.Request
	// Result is what an Invoker returns.
	Result = invoker.Result
	// Status is an invocation outcome.
	Status = invoker.Status
)

// Invocation outcome statuses.
const (
	StatusOK      = invoker.StatusOK
	StatusError   = invoker.StatusError
	StatusRetry   = invoker.StatusRetry
	StatusPending = invoker.StatusPending
)

// Built-in nats-task worker contract, re-exported for in-process task workers.
type (
	// Handler implements a nats-task worker's business logic.
	Handler = protocol.Handler
	// TaskRequest is the envelope delivered to a nats-task handler.
	TaskRequest = protocol.TaskRequest
	// TaskResponse is the envelope a nats-task handler returns.
	TaskResponse = protocol.TaskResponse
)

// NATSTaskKind is the invoker kind of the always-registered built-in transport.
const NATSTaskKind = natstask.Kind

// Built-in nats-task worker response statuses (the string values a Handler sets
// on a TaskResponse). For Invoker implementations, use the Status* constants.
const (
	TaskOK    = protocol.StatusOK
	TaskError = protocol.StatusError
	TaskRetry = protocol.StatusRetry
)

// Execution statuses.
const (
	ExecRunning   = store.StatusRunning
	ExecWaiting   = store.StatusWaiting
	ExecCompleted = store.StatusCompleted
	ExecFailed    = store.StatusFailed
	ExecCancelled = store.StatusCancelled
)

// ErrNotFound is returned by Get when an execution does not exist.
var ErrNotFound = store.ErrNotFound

// defaultResultCacheTTL bounds the result-cache bucket: an entry is only ever
// read during the redelivery window of its own attempt (ack wait × delivery
// cap, i.e. minutes), so 24h is a generous ceiling that keeps the bucket from
// growing with all-time execution volume.
const defaultResultCacheTTL = 24 * time.Hour

// resourceTokenPattern bounds the namespace prefix and async invoker kinds,
// which become NATS stream/bucket name segments and subject tokens.
var resourceTokenPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// Server is an embeddable packtrail engine instance: it runs the work consumer,
// visibility indexer and (optionally) reconciliation, and can host built-in
// nats-task workers in the same process.
//
// Construction is pure (see New); every NATS resource is provisioned by Init —
// explicitly for fail-fast startups, or implicitly by the first operation that
// needs it.
type Server struct {
	nc       *nats.Conn
	js       jetstream.JetStream
	names    names.Names
	prefix   string
	cfg      config
	flowDefs map[string]*dsl.Flow
	flows    []string

	// initialized flips once Init has provisioned everything below; initMu
	// serializes provisioning attempts (a failed Init leaves initialized false,
	// so a later call retries).
	initMu      sync.Mutex
	initialized atomic.Bool

	// Built by Init; nil until then. Methods reach them only after Init.
	store         *store.Store
	engine        *runtime.Engine
	indexer       *visibility.Indexer
	signals       *signal.Signals
	flowsKV       jetstream.KeyValue
	resultCacheKV jetstream.KeyValue // nil unless WithResultCache; Run wraps async workers with it

	mu   sync.Mutex
	subs []*nats.Subscription
}

// New builds a Server against an existing NATS connection. It parses and
// validates the configured flows and options but performs no NATS I/O: every
// bucket and stream is provisioned by Init, which the first operation that
// needs NATS (Start, Run, Get, …) calls implicitly with its own context. Call
// Init yourself at startup to surface provisioning errors fail-fast.
func New(nc *nats.Conn, opts ...Option) (*Server, error) {
	var c config
	for _, o := range opts {
		o(&c)
	}

	// The namespace prefixes every bucket, stream, subject and durable name; an
	// unsafe one would otherwise fail much later with an opaque NATS error.
	if c.prefix != "" && !resourceTokenPattern.MatchString(c.prefix) {
		return nil, fmt.Errorf("invalid namespace %q: must match [A-Za-z0-9_-]{1,64}", c.prefix)
	}

	// An async invoker kind names its work-queue stream and subject. Kinds must
	// also be unambiguous: the registry silently overwrites on re-register, so
	// a kind registered twice (or both sync and async, or shadowing the
	// built-in nats-task) would drop an invoker without a trace.
	asyncKinds := make(map[string]bool, len(c.asyncInvokers))

	for _, ai := range c.asyncInvokers {
		if !resourceTokenPattern.MatchString(ai.kind) {
			return nil, fmt.Errorf("invalid async invoker kind %q: must match [A-Za-z0-9_-]{1,64}", ai.kind)
		}

		if ai.kind == dsl.DefaultInvoker {
			return nil, fmt.Errorf("async invoker kind %q collides with the built-in nats-task invoker", ai.kind)
		}

		if asyncKinds[ai.kind] {
			return nil, fmt.Errorf("async invoker kind %q registered twice", ai.kind)
		}

		asyncKinds[ai.kind] = true

		if _, alsoSync := c.invokers[ai.kind]; alsoSync {
			return nil, fmt.Errorf("invoker kind %q registered with both WithInvoker and WithAsyncInvoker", ai.kind)
		}
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	flows, err := loadFlows(c)
	if err != nil {
		return nil, err
	}

	// Compile every choice expression now so a bad rule is a construction
	// error, not a deferred Init error (the engine re-compiles at Init; the
	// compilation is cheap).
	if err = compileChoiceRules(flows); err != nil {
		return nil, err
	}

	// Every task node must name a registered invoker kind: a typo'd kind is a
	// construction error here, not a runtime failure on the first execution to
	// reach that node.
	if err = validateInvokerKinds(flows, &c); err != nil {
		return nil, err
	}

	n := names.New(c.prefix)

	return &Server{
		nc:       nc,
		js:       js,
		names:    n,
		prefix:   n.Prefix,
		cfg:      c,
		flowDefs: flows,
		flows:    flowNames(flows),
	}, nil
}

// validateInvokerKinds checks that every task node's invoker kind resolves to a
// registered invoker: the built-in nats-task, a WithInvoker registration, or a
// WithAsyncInvoker kind.
func validateInvokerKinds(flows map[string]*dsl.Flow, c *config) error {
	known := map[string]bool{dsl.DefaultInvoker: true}

	for kind := range c.invokers {
		known[kind] = true
	}

	for _, ai := range c.asyncInvokers {
		known[ai.kind] = true
	}

	for _, f := range flows {
		for i := range f.Nodes {
			n := &f.Nodes[i]
			if n.Type != dsl.NodeTask {
				continue
			}

			if kind := n.InvokerKind(); !known[kind] {
				return fmt.Errorf(
					"flow %q: task node %q uses invoker kind %q, but no such invoker is registered "+
						"(register it with WithInvoker or WithAsyncInvoker)",
					f.Name, n.ID, kind)
			}
		}
	}

	return nil
}

// Init provisions every NATS resource the Server needs — KV buckets, streams,
// the flow-graph registry — and builds the engine on top of them. It is
// idempotent and safe for concurrent use, and a failed attempt leaves the
// Server unprovisioned so a later call retries. Calling it explicitly at
// startup gives fail-fast provisioning errors; otherwise the first operation
// that needs NATS calls it implicitly, bounded by that operation's ctx.
func (s *Server) Init(ctx context.Context) error {
	if s.initialized.Load() {
		return nil
	}

	s.initMu.Lock()
	defer s.initMu.Unlock()

	if s.initialized.Load() {
		return nil
	}

	st, err := openStore(ctx, s.js, s.names, s.cfg)
	if err != nil {
		return err
	}

	sch := scheduler.New(s.js, s.names)
	if err = sch.EnsureStream(ctx); err != nil {
		return err
	}

	// Build the invoker registry: built-in nats-task plus any user invokers.
	reg := invoker.NewRegistry()
	reg.Register(natstask.Kind, natstask.New(s.nc, s.prefix))

	for kind, inv := range s.cfg.invokers {
		reg.Register(kind, inv)
	}

	// Async invokers: ensure each kind's work-queue stream and register its
	// Dispatcher (the parking side). The Worker (execution side) is started by Run.
	for _, ai := range s.cfg.asyncInvokers {
		if err = asyncqueue.EnsureStream(ctx, s.js, s.prefix, ai.kind, ai.opts...); err != nil {
			return err
		}

		reg.Register(ai.kind, asyncqueue.NewDispatcher(s.js, s.prefix, ai.kind))
	}

	var inv invoker.Invoker = reg

	if s.cfg.resultCache {
		// Entries are only consulted during the redelivery window of their own
		// (execution, node, attempt), so they expire after a TTL rather than
		// accumulating forever. resultCacheTTL: 0 = default, < 0 = no expiry.
		ttl := s.cfg.resultCacheTTL
		if ttl == 0 {
			ttl = defaultResultCacheTTL
		} else if ttl < 0 {
			ttl = 0 // no expiry (unbounded)
		}

		var cacheKV jetstream.KeyValue

		cacheKV, err = s.js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: s.names.BucketResultCache, TTL: ttl})
		if err != nil {
			return fmt.Errorf("result cache bucket: %w", err)
		}

		inv = invoker.NewCache(cacheKV, reg)
		s.resultCacheKV = cacheKV
	}

	// The signals stream is ensured here (with Init's context, like the
	// scheduler's) and the same instance serves both the engine's consumer and
	// Server.Signal's publishes.
	signals := signal.New(s.js, s.names)
	if err = signals.EnsureStream(ctx); err != nil {
		return err
	}

	eng, err := runtime.New(inv, st, sch, signals, s.flowDefs, runtime.Config{
		OwnerID:        s.cfg.ownerID,
		LeaseTTL:       s.cfg.leaseTTL,
		MaxConcurrency: s.cfg.maxConcurrency,
		DefaultTimeout: s.cfg.defaultTimeout,
		MaxDeliver:     s.cfg.maxDeliver,
		DrainTimeout:   s.cfg.drainTimeout,
	})
	if err != nil {
		return err
	}

	// Publish each flow's graph to a KV registry so observability tools (e.g.
	// packtrail-ui) can render flows without access to the source YAML.
	flowsKV, err := s.js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: s.names.BucketFlows})
	if err != nil {
		return fmt.Errorf("flows bucket: %w", err)
	}

	if err = publishFlowGraphs(ctx, flowsKV, s.flowDefs); err != nil {
		return err
	}

	s.store = st
	s.engine = eng
	s.indexer = visibility.New(st)
	s.signals = signals
	s.flowsKV = flowsKV

	s.initialized.Store(true)

	return nil
}

// compileChoiceRules compiles every non-default choice expression so an invalid
// rule fails at construction time.
func compileChoiceRules(flows map[string]*dsl.Flow) error {
	for _, f := range flows {
		for i := range f.Nodes {
			n := &f.Nodes[i]
			if n.Type != dsl.NodeChoice {
				continue
			}

			for _, r := range n.Rules {
				if r.Default || r.When == "" {
					continue
				}

				if _, err := rules.Compile(r.When); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// publishFlowGraphs writes each flow's graph to the KV registry so observability
// tools (e.g. packtrail-ui) can render flows without access to the source YAML.
func publishFlowGraphs(ctx context.Context, flowsKV jetstream.KeyValue, flows map[string]*dsl.Flow) error {
	for name, f := range flows {
		data, marshalErr := json.Marshal(buildFlowGraph(f))
		if marshalErr != nil {
			return marshalErr
		}

		if _, putErr := flowsKV.Put(ctx, name, data); putErr != nil {
			return fmt.Errorf("publish flow %q: %w", name, putErr)
		}
	}

	return nil
}

// openStore opens the store and, when archival is configured, enables the cold
// archive bucket before returning.
func openStore(ctx context.Context, js jetstream.JetStream, n names.Names, c config) (*store.Store, error) {
	st, err := store.Open(ctx, js, n)
	if err != nil {
		return nil, err
	}

	// Zero means "keep the store's built-in default"; a negative value disables
	// the guard (SetMaxPayloadBytes treats <= 0 as off).
	if c.maxPayloadBytes != 0 {
		st.SetMaxPayloadBytes(c.maxPayloadBytes)
	}

	if c.archiveRetention > 0 {
		if err = st.EnableArchive(ctx, c.archiveRetention); err != nil {
			return nil, err
		}
	}

	if c.historyRetention > 0 {
		if err = st.EnableHistory(ctx, c.historyRetention); err != nil {
			return nil, err
		}
	}

	return st, nil
}

// Handle registers a built-in nats-task worker for subject (NATS wildcards
// allowed, e.g. "tasks.triage.*") in this process. The namespace prefix is
// prepended automatically, so the worker subscribes to
// "<namespace>.tasks.triage.*". ctx is the worker's lifetime: every handler
// invocation derives its context from it, so cancelling ctx cancels in-flight
// handlers. The subscription itself is drained when Run returns or Close is
// called.
func (s *Server) Handle(ctx context.Context, subject string, h Handler) error {
	sub, err := protocol.Serve(ctx, s.nc, s.prefix+"."+subject, h)
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.subs = append(s.subs, sub)
	s.mu.Unlock()

	return nil
}

// Start creates a new execution of flow with the given initial payload and
// returns its (freshly minted) id. The payload must be a JSON object (the keyed
// context that task/branch/signal results merge into); nil or empty defaults to
// {}, and a non-object is rejected with an error.
func (s *Server) Start(ctx context.Context, flow string, payload json.RawMessage) (string, error) {
	if err := s.Init(ctx); err != nil {
		return "", err
	}

	return s.engine.Start(ctx, flow, payload)
}

// StartWithID is an idempotent Start keyed by a caller-supplied execution id
// (an idempotency key): the first call creates the execution, and any retry with
// the same id returns that id without creating a duplicate. Use it to make Start
// safe to retry — e.g. key it on a domain id so a timed-out Start does not spawn
// a second execution. First-write wins; the id must match [A-Za-z0-9_-]{1,128}.
// Like Start, the payload must be a JSON object (or empty).
func (s *Server) StartWithID(ctx context.Context, execID, flow string, payload json.RawMessage) (string, error) {
	if err := s.Init(ctx); err != nil {
		return "", err
	}

	return s.engine.StartWithID(ctx, execID, flow, payload)
}

// ScheduleFlow installs a recurring schedule named name that starts flow on the
// given 6-field cron expression. Reusing name replaces the schedule.
func (s *Server) ScheduleFlow(ctx context.Context, name, flow, cronExpr string, payload json.RawMessage) error {
	if err := s.Init(ctx); err != nil {
		return err
	}

	return s.engine.ScheduleFlow(ctx, name, flow, cronExpr, payload)
}

// Signal sends an external signal to an execution.
func (s *Server) Signal(ctx context.Context, execID, name string, payload json.RawMessage) error {
	if err := s.Init(ctx); err != nil {
		return err
	}

	return s.signals.Publish(ctx, execID, name, payload)
}

// CompleteActivity settles an asynchronous activity a node's Invoker previously
// reported as StatusPending. node and attempt identify the dispatched work (from
// Request.NodeID / Request.Attempt); res is its outcome. It is idempotent and
// stale-safe — a duplicate or out-of-date completion is a no-op — so an
// at-least-once worker can call it freely.
func (s *Server) CompleteActivity(ctx context.Context, execID, node string, attempt int, res Result) error {
	if err := s.Init(ctx); err != nil {
		return err
	}

	return s.engine.CompleteActivity(ctx, execID, node, attempt, res)
}

// Resume revives a failed execution, re-running the node it failed on with a
// fresh retry budget (the durable payload is preserved). Only failed executions
// can be resumed. It is durable: any running engine for the namespace picks up
// the resumed work.
func (s *Server) Resume(ctx context.Context, execID string) error {
	if err := s.Init(ctx); err != nil {
		return err
	}

	return s.engine.Resume(ctx, execID)
}

// Cancel transitions a running or waiting execution to the terminal cancelled
// state with an optional reason (stored on the execution's error field). It is
// idempotent and stale-safe: cancelling an already-terminal execution is a
// no-op, and any in-flight work — pending retries, fanin joins, signal waits, or
// an async activity later settled via CompleteActivity — no-ops once the
// execution is cancelled. A cancelled execution is terminal and, unlike a failed
// one, cannot be resumed.
func (s *Server) Cancel(ctx context.Context, execID, reason string) error {
	if err := s.Init(ctx); err != nil {
		return err
	}

	return s.engine.Cancel(ctx, execID, reason)
}

// Get returns a snapshot of an execution, or ErrNotFound. The execution KV is
// the source of truth; read it (not the indexes) for correctness decisions.
func (s *Server) Get(ctx context.Context, execID string) (*Execution, error) {
	if err := s.Init(ctx); err != nil {
		return nil, err
	}

	ex, err := s.store.Get(ctx, execID)
	if err != nil {
		return nil, err
	}

	e := fromStore(ex)

	return &e, nil
}

// Results assembles an execution's data-plane view — {"input": <start
// payload>, "results": {<node>: <output>, …}, "signals": {<name>: <payload>,
// …}} — the same context document invokers and choice rules see. Entries of an
// archived execution are dropped by the archive sweep, so Results of an
// archived id returns only what remains.
func (s *Server) Results(ctx context.Context, execID string) (json.RawMessage, error) {
	if err := s.Init(ctx); err != nil {
		return nil, err
	}

	return s.engine.Results(ctx, execID)
}

// ByStatus returns the ids of executions currently indexed under status. The
// index is eventually consistent (best-effort visibility).
func (s *Server) ByStatus(ctx context.Context, status string) ([]string, error) {
	if err := s.Init(ctx); err != nil {
		return nil, err
	}

	return s.indexer.ByStatus(ctx, status)
}

// ByFlow returns the ids of executions belonging to flow.
func (s *Server) ByFlow(ctx context.Context, flow string) ([]string, error) {
	if err := s.Init(ctx); err != nil {
		return nil, err
	}

	return s.indexer.ByFlow(ctx, flow)
}

// List returns the execution ids in the hot bucket. With archival enabled (see
// WithArchive) this is the active set plus recently-completed executions, not
// every execution ever — archived executions are still readable via Get but are
// not listed here. Without archival it is every execution.
func (s *Server) List(ctx context.Context) ([]string, error) {
	if err := s.Init(ctx); err != nil {
		return nil, err
	}

	return s.store.ListExecutionKeys(ctx)
}

// ListFunc streams the hot-bucket execution ids to fn instead of materialising
// them into a slice, so a large active set can be rendered incrementally and a
// caller can stop early by returning a sentinel error (returned as-is). It
// covers the same set as List.
func (s *Server) ListFunc(ctx context.Context, fn func(id string) error) error {
	if err := s.Init(ctx); err != nil {
		return err
	}

	return s.store.ForEachExecutionKey(ctx, fn)
}

// Reconcile rebuilds the visibility indexes from the source of truth with a
// full scan of every execution. It is the authoritative deep backstop; its cost
// grows with total execution volume, so schedule it sparingly (the scheduled
// reconcile already runs it periodically — see Run).
func (s *Server) Reconcile(ctx context.Context) error {
	if err := s.Init(ctx); err != nil {
		return err
	}

	return s.indexer.Reconcile(ctx)
}

// ReconcileActive re-asserts the indexes for only the in-flight (running or
// waiting) executions. It is cheap enough to run frequently and fixes the
// common drift where a finished execution is still indexed as active; it does
// not catch active executions missing from the index entirely, for which
// Reconcile is the backstop.
func (s *Server) ReconcileActive(ctx context.Context) error {
	if err := s.Init(ctx); err != nil {
		return err
	}

	return s.indexer.ReconcileActive(ctx)
}

// RedriveStalled scans the in-flight executions the visibility index knows of
// and re-drives any that look stranded — active, quiet past the stall
// threshold (WithStallRedrive; default 5× the engine ack wait), outside any
// scheduled retry backoff, and with no live ownership lease. It heals
// executions whose committed follow-on messages were never flushed (a crash
// between the outbox commit and its publish) without a manual Resume. The reconcile-active
// schedule runs it automatically after each index pass; it is exported for
// manual or test-driven runs. It returns how many executions were re-driven.
func (s *Server) RedriveStalled(ctx context.Context) (int, error) {
	if err := s.Init(ctx); err != nil {
		return 0, err
	}

	return s.redriveStalled(ctx)
}

// redriveStalled walks the running/waiting index memberships and asks the
// engine to re-drive each stalled one. The index is eventually consistent, so
// an active execution missing from it entirely is not seen here — the full
// Reconcile pass restores its membership and the next watchdog pass picks it
// up.
func (s *Server) redriveStalled(ctx context.Context) (int, error) {
	redriven := 0
	seen := make(map[string]struct{})

	for _, status := range []string{store.StatusRunning, store.StatusWaiting} {
		ids, err := s.indexer.ByStatus(ctx, status)
		if err != nil {
			return redriven, err
		}

		for _, id := range ids {
			if _, dup := seen[id]; dup {
				continue
			}

			seen[id] = struct{}{}

			ok, redriveErr := s.engine.RedriveStalled(ctx, id, s.cfg.stallRedrive)
			if redriveErr != nil {
				return redriven, redriveErr
			}

			if ok {
				redriven++
			}
		}
	}

	if redriven > 0 {
		slog.Warn("re-drove stalled executions", "count", redriven)
	}

	return redriven, nil
}

// ArchiveTerminal sweeps terminal, non-resumable executions (completed or
// cancelled) out of the hot bucket into the cold archive; failed executions stay
// hot so they remain resumable. It is a no-op unless archival is enabled (see
// WithArchive). The full-reconcile schedule runs it automatically; it is exported
// for manual or test-driven sweeps.
func (s *Server) ArchiveTerminal(ctx context.Context) (int, error) {
	if err := s.Init(ctx); err != nil {
		return 0, err
	}

	return s.store.ArchiveTerminal(ctx)
}

// reconcileFull is the full-reconcile schedule's hook: it rebuilds the indexes
// from the hot bucket and, when archival is enabled, sweeps terminal executions
// into the cold archive and prunes index entries orphaned by expired archives —
// all on the same cadence.
func (s *Server) reconcileFull(ctx context.Context) error {
	if err := s.indexer.Reconcile(ctx); err != nil {
		return err
	}

	if s.cfg.archiveRetention <= 0 {
		return nil
	}

	if _, err := s.store.ArchiveTerminal(ctx); err != nil {
		return err
	}

	if _, err := s.indexer.GC(ctx, s.cfg.archiveRetention); err != nil {
		return err
	}

	return nil
}

// GCIndex prunes index entries whose execution has expired out of the archive.
// It is a no-op unless archival is enabled. The full-reconcile schedule runs it
// automatically; it is exported for manual or test-driven runs.
func (s *Server) GCIndex(ctx context.Context) (int, error) {
	if s.cfg.archiveRetention <= 0 {
		return 0, nil
	}

	if err := s.Init(ctx); err != nil {
		return 0, err
	}

	return s.indexer.GC(ctx, s.cfg.archiveRetention)
}

// Flows returns the names of the flows this server knows.
func (s *Server) Flows() []string { return append([]string(nil), s.flows...) }

// Run starts the engine, the visibility indexer and (if configured) the
// reconciliation schedule, and blocks until ctx is cancelled. Registered task
// workers are drained on return.
func (s *Server) Run(ctx context.Context) error {
	if err := s.Init(ctx); err != nil {
		return err
	}

	cc, err := s.indexer.Run(ctx)
	if err != nil {
		return err
	}
	defer cc.Stop()

	if err = s.scheduleReconciles(ctx); err != nil {
		return err
	}

	wg := s.startAsyncWorkers(ctx)

	defer wg.Wait()
	defer s.Close()

	return s.engine.Run(ctx)
}

// scheduleReconciles wires the active-set and full reconcile passes and, if
// configured, their durable cron schedules. The two run on independent
// durable schedules; both survive restarts and fire on a single instance, so
// the deep backstop runs on its own cadence regardless of process churn or
// HA. The active pass also runs the stall watchdog (unless disabled): first
// the index is re-asserted from the source of truth, then stranded executions
// are re-driven.
func (s *Server) scheduleReconciles(ctx context.Context) error {
	s.engine.OnReconcileActive(func(ctx context.Context) error {
		if reconcileErr := s.indexer.ReconcileActive(ctx); reconcileErr != nil {
			return reconcileErr
		}

		if s.cfg.stallRedrive < 0 {
			return nil // watchdog explicitly disabled
		}

		_, redriveErr := s.redriveStalled(ctx)

		return redriveErr
	})
	s.engine.OnReconcileFull(s.reconcileFull)

	if s.cfg.reconcileActiveCron != "" {
		if err := s.engine.ScheduleReconcileActive(ctx, s.cfg.reconcileActiveCron); err != nil {
			return err
		}
	}

	if s.cfg.reconcileFullCron != "" {
		if err := s.engine.ScheduleReconcileFull(ctx, s.cfg.reconcileFullCron); err != nil {
			return err
		}
	}

	return nil
}

// startAsyncWorkers hosts an in-process worker for each async invoker kind.
// Each consumes its kind's work-queue and settles results via this Server.
// They stop when ctx is cancelled (i.e. when engine.Run returns); the caller
// must wait on the returned WaitGroup for them to drain.
func (s *Server) startAsyncWorkers(ctx context.Context) *sync.WaitGroup {
	var wg sync.WaitGroup

	js := s.store.JS()

	// Sink async-worker dead-letters into the shared dead-letter stream, so a
	// dropped job is observable alongside the other consumers' dead-letters.
	asyncSink := asyncqueue.WithDeadLetterSink(
		func(ctx context.Context, key, reason string, deliveries uint64) {
			dl := store.DeadLetter{Kind: store.DeadLetterAsync, Key: key, Reason: reason, Deliveries: deliveries}
			if emitErr := s.store.EmitDeadLetter(ctx, dl); emitErr != nil {
				slog.Debug("emit async dead-letter", "key", key, "err", emitErr)
			}
		})

	for _, ai := range s.cfg.asyncInvokers {
		// With the result cache enabled, the worker's execution is deduped too:
		// a job redelivered after a worker crash (invoked, but not yet settled
		// and acked) serves the cached result instead of re-firing the target.
		// The "w." keyspace keeps these entries apart from the engine-side
		// dispatch cache, which stores StatusPending under the same
		// (execution, node, attempt) triple.
		exec := ai.exec
		if s.resultCacheKV != nil {
			exec = invoker.NewCacheKeyed(s.resultCacheKV, ai.exec, "w.")
		}

		w := asyncqueue.NewWorker(js, s.prefix, ai.kind, exec, s, append(ai.opts, asyncSink)...)

		wg.Add(1)

		go func(w *asyncqueue.Worker, kind string) {
			defer wg.Done()

			if runErr := w.Run(ctx); runErr != nil && ctx.Err() == nil {
				slog.Error("asyncqueue worker stopped unexpectedly", "kind", kind, "err", runErr)
			}
		}(w, ai.kind)
	}

	return &wg
}

// Close drains any registered task workers. It does not close the NATS
// connection, which the caller owns.
func (s *Server) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, sub := range s.subs {
		_ = sub.Drain()
	}

	s.subs = nil
}

// loadFlows merges flows loaded from the configured directory and inline YAML
// documents, rejecting duplicate flow names.
func loadFlows(c config) (map[string]*dsl.Flow, error) {
	flows := map[string]*dsl.Flow{}

	if c.flowsDir != "" {
		dirFlows, err := dsl.LoadDir(c.flowsDir)
		if err != nil {
			return nil, fmt.Errorf("load flows %s: %w", c.flowsDir, err)
		}

		maps.Copy(flows, dirFlows)
	}

	for _, doc := range c.flowDocs {
		f, err := dsl.Parse(doc)
		if err != nil {
			return nil, err
		}

		if _, dup := flows[f.Name]; dup {
			return nil, fmt.Errorf("duplicate flow %q", f.Name)
		}

		flows[f.Name] = f
	}

	for _, def := range c.flowDefs {
		f, err := flowDefToDSL(def)
		if err != nil {
			return nil, err
		}

		if _, dup := flows[f.Name]; dup {
			return nil, fmt.Errorf("duplicate flow %q", f.Name)
		}

		flows[f.Name] = f
	}

	return flows, nil
}

func flowNames(flows map[string]*dsl.Flow) []string {
	out := make([]string, 0, len(flows))
	for n := range flows {
		out = append(out, n)
	}

	return out
}

// Execution is a read-only snapshot of a flow instance's control state.
// Payloads live in the data plane and are not carried on the snapshot — read
// them with Server.Results, which assembles {input, results, signals}.
type Execution struct {
	ID          string            `json:"id"`
	Flow        string            `json:"flow"`
	Status      string            `json:"status"`
	CurrentNode string            `json:"current_node"`
	Attempt     int               `json:"attempt"`
	Outputs     []string          `json:"outputs,omitempty"` // node ids with a stored output, in settle order
	Branches    map[string]Branch `json:"branches,omitempty"`
	Signals     []string          `json:"signals,omitempty"` // received-but-unconsumed signal names
	WaitSignal  string            `json:"wait_signal,omitempty"`
	Error       string            `json:"error,omitempty"`
	UpdatedAt   time.Time         `json:"updated_at"`
}

// Branch is the control state of a single fan-out branch; a completed branch's
// result appears under results.<branch> in Server.Results.
type Branch struct {
	Node   string `json:"node"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

func fromStore(ex *store.Execution) Execution {
	e := Execution{
		ID:          ex.ID,
		Flow:        ex.FlowName,
		Status:      ex.Status,
		CurrentNode: ex.CurrentNode,
		Attempt:     ex.Attempt,
		Outputs:     append([]string(nil), ex.Outputs...),
		WaitSignal:  ex.WaitSignal,
		Error:       ex.Error,
		UpdatedAt:   ex.UpdatedAt,
	}

	for name := range ex.Signals {
		e.Signals = append(e.Signals, name)
	}

	if len(ex.Branches) > 0 {
		e.Branches = make(map[string]Branch, len(ex.Branches))
		for k, b := range ex.Branches {
			e.Branches[k] = Branch{Node: b.NodeID, Status: b.Status, Error: b.Error}
		}
	}

	return e
}
