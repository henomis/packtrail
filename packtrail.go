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
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail/internal/dsl"
	"github.com/henomis/packtrail/internal/names"
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
)

// ErrNotFound is returned by Get when an execution does not exist.
var ErrNotFound = store.ErrNotFound

// Server is an embeddable packtrail engine instance: it runs the work consumer,
// visibility indexer and (optionally) reconciliation, and can host built-in
// nats-task workers in the same process.
type Server struct {
	nc      *nats.Conn
	prefix  string
	store   *store.Store
	engine  *runtime.Engine
	indexer *visibility.Indexer
	signals *signal.Signals
	flows   []string
	flowsKV jetstream.KeyValue

	reconcileActiveCron string
	reconcileFullCron   string
	archiveRetention    time.Duration

	asyncInvokers []asyncInvoker

	mu   sync.Mutex
	subs []*nats.Subscription
}

// New builds a Server against an existing NATS connection. It bootstraps all
// buckets and streams under the configured namespace, registers the built-in
// nats-task invoker plus any supplied via WithInvoker, and loads the configured
// flows. It does not start processing until Run is called.
func New(nc *nats.Conn, opts ...Option) (*Server, error) {
	var c config
	for _, o := range opts {
		o(&c)
	}

	ctx := context.Background()

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}

	n := names.New(c.prefix)

	st, err := openStore(ctx, js, n, c)
	if err != nil {
		return nil, err
	}

	sch, err := scheduler.New(ctx, js, n)
	if err != nil {
		return nil, err
	}

	flows, err := loadFlows(c)
	if err != nil {
		return nil, err
	}

	// Build the invoker registry: built-in nats-task plus any user invokers.
	reg := invoker.NewRegistry()
	reg.Register(natstask.Kind, natstask.New(nc, n.Prefix))

	for kind, inv := range c.invokers {
		reg.Register(kind, inv)
	}

	// Async invokers: ensure each kind's work-queue stream and register its
	// Dispatcher (the parking side). The Worker (execution side) is started by Run.
	for _, ai := range c.asyncInvokers {
		if err = asyncqueue.EnsureStream(ctx, js, n.Prefix, ai.kind, ai.opts...); err != nil {
			return nil, err
		}

		reg.Register(ai.kind, asyncqueue.NewDispatcher(js, n.Prefix, ai.kind))
	}

	var inv invoker.Invoker = reg

	if c.resultCache {
		var cacheKV jetstream.KeyValue

		cacheKV, err = js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: n.BucketResultCache})
		if err != nil {
			return nil, fmt.Errorf("result cache bucket: %w", err)
		}

		inv = invoker.NewCache(cacheKV, reg)
	}

	eng, err := runtime.New(inv, st, sch, flows, runtime.Config{
		OwnerID:        c.ownerID,
		LeaseTTL:       c.leaseTTL,
		MaxConcurrency: c.maxConcurrency,
		DefaultTimeout: c.defaultTimeout,
	})
	if err != nil {
		return nil, err
	}

	// The signals stream is ensured by runtime.New (during engine construction
	// above); this Signals instance only publishes to it.
	sig := signal.New(js, n)

	// Publish each flow's graph to a KV registry so observability tools (e.g.
	// packtrail-ui) can render flows without access to the source YAML.
	flowsKV, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: n.BucketFlows})
	if err != nil {
		return nil, fmt.Errorf("flows bucket: %w", err)
	}

	for name, f := range flows {
		data, marshalErr := json.Marshal(buildFlowGraph(f))
		if marshalErr != nil {
			return nil, marshalErr
		}

		if _, putErr := flowsKV.Put(ctx, name, data); putErr != nil {
			return nil, fmt.Errorf("publish flow %q: %w", name, putErr)
		}
	}

	return &Server{
		nc:                  nc,
		prefix:              n.Prefix,
		store:               st,
		engine:              eng,
		indexer:             visibility.New(st),
		signals:             sig,
		flows:               flowNames(flows),
		flowsKV:             flowsKV,
		reconcileActiveCron: c.reconcileActiveCron,
		reconcileFullCron:   c.reconcileFullCron,
		archiveRetention:    c.archiveRetention,
		asyncInvokers:       c.asyncInvokers,
	}, nil
}

// openStore opens the store and, when archival is configured, enables the cold
// archive bucket before returning.
func openStore(ctx context.Context, js jetstream.JetStream, n names.Names, c config) (*store.Store, error) {
	st, err := store.Open(ctx, js, n)
	if err != nil {
		return nil, err
	}

	if c.archiveRetention > 0 {
		if err = st.EnableArchive(ctx, c.archiveRetention); err != nil {
			return nil, err
		}
	}

	return st, nil
}

// Handle registers a built-in nats-task worker for subject (NATS wildcards
// allowed, e.g. "tasks.triage.*") in this process. The namespace prefix is
// prepended automatically, so the worker subscribes to
// "<namespace>.tasks.triage.*". Workers are drained when Run returns or Close
// is called.
func (s *Server) Handle(subject string, h Handler) error {
	sub, err := protocol.Serve(s.nc, s.prefix+"."+subject, h)
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.subs = append(s.subs, sub)
	s.mu.Unlock()

	return nil
}

// Start creates a new execution of flow with the given initial payload and
// returns its id.
func (s *Server) Start(ctx context.Context, flow string, payload json.RawMessage) (string, error) {
	return s.engine.Start(ctx, flow, payload)
}

// ScheduleFlow installs a recurring schedule named name that starts flow on the
// given 6-field cron expression. Reusing name replaces the schedule.
func (s *Server) ScheduleFlow(ctx context.Context, name, flow, cronExpr string, payload json.RawMessage) error {
	return s.engine.ScheduleFlow(ctx, name, flow, cronExpr, payload)
}

// Signal sends an external signal to an execution.
func (s *Server) Signal(ctx context.Context, execID, name string, payload json.RawMessage) error {
	return s.signals.Publish(ctx, execID, name, payload)
}

// CompleteActivity settles an asynchronous activity a node's Invoker previously
// reported as StatusPending. node and attempt identify the dispatched work (from
// Request.NodeID / Request.Attempt); res is its outcome. It is idempotent and
// stale-safe — a duplicate or out-of-date completion is a no-op — so an
// at-least-once worker can call it freely.
func (s *Server) CompleteActivity(ctx context.Context, execID, node string, attempt int, res Result) error {
	return s.engine.CompleteActivity(ctx, execID, node, attempt, res)
}

// Resume revives a failed execution, re-running the node it failed on with a
// fresh retry budget (the durable payload is preserved). Only failed executions
// can be resumed. It is durable: any running engine for the namespace picks up
// the resumed work.
func (s *Server) Resume(ctx context.Context, execID string) error {
	return s.engine.Resume(ctx, execID)
}

// Get returns a snapshot of an execution, or ErrNotFound. The execution KV is
// the source of truth; read it (not the indexes) for correctness decisions.
func (s *Server) Get(ctx context.Context, execID string) (*Execution, error) {
	ex, err := s.store.Get(ctx, execID)
	if err != nil {
		return nil, err
	}

	e := fromStore(ex)

	return &e, nil
}

// ByStatus returns the ids of executions currently indexed under status. The
// index is eventually consistent (best-effort visibility).
func (s *Server) ByStatus(ctx context.Context, status string) ([]string, error) {
	return s.indexer.ByStatus(ctx, status)
}

// ByFlow returns the ids of executions belonging to flow.
func (s *Server) ByFlow(ctx context.Context, flow string) ([]string, error) {
	return s.indexer.ByFlow(ctx, flow)
}

// List returns the execution ids in the hot bucket. With archival enabled (see
// WithArchive) this is the active set plus recently-completed executions, not
// every execution ever — archived executions are still readable via Get but are
// not listed here. Without archival it is every execution.
func (s *Server) List(ctx context.Context) ([]string, error) {
	return s.store.ListExecutionKeys(ctx)
}

// ListFunc streams the hot-bucket execution ids to fn instead of materialising
// them into a slice, so a large active set can be rendered incrementally and a
// caller can stop early by returning a sentinel error (returned as-is). It
// covers the same set as List.
func (s *Server) ListFunc(ctx context.Context, fn func(id string) error) error {
	return s.store.ForEachExecutionKey(ctx, fn)
}

// Reconcile rebuilds the visibility indexes from the source of truth with a
// full scan of every execution. It is the authoritative deep backstop; its cost
// grows with total execution volume, so schedule it sparingly (the scheduled
// reconcile already runs it periodically — see Run).
func (s *Server) Reconcile(ctx context.Context) error { return s.indexer.Reconcile(ctx) }

// ReconcileActive re-asserts the indexes for only the in-flight (running or
// waiting) executions. It is cheap enough to run frequently and fixes the
// common drift where a finished execution is still indexed as active; it does
// not catch active executions missing from the index entirely, for which
// Reconcile is the backstop.
func (s *Server) ReconcileActive(ctx context.Context) error { return s.indexer.ReconcileActive(ctx) }

// ArchiveCompleted sweeps completed executions out of the hot bucket into the
// cold archive. It is a no-op unless archival is enabled (see WithArchive). The
// full-reconcile schedule runs it automatically; it is exported for manual or
// test-driven sweeps.
func (s *Server) ArchiveCompleted(ctx context.Context) (int, error) {
	return s.store.ArchiveCompleted(ctx)
}

// reconcileFull is the full-reconcile schedule's hook: it rebuilds the indexes
// from the hot bucket and, when archival is enabled, sweeps completed executions
// into the cold archive and prunes index entries orphaned by expired archives —
// all on the same cadence.
func (s *Server) reconcileFull(ctx context.Context) error {
	if err := s.indexer.Reconcile(ctx); err != nil {
		return err
	}

	if s.archiveRetention <= 0 {
		return nil
	}

	if _, err := s.store.ArchiveCompleted(ctx); err != nil {
		return err
	}

	if _, err := s.indexer.GC(ctx, s.archiveRetention); err != nil {
		return err
	}

	return nil
}

// GCIndex prunes index entries whose execution has expired out of the archive.
// It is a no-op unless archival is enabled. The full-reconcile schedule runs it
// automatically; it is exported for manual or test-driven runs.
func (s *Server) GCIndex(ctx context.Context) (int, error) {
	if s.archiveRetention <= 0 {
		return 0, nil
	}

	return s.indexer.GC(ctx, s.archiveRetention)
}

// Flows returns the names of the flows this server knows.
func (s *Server) Flows() []string { return append([]string(nil), s.flows...) }

// Run starts the engine, the visibility indexer and (if configured) the
// reconciliation schedule, and blocks until ctx is cancelled. Registered task
// workers are drained on return.
func (s *Server) Run(ctx context.Context) error {
	cc, err := s.indexer.Run(ctx)
	if err != nil {
		return err
	}
	defer cc.Stop()

	// The active-set and full reconciles run on independent durable schedules.
	// Both schedules survive restarts and fire on a single instance, so the deep
	// backstop runs on its own cadence regardless of process churn or HA.
	s.engine.OnReconcileActive(s.indexer.ReconcileActive)
	s.engine.OnReconcileFull(s.reconcileFull)

	if s.reconcileActiveCron != "" {
		if err = s.engine.ScheduleReconcileActive(ctx, s.reconcileActiveCron); err != nil {
			return err
		}
	}

	if s.reconcileFullCron != "" {
		if err = s.engine.ScheduleReconcileFull(ctx, s.reconcileFullCron); err != nil {
			return err
		}
	}

	// Host an in-process worker for each async invoker kind. Each consumes its
	// kind's work-queue and settles results via this Server. They stop when ctx
	// is cancelled (i.e. when engine.Run returns below).
	var wg sync.WaitGroup

	js := s.store.JS()

	for _, ai := range s.asyncInvokers {
		w := asyncqueue.NewWorker(js, s.prefix, ai.kind, ai.exec, s, ai.opts...)

		wg.Add(1)

		go func(w *asyncqueue.Worker, kind string) {
			defer wg.Done()

			if runErr := w.Run(ctx); runErr != nil && ctx.Err() == nil {
				slog.Error("asyncqueue worker stopped unexpectedly", "kind", kind, "err", runErr)
			}
		}(w, ai.kind)
	}

	defer wg.Wait()
	defer s.Close()

	return s.engine.Run(ctx)
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

// Execution is a read-only snapshot of a running flow instance.
type Execution struct {
	ID          string                     `json:"id"`
	Flow        string                     `json:"flow"`
	Status      string                     `json:"status"`
	CurrentNode string                     `json:"current_node"`
	Payload     json.RawMessage            `json:"payload"`
	Attempt     int                        `json:"attempt"`
	Branches    map[string]Branch          `json:"branches,omitempty"`
	Signals     map[string]json.RawMessage `json:"signals,omitempty"`
	WaitSignal  string                     `json:"wait_signal,omitempty"`
	Error       string                     `json:"error,omitempty"`
	UpdatedAt   time.Time                  `json:"updated_at"`
}

// Branch is the state of a single fan-out branch.
type Branch struct {
	Node   string          `json:"node"`
	Status string          `json:"status"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  string          `json:"error,omitempty"`
}

func fromStore(ex *store.Execution) Execution {
	e := Execution{
		ID:          ex.ID,
		Flow:        ex.FlowName,
		Status:      ex.Status,
		CurrentNode: ex.CurrentNode,
		Payload:     ex.Payload,
		Attempt:     ex.Attempt,
		Signals:     ex.Signals,
		WaitSignal:  ex.WaitSignal,
		Error:       ex.Error,
		UpdatedAt:   ex.UpdatedAt,
	}
	if len(ex.Branches) > 0 {
		e.Branches = make(map[string]Branch, len(ex.Branches))
		for k, b := range ex.Branches {
			e.Branches[k] = Branch{Node: b.NodeID, Status: b.Status, Result: b.Result, Error: b.Error}
		}
	}

	return e
}
