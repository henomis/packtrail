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

package packtrail

import (
	"time"

	"github.com/henomis/packtrail/invoker"
	"github.com/henomis/packtrail/invoker/asyncqueue"
)

// Option configures a Server. Pass options to New.
type Option func(*config)

// asyncInvoker records an async invoker kind registered via WithAsyncInvoker:
// New ensures its work-queue stream and registers the Dispatcher; Run hosts a
// Worker that executes exec and settles results via the Server.
type asyncInvoker struct {
	kind string
	exec invoker.Invoker
	opts []asyncqueue.Option
}

type config struct {
	prefix   string
	flowsDir string
	flowDocs [][]byte
	flowDefs []FlowDef

	reconcileActiveCron string
	reconcileFullCron   string
	archiveRetention    time.Duration
	historyRetention    time.Duration
	stallRedrive        time.Duration // 0 = engine default (5×AckWait); < 0 disables the watchdog

	invokers       map[string]invoker.Invoker
	asyncInvokers  []asyncInvoker
	resultCache    bool
	resultCacheTTL time.Duration

	ownerID         string
	leaseTTL        time.Duration
	maxConcurrency  int
	defaultTimeout  time.Duration
	maxDeliver      int
	maxPayloadBytes int
	maxDocBytes     int
	drainTimeout    time.Duration
}

// WithNamespace sets the resource prefix for every NATS bucket, stream, subject
// and durable consumer (default "packtrail"). Give each independent deployment a
// distinct namespace to let them share a NATS cluster without colliding.
func WithNamespace(prefix string) Option { return func(c *config) { c.prefix = prefix } }

// WithFlowsDir loads every *.yaml / *.yml flow definition in dir.
func WithFlowsDir(dir string) Option { return func(c *config) { c.flowsDir = dir } }

// WithFlow registers a single flow from its YAML document. It may be passed
// multiple times.
func WithFlow(yamlDoc []byte) Option {
	return func(c *config) { c.flowDocs = append(c.flowDocs, yamlDoc) }
}

// WithFlowDef registers a single flow from a Go struct. It may be passed
// multiple times and combined with WithFlow / WithFlowsDir.
func WithFlowDef(f FlowDef) Option {
	return func(c *config) { c.flowDefs = append(c.flowDefs, f) }
}

// WithInvoker registers an Invoker under kind, the value a flow node selects via
// its `invoker:` field. The built-in "nats-task" kind is always registered and
// may be overridden by passing WithInvoker("nats-task", ...).
func WithInvoker(kind string, inv invoker.Invoker) Option {
	return func(c *config) {
		if c.invokers == nil {
			c.invokers = map[string]invoker.Invoker{}
		}

		c.invokers[kind] = inv
	}
}

// WithAsyncInvoker registers an asynchronous Invoker under kind. Unlike
// WithInvoker, exec does not run on the engine's critical path: a flow node
// selecting this kind is dispatched to a durable JetStream work-queue (the
// engine parks the execution) and exec is run later by an in-process worker,
// with at-least-once delivery. Use it for slow nodes — an agent call, an HTTP
// request — so they never hold an engine slot. exec is an ordinary synchronous
// Invoker returning StatusOK/StatusError/StatusRetry; the durability, retries
// and completion are handled for you. opts tune the worker and its stream (see
// the asyncqueue package). It may be passed multiple times for distinct kinds.
//
// Delivery to exec is at-least-once: a job redelivered after a worker crash
// (invoked, but not yet settled and acked) runs exec again. For a target whose
// side effects must not fire twice, enable WithResultCache — it dedups the
// worker's execution as well — or make the target idempotent.
func WithAsyncInvoker(kind string, exec invoker.Invoker, opts ...asyncqueue.Option) Option {
	return func(c *config) {
		c.asyncInvokers = append(c.asyncInvokers, asyncInvoker{kind: kind, exec: exec, opts: opts})
	}
}

// WithResultCache enables idempotent invocation: every node result is cached by
// (execution, node, attempt) in a KV bucket, so a re-invocation returns the
// cached result instead of running the node again. Node invocation is otherwise
// at-least-once — a work item redelivered after a crash, or a lease taken over
// while an instance is paused, can run the same node twice (the execution-doc CAS
// fences state, not external side-effects). Enable this (or make targets
// idempotent) whenever an invocation has a side effect that must not run twice.
// The cache covers both invocation paths: the engine-side dispatch (including
// an async node's StatusPending, so a redelivered work item re-parks instead of
// dispatching a second job) and the async worker's execution of the target
// (under a separate keyspace in the same bucket, so a redelivered job serves
// the completed result instead of re-firing the side effect).
// Cache entries expire after a TTL (default 24h — see WithResultCacheTTL): an
// entry is only ever consulted during the redelivery window of its own attempt,
// so retaining it longer would just grow the bucket without bound.
func WithResultCache() Option { return func(c *config) { c.resultCache = true } }

// WithResultCacheTTL enables the result cache with a custom entry TTL. Entries
// need only outlive the redelivery window of a single attempt (ack wait ×
// delivery cap), so the 24h default is already generous; raise it if your
// redelivery horizon is unusually long. A negative TTL disables expiry (the
// bucket then grows without bound — the pre-TTL behaviour). Implies
// WithResultCache.
func WithResultCacheTTL(d time.Duration) Option {
	return func(c *config) {
		c.resultCache = true
		c.resultCacheTTL = d
	}
}

// WithHistory enables the durable per-execution history: every state
// transition is also appended, best-effort, to a `<namespace>-history` stream
// retained for the given duration, and Server.History returns an execution's
// ordered step-by-step trace. Without it, transition events live only in the
// short-retention events stream that feeds the visibility indexes. The trace
// is observability, not operational truth.
func WithHistory(retention time.Duration) Option {
	return func(c *config) { c.historyRetention = retention }
}

// WithStallRedrive sets the quiet-time threshold after which the stall
// watchdog re-drives an active execution (default 5× the engine ack wait; a
// negative value disables the watchdog). The watchdog runs after each
// reconcile-active pass (see WithReconcileActive — without that schedule it
// only runs via Server.RedriveStalled), and it heals executions whose driving
// work item was lost in a crash window: still active, quiet past the
// threshold, not inside a scheduled retry backoff, and not lease-held by a
// live instance. Set the threshold above your longest legitimate quiet period;
// a false positive is state-safe (guarded transitions) but duplicates an
// invocation within the at-least-once contract.
func WithStallRedrive(d time.Duration) Option {
	return func(c *config) { c.stallRedrive = d }
}

// WithReconcileActive installs the recurring active-set reconcile: a cheap pass
// over only the in-flight (running/waiting) executions, on the given 6-field
// cron expression ("sec min hour dom mon dow"), e.g. "0 */5 * * * *". Its cost
// is independent of accumulated terminal executions, so it is safe to run
// often. It fixes the common drift where a finished execution is still indexed
// as active, but cannot recover an execution missing from the index entirely.
func WithReconcileActive(cronExpr string) Option {
	return func(c *config) { c.reconcileActiveCron = cronExpr }
}

// WithReconcileFull installs the recurring full reconcile: an authoritative scan
// of every execution on the given 6-field cron expression, e.g. "0 0 * * * *"
// for hourly. It is the deep backstop that recovers index entries the active
// pass cannot see; its cost grows with total execution volume, so schedule it
// well below the active cadence. Without either option the indexer still runs
// but no periodic reconcile is scheduled.
func WithReconcileFull(cronExpr string) Option {
	return func(c *config) { c.reconcileFullCron = cronExpr }
}

// WithArchive enables execution archival: completed executions are swept out of
// the hot executions bucket into a cold archive bucket that retains them for
// roughly retention before they expire. This bounds the hot bucket — and the
// List/Keys and full-reconcile scans over it — by in-flight volume rather than
// all-time volume. The sweep runs on the full-reconcile schedule (see
// WithReconcileFull), so pair the two. Failed executions are left hot so they
// remain resumable. Without this option the hot bucket retains every execution.
func WithArchive(retention time.Duration) Option {
	return func(c *config) { c.archiveRetention = retention }
}

// WithOwnerID sets this instance's ownership-lease owner id. Defaults to a
// random id; only set it if you need a stable, distinct id per instance.
func WithOwnerID(id string) Option { return func(c *config) { c.ownerID = id } }

// WithLeaseTTL sets the per-execution ownership lease TTL (default 30s). A
// crashed instance's executions become available to others after roughly this.
func WithLeaseTTL(d time.Duration) Option { return func(c *config) { c.leaseTTL = d } }

// WithMaxConcurrency caps how many work items this instance processes at once
// (default 64).
func WithMaxConcurrency(n int) Option { return func(c *config) { c.maxConcurrency = n } }

// WithDefaultTimeout sets the invocation timeout used when a node omits one
// (default 30s).
func WithDefaultTimeout(d time.Duration) Option {
	return func(c *config) { c.defaultTimeout = d }
}

// WithMaxDeliver caps how many times a single message is delivered before the
// engine dead-letters it instead of redelivering forever (default 10). It bounds
// the blast radius of a message that can never succeed (e.g. its flow or node was
// removed) or one that keeps hitting a transient fault. The cap applies to all
// three of the engine's durable consumers: the work stream (failing the execution
// with a descriptive reason), the fired-schedule stream (a removed-flow cron tick
// or persistently-failing reconcile), and the signal stream. A terminal error on
// any of them is dead-lettered immediately, regardless of this cap. A
// non-positive value is treated as the default — the cap cannot be disabled,
// or a poisoned message could redeliver forever. (The async invoker worker has
// its own asyncqueue.WithMaxDeliver, which does allow an explicit opt-out.)
func WithMaxDeliver(n int) Option { return func(c *config) { c.maxDeliver = n } }

// WithDrainTimeout sets how long a graceful shutdown waits for in-flight work
// items to settle and ack before aborting the stragglers (default 30s). When Run
// returns because its context was cancelled, the engine stops accepting new work
// and drains what is already running within this window — so a clean restart does
// not abandon in-flight invocations to redelivery (which would double-fire
// naturally non-idempotent targets). Stragglers exceeding the window are cancelled
// and their work redelivers. A hard crash is unaffected (it always relies on
// redelivery).
func WithDrainTimeout(d time.Duration) Option { return func(c *config) { c.drainTimeout = d } }

// WithMaxPayloadBytes caps the size of an execution's payload (default 512 KiB,
// store.DefaultMaxPayloadBytes). The payload grows as task results, branch
// results and signal payloads are merged in; a transition that would exceed the
// limit fails the execution with a clear reason instead of producing an opaque
// KV write error. The default leaves headroom below NATS's 1 MiB max message
// size for the rest of the execution document. Pass a negative value to disable
// the guard; zero keeps the default.
func WithMaxPayloadBytes(n int) Option { return func(c *config) { c.maxPayloadBytes = n } }

// WithMaxDocumentBytes caps the serialized size of an execution's control
// document (default 768 KiB, store.DefaultMaxDocumentBytes). The document is
// small control metadata, but a very wide fanout (one BranchState per branch) or
// a large transient outbox can grow it toward NATS's 1 MiB ceiling; a write that
// would exceed the limit is rejected with a typed error (and, on the fanout path,
// fails the node with a clear reason) instead of an opaque NATS publish error.
// Pass a negative value to disable the guard; zero keeps the default.
func WithMaxDocumentBytes(n int) Option { return func(c *config) { c.maxDocBytes = n } }
