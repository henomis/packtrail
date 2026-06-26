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

	invokers      map[string]invoker.Invoker
	asyncInvokers []asyncInvoker
	resultCache   bool

	ownerID        string
	leaseTTL       time.Duration
	maxConcurrency int
	defaultTimeout time.Duration
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
func WithAsyncInvoker(kind string, exec invoker.Invoker, opts ...asyncqueue.Option) Option {
	return func(c *config) {
		c.asyncInvokers = append(c.asyncInvokers, asyncInvoker{kind: kind, exec: exec, opts: opts})
	}
}

// WithResultCache enables idempotent invocation: every node result is cached by
// (execution, node, attempt) in a KV bucket, so a work item redelivered after a
// crash returns the cached result instead of re-invoking the node. Enable it
// whenever invocations have side effects that must not run twice.
func WithResultCache() Option { return func(c *config) { c.resultCache = true } }

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
