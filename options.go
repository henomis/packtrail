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
)

// Option configures a Server. Pass options to New.
type Option func(*config)

type config struct {
	prefix        string
	flowsDir      string
	flowDocs      [][]byte
	flowDefs      []FlowDef
	reconcileCron string
	invokers      map[string]invoker.Invoker
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

// WithResultCache enables idempotent invocation: every node result is cached by
// (execution, node, attempt) in a KV bucket, so a work item redelivered after a
// crash returns the cached result instead of re-invoking the node. Enable it
// whenever invocations have side effects that must not run twice.
func WithResultCache() Option { return func(c *config) { c.resultCache = true } }

// WithReconcile installs a recurring visibility reconciliation on the given
// 6-field cron expression ("sec min hour dom mon dow"), e.g. "0 */5 * * * *".
// Without it, the indexer still runs but no periodic reconcile is scheduled.
func WithReconcile(cronExpr string) Option {
	return func(c *config) { c.reconcileCron = cronExpr }
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
