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

// Package invoker defines packtrail's agnostic node-invocation contract. The engine
// never speaks a wire protocol directly: every task/branch node is executed
// through an Invoker. This is the single seam that makes packtrail reusable beyond
// any one ecosystem — a project plugs in its own Invoker (an agent caller,
// an HTTP client, a NATS request/reply worker) and inherits all of packtrail's
// durability, retries, fan-in policies, choice routing, signals and timers.
package invoker

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"
)

// Status is the outcome an Invoker reports for a single invocation. The string
// values intentionally match the built-in nats-task wire contract so transports
// can map onto them directly.
type Status string

const (
	// StatusOK means the node succeeded; Result.Payload is the new shared context.
	StatusOK Status = "ok"
	// StatusError means the node failed permanently; the engine does not retry.
	StatusError Status = "error"
	// StatusRetry means the node asks to be retried per its node retry policy.
	StatusRetry Status = "retry"
	// StatusPending means the node was dispatched asynchronously and will be
	// settled later via Engine.CompleteActivityWithGeneration. The engine parks
	// the execution (waiting) and frees its work slot; Result.Payload is ignored.
	// Use this for long-running activities (e.g. an agent call) so the engine does
	// not block.
	StatusPending Status = "pending"
)

// Request is everything an Invoker needs to execute one node invocation. It is
// transport-agnostic: Target is interpreted by the chosen Invoker (a subject, an
// agent name, a URL, …) and any {execution_id} placeholder is already resolved.
type Request struct {
	Invoker     string          `json:"invoker"`      // invoker kind selected for this node
	Target      string          `json:"target"`       // invoker-specific target (resolved)
	ExecutionID string          `json:"execution_id"` // owning execution
	NodeID      string          `json:"node_id"`      // node being executed
	Payload     json.RawMessage `json:"payload"`      // shared execution context
	Generation  uint64          `json:"generation"`   // execution-scoped visit generation for this node
	Attempt     int             `json:"attempt"`      // 0-based attempt number
	Deadline    time.Time       `json:"deadline"`     // hard deadline for this attempt
}

// Result is what an Invoker returns. A non-nil error from Invoke is treated as a
// transient transport failure (equivalent to StatusRetry) and is never cached.
type Result struct {
	Status  Status          `json:"status"`
	Payload json.RawMessage `json:"payload,omitempty"`
	Error   string          `json:"error,omitempty"`
}

// Invoker executes a single node invocation. Implementations must be safe for
// concurrent use: the engine invokes many nodes in parallel.
type Invoker interface {
	Invoke(ctx context.Context, req Request) (Result, error)
}

// Func adapts a plain function to the Invoker interface.
type Func func(ctx context.Context, req Request) (Result, error)

// Invoke implements Invoker.
func (f Func) Invoke(ctx context.Context, req Request) (Result, error) { return f(ctx, req) }

// Registry dispatches an invocation to a registered Invoker by kind
// (Request.Invoker). It is itself an Invoker, so the engine holds exactly one
// Invoker regardless of how many kinds are configured.
//
// The kind map is not synchronized: all Register calls must complete during
// setup (before Server.Run starts concurrent Invoke dispatch). Registering a
// kind while invocations are in flight is a data race — the Registry is
// build-once, read-only-thereafter.
type Registry struct {
	m map[string]Invoker
}

// NewRegistry returns an empty Registry.
func NewRegistry() *Registry { return &Registry{m: map[string]Invoker{}} }

// Register binds kind to inv. It rejects nil invokers and duplicate kinds so a
// registration mistake cannot silently replace the invoker that will execute
// later work. Use Replace when replacement is intentional.
func (r *Registry) Register(kind string, inv Invoker) error {
	if isNilInvoker(inv) {
		return fmt.Errorf("invoker: kind %q registered with nil Invoker", kind)
	}

	if _, ok := r.m[kind]; ok {
		return fmt.Errorf("invoker: kind %q already registered", kind)
	}

	r.m[kind] = inv

	return nil
}

// Replace binds kind to inv, replacing any previous binding for kind.
func (r *Registry) Replace(kind string, inv Invoker) error {
	if isNilInvoker(inv) {
		return fmt.Errorf("invoker: kind %q registered with nil Invoker", kind)
	}

	r.m[kind] = inv

	return nil
}

func isNilInvoker(inv Invoker) bool {
	if inv == nil {
		return true
	}

	v := reflect.ValueOf(inv)
	kind := v.Kind()

	return (kind == reflect.Chan ||
		kind == reflect.Func ||
		kind == reflect.Interface ||
		kind == reflect.Map ||
		kind == reflect.Pointer ||
		kind == reflect.Slice) && v.IsNil()
}

// Has reports whether kind is registered.
func (r *Registry) Has(kind string) bool { _, ok := r.m[kind]; return ok }

// Invoke routes req to the Invoker registered under req.Invoker.
func (r *Registry) Invoke(ctx context.Context, req Request) (Result, error) {
	inv, ok := r.m[req.Invoker]
	if !ok {
		return Result{}, fmt.Errorf("invoker: no invoker registered for kind %q", req.Invoker)
	}

	return inv.Invoke(ctx, req)
}
