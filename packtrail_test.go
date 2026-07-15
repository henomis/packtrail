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

package packtrail_test

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/internal/natstest"
)

const agnosticFlow = `
version: "1.0"
name: agnostic
nodes:
  - id: a
    type: task
    invoker: custom
    target: agent-a
  - id: b
    type: task
    invoker: custom
    target: agent-b
edges:
  - {from: a, to: b}
`

// TestCustomInvokerEndToEnd drives a flow whose task nodes select a custom,
// non-NATS invoker registered via WithInvoker — proving the engine orchestrates
// any transport (durability, ordering, payload threading) with no built-in
// protocol involved.
// TestWithFlowDefEndToEnd is the struct-based equivalent of TestCustomInvokerEndToEnd:
// it registers the same two-node flow via WithFlowDef instead of YAML and verifies
// the engine runs it identically.
func TestWithFlowDefEndToEnd(t *testing.T) {
	srv := natstest.Start(t)

	var (
		seen []string
		mu   sync.Mutex
	)

	custom := packtrail.InvokerFunc(func(_ context.Context, req packtrail.Request) (packtrail.Result, error) {
		mu.Lock()

		seen = append(seen, req.Target)
		mu.Unlock()

		out, _ := json.Marshal(map[string]string{"last": req.Target}) //nolint:errchkjson

		return packtrail.Result{Status: packtrail.StatusOK, Payload: out}, nil
	})

	s, err := packtrail.New(srv.NC,
		packtrail.WithNamespace("t-flowdef"),
		packtrail.WithFlowDef(packtrail.FlowDef{
			Name: "agnostic-def",
			Nodes: []packtrail.NodeDef{
				{ID: "a", Type: "task", Invoker: "custom", Target: "agent-a"},
				{ID: "b", Type: "task", Invoker: "custom", Target: "agent-b"},
			},
			Edges: []packtrail.EdgeDef{
				{From: "a", To: "b"},
			},
		}),
		packtrail.WithInvoker("custom", custom),
		packtrail.WithResultCache(),
	)
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = s.Run(ctx) }()

	id, err := s.Start(ctx, "agnostic-def", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		ex, getErr := s.Get(ctx, id)
		if getErr == nil && ex.Status == packtrail.ExecCompleted {
			doc, resErr := s.Results(ctx, id)
			if resErr != nil {
				t.Fatalf("results: %v", resErr)
			}

			if !strings.Contains(string(doc), `"b":{"last":"agent-b"}`) {
				t.Fatalf("results = %s, want results.b = {\"last\":\"agent-b\"}", doc)
			}

			mu.Lock()
			order := make([]string, len(seen))
			copy(order, seen)
			mu.Unlock()

			if len(order) != 2 || order[0] != "agent-a" || order[1] != "agent-b" {
				t.Fatalf("invocation order = %v, want [agent-a agent-b]", order)
			}

			return
		}

		time.Sleep(20 * time.Millisecond)
	}

	t.Fatal("execution did not complete in time")
}

func TestCustomInvokerEndToEnd(t *testing.T) {
	srv := natstest.Start(t)

	var (
		seen []string
		mu   sync.Mutex
	)

	custom := packtrail.InvokerFunc(func(_ context.Context, req packtrail.Request) (packtrail.Result, error) {
		mu.Lock()

		seen = append(seen, req.Target)
		mu.Unlock()
		// Echo the target into the shared payload to prove threading works.
		out, _ := json.Marshal(map[string]string{"last": req.Target}) //nolint:errchkjson // map[string]string is always safe

		return packtrail.Result{Status: packtrail.StatusOK, Payload: out}, nil
	})

	s, err := packtrail.New(srv.NC,
		packtrail.WithNamespace("t1"),
		packtrail.WithFlow([]byte(agnosticFlow)),
		packtrail.WithInvoker("custom", custom),
		packtrail.WithResultCache(),
	)
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = s.Run(ctx) }()

	id, err := s.Start(ctx, "agnostic", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		ex, getErr := s.Get(ctx, id)
		if getErr == nil && ex.Status == packtrail.ExecCompleted {
			doc, resErr := s.Results(ctx, id)
			if resErr != nil {
				t.Fatalf("results: %v", resErr)
			}

			if !strings.Contains(string(doc), `"b":{"last":"agent-b"}`) {
				t.Fatalf("results = %s, want results.b = {\"last\":\"agent-b\"}", doc)
			}

			mu.Lock()
			order := make([]string, len(seen))
			copy(order, seen)
			mu.Unlock()

			if len(order) != 2 || order[0] != "agent-a" || order[1] != "agent-b" {
				t.Fatalf("invocation order = %v, want [agent-a agent-b]", order)
			}

			return
		}

		time.Sleep(20 * time.Millisecond)
	}

	t.Fatal("execution did not complete in time")
}

const natsTaskFlow = `
version: "1.0"
name: nt
nodes:
  - id: x
    type: task
    subject: "tasks.x.{execution_id}"
edges: []
`

// waitCompleted polls until the execution completes (returning its assembled
// results document) or the deadline passes.
func waitCompleted(ctx context.Context, t *testing.T, s *packtrail.Server, id string) json.RawMessage {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		ex, err := s.Get(ctx, id)
		if err == nil && ex.Status == packtrail.ExecCompleted {
			doc, resErr := s.Results(ctx, id)
			if resErr != nil {
				t.Fatalf("results: %v", resErr)
			}

			return doc
		}

		if err == nil && ex.Status == packtrail.ExecFailed {
			t.Fatalf("execution %s failed: %s", id, ex.Error)
		}

		time.Sleep(20 * time.Millisecond)
	}

	t.Fatalf("execution %s did not complete in time", id)

	return nil
}

// TestNATSTaskHandleNamespaced drives the built-in nats-task path end to end
// under a NON-default namespace: a flow node publishes to "tasks.x.*" via the
// nats-task invoker, an in-process worker is registered with Server.Handle, and
// the flow must complete. It proves the invoker (publish side) and Handle
// (subscribe side) agree on the namespaced subject ("acme.tasks.x.<execID>") —
// the property the namespace-prefix change exists to guarantee.
func TestNATSTaskHandleNamespaced(t *testing.T) {
	srv := natstest.Start(t)

	s, err := packtrail.New(srv.NC,
		packtrail.WithNamespace("acme"),
		packtrail.WithFlow([]byte(natsTaskFlow)),
	)
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	if handleErr := s.Handle(context.Background(), "tasks.x.*", func(_ context.Context, req packtrail.TaskRequest) (packtrail.TaskResponse, error) {
		out, _ := json.Marshal(map[string]string{"handled": req.NodeID})
		return packtrail.TaskResponse{Status: packtrail.TaskOK, Payload: out}, nil
	}); handleErr != nil {
		t.Fatalf("handle: %v", handleErr)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = s.Run(ctx) }()

	id, err := s.Start(ctx, "nt", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	if got := string(waitCompleted(ctx, t, s, id)); !strings.Contains(got, `"x":{"handled":"x"}`) {
		t.Fatalf("results = %s, want results.x = {\"handled\":\"x\"}", got)
	}
}

// TestNATSTaskNamespaceIsolation proves two deployments on the same NATS cluster
// do not cross-serve built-in nats-task work: a worker registered in namespace
// "nsB" must never receive work from a flow started in namespace "nsA". Each
// flow is served only by its own namespace's worker.
func TestNATSTaskNamespaceIsolation(t *testing.T) {
	srv := natstest.Start(t)

	newServer := func(ns string, hits *int32) *packtrail.Server {
		s, err := packtrail.New(srv.NC,
			packtrail.WithNamespace(ns),
			packtrail.WithFlow([]byte(natsTaskFlow)),
		)
		if err != nil {
			t.Fatalf("new %s: %v", ns, err)
		}

		if handleErr := s.Handle(context.Background(), "tasks.x.*", func(_ context.Context, req packtrail.TaskRequest) (packtrail.TaskResponse, error) {
			atomic.AddInt32(hits, 1)
			return packtrail.TaskResponse{Status: packtrail.TaskOK, Payload: req.Payload}, nil
		}); handleErr != nil {
			t.Fatalf("handle %s: %v", ns, handleErr)
		}

		return s
	}

	var hitsA, hitsB int32

	sA := newServer("nsA", &hitsA)
	sB := newServer("nsB", &hitsB)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = sA.Run(ctx) }()
	go func() { _ = sB.Run(ctx) }()

	id, err := sA.Start(ctx, "nt", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	waitCompleted(ctx, t, sA, id)

	if got := atomic.LoadInt32(&hitsA); got != 1 {
		t.Fatalf("nsA worker hits = %d, want 1", got)
	}

	if got := atomic.LoadInt32(&hitsB); got != 0 {
		t.Fatalf("nsB worker hits = %d, want 0 (isolation breached)", got)
	}
}
