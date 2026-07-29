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
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/internal/natstest"
)

// branchingFlow exercises the start-node derivation on a shape where "the first
// node listed" and "the node with no inbound transition" are different, so a
// lazy derivation would pick the wrong one.
const branchingFlow = `
version: "1.0"
name: remote
nodes:
  - {id: worker, type: task, subject: "tasks.worker.{execution_id}"}
  - {id: head, type: task, subject: "tasks.head.{execution_id}"}
  - {id: gate, type: choice, rules: [{default: true, to: worker}]}
edges:
  - {from: head, to: gate}
`

// TestClientStartsWithoutTheFlowDefinition is the property M13 is about: a
// process holding only a namespace can start a flow.
//
// Reads already worked that way — ListFlows and FlowGraph serve an observer from
// the published registry, which is how packtrail-ui renders flows it has no
// source for. Start was the sole exception, and only because it needed one field
// of the definition. An embedder's workaround was to stand up its own NATS
// request/reply front door so the engine could start flows on a client's behalf —
// ~90 lines whose subtlety is that the responder must join a queue group, since
// without one every replica answers the same request, each minting its own
// execution id, and the flow runs once per replica at full cost.
func TestClientStartsWithoutTheFlowDefinition(t *testing.T) {
	srv := natstest.Start(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var headRan, workerRan atomic.Bool

	// The engine: loads the flow and serves its tasks.
	engine, err := packtrail.New(srv.NC,
		packtrail.WithNamespace("remote"),
		packtrail.WithFlow([]byte(branchingFlow)),
	)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	serve := func(subject string, ran *atomic.Bool) {
		if handleErr := engine.Handle(ctx, subject,
			func(_ context.Context, req packtrail.TaskRequest) (packtrail.TaskResponse, error) {
				ran.Store(true)
				return packtrail.TaskResponse{Status: packtrail.TaskOK, Payload: req.Payload}, nil
			}); handleErr != nil {
			t.Errorf("handle %s: %v", subject, handleErr)
		}
	}

	serve("tasks.head.*", &headRan)
	serve("tasks.worker.*", &workerRan)

	go func() { _ = engine.Run(ctx) }()

	// Wait for the engine to publish its flow registry.
	waitFlowRegistered(ctx, t, srv.NC, "remote", "remote")

	// The client: a separate connection, a separate Server, and no flows at all.
	clientConn := srv.Connect(t)

	client, err := packtrail.New(clientConn, packtrail.WithNamespace("remote"))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	id, err := client.Start(ctx, "remote", json.RawMessage(`{"n":1}`))
	if err != nil {
		t.Fatalf("client Start: %v", err)
	}

	waitExecStatus(ctx, t, engine, id, packtrail.ExecCompleted)

	// It began at "head" — the node with no inbound transition — not at "worker",
	// which is listed first but is only reachable through the choice node.
	if !headRan.Load() {
		t.Error("the execution did not begin at the start node")
	}

	if !workerRan.Load() {
		t.Error("the execution did not run to completion through the choice node")
	}

	// And the client can read it back over the same flowless handle.
	ex, err := client.Get(ctx, id)
	if err != nil {
		t.Fatalf("client Get: %v", err)
	}

	if ex.Flow != "remote" {
		t.Errorf("execution flow = %q, want remote", ex.Flow)
	}
}

// TestClientStartWithIDIsIdempotent: the idempotency key works from a flowless
// client too — the property that makes a timed-out start safe to retry, and the
// one a broadcast front door could not preserve across replicas.
func TestClientStartWithIDIsIdempotent(t *testing.T) {
	srv := natstest.Start(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	engine, err := packtrail.New(srv.NC,
		packtrail.WithNamespace("remote2"),
		packtrail.WithFlow([]byte(branchingFlow)),
	)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	if initErr := engine.Init(ctx); initErr != nil {
		t.Fatalf("init: %v", initErr)
	}

	client, err := packtrail.New(srv.Connect(t), packtrail.WithNamespace("remote2"))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	first, err := client.StartWithID(ctx, "order-42", "remote", json.RawMessage(`{"n":1}`))
	if err != nil {
		t.Fatalf("first StartWithID: %v", err)
	}

	second, err := client.StartWithID(ctx, "order-42", "remote", json.RawMessage(`{"n":1}`))
	if err != nil {
		t.Fatalf("retried StartWithID: %v", err)
	}

	if first != second || first != "order-42" {
		t.Fatalf("StartWithID returned %q then %q, want both to be order-42", first, second)
	}

	ids, err := client.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	if len(ids) != 1 {
		t.Fatalf("executions = %v, want exactly one (the retry must not create a second)", ids)
	}
}

// TestClientStartUnknownFlowStillFails: a name in neither the local set nor the
// registry must fail at the call, as it always has.
func TestClientStartUnknownFlowStillFails(t *testing.T) {
	srv := natstest.Start(t)

	client, err := packtrail.New(srv.NC, packtrail.WithNamespace("remote3"))
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	_, err = client.Start(context.Background(), "no-such-flow", nil)
	if err == nil {
		t.Fatal("Start accepted a flow that is in neither the local set nor the registry")
	}

	if !strings.Contains(err.Error(), "no-such-flow") {
		t.Errorf("error %q should name the flow", err)
	}
}

// waitFlowRegistered blocks until name appears in the namespace's flow registry.
func waitFlowRegistered(ctx context.Context, t *testing.T, nc *nats.Conn, ns, name string) {
	t.Helper()

	s, err := packtrail.New(nc, packtrail.WithNamespace(ns))
	if err != nil {
		t.Fatalf("registry probe: %v", err)
	}

	deadline := time.Now().Add(15 * time.Second)

	for time.Now().Before(deadline) {
		if g, gerr := s.FlowGraph(ctx, name); gerr == nil {
			if g.Start == "" {
				t.Fatalf("flow %q was published without a start node", name)
			}

			return
		}

		time.Sleep(50 * time.Millisecond)
	}

	t.Fatalf("flow %q never appeared in the registry", name)
}
