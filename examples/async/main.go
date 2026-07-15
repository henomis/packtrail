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

// Command async demonstrates long-running work off the engine's critical path.
// The flow (a Go-struct FlowDef — no YAML on disk) fans out three slow tasks;
// their invoker kind is registered with WithAsyncInvoker, so each dispatch goes
// to a durable JetStream work-queue and runs on an in-process worker pool while
// the execution parks as `waiting`, holding no engine slot. The engine settles
// each branch via CompleteActivity under the hood.
//
// Run a NATS server (2.12+, JetStream enabled) and:
//
//	go run ./examples/async --nats nats://127.0.0.1:4222
//
// Watch the status flip running → waiting (jobs queued, slots free) → completed.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/invoker/asyncqueue"
)

func main() {
	url := flag.String("nats", nats.DefaultURL, "NATS server URL")

	flag.Parse()

	nc, err := nats.Connect(*url)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer nc.Close()

	// The slow work itself is an ordinary synchronous Invoker — no queue, ack
	// or heartbeat code. asyncqueue adds the durability around it.
	slow := packtrail.InvokerFunc(func(ctx context.Context, req packtrail.Request) (packtrail.Result, error) {
		log.Printf("worker: %s/%s started (simulating slow work)", req.ExecutionID, req.NodeID)

		select {
		case <-time.After(2 * time.Second):
		case <-ctx.Done():
			return packtrail.Result{}, ctx.Err() // transient → redelivered
		}

		out, mErr := json.Marshal(map[string]string{"node": req.NodeID, "took": "2s"})
		if mErr != nil {
			return packtrail.Result{}, mErr
		}

		return packtrail.Result{Status: packtrail.StatusOK, Payload: out}, nil
	})

	srv, err := packtrail.New(nc,
		packtrail.WithNamespace("async-demo"),
		packtrail.WithFlowDef(analysisFlow()),
		// Nodes with `invoker: slow` dispatch to a work-queue and park the
		// execution; two worker slots grind through the three branches.
		packtrail.WithAsyncInvoker("slow", slow, asyncqueue.WithConcurrency(2)),
	)
	if err != nil {
		log.Fatalf("new: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		time.Sleep(500 * time.Millisecond)

		id, startErr := srv.Start(ctx, "async-analysis", json.RawMessage(`{"dataset":"q3-numbers"}`))
		if startErr != nil {
			log.Printf("start: %v", startErr)
			return
		}

		log.Printf("started %s", id)

		last := ""
		deadline := time.Now().Add(30 * time.Second)

		for time.Now().Before(deadline) {
			ex, getErr := srv.Get(ctx, id)
			if getErr != nil {
				time.Sleep(50 * time.Millisecond)
				continue
			}

			if s := ex.Status + "@" + ex.CurrentNode; s != last {
				last = s

				log.Printf("status=%s node=%s branches=%d settled=%d",
					ex.Status, ex.CurrentNode, len(ex.Branches), settled(ex))
			}

			if ex.Status == packtrail.ExecCompleted || ex.Status == packtrail.ExecFailed {
				res, _ := srv.Results(ctx, id)
				log.Printf("results: %s", res)

				break
			}

			time.Sleep(100 * time.Millisecond)
		}

		stop()
	}()

	log.Printf("packtrail async — slow branches on a durable work-queue, engine slots stay free")

	if err := srv.Run(ctx); err != nil {
		log.Fatalf("run: %v", err)
	}
}

// analysisFlow builds the flow programmatically: prepare, fan out three slow
// analyses, join them all, then summarize.
func analysisFlow() packtrail.FlowDef {
	return packtrail.FlowDef{
		Name: "async-analysis",
		Nodes: []packtrail.NodeDef{
			{ID: "prepare", Type: "task", Invoker: "slow", Target: "prepare"},
			{ID: "fan", Type: "fanout", Branches: []string{"trends", "outliers", "forecast"}},
			{ID: "trends", Type: "task", Invoker: "slow", Target: "trends"},
			{ID: "outliers", Type: "task", Invoker: "slow", Target: "outliers"},
			{ID: "forecast", Type: "task", Invoker: "slow", Target: "forecast"},
			{ID: "join", Type: "fanin", WaitFor: []string{"trends", "outliers", "forecast"}, JoinPolicy: "all"},
			{ID: "summarize", Type: "task", Invoker: "slow", Target: "summarize"},
		},
		Edges: []packtrail.EdgeDef{
			{From: "prepare", To: "fan"},
			{From: "fan", To: "join"},
			{From: "join", To: "summarize"},
		},
	}
}

func settled(ex *packtrail.Execution) int {
	n := 0

	for _, b := range ex.Branches {
		if b.Status != "pending" {
			n++
		}
	}

	return n
}
