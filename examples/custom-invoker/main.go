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

// Command custom-invoker demonstrates packtrail's defining feature: the
// pluggable Invoker seam. The agent-pipeline flow's task nodes select
// `invoker: agent` and name a target agent; a tiny in-process Invoker plays
// the agent framework, and a choice node routes on the triage agent's output
// (results.triage.category). WithResultCache makes the (simulated) agent calls
// idempotent under packtrail's at-least-once delivery.
//
// Run a NATS server (2.12+, JetStream enabled) and, from the repo root (the
// flow is loaded from examples/custom-invoker/flows):
//
//	go run ./examples/custom-invoker --nats nats://127.0.0.1:4222
//
// It runs two executions — one billing, one general — and prints the route
// each one took plus its assembled results.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/henomis/packtrail"
)

func main() {
	url := flag.String("nats", nats.DefaultURL, "NATS server URL")

	flag.Parse()

	nc, err := nats.Connect(*url)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer nc.Close()

	srv, err := packtrail.New(nc,
		packtrail.WithNamespace("agents"),
		packtrail.WithFlowsDir("examples/custom-invoker/flows"),
		// The single seam: every `invoker: agent` node dispatches here.
		packtrail.WithInvoker("agent", packtrail.InvokerFunc(callAgent)),
		// Agent calls are side effects; dedupe redeliveries by (execution, node, attempt).
		packtrail.WithResultCache(),
	)
	if err != nil {
		log.Fatalf("new: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// The flow's final node is a built-in nats-task; host its worker in-process.
	if err := srv.Handle(ctx, "tasks.notify.*", notify); err != nil {
		log.Fatalf("handle: %v", err)
	}

	go func() {
		time.Sleep(500 * time.Millisecond)

		for _, input := range []string{
			`{"customer":"alice","request":"double charge on invoice 42"}`,
			`{"customer":"bob","request":"how do I export my data?"}`,
		} {
			id, startErr := srv.Start(ctx, "agent-pipeline", json.RawMessage(input))
			if startErr != nil {
				log.Printf("start: %v", startErr)
				continue
			}

			waitAndReport(ctx, srv, id)
		}

		stop() // both executions settled — shut down
	}()

	log.Printf("packtrail custom-invoker — an %q Invoker drives the agent-pipeline flow", "agent")

	if err := srv.Run(ctx); err != nil {
		log.Fatalf("run: %v", err)
	}
}

// callAgent plays the agent framework behind the Invoker seam. req.Target is
// the agent name from the flow definition; req.Payload is the assembled
// {input, results, signals} context.
func callAgent(_ context.Context, req packtrail.Request) (packtrail.Result, error) {
	var doc struct {
		Input struct {
			Request string `json:"request"`
		} `json:"input"`
	}

	_ = json.Unmarshal(req.Payload, &doc)

	var out any

	switch req.Target {
	case "triage-agent":
		// A real agent would classify the request; keyword matching stands in.
		category := "general"
		if containsAny(doc.Input.Request, "charge", "invoice", "refund") {
			category = "billing"
		}

		out = map[string]string{"category": category}
	default: // billing-agent, general-agent
		out = map[string]string{"answer": fmt.Sprintf("handled by %s", req.Target)}
	}

	payload, err := json.Marshal(out)
	if err != nil {
		return packtrail.Result{}, err // transient → retried per the node policy
	}

	return packtrail.Result{Status: packtrail.StatusOK, Payload: payload}, nil
}

// notify is the nats-task worker for the flow's final node.
func notify(_ context.Context, req packtrail.TaskRequest) (packtrail.TaskResponse, error) {
	log.Printf("notify: execution %s settled", req.ExecutionID)

	return packtrail.TaskResponse{Status: packtrail.TaskOK, Payload: json.RawMessage(`{"notified":true}`)}, nil
}

func waitAndReport(ctx context.Context, srv *packtrail.Server, id string) {
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		ex, err := srv.Get(ctx, id)
		if err == nil && (ex.Status == packtrail.ExecCompleted || ex.Status == packtrail.ExecFailed) {
			route := "general-agent"

			for _, node := range ex.Outputs {
				if node == "billing-agent" {
					route = "billing-agent"
				}
			}

			res, _ := srv.Results(ctx, id)
			log.Printf("%s: status=%s route=%s\nresults: %s", id, ex.Status, route, res)

			return
		}

		time.Sleep(50 * time.Millisecond)
	}

	log.Printf("%s: did not settle in time", id)
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}

	return false
}
