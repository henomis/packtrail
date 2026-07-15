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

// Command approval demonstrates human-in-the-loop control: a signal node parks
// the execution until an external approval arrives (with a timeout fallback),
// Cancel abandons an execution an operator no longer wants, and WithHistory
// records a durable step-by-step trace queryable with Server.History.
//
// Run a NATS server (2.12+, JetStream enabled) and:
//
//	go run ./examples/approval --nats nats://127.0.0.1:4222
//
// It runs two orders: one is approved via Signal and fulfilled; the other is
// cancelled while waiting. Each execution's history trace is printed at the end.
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
)

// The flow is registered from an inline YAML document — no files on disk. The
// reject node is reached only through the signal's on_timeout route.
const orderFlow = `
version: "1.0"
name: order-approval
nodes:
  - {id: validate, type: task, invoker: svc, target: validator}
  - {id: gate, type: signal, signal_name: approval, timeout: 30s, on_timeout: reject}
  - {id: fulfill, type: task, invoker: svc, target: fulfillment}
  - {id: reject, type: task, invoker: svc, target: rejection}
edges:
  - {from: validate, to: gate}
  - {from: gate, to: fulfill}
`

func main() {
	url := flag.String("nats", nats.DefaultURL, "NATS server URL")

	flag.Parse()

	nc, err := nats.Connect(*url)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer nc.Close()

	svc := packtrail.InvokerFunc(func(_ context.Context, req packtrail.Request) (packtrail.Result, error) {
		log.Printf("service %s ran for node %s", req.Target, req.NodeID)

		out, mErr := json.Marshal(map[string]string{"service": req.Target, "state": "ok"})
		if mErr != nil {
			return packtrail.Result{}, mErr
		}

		return packtrail.Result{Status: packtrail.StatusOK, Payload: out}, nil
	})

	srv, err := packtrail.New(nc,
		packtrail.WithNamespace("orders"),
		packtrail.WithFlow([]byte(orderFlow)),
		packtrail.WithInvoker("svc", svc),
		// Keep a durable per-execution trace for a day; Server.History reads it.
		packtrail.WithHistory(24*time.Hour),
	)
	if err != nil {
		log.Fatalf("new: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		time.Sleep(500 * time.Millisecond)

		// Order 1: approved by a human (here: us, two seconds later).
		approved, startErr := srv.Start(ctx, "order-approval", json.RawMessage(`{"order":"A-1001"}`))
		if startErr != nil {
			log.Printf("start: %v", startErr)
			return
		}

		waitForSignalGate(ctx, srv, approved)
		log.Printf("%s parked at the gate — approving", approved)

		if sigErr := srv.Signal(ctx, approved, "approval", json.RawMessage(`{"approved":true,"by":"alice"}`)); sigErr != nil {
			log.Printf("signal: %v", sigErr)
		}

		waitForTerminal(ctx, srv, approved)

		// Order 2: the customer withdraws while it waits — Cancel abandons it.
		// Cancelled is a distinct terminal status (not failed), so Resume won't
		// revive it and dashboards show it as operator-driven.
		cancelled, startErr := srv.Start(ctx, "order-approval", json.RawMessage(`{"order":"A-1002"}`))
		if startErr != nil {
			log.Printf("start: %v", startErr)
			return
		}

		waitForSignalGate(ctx, srv, cancelled)
		log.Printf("%s parked at the gate — cancelling", cancelled)

		if cErr := srv.Cancel(ctx, cancelled, "customer withdrew the order"); cErr != nil {
			log.Printf("cancel: %v", cErr)
		}

		waitForTerminal(ctx, srv, cancelled)

		for _, id := range []string{approved, cancelled} {
			printHistory(ctx, srv, id)
		}

		if res, rErr := srv.Results(ctx, approved); rErr == nil {
			// signals.approval carries the approval payload alice sent.
			log.Printf("%s results: %s", approved, res)
		}

		stop()
	}()

	log.Printf("packtrail approval — signal gate + Cancel + durable history")

	if err := srv.Run(ctx); err != nil {
		log.Fatalf("run: %v", err)
	}
}

func waitForSignalGate(ctx context.Context, srv *packtrail.Server, id string) {
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		ex, err := srv.Get(ctx, id)
		if err == nil && ex.Status == packtrail.ExecWaiting && ex.WaitSignal == "approval" {
			return
		}

		time.Sleep(50 * time.Millisecond)
	}
}

func waitForTerminal(ctx context.Context, srv *packtrail.Server, id string) {
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		ex, err := srv.Get(ctx, id)
		if err == nil {
			switch ex.Status {
			case packtrail.ExecCompleted, packtrail.ExecFailed, packtrail.ExecCancelled:
				log.Printf("%s settled: %s", id, ex.Status)
				return
			}
		}

		time.Sleep(50 * time.Millisecond)
	}
}

func printHistory(ctx context.Context, srv *packtrail.Server, id string) {
	trace, err := srv.History(ctx, id, 50)
	if err != nil {
		log.Printf("history %s: %v", id, err)
		return
	}

	log.Printf("history of %s (%d transitions):", id, len(trace))

	for _, ev := range trace {
		node := ev.Node
		if node == "" {
			node = "—"
		}

		log.Printf("  %s  %-9s node=%s %s", ev.Time.Format("15:04:05.000"), ev.Status, node, ev.Error)
	}
}
