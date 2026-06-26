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

// Command embedded shows packtrail running as a single microservice: the engine
// and the task workers live in one process, importing only the public
// github.com/henomis/packtrail package.
//
// Run an external NATS server and then:
//
//	go run ./examples/embedded --nats nats://127.0.0.1:4222
//
// It starts one execution of the research pipeline and prints progress.
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

func main() {
	url := flag.String("nats", nats.DefaultURL, "NATS server URL")

	flag.Parse()

	nc, err := nats.Connect(*url)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer nc.Close()

	// One process, custom namespace, flows loaded from ./examples.
	srv, err := packtrail.New(nc,
		packtrail.WithNamespace("acme"),
		packtrail.WithFlowsDir("examples"),
		packtrail.WithReconcileActive("0 */5 * * * *"),
		packtrail.WithReconcileFull("0 0 * * * *"),
	)
	if err != nil {
		log.Fatalf("new: %v", err)
	}

	// Co-locate the task workers in the same binary. echo marks each node done.
	for _, subject := range []string{
		"tasks.triage.*", "tasks.tech-research.*", "tasks.market-research.*",
		"tasks.legal-research.*", "tasks.synthesis.*", "tasks.escalation.*",
	} {
		if err := srv.Handle(subject, echo); err != nil {
			log.Fatalf("handle %s: %v", subject, err)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Kick off one execution shortly after the engine starts.
	go func() {
		time.Sleep(500 * time.Millisecond)

		id, err := srv.Start(ctx, "research-pipeline", json.RawMessage(`{"risk_score":10}`))
		if err != nil {
			log.Printf("start: %v", err)
			return
		}

		log.Printf("started %s", id)

		for range 10 {
			time.Sleep(time.Second)

			ex, err := srv.Get(ctx, id)
			if err != nil {
				continue
			}

			log.Printf("status=%s node=%s", ex.Status, ex.CurrentNode)

			if ex.Status == packtrail.ExecWaiting {
				_ = srv.Signal(ctx, id, "approval", json.RawMessage(`{"ok":true}`))
			}

			if ex.Status == packtrail.ExecCompleted || ex.Status == packtrail.ExecFailed {
				break
			}
		}
	}()

	log.Printf("packtrail embedded — engine + workers in one process (namespace acme)")

	if err := srv.Run(ctx); err != nil {
		log.Fatalf("run: %v", err)
	}
}

// echo marks the current node done in the shared payload and returns it.
func echo(_ context.Context, req packtrail.TaskRequest) (packtrail.TaskResponse, error) {
	root := map[string]json.RawMessage{}
	_ = json.Unmarshal(req.Payload, &root)
	root[req.NodeID] = json.RawMessage(`"done"`)

	out, err := json.Marshal(root)
	if err != nil {
		return packtrail.TaskResponse{Status: packtrail.TaskError, Error: err.Error()}, nil
	}

	return packtrail.TaskResponse{Status: packtrail.TaskOK, Payload: out}, nil
}
