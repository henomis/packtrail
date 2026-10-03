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

// Example events: executions started by time and by messages.
//
//   - A cron schedule (a JetStream message schedule — no process keeps time)
//     starts `report` every second; each firing becomes execution
//     "<schedule>-<sequence>".
//
//   - A trigger starts `order` for every message published on orders.created;
//     a message's Nats-Msg-Id becomes the execution id, so a redelivered or
//     republished message starts nothing twice.
//
//     go run ./examples/events
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/examples/internal/devserver"
	"github.com/henomis/packtrail/examples/internal/exutil"
	"github.com/henomis/packtrail/worker"
)

const ns = "example-events"

const reportYAML = `
name: report
nodes:
  - {id: build, type: task, kind: work}
`

const orderYAML = `
name: order
triggers:
  - {subject: orders.created}
nodes:
  - {id: fulfil, type: task, kind: work}
`

func main() {
	nc, closeAll := devserver.Connect()
	defer closeAll()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	g := exutil.NewGroup(ctx)
	defer g.Stop()

	eng := g.Engine(nc, packtrail.WithNamespace(ns),
		packtrail.WithFlowYAML([]byte(reportYAML)), packtrail.WithFlowYAML([]byte(orderYAML)),
		packtrail.WithSchedule("every-second", "report", "@every 1s", map[string]any{"kind": "heartbeat"}))

	g.Serve(nc, ns, "work", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		fmt.Printf("  ran %s for %s\n", j.Flow, j.ExecID)

		return &worker.Result{Output: map[string]any{"ok": true}}, nil
	})

	c := eng.Client()

	// Follow every execution that ends from now on.
	ended, err := c.WatchTerminal(ctx, 0)
	if err != nil {
		log.Fatal(err)
	}

	// The engine is ready, so the trigger is already subscribed.
	for i, id := range []string{"order-1001", "order-1002", "order-1001"} { // the last one is a duplicate
		m := nats.NewMsg("orders.created")
		m.Header.Set("Nats-Msg-Id", id)
		m.Data = fmt.Appendf(nil, `{"n":%d}`, i)

		if err := nc.PublishMsg(m); err != nil {
			log.Fatal(err)
		}
	}

	// Two orders (the duplicate starts nothing) and a few cron firings.
	done := map[string]int{}

	for done["order"] < 2 || done["report"] < 3 {
		var e packtrail.Ended

		select {
		case e = <-ended:
		case <-ctx.Done():
			log.Fatalf("still waiting: %v", done)
		}

		st, err := c.Get(ctx, e.ExecID)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("  %s %s %s\n", st.Flow, e.ExecID, e.Status)
		done[st.Flow]++
	}

	if err := c.Unschedule(ctx, "every-second"); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("orders: %d, reports: %d\n", done["order"], done["report"])
}
