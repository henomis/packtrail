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

package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/event"
	"github.com/henomis/packtrail/worker"
)

// digestFlow is started by a cron schedule and by request messages. Fetching
// is cached (same request, same result); rendering runs one at a time per
// tenant across every worker process.
const digestFlow = `
name: digest
triggers:
  - {subject: e2e.digest.requested}
nodes:
  - {id: fetch, type: task, kind: fetch, cache: {ttl: 1h}, next: render}
  - {id: render, type: task, kind: render, concurrency: {key: input.tenant, max: 1}}
`

type digestRequest struct {
	Tenant string `json:"tenant"`
	Topic  string `json:"topic"`
}

// tenantGauge tracks how many renders run at once per tenant.
type tenantGauge struct {
	mu            sync.Mutex
	running, peak map[string]int
}

func (g *tenantGauge) enter(tenant string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.running[tenant]++
	g.peak[tenant] = max(g.peak[tenant], g.running[tenant])
}

func (g *tenantGauge) leave(tenant string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.running[tenant]--
}

// endings collects the executions WatchTerminal reports.
type endings struct {
	ch   <-chan packtrail.Ended
	seen map[string]int
}

// until reads endings until done holds on them.
func (e *endings) until(cl *cluster, what string, done func() bool) {
	cl.t.Helper()

	for !done() {
		select {
		case x, ok := <-e.ch:
			if !ok {
				cl.t.Fatalf("WatchTerminal closed waiting for %s", what)
			}

			e.seen[x.ExecID]++
		case <-cl.ctx.Done():
			cl.t.Fatalf("never: %s (seen %d)", what, len(e.seen))
		}
	}
}

func (e *endings) count(prefix string) int {
	n := 0

	for id := range e.seen {
		if strings.HasPrefix(id, prefix) {
			n++
		}
	}

	return n
}

// TestEventDrivenPipeline feeds one flow from a cron schedule and from
// request messages (each published twice), while an engine crashes and NATS
// restarts. Every request starts exactly one execution, repeated requests are
// served from the cache across the outage, renders never overlap within a
// tenant, and nothing fires after the schedule is removed.
func TestEventDrivenPipeline(t *testing.T) {
	cl := newCluster(t, []string{digestFlow})
	cl.startEngine()

	gauge := &tenantGauge{running: map[string]int{}, peak: map[string]int{}}

	cl.worker("fetch", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		var r digestRequest
		if err := j.Input(&r); err != nil {
			return nil, worker.Permanent(err)
		}

		return &worker.Result{Output: map[string]any{"articles": len(r.Topic)}}, nil
	}, worker.WithConcurrency(8))

	render := func(ctx context.Context, j *worker.Job) (*worker.Result, error) {
		var r digestRequest
		if err := j.Input(&r); err != nil {
			return nil, worker.Permanent(err)
		}

		gauge.enter(r.Tenant)
		defer gauge.leave(r.Tenant)

		select {
		case <-time.After(30 * time.Millisecond):
		case <-ctx.Done():
			return nil, ctx.Err()
		}

		return &worker.Result{Output: map[string]any{"rendered": r.Topic}}, nil
	}

	// Two render processes: the per-tenant limit holds across them.
	cl.worker("render", render, worker.WithConcurrency(8))
	cl.worker("render", render, worker.WithConcurrency(8))

	ch, werr := cl.c.WatchTerminal(cl.ctx, 0)
	if werr != nil {
		t.Fatal(werr)
	}

	ends := &endings{ch: ch, seen: map[string]int{}}

	if err := cl.c.Schedule(cl.ctx, "nightly", "digest", "@every 1s", digestRequest{Tenant: "cron", Topic: "daily"}); err != nil {
		t.Fatal(err)
	}

	pub := cl.s.Connect(t)
	tenants := []string{"acme", "globex", "initech"}

	// publish sends one request per tenant and topic, each message twice.
	publish := func(round string, topics ...string) []string {
		ids := make([]string, 0, len(tenants)*len(topics))

		for _, tenant := range tenants {
			for _, topic := range topics {
				id := fmt.Sprintf("req-%s-%s-%s", round, tenant, topic)
				ids = append(ids, "digest-"+id)                                      // the trigger's execution id is <flow>-<Nats-Msg-Id>
				body, _ := json.Marshal(digestRequest{Tenant: tenant, Topic: topic}) //nolint:errchkjson // plain struct.

				for range 2 {
					m := nats.NewMsg("e2e.digest.requested")
					m.Header.Set("Nats-Msg-Id", id)
					m.Data = body

					if err := pub.PublishMsg(m); err != nil {
						t.Fatal(err)
					}
				}
			}
		}

		if err := pub.Flush(); err != nil {
			t.Fatal(err)
		}

		return ids
	}

	ended := func(ids []string) func() bool {
		return func() bool {
			for _, id := range ids {
				if ends.seen[id] == 0 {
					return false
				}
			}

			return true
		}
	}

	first := publish("a", "news", "sports")
	ends.until(cl, "first requests done", ended(first))

	// An engine crashes, NATS restarts; the engines are replaced so the
	// trigger subscriptions are known to be back.
	cl.engines[0].crash()
	cl.startEngine()
	cl.s.Restart(t)
	cl.stopEngines()
	cl.startEngine()
	cl.startEngine()

	// The same requests again (cached), and new ones that pile up per tenant.
	again := publish("b", "news", "sports")
	fresh := publish("c", "weather", "markets", "travel", "food")

	ends.until(cl, "every request done", func() bool { return ended(again)() && ended(fresh)() })

	// A few more firings once everything has settled.
	base := ends.count("nightly-")
	ends.until(cl, "three more cron runs", func() bool { return ends.count("nightly-") >= base+3 })

	if err := cl.c.Unschedule(cl.ctx, "nightly"); err != nil {
		t.Fatal(err)
	}

	if scheds, serr := cl.c.Schedules(cl.ctx); serr != nil || len(scheds) != 0 {
		t.Fatalf("schedules after Unschedule: %+v %v", scheds, serr)
	}

	// Let an in-flight firing finish, then make sure nothing else fires.
	settle := func(d time.Duration) int {
		deadline := time.After(d)

		for {
			select {
			case x := <-ch:
				ends.seen[x.ExecID]++
			case <-deadline:
				return ends.count("nightly-")
			}
		}
	}

	settled := settle(1500 * time.Millisecond)
	if later := settle(3 * time.Second); later != settled {
		t.Fatalf("%d cron runs after Unschedule", later-settled)
	}

	// Exactly the requests published, each once.
	if n := ends.count("digest-req-"); n != len(first)+len(again)+len(fresh) {
		t.Fatalf("%d request executions, want %d", n, len(first)+len(again)+len(fresh))
	}

	cached := map[string]bool{}

	// When each cron run's fetch was scheduled, and when the first one done
	// without the cache wrote its entry.
	cronScheduled := map[string]time.Time{}

	var firstWrite time.Time

	for id, n := range ends.seen {
		if n != 1 {
			t.Fatalf("%s ended %d times", id, n)
		}

		st := cl.wait(id)
		f := cl.check(id)

		if st.Status != packtrail.StatusCompleted {
			t.Fatalf("%s: %s (%s)", id, st.Status, st.Error)
		}

		cron := strings.HasPrefix(id, "nightly-")

		for _, ev := range f.events {
			switch d := ev.Data.(type) {
			case *event.Scheduled:
				if cron && d.Node == "fetch" {
					cronScheduled[id] = ev.Time
				}
			case *event.NodeDone:
				if d.Node != "fetch" {
					continue
				}

				cached[id] = d.Cached

				if cron && !d.Cached && (firstWrite.IsZero() || ev.Time.Before(firstWrite)) {
					firstWrite = ev.Time
				}
			}
		}
	}

	// The cache outlives the engine crash and the NATS restart.
	for _, id := range again {
		if !cached[id] {
			t.Fatalf("%s repeated a request but was not served from the cache", id)
		}
	}

	for _, id := range append(first, fresh...) {
		if cached[id] {
			t.Fatalf("%s is a new request but was served from the cache", id)
		}
	}

	// Cron runs share one input. The cache is best effort (runs in flight
	// together all miss, as the ones held up by the outage do), but a run
	// fetching well after an earlier one finished is served from it.
	hits := 0

	for id, at := range cronScheduled {
		if at.After(firstWrite.Add(time.Second)) {
			if !cached[id] {
				t.Fatalf("cron run %s (fetch scheduled %s) missed the cache written at %s", id, at, firstWrite)
			}

			hits++
		}
	}

	if hits < 2 {
		t.Fatalf("only %d cron runs late enough to use the cache", hits)
	}

	for tenant, p := range gauge.peak {
		if p != 1 {
			t.Fatalf("tenant %s rendered %d at once, limit 1", tenant, p)
		}
	}
}
