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

package acceptance

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/worker"
)

func TestM4FailoverAcrossEngines(t *testing.T) {
	e := NewEnv(t, []string{m1Linear})
	e.StartEngine()
	e.StartEngine()
	e.Worker("echo", Echo, worker.WithConcurrency(16))

	ids := make([]string, 0, 40)

	for i := range 40 {
		ids = append(ids, e.Start("linear", map[string]any{"i": i}))

		if i%10 == 9 {
			// Rotate: kill everything and bring up a new engine; pinned
			// consumers hand the partitions to whoever pulls next.
			e.RestartEngine()
			e.StartEngine()
		}
	}

	for _, id := range ids {
		e.Completed(id)
	}
}

func TestM4FlowVersionsAreImmutable(t *testing.T) {
	v1 := `
name: versioned
nodes:
  - {id: wait, type: await, signal: go, timeout: 1h, next: old}
  - {id: old, type: task, kind: echo}
`
	e := NewEnv(t, []string{v1})
	e.Worker("echo", Echo)

	inflight := e.Start("versioned", nil)
	e.WaitStatus(inflight, packtrail.StatusWaiting)

	h2 := e.Register(`
name: versioned
nodes:
  - {id: wait, type: await, signal: go, timeout: 1h, next: new}
  - {id: new, type: task, kind: echo}
`)

	fresh := e.Start("versioned", nil)
	e.WaitStatus(fresh, packtrail.StatusWaiting)

	for _, id := range []string{inflight, fresh} {
		if err := e.Client.Signal(e.Ctx, id, "go", nil); err != nil {
			t.Fatal(err)
		}
	}

	if st := e.Completed(inflight); st.LastNode != "old" {
		t.Fatalf("in-flight execution switched version: %s", st.LastNode)
	}

	if st := e.Completed(fresh); st.LastNode != "new" || st.FlowHash != h2 {
		t.Fatalf("new execution on old version: %s", st.LastNode)
	}

	vs, err := e.Client.Flows(e.Ctx)
	if err != nil || len(vs) != 2 {
		t.Fatalf("versions %+v %v", vs, err)
	}
}

func TestM4Visibility(t *testing.T) {
	e := NewEnv(t, []string{`
name: shop
search_attributes: {customer: input.customer}
nodes:
  - {id: wait, type: await, signal: pay, timeout: 1h, next: ship}
  - {id: ship, type: task, kind: echo}
`})
	e.Worker("echo", Echo)

	a := e.Start("shop", map[string]any{"customer": "acme"})
	b := e.Start("shop", map[string]any{"customer": "globex"})
	e.WaitStatus(a, packtrail.StatusWaiting)
	e.WaitStatus(b, packtrail.StatusWaiting)

	_ = e.Client.Signal(e.Ctx, a, "pay", nil)
	e.Completed(a)

	list := func(f packtrail.ListFilter) []packtrail.Summary {
		var out []packtrail.Summary

		e.Eventually(func() bool {
			var err error

			out, err = e.Client.List(e.Ctx, f)

			return err == nil && len(out) == 1
		}, func() string { return fmt.Sprintf("list %+v = %+v", f, out) })

		return out
	}

	if l := list(packtrail.ListFilter{Status: packtrail.StatusCompleted}); l[0].ExecID != a {
		t.Fatal("status index")
	}

	if l := list(packtrail.ListFilter{Status: packtrail.StatusWaiting}); l[0].ExecID != b {
		t.Fatal("status index moved wrongly")
	}

	if l := list(packtrail.ListFilter{Attr: "customer", Value: "globex"}); l[0].ExecID != b {
		t.Fatal("attribute index")
	}

	if _, err := e.Client.List(e.Ctx, packtrail.ListFilter{Flow: "*"}); !errors.Is(err, packtrail.ErrInvalidArgument) {
		t.Fatalf("wildcard filter accepted: %v", err)
	}
}

func TestM4ArchiveAndRetention(t *testing.T) {
	e := NewEnv(t, []string{`
name: shortlived
retention: 1s
nodes:
  - {id: a, type: task, kind: echo}
`})
	e.Worker("echo", Echo)

	id := e.Start("shortlived", map[string]any{"k": "v"}, packtrail.WithExecutionID("short-1"))
	e.Completed(id)

	e.Eventually(func() bool {
		st, err := e.Client.Get(e.Ctx, id)

		return err == nil && st.Archived
	}, func() string { return "execution never archived" })

	st, _ := e.Client.Get(e.Ctx, id)
	if st.Status != packtrail.StatusCompleted || string(st.Input) != `{"k":"v"}` {
		t.Fatalf("archived state %+v", st)
	}

	if err := e.Client.Cancel(e.Ctx, id, ""); !errors.Is(err, packtrail.ErrArchived) {
		t.Fatalf("cancel archived = %v (I-17)", err)
	}

	if evs, err := e.Client.History(e.Ctx, id); err != nil || len(evs) == 0 {
		t.Fatalf("archived history %v", err)
	}

	// Starting the same id again is a duplicate, not a new execution.
	e.Start("shortlived", nil, packtrail.WithExecutionID("short-1"))
	time.Sleep(500 * time.Millisecond)

	if again, _ := e.Client.Get(e.Ctx, id); !again.Archived {
		t.Fatal("archived id was restarted")
	}

	if _, err := e.Client.Fork(e.Ctx, id, 1); err != nil {
		t.Fatalf("fork of archived execution: %v", err)
	}
}

func TestM4GracefulDrain(t *testing.T) {
	e := NewEnv(t, []string{m1Linear})

	var finished atomic.Int32

	started := make(chan struct{}, 1)

	w, err := worker.New(e.NC(), "echo", func(ctx context.Context, j *worker.Job) (*worker.Result, error) {
		if j.Node == "a" {
			started <- struct{}{}

			time.Sleep(500 * time.Millisecond)
			finished.Add(1)
		}

		return Echo(ctx, j)
	}, worker.WithDrainTimeout(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}

	ctx, stop := context.WithCancel(e.Ctx)
	done := make(chan struct{})

	go func() {
		defer close(done)

		_ = w.Run(ctx)
	}()

	id := e.Start("linear", nil)

	<-started
	stop() // shutdown while a job is in flight
	<-done

	if finished.Load() != 1 {
		t.Fatal("in-flight job was not drained (I-15)")
	}

	e.Worker("echo", Echo)

	st := e.Completed(id)
	if c := countEvents(t, e, id, "NodeCompleted", "a"); c != 1 || st.LastNode != "c" {
		t.Fatalf("a completed %d times", c)
	}
}

func TestM4PerKeyConcurrency(t *testing.T) {
	e := NewEnv(t, []string{`
name: tenant
nodes:
  - {id: work, type: task, kind: limited, concurrency: {key: input.tenant, max: 1}}
`})

	var (
		mu      sync.Mutex
		running = map[string]int{}
		peak    = map[string]int{}
	)

	e.Worker("limited", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		var in struct{ Tenant string }

		_ = j.Input(&in)

		mu.Lock()
		running[in.Tenant]++
		peak[in.Tenant] = max(peak[in.Tenant], running[in.Tenant])
		mu.Unlock()

		time.Sleep(100 * time.Millisecond)

		mu.Lock()
		running[in.Tenant]--
		mu.Unlock()

		return &worker.Result{}, nil
	}, worker.WithConcurrency(8))

	ids := make([]string, 0, 6)
	for i := range 6 {
		ids = append(ids, e.Start("tenant", map[string]any{"tenant": []string{"a", "b"}[i%2]}))
	}

	for _, id := range ids {
		e.Completed(id)
	}

	if peak["a"] != 1 || peak["b"] != 1 {
		t.Fatalf("peak concurrency %v", peak)
	}
}

func TestM4Triggers(t *testing.T) {
	e := NewEnv(t, []string{`
name: on-order
triggers:
  - {subject: orders.created}
  - {subject: orders.js.created, stream: ORDERS}
nodes:
  - {id: handle, type: task, kind: echo}
`})
	e.Worker("echo", Echo)

	if _, err := e.S.JS.CreateStream(e.Ctx, jetstream.StreamConfig{Name: "ORDERS", Subjects: []string{"orders.js.>"}}); err != nil {
		t.Fatal(err)
	}

	// The stream trigger consumer is created by Run; restart to pick it up.
	e.RestartEngine()
	time.Sleep(300 * time.Millisecond)

	nc := e.NC()
	if err := nc.Publish("orders.created", []byte(`{"order":1}`)); err != nil {
		t.Fatal(err)
	}

	m := nats.NewMsg("orders.js.created")
	m.Header.Set("Nats-Msg-Id", "order-2")
	m.Data = []byte(`{"order":2}`)

	if _, err := e.S.JS.PublishMsg(e.Ctx, m); err != nil {
		t.Fatal(err)
	}

	e.Eventually(func() bool {
		l, err := e.Client.List(e.Ctx, packtrail.ListFilter{Flow: "on-order", Status: packtrail.StatusCompleted})

		return err == nil && len(l) == 2
	}, func() string { return "triggers did not start two executions" })

	if st := e.Completed("order-2"); string(st.Input) != `{"order":2}` {
		t.Fatalf("trigger input %s", st.Input)
	}
}

func TestM4TraceparentAndMetrics(t *testing.T) {
	e := NewEnv(t, []string{m1Linear})

	const tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

	got := make(chan string, 3)

	e.Worker("echo", func(ctx context.Context, j *worker.Job) (*worker.Result, error) {
		got <- j.Traceparent

		return Echo(ctx, j)
	})

	id := e.Start("linear", nil, packtrail.WithTraceparent(tp))
	e.Completed(id)

	if first := <-got; first != tp {
		t.Fatalf("traceparent %q not propagated to the job", first)
	}

	m := e.Engine.Metrics(e.Ctx)
	if m.Commands == 0 || m.EventsAppended == 0 || m.JobsDispatched != 3 {
		t.Fatalf("metrics %+v", m)
	}
}
