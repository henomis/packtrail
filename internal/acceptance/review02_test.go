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
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/internal/dispatch"
	"github.com/henomis/packtrail/internal/infra"
	"github.com/henomis/packtrail/internal/names"
	"github.com/henomis/packtrail/worker"
)

const twoKeys = `
name: keyed
nodes:
  - {id: work, type: task, kind: keyed, concurrency: {key: input.tenant, max: 1}}
`

// TestReviewF202HotKeyDoesNotStarveIdleKey: a backlog on one key must not
// occupy every worker slot: a job for another key runs right away (F2-02).
func TestReviewF202HotKeyDoesNotStarveIdleKey(t *testing.T) {
	e := NewEnv(t, []string{twoKeys})
	e.Worker("keyed", func(context.Context, *worker.Job) (*worker.Result, error) {
		time.Sleep(300 * time.Millisecond)

		return &worker.Result{}, nil
	}, worker.WithConcurrency(4))

	hot := make([]string, 0, 12)
	for range 12 {
		hot = append(hot, e.Start("keyed", map[string]any{"tenant": "a"}))
	}

	time.Sleep(200 * time.Millisecond) // the backlog is queued

	start := time.Now()
	idle := e.Start("keyed", map[string]any{"tenant": "b"})
	e.Completed(idle)

	if took := time.Since(start); took > 1500*time.Millisecond {
		t.Fatalf("idle key waited %v behind the hot key's backlog", took)
	}

	for _, id := range hot {
		e.Completed(id)
	}
}

// TestReviewF206ShutdownDoesNotWaitForSlotWaiters: stopping a worker with
// jobs queued behind a busy key returns promptly, not after the drain budget
// (F2-06).
func TestReviewF206ShutdownDoesNotWaitForSlotWaiters(t *testing.T) {
	e := NewEnv(t, []string{twoKeys})

	w, err := worker.New(e.NC(), "keyed", func(ctx context.Context, _ *worker.Job) (*worker.Result, error) {
		select {
		case <-time.After(1500 * time.Millisecond):
		case <-ctx.Done():
		}

		return &worker.Result{}, nil
	}, worker.WithConcurrency(4), worker.WithDrainTimeout(20*time.Second))
	if err != nil {
		t.Fatal(err)
	}

	ctx, stop := context.WithCancel(e.Ctx)
	done := make(chan struct{})

	go func() {
		defer close(done)

		_ = w.Run(ctx)
	}()

	ids := make([]string, 0, 4)
	for range 4 {
		ids = append(ids, e.Start("keyed", map[string]any{"tenant": "a"}))
	}

	time.Sleep(300 * time.Millisecond)

	start := time.Now()

	stop()
	<-done

	// The running job drains (≤ 1.5s); the three waiters must be handed back
	// at once instead of each running 1.5s more.
	if took := time.Since(start); took > 2500*time.Millisecond {
		t.Fatalf("shutdown took %v with slot waiters", took)
	}

	// The waiting jobs were handed back, not lost.
	e.Worker("keyed", func(context.Context, *worker.Job) (*worker.Result, error) { return &worker.Result{}, nil })

	for _, id := range ids {
		e.Completed(id)
	}
}

// TestReviewF203TransientOutageDoesNotQuarantine: an infrastructure outage
// longer than a few retries stalls dispatching but quarantines nothing (F2-03).
func TestReviewF203TransientOutageDoesNotQuarantine(t *testing.T) {
	e := NewEnv(t, []string{m1Linear})
	e.Worker("echo", Echo)

	kv, err := e.S.JS.KeyValue(e.Ctx, "packtrail-index")
	if err != nil {
		t.Fatal(err)
	}

	cfg := kv.(interface {
		Status(ctx context.Context) (jetstream.KeyValueStatus, error)
	})

	status, err := cfg.Status(e.Ctx)
	if err != nil {
		t.Fatal(err)
	}

	if err = e.S.JS.DeleteKeyValue(e.Ctx, "packtrail-index"); err != nil {
		t.Fatal(err)
	}

	id := e.Start("linear", nil)

	time.Sleep(8 * time.Second) // well past the old 5-delivery quarantine

	// The stall is visible to operators.
	if m := e.Engine.Metrics(e.Ctx); m.DispatchStall < 5*time.Second || len(m.DispatchStalls) == 0 {
		t.Fatalf("stall not reported: %v %v", m.DispatchStall, m.DispatchStalls)
	}

	if _, err = e.S.JS.CreateKeyValue(e.Ctx, jetstream.KeyValueConfig{
		Bucket: "packtrail-index", History: uint8(status.History()), //nolint:gosec // small.
	}); err != nil {
		t.Fatal(err)
	}

	e.Completed(id)

	if q, qerr := e.Client.Quarantined(e.Ctx); qerr != nil || len(q) != 0 {
		t.Fatalf("quarantined after a transient outage: %v %v", q, qerr)
	}
}

func attach(t *testing.T, e *Env) *infra.Infra {
	t.Helper()

	in, err := infra.New(e.NC(), names.New(""), nil)
	if err != nil {
		t.Fatal(err)
	}

	if err = in.Attach(e.Ctx); err != nil {
		t.Fatal(err)
	}

	return in
}

// TestReviewF204QuarantinedChildStillClosesItsParent: the terminal effects of
// a quarantined execution still run (F2-04).
func TestReviewF204QuarantinedChildStillClosesItsParent(t *testing.T) {
	e := NewEnv(t, []string{m2Parent, `
name: child
nodes:
  - {id: wait, type: await, signal: never, timeout: 1h}
`})

	parent := e.Start("parent", map[string]any{"child": map[string]any{}})
	st := e.WaitStatus(parent, packtrail.StatusRunning)

	var child string

	e.Eventually(func() bool {
		st, _ = e.Client.Get(e.Ctx, parent)
		for _, c := range st.Children {
			child = c.ChildID
		}

		if child == "" {
			return false
		}

		cst, err := e.Client.Get(e.Ctx, child)

		return err == nil && cst.Status == packtrail.StatusWaiting
	}, func() string { return "child never started" })

	if err := dispatch.Quarantine(e.Ctx, attach(t, e), child, 1, "test quarantine"); err != nil {
		t.Fatal(err)
	}

	time.Sleep(200 * time.Millisecond)

	if err := e.Client.Cancel(e.Ctx, child, "operator"); err != nil {
		t.Fatal(err)
	}

	if pst := e.Wait(parent); pst.Status != packtrail.StatusFailed || pst.Reason != "child_failed" {
		t.Fatalf("parent %s %s", pst.Status, pst.Reason)
	}

	e.Eventually(func() bool {
		l, _ := e.Client.List(e.Ctx, packtrail.ListFilter{Status: packtrail.StatusCancelled, Flow: "child"})

		return len(l) == 1 && l[0].Quarantined
	}, func() string { return "quarantined child not indexed as cancelled" })
}

// TestReviewF204UnquarantineReplaysSkippedEffects: events skipped while
// quarantined have their effects replayed on unquarantine (F2-04).
func TestReviewF204UnquarantineReplaysSkippedEffects(t *testing.T) {
	e := NewEnv(t, []string{strings.Replace(m2Await, "%s", "1h", 1)})
	e.Worker("echo", Echo)
	e.Worker("slow-echo", Echo)

	id := e.Start("approval", nil)
	st := e.WaitStatus(id, packtrail.StatusWaiting)

	if err := dispatch.Quarantine(e.Ctx, attach(t, e), id, st.LastSeq+1, "test quarantine"); err != nil {
		t.Fatal(err)
	}

	time.Sleep(200 * time.Millisecond)

	if err := e.Client.Signal(e.Ctx, id, "approve", nil); err != nil {
		t.Fatal(err)
	}

	// The signal is decided (task "done" scheduled) but no job is dispatched.
	e.Eventually(func() bool {
		cur, err := e.Client.Get(e.Ctx, id)

		return err == nil && cur.Tasks["done"] != nil
	}, func() string { return "signal not decided" })

	time.Sleep(500 * time.Millisecond)

	if cur, _ := e.Client.Get(e.Ctx, id); cur.Status.Terminal() {
		t.Fatal("a quarantined execution's job was dispatched")
	}

	if err := e.Client.Unquarantine(e.Ctx, id); err != nil {
		t.Fatal(err)
	}

	if done := e.Completed(id); done.LastNode != "done" {
		t.Fatalf("last node %s", done.LastNode)
	}

	if q, _ := e.Client.Quarantined(e.Ctx); len(q) != 0 {
		t.Fatalf("still quarantined: %v", q)
	}
}

// TestReviewF302SemaphoreOutageDoesNotFailJobs: while the semaphore bucket is
// unavailable, a job handed back past its delivery cap is not declared
// exhausted: an unreadable busy count is not a count of zero (F3-02).
func TestReviewF302SemaphoreOutageDoesNotFailJobs(t *testing.T) {
	e := NewEnv(t, []string{twoKeys})
	e.Worker("keyed", func(context.Context, *worker.Job) (*worker.Result, error) {
		time.Sleep(2 * time.Second)

		return &worker.Result{}, nil
	}, worker.WithConcurrency(4), worker.WithMaxDeliver(2))

	ids := make([]string, 0, 4)
	for range 4 {
		ids = append(ids, e.Start("keyed", map[string]any{"tenant": "a"}))
	}

	time.Sleep(700 * time.Millisecond) // hand-backs have happened

	kv, err := e.S.JS.KeyValue(e.Ctx, "packtrail-sem")
	if err != nil {
		t.Fatal(err)
	}

	status, err := kv.Status(e.Ctx)
	if err != nil {
		t.Fatal(err)
	}

	if err = e.S.JS.DeleteKeyValue(e.Ctx, "packtrail-sem"); err != nil {
		t.Fatal(err)
	}

	time.Sleep(3 * time.Second) // redeliveries past max-deliver during the outage

	if _, err = e.S.JS.CreateKeyValue(e.Ctx, jetstream.KeyValueConfig{
		Bucket: "packtrail-sem", History: uint8(status.History()), //nolint:gosec // small.
	}); err != nil {
		t.Fatal(err)
	}

	for _, id := range ids {
		e.Completed(id)
	}
}

// TestReviewF304QuarantinedChildReportsCounters: a quarantined child closes
// with the counters folded up to the bad event, so the parent's budgets see
// them (F3-04).
func TestReviewF304QuarantinedChildReportsCounters(t *testing.T) {
	e := NewEnv(t, []string{m2Parent, `
name: child
nodes:
  - {id: spend, type: task, kind: spender, next: wait}
  - {id: wait, type: await, signal: never, timeout: 1h}
`})
	e.Worker("spender", func(context.Context, *worker.Job) (*worker.Result, error) {
		return &worker.Result{Usage: map[string]float64{"units": 5}}, nil
	})

	parent := e.Start("parent", map[string]any{"child": map[string]any{}})

	var (
		child string
		cst   *packtrail.State
	)

	e.Eventually(func() bool {
		st, err := e.Client.Get(e.Ctx, parent)
		if err != nil {
			return false
		}

		for _, c := range st.Children {
			child = c.ChildID
		}

		if child == "" {
			return false
		}

		cst, err = e.Client.Get(e.Ctx, child)

		return err == nil && cst.Status == packtrail.StatusWaiting
	}, func() string { return "child never reached its await" })

	if err := dispatch.Quarantine(e.Ctx, attach(t, e), child, cst.LastSeq+1, "test quarantine"); err != nil {
		t.Fatal(err)
	}

	time.Sleep(200 * time.Millisecond)

	if err := e.Client.Cancel(e.Ctx, child, "operator"); err != nil {
		t.Fatal(err)
	}

	pst := e.Wait(parent)
	if pst.Status != packtrail.StatusFailed || pst.Counters["units"] != 5 {
		t.Fatalf("parent %s counters %v", pst.Status, pst.Counters)
	}
}
