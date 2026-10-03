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
	"testing"
	"time"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/packtrailtest"
	"github.com/henomis/packtrail/worker"
)

// jobFlow waits for a go signal, then works. Both namespaces deploy it.
const jobFlow = `
name: job
nodes:
  - {id: gate, type: await, signal: go, timeout: 1h, next: work}
  - {id: work, type: task, kind: work}
`

const onlyAFlow = `
name: only-a
nodes:
  - {id: work, type: task, kind: work}
`

// TestNamespacesAreIsolated runs two namespaces on one NATS server with the
// same flow names, worker kinds and execution ids. Nothing crosses over:
// jobs, signals, cancels, flows, the index, terminal notifications and the
// store all stay within their namespace.
func TestNamespacesAreIsolated(t *testing.T) {
	s := packtrailtest.Start(t)

	a := newClusterOn(t, s, "tenant-a", []string{jobFlow, onlyAFlow})
	b := newClusterOn(t, s, "tenant-b", []string{jobFlow})

	for _, cl := range []*cluster{a, b} {
		cl.worker("work", func(_ context.Context, _ *worker.Job) (*worker.Result, error) {
			return &worker.Result{Output: map[string]any{"ns": cl.ns}}, nil
		})
	}

	endsA, err := a.c.WatchTerminal(a.ctx, 0)
	if err != nil {
		t.Fatal(err)
	}

	endsB, err := b.c.WatchTerminal(b.ctx, 0)
	if err != nil {
		t.Fatal(err)
	}

	// The same execution id in both namespaces: two executions.
	const id = "shared-1"

	for _, cl := range []*cluster{a, b} {
		cl.start("job", nil, packtrail.WithExecutionID(id))
		cl.parkedAt(id, "gate")
	}

	// A signal in A releases only A's execution.
	if err = a.c.Signal(a.ctx, id, "go", nil); err != nil {
		t.Fatal(err)
	}

	st := a.wait(id)
	a.check(id)

	var out struct{ NS string }
	if st.Status != packtrail.StatusCompleted || st.Result("work", &out) != nil || out.NS != "tenant-a" {
		t.Fatalf("A: %s, work ran in %q", st.Status, out.NS)
	}

	if st, err = b.c.Get(b.ctx, id); err != nil || st.Status != packtrail.StatusWaiting {
		t.Fatalf("B moved with A's signal: %+v %v", st, err)
	}

	// A cancel in B stops only B's.
	if err = b.c.Cancel(b.ctx, id, "tenant left"); err != nil {
		t.Fatal(err)
	}

	if st = b.wait(id); st.Status != packtrail.StatusCancelled {
		t.Fatalf("B: %s", st.Status)
	}

	b.check(id)

	if st = a.wait(id); st.Status != packtrail.StatusCompleted {
		t.Fatalf("A changed with B's cancel: %s", st.Status)
	}

	// Flows are per namespace.
	if _, err = b.c.Start(b.ctx, "only-a", nil); err == nil {
		t.Fatal("B started a flow only A deployed")
	}

	other := a.start("only-a", nil)
	if st = a.wait(other); st.Status != packtrail.StatusCompleted {
		t.Fatalf("A: only-a %s", st.Status)
	}

	// Terminal notifications: A sees its two endings, B its one.
	for _, w := range []struct {
		cl   *cluster
		ch   <-chan packtrail.Ended
		want map[string]packtrail.Status
	}{
		{a, endsA, map[string]packtrail.Status{id: packtrail.StatusCompleted, other: packtrail.StatusCompleted}},
		{b, endsB, map[string]packtrail.Status{id: packtrail.StatusCancelled}},
	} {
		seen := map[string]packtrail.Status{}

		for len(seen) < len(w.want) {
			select {
			case e := <-w.ch:
				seen[e.ExecID] = e.Status
			case <-time.After(20 * time.Second):
				t.Fatalf("%s: WatchTerminal reported %v", w.cl.ns, seen)
			}
		}

		select {
		case e := <-w.ch:
			t.Fatalf("%s: WatchTerminal reported %s from elsewhere", w.cl.ns, e.ExecID)
		case <-time.After(500 * time.Millisecond):
		}

		for x, status := range w.want {
			if seen[x] != status {
				t.Fatalf("%s: WatchTerminal reported %v, want %v", w.cl.ns, seen, w.want)
			}
		}
	}

	// The index: each namespace lists its own.
	a.eventually("A indexed", func() bool {
		l, lerr := a.c.List(a.ctx, packtrail.ListFilter{Status: packtrail.StatusCompleted})

		return lerr == nil && len(l) == 2
	})

	b.eventually("B indexed", func() bool {
		l, lerr := b.c.List(b.ctx, packtrail.ListFilter{})

		return lerr == nil && len(l) == 1 && l[0].Status == packtrail.StatusCancelled
	})

	// The store: one key, two values.
	for _, cl := range []*cluster{a, b} {
		if err = cl.c.Store().Put(cl.ctx, "prefs", "theme", cl.ns); err != nil {
			t.Fatal(err)
		}
	}

	for _, cl := range []*cluster{a, b} {
		v, gerr := cl.c.Store().Get(cl.ctx, "prefs", "theme")
		if gerr != nil || !jsonEqual(v, mustJSON(cl.ns)) {
			t.Fatalf("%s store: %s %v", cl.ns, v, gerr)
		}
	}

	for _, cl := range []*cluster{a, b} {
		if dl, derr := cl.c.DeadLetters(cl.ctx, 10); derr != nil || len(dl) != 0 {
			t.Fatalf("%s dead letters: %+v %v", cl.ns, dl, derr)
		}
	}
}
