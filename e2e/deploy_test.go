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
	"slices"
	"testing"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/event"
	"github.com/henomis/packtrail/worker"
)

// ticketV1 and ticketV2 are two versions of one flow: v2 inserts a review
// after the hold, and the nodes' meta (the configuration workers read)
// changes too.
const ticketV1 = `
name: ticket
nodes:
  - {id: triage, type: task, kind: triage, meta: {model: small}, next: hold}
  - {id: hold, type: await, signal: release, timeout: 1h, next: resolve}
  - {id: resolve, type: task, kind: resolve, meta: {team: blue}}
`

const ticketV2 = `
name: ticket
nodes:
  - {id: triage, type: task, kind: triage, meta: {model: large}, next: hold}
  - {id: hold, type: await, signal: release, timeout: 1h, next: review}
  - {id: review, type: task, kind: resolve, meta: {team: red}, next: resolve}
  - {id: resolve, type: task, kind: resolve, meta: {team: green}}
`

// ticketWorkers serve both versions: each reports the meta it was given.
func ticketWorkers(cl *cluster) {
	cl.worker("triage", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		var m struct{ Model string }
		if err := j.DecodeMeta(&m); err != nil {
			return nil, worker.Permanent(err)
		}

		return &worker.Result{Output: map[string]any{"model": m.Model}}, nil
	})

	cl.worker("resolve", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		var m struct{ Team string }
		if err := j.DecodeMeta(&m); err != nil {
			return nil, worker.Permanent(err)
		}

		return &worker.Result{Output: map[string]any{"team": m.Team}}, nil
	})
}

// verifyTicket checks a finished ticket against the version it must have run.
func (cl *cluster) verifyTicket(id, hash string, v2 bool) {
	cl.t.Helper()

	t := cl.t
	st := cl.wait(id)
	cl.check(id)

	if st.Status != packtrail.StatusCompleted || st.FlowHash != hash {
		t.Fatalf("%s: %s on version %s, want completed on %s (%s)", id, st.Status, st.FlowHash, hash, st.Error)
	}

	var triage struct{ Model string }

	var resolve, review struct{ Team string }

	_ = st.Result("triage", &triage)
	_ = st.Result("resolve", &resolve)
	reviewed := st.Result("review", &review) == nil

	want := struct {
		model, resolve string
		reviewed       bool
	}{"small", "blue", false}
	if v2 {
		want.model, want.resolve, want.reviewed = "large", "green", true
	}

	if triage.Model != want.model || resolve.Team != want.resolve || reviewed != want.reviewed ||
		(v2 && review.Team != "red") {
		t.Fatalf("%s ran %s/%s reviewed=%v (%s), want %+v", id, triage.Model, resolve.Team, reviewed, review.Team, want)
	}
}

// TestRollingDeploy replaces every engine, one at a time, by engines
// deploying a new version of a flow while executions of the old one are
// parked. Executions keep the version they started with — its graph and its
// meta — through the deploy, a pinned start, a fork and a rerun; new starts
// get the new version.
func TestRollingDeploy(t *testing.T) {
	cl := newCluster(t, []string{ticketV1})
	cl.startEngine()
	ticketWorkers(cl)

	old := make([]string, 4)
	for i := range old {
		old[i] = cl.start("ticket", nil)
	}

	v1 := cl.parkedAt(old[0], "hold").FlowHash
	for _, id := range old[1:] {
		cl.parkedAt(id, "hold")
	}

	// Roll: a v2 engine joins, then an old one leaves, until none is left.
	cl.mu.Lock()
	olds := slices.Clone(cl.engines)
	cl.mu.Unlock()

	cl.deploy([]string{ticketV2})

	fresh := make([]string, 0, len(olds)+2) //nolint:mnd // two more after the deploy.

	for _, p := range olds {
		cl.startEngine()
		// Mid-deploy, old and new engines serve the namespace together.
		fresh = append(fresh, cl.start("ticket", nil))

		p.stop()
	}

	fresh = append(fresh, cl.start("ticket", nil), cl.start("ticket", nil))

	v2 := cl.parkedAt(fresh[0], "hold").FlowHash
	if v2 == v1 {
		t.Fatalf("new starts still run version %s", v1)
	}

	for _, id := range fresh {
		if h := cl.parkedAt(id, "hold").FlowHash; h != v2 {
			t.Fatalf("%s started on %s after the deploy, want %s", id, h, v2)
		}
	}

	pinned := cl.start("ticket", nil, packtrail.WithVersion(v1))
	cl.parkedAt(pinned, "hold")

	all := slices.Concat(old, fresh, []string{pinned})
	for _, id := range all {
		if err := cl.c.Signal(cl.ctx, id, "release", nil); err != nil {
			t.Fatal(err)
		}
	}

	for _, id := range append(old, pinned) {
		cl.verifyTicket(id, v1, false)
	}

	for _, id := range fresh {
		cl.verifyTicket(id, v2, true)
	}

	// A fork of a v1 execution, taken after its triage, stays on v1.
	var afterTriage uint64

	for _, ev := range cl.check(old[0]).events {
		if d, ok := ev.Data.(*event.NodeDone); ok && d.Node == "triage" {
			afterTriage = ev.Seq
		}
	}

	fork, err := cl.c.Fork(cl.ctx, old[0], afterTriage)
	if err != nil {
		t.Fatal(err)
	}

	cl.parkedAt(fork, "hold")

	if err = cl.c.Signal(cl.ctx, fork, "release", nil); err != nil {
		t.Fatal(err)
	}

	cl.verifyTicket(fork, v1, false)

	// So does a rerun of its last node.
	rerun, err := cl.c.Rerun(cl.ctx, old[1], "resolve")
	if err != nil {
		t.Fatal(err)
	}

	cl.verifyTicket(rerun, v1, false)

	// A rerun of its await waits again, on v1.
	rehold, err := cl.c.Rerun(cl.ctx, old[2], "hold")
	if err != nil {
		t.Fatal(err)
	}

	cl.parkedAt(rehold, "hold")

	if err = cl.c.Signal(cl.ctx, rehold, "release", nil); err != nil {
		t.Fatal(err)
	}

	cl.verifyTicket(rehold, v1, false)

	// Both versions stay registered; v2 is the latest.
	vs, err := cl.c.Flows(cl.ctx)
	if err != nil {
		t.Fatal(err)
	}

	latest := map[string]bool{}

	for _, v := range vs {
		if v.Name == "ticket" {
			latest[v.Hash] = v.Latest
		}
	}

	if len(latest) != 2 || latest[v1] || !latest[v2] {
		t.Fatalf("ticket versions %+v", vs)
	}
}
