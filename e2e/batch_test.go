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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/event"
	"github.com/henomis/packtrail/worker"
)

// screenBatchFlow screens every person of a batch in a child flow of its own, two
// at a time (a map of subflows); a failed screening aborts the batch and
// reports.
const screenBatchFlow = `
name: batch
nodes:
  - id: screen
    type: map
    flow: screening
    over: input.people
    input: "{'name': item, 'slot': index, 'batch': input.batch}"
    max_parallel: 2
    on_failure: report
    next: done
  - {id: done, type: task, kind: summary}
  - {id: report, type: task, kind: summary}
`

// screeningFlow checks a person, then clears them. Without channels its
// output is the result of its last node.
const screeningFlow = `
name: screening
nodes:
  - {id: check, type: task, kind: checker, next: clear}
  - {id: clear, type: task, kind: clearer}
`

type person struct {
	Name  string `json:"name"`
	Slot  int    `json:"slot"`
	Batch string `json:"batch"`
}

// batchWorkers serve both flows; gauge counts screenings in flight per batch
// (from check to clear). Slow names are held long enough to be cancelled.
func batchWorkers(cl *cluster, gauge *tenantGauge, slow map[string]bool) {
	cl.worker("checker", func(ctx context.Context, j *worker.Job) (*worker.Result, error) {
		var p person
		if err := j.Input(&p); err != nil {
			return nil, worker.Permanent(err)
		}

		if p.Name == "mallory" {
			return nil, worker.Permanent(errors.New("failed screening"))
		}

		gauge.enter(p.Batch)

		d := 80 * time.Millisecond
		if slow[p.Name] {
			d = 2 * time.Second
		}

		select {
		case <-time.After(d):
		case <-ctx.Done():
			gauge.leave(p.Batch)

			return nil, ctx.Err()
		}

		return &worker.Result{Output: map[string]any{"checked": p.Name}}, nil
	}, worker.WithConcurrency(8))

	cl.worker("clearer", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		var p person
		if err := j.Input(&p); err != nil {
			return nil, worker.Permanent(err)
		}

		gauge.leave(p.Batch)

		return &worker.Result{Output: map[string]any{"name": p.Name, "slot": p.Slot, "cleared": true}}, nil
	}, worker.WithConcurrency(8))

	cl.worker("summary", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		return &worker.Result{Output: map[string]any{"node": j.Node, "errors": j.Context.Errors}}, nil
	})
}

// itemChildren returns the child executions a map started, by item key.
func itemChildren(f *facts) map[string]string {
	out := map[string]string{}

	for _, ev := range f.events {
		if d, ok := ev.Data.(*event.Child); ok {
			out[d.Key] = d.ChildID
		}
	}

	return out
}

// TestMapOfSubflows screens a batch with one child execution per person,
// two at a time, while an engine crashes: the map's result holds every
// child's output in item order, never more than two children run at once,
// and every child is linked to the batch.
func TestMapOfSubflows(t *testing.T) {
	cl := newCluster(t, []string{screenBatchFlow, screeningFlow})
	spare := cl.startEngine()

	gauge := &tenantGauge{running: map[string]int{}, peak: map[string]int{}}
	batchWorkers(cl, gauge, nil)

	people := []string{"ann", "bob", "cy", "dee", "eve", "fay"}
	id := cl.start("batch", map[string]any{"batch": "b1", "people": people})

	cl.waitUntil(id, "half way", func(st *packtrail.State) bool { return len(st.Children) > 0 && st.Steps > 0 })
	spare.crash()

	st := cl.wait(id)
	f := cl.check(id)

	if st.Status != packtrail.StatusCompleted || st.LastNode != "done" {
		t.Fatalf("batch: %s at %s (%s)", st.Status, st.LastNode, st.Error)
	}

	var screened []person
	if err := st.Result("screen", &screened); err != nil || len(screened) != len(people) {
		t.Fatalf("screen result %s", st.Results["screen"])
	}

	for i, p := range screened {
		if p.Name != people[i] || p.Slot != i {
			t.Fatalf("result %d is %+v, want %s in item order", i, p, people[i])
		}
	}

	if gauge.peak["b1"] != 2 {
		t.Fatalf("%d screenings at once, max_parallel 2", gauge.peak["b1"])
	}

	kids := itemChildren(f)
	if len(kids) != len(people) {
		t.Fatalf("%d children for %d people", len(kids), len(people))
	}

	for i := range people {
		child := kids[fmt.Sprintf("screen#%d", i)]

		cst := cl.wait(child)
		cl.check(child)

		var in person
		if cst.Status != packtrail.StatusCompleted || cst.Parent == nil || cst.Parent.ExecID != id ||
			cst.DecodeInput(&in) != nil || in.Name != people[i] || in.Batch != "b1" {
			t.Fatalf("child %d (%s): %s parent %+v input %s", i, child, cst.Status, cst.Parent, cst.Input)
		}
	}
}

// TestMapOfSubflowsAborts: one child fails, so the map aborts: the sibling
// still running is cancelled, the items not started never start, and the
// batch routes to its report with the failed item.
func TestMapOfSubflowsAborts(t *testing.T) {
	cl := newCluster(t, []string{screenBatchFlow, screeningFlow})

	gauge := &tenantGauge{running: map[string]int{}, peak: map[string]int{}}
	batchWorkers(cl, gauge, map[string]bool{"ann": true})

	id := cl.start("batch", map[string]any{"batch": "b2", "people": []string{"ann", "mallory", "bob", "cy"}})

	st := cl.wait(id)
	f := cl.check(id)

	if st.Status != packtrail.StatusCompleted || st.LastNode != "report" {
		t.Fatalf("batch: %s at %s (%s)", st.Status, st.LastNode, st.Error)
	}

	if e := st.Errors["screen"]; e.Index == nil || *e.Index != 1 || e.Reason != event.ReasonChild {
		t.Fatalf("errors.screen = %+v", e)
	}

	kids := itemChildren(f)
	if len(kids) != 2 || f.count[event.MapAborted] != 1 {
		t.Fatalf("children %v, aborts %d", kids, f.count[event.MapAborted])
	}

	for key, want := range map[string]packtrail.Status{
		"screen#0": packtrail.StatusCancelled, "screen#1": packtrail.StatusFailed,
	} {
		cst := cl.wait(kids[key])
		cl.check(kids[key])

		if cst.Status != want {
			t.Fatalf("%s (%s): %s, want %s", key, kids[key], cst.Status, want)
		}
	}
}
