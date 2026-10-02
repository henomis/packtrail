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
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/event"
	"github.com/henomis/packtrail/internal/fold"
	"github.com/henomis/packtrail/worker"
)

func TestM3ReducersUnderConcurrentFanout(t *testing.T) {
	e := NewEnv(t, []string{`
name: gather
channels:
  notes: {reducer: append}
  total: {reducer: sum}
  seen:  {reducer: merge}
nodes:
  - {id: split, type: fanout, branches: [a, b, c, d], next: join}
  - {id: a, type: task, kind: note}
  - {id: b, type: task, kind: note}
  - {id: c, type: task, kind: note}
  - {id: d, type: task, kind: note}
  - {id: join, type: join}
`})
	e.Worker("note", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		return &worker.Result{Writes: map[string]any{
			"notes": j.Node, "total": 1, "seen": map[string]bool{j.Node: true},
		}}, nil
	}, worker.WithConcurrency(8))

	st := e.Completed(e.Start("gather", nil))

	var out struct {
		Notes []string
		Total float64
		Seen  map[string]bool
	}

	if err := json.Unmarshal(st.Output, &out); err != nil {
		t.Fatal(err)
	}

	if len(out.Notes) != 4 || out.Total != 4 || len(out.Seen) != 4 {
		t.Fatalf("channels %s", st.Output)
	}
}

const m3Story = `
name: story
channels: {log: {reducer: append}}
nodes:
  - {id: a, type: task, kind: step, next: b}
  - {id: b, type: task, kind: step, next: c}
  - {id: c, type: task, kind: step}
`

func stepWorker(fail *atomic.Bool) worker.Handler {
	return func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		var in struct{ Tag string }

		_ = j.Input(&in)

		if j.Node == "c" && fail != nil && fail.Load() {
			return nil, worker.Permanent(errors.New("c is broken"))
		}

		return &worker.Result{Output: map[string]any{"node": j.Node}, Writes: map[string]any{"log": j.Node + in.Tag}},
			nil
	}
}

func TestM3HistoryStateAtAndOutputHistory(t *testing.T) {
	e := NewEnv(t, []string{m3Story})
	e.Worker("step", stepWorker(nil))

	id := e.Start("story", map[string]any{"tag": "!"})
	final := e.Completed(id)

	evs, err := e.Client.History(e.Ctx, id)
	if err != nil {
		t.Fatal(err)
	}

	// Time travel: the state right after b completed has a and b only.
	var afterB uint64

	for _, ev := range evs {
		if d, ok := ev.Data.(*event.NodeDone); ok && d.Node == "b" {
			afterB = ev.Seq
		}
	}

	st, err := e.Client.StateAt(e.Ctx, id, afterB)
	if err != nil {
		t.Fatal(err)
	}

	if string(st.Channels["log"]) != `["a!","b!"]` || st.Status.Terminal() {
		t.Fatalf("state at %d: %s %s", afterB, st.Channels["log"], st.Status)
	}

	if string(final.Channels["log"]) != `["a!","b!","c!"]` {
		t.Fatalf("final %s", final.Channels["log"])
	}

	outs, err := e.Client.OutputHistory(e.Ctx, id, "b")
	if err != nil || len(outs) != 1 || string(outs[0]) != `{"node":"b"}` {
		t.Fatalf("output history %s %v", outs, err)
	}
}

func TestM3ForkDiverges(t *testing.T) {
	e := NewEnv(t, []string{m3Story})
	e.Worker("step", stepWorker(nil))

	id := e.Start("story", map[string]any{"tag": "1"})
	e.Completed(id)

	evs, _ := e.Client.History(e.Ctx, id)

	var afterA uint64

	for _, ev := range evs {
		if d, ok := ev.Data.(*event.NodeDone); ok && d.Node == "a" {
			afterA = ev.Seq
		}
	}

	fork, err := e.Client.Fork(e.Ctx, id, afterA, packtrail.WithForkID("story-fork"))
	if err != nil {
		t.Fatal(err)
	}

	st := e.Completed(fork)
	if st.ForkedFrom != id || string(st.Channels["log"]) != `["a1","b1","c1"]` {
		t.Fatalf("fork %+v %s", st.ForkedFrom, st.Channels["log"])
	}

	if c := countEvents(t, e, fork, event.NodeCompleted, "a"); c != 0 {
		t.Fatal("the fork must not re-run what happened before the fork point")
	}

	src, _ := e.Client.Get(e.Ctx, id)
	if src.Status != packtrail.StatusCompleted || src.Events != len(evs) {
		t.Fatal("the source must be untouched")
	}
}

func TestM3RerunFailedNode(t *testing.T) {
	e := NewEnv(t, []string{m3Story})

	var broken atomic.Bool

	broken.Store(true)
	e.Worker("step", stepWorker(&broken))

	id := e.Start("story", nil)
	if st := e.Wait(id); st.Status != packtrail.StatusFailed || st.FailedNode != "c" {
		t.Fatalf("expected failure at c: %+v", st)
	}

	broken.Store(false)

	re, err := e.Client.Rerun(e.Ctx, id, "c")
	if err != nil {
		t.Fatal(err)
	}

	st := e.Completed(re)
	if string(st.Channels["log"]) != `["a","b","c"]` {
		t.Fatalf("rerun log %s", st.Channels["log"])
	}
}

func TestM3BudgetExceededInParallelBranch(t *testing.T) {
	e := NewEnv(t, []string{`
name: costly
budget: {cost: 10}
nodes:
  - {id: split, type: fanout, branches: [cheap, pricey], next: join}
  - {id: cheap, type: task, kind: spend}
  - {id: pricey, type: task, kind: spend}
  - {id: join, type: join}
`})
	e.Worker("spend", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		cost := 1.0
		if j.Node == "pricey" {
			cost = 50
		}

		return &worker.Result{Usage: map[string]float64{"cost": cost}}, nil
	}, worker.WithConcurrency(4))

	st := e.Wait(e.Start("costly", nil))
	if st.Status != packtrail.StatusFailed || st.Reason != event.ReasonBudget {
		t.Fatalf("status %s reason %s", st.Status, st.Reason)
	}
}

func TestM3OutputSchema(t *testing.T) {
	e := NewEnv(t, []string{`
name: typed
nodes:
  - id: extract
    type: task
    kind: extract
    retry: {max_attempts: 2, backoff: fixed, delay: 50ms}
    output_schema:
      type: object
      required: [name]
      properties: {name: {type: string}}
`})
	e.Worker("extract", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		if j.Attempt == 1 {
			return &worker.Result{Output: map[string]any{"nam": "typo"}}, nil
		}

		return &worker.Result{Output: map[string]any{"name": "ok"}}, nil
	})

	st := e.Completed(e.Start("typed", nil))
	if string(st.Results["extract"]) != `{"name":"ok"}` {
		t.Fatalf("result %s", st.Results["extract"])
	}
}

func TestM3ResultCache(t *testing.T) {
	e := NewEnv(t, []string{`
name: cached
nodes:
  - {id: expensive, type: task, kind: expensive, cache: {ttl: 1h}}
`})

	var calls atomic.Int32

	e.Worker("expensive", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		calls.Add(1)

		var in struct{ Q string }

		_ = j.Input(&in)

		return &worker.Result{Output: map[string]any{"answer": in.Q + "!"}}, nil
	})

	a := e.Completed(e.Start("cached", map[string]any{"q": "x"}))
	b := e.Completed(e.Start("cached", map[string]any{"q": "x"}))
	c := e.Completed(e.Start("cached", map[string]any{"q": "y"}))

	if calls.Load() != 2 {
		t.Fatalf("worker called %d times, want 2 (I-05)", calls.Load())
	}

	if string(a.Results["expensive"]) != string(b.Results["expensive"]) || string(c.Results["expensive"]) !=
		`{"answer":"y!"}` {
		t.Fatal("cached result differs")
	}
}

func TestM3SnapshotPlusTailEqualsFullFold(t *testing.T) {
	e := NewEnv(t, []string{`
name: loopy
start: work
max_steps: 400
channels: {n: {reducer: sum}}
nodes:
  - {id: work, type: task, kind: inc, next: again}
  - id: again
    type: choice
    rules: [{when: "channels.n < 40", to: work}, {default: true, to: done}]
  - {id: done, type: task, kind: inc}
`}, packtrail.WithSnapshotEvery(7))
	e.Worker("inc", func(context.Context, *worker.Job) (*worker.Result, error) {
		return &worker.Result{Writes: map[string]any{"n": 1}}, nil
	})

	id := e.Start("loopy", nil)
	st := e.Completed(id)

	if st.Visits["work"] != 40 {
		t.Fatalf("visits %d", st.Visits["work"])
	}

	// Get folds snapshot + tail; folding the whole history from scratch must
	// give the same state.
	snaps := 0

	if kv, err := e.S.JS.KeyValue(e.Ctx, "packtrail-snapshots"); err == nil {
		if _, err = kv.Get(e.Ctx, id); err == nil {
			snaps++
		}
	}

	if snaps == 0 {
		t.Fatal("no snapshot was written")
	}

	evs, err := e.Client.History(e.Ctx, id)
	if err != nil {
		t.Fatal(err)
	}

	def, err := e.Client.Flow(e.Ctx, "loopy", st.FlowHash)
	if err != nil {
		t.Fatal(err)
	}

	full, err := fold.Fold(def, id, evs)
	if err != nil {
		t.Fatal(err)
	}

	a, _ := json.Marshal(st)
	b, _ := json.Marshal(full)

	if string(a) != string(b) {
		t.Fatalf("snapshot+tail != full fold\n%s\n%s", a, b)
	}
}
