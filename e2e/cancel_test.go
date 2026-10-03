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
	"testing"
	"time"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/event"
	"github.com/henomis/packtrail/worker"
)

// batchFlow runs its work in a child, which runs a map of slow items: a
// cancel of the parent must reach the child and the child's running jobs.
const batchFlow = `
name: batch
nodes:
  - {id: head, type: task, kind: quick, next: work}
  - {id: work, type: subflow, flow: sleeper, input: input}
`

const sleeperFlow = `
name: sleeper
nodes:
  - {id: bulk, type: map, kind: slow, over: input.items, max_parallel: 4, next: nap}
  - {id: nap, type: await, signal: wake, timeout: 1h}
`

// TestCancelCascades: cancelling a parent whose child has a map in flight
// cancels the child, stops every running job with ErrCancelled, dispatches
// nothing more, and WatchTerminal reports both ends.
func TestCancelCascades(t *testing.T) {
	cl := newCluster(t, []string{batchFlow, sleeperFlow})

	started := make(chan string, 16)
	stopped := make(chan error, 16)

	cl.worker("quick", func(_ context.Context, _ *worker.Job) (*worker.Result, error) {
		return &worker.Result{}, nil
	})

	cl.worker("slow", func(ctx context.Context, j *worker.Job) (*worker.Result, error) {
		started <- j.ExecID + "/" + j.Key

		<-ctx.Done()

		stopped <- context.Cause(ctx)

		return nil, ctx.Err()
	}, worker.WithConcurrency(8))

	ends, err := cl.c.WatchTerminal(cl.ctx, 0)
	if err != nil {
		t.Fatal(err)
	}

	id := cl.start("batch", map[string]any{"items": []int{1, 2, 3, 4, 5, 6}})

	// Four map items (max_parallel) of the child run at once.
	const running = 4
	for range running {
		select {
		case <-started:
		case <-time.After(20 * time.Second):
			t.Fatal("jobs did not start")
		}
	}

	parent := cl.waitStatus(id, packtrail.StatusRunning)

	var child string
	for _, c := range parent.Children {
		child = c.ChildID
	}

	if child == "" {
		t.Fatalf("no child running: %+v", parent.Children)
	}

	if err = cl.c.Cancel(cl.ctx, id, "operator stop"); err != nil {
		t.Fatal(err)
	}

	for range running {
		select {
		case cause := <-stopped:
			if !errors.Is(cause, worker.ErrCancelled) {
				t.Fatalf("job stopped by %v, want ErrCancelled", cause)
			}
		case <-time.After(20 * time.Second):
			t.Fatal("running jobs were not stopped")
		}
	}

	for _, x := range []string{id, child} {
		st := cl.wait(x)
		cl.check(x)

		if st.Status != packtrail.StatusCancelled {
			t.Fatalf("%s: %s", x, st.Status)
		}
	}

	seen := map[string]packtrail.Status{}
	for len(seen) < 2 {
		select {
		case e := <-ends:
			seen[e.ExecID] = e.Status
		case <-time.After(20 * time.Second):
			t.Fatalf("WatchTerminal reported %v", seen)
		}
	}

	if seen[id] != packtrail.StatusCancelled || seen[child] != packtrail.StatusCancelled {
		t.Fatalf("WatchTerminal reported %v", seen)
	}

	// Nothing else was dispatched after the cancel.
	select {
	case s := <-started:
		t.Fatalf("job %s started after the cancel", s)
	case <-time.After(time.Second):
	}

	if f := cl.check(child); f.count[event.MapCompleted] != 0 || f.count[event.NodeCompleted] != 0 {
		t.Fatalf("the child's map settled after the cancel: %v", f.count)
	}
}
