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
	"sync"
	"testing"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/event"
	"github.com/henomis/packtrail/worker"
)

const g501Saga = `
name: order
nodes:
  - {id: reserve, type: task, kind: shop, next: pay}
  - {id: pay, type: task, kind: shop, on_failure: release, next: ship}
  - {id: ship, type: task, kind: shop}
  - {id: release, type: task, kind: shop}
`

// TestG501SagaCompensates: the last step fails permanently; the execution
// routes to the compensation step, whose worker sees the error and what the
// earlier steps produced, and completes instead of failing (G5-01).
func TestG501SagaCompensates(t *testing.T) {
	e := NewEnv(t, []string{g501Saga})

	var (
		mu  sync.Mutex
		ctx worker.Context
	)

	e.Worker("shop", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		switch j.Node {
		case "reserve":
			return &worker.Result{Output: map[string]any{"hold": "h-1"}}, nil
		case "pay":
			return nil, worker.Permanent(errors.New("card declined"))
		case "release":
			mu.Lock()
			ctx = j.Context
			mu.Unlock()

			return &worker.Result{Output: map[string]any{"released": true}}, nil
		default:
			return nil, worker.Permanent(errors.New("must not run: " + j.Node))
		}
	})

	id := e.Start("order", nil)
	st := e.Completed(id)

	if string(st.Results["release"]) != `{"released":true}` || st.Results["ship"] != nil {
		t.Fatalf("results %v", st.Results)
	}

	mu.Lock()
	defer mu.Unlock()

	if ctx.LastNode != "pay" || ctx.Errors["pay"].Error != "card declined" ||
		string(ctx.Results["reserve"]) != `{"hold":"h-1"}` {
		t.Fatalf("handler context: last_node %q errors %+v results %v", ctx.LastNode, ctx.Errors, ctx.Results)
	}

	if c := countEvents(t, e, id, event.NodeFailed, "pay"); c != 1 {
		t.Fatalf("pay failed %d times, want 1 (a permanent error is not retried)", c)
	}
}

// TestG501WithoutOnFailureStillFails: without on_failure nothing changes.
func TestG501WithoutOnFailureStillFails(t *testing.T) {
	e := NewEnv(t, []string{m1Linear})
	e.Worker("echo", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		if j.Node == "b" {
			return nil, worker.Permanent(errors.New("no"))
		}

		return &worker.Result{Output: map[string]any{}}, nil
	})

	st := e.WaitStatus(e.Start("linear", nil), packtrail.StatusFailed)
	if st.FailedNode != "b" {
		t.Fatalf("failed node %q", st.FailedNode)
	}
}
