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

package runtime

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/henomis/packtrail/internal/dsl"
	"github.com/henomis/packtrail/internal/names"
	"github.com/henomis/packtrail/internal/natstest"
	"github.com/henomis/packtrail/internal/scheduler"
	"github.com/henomis/packtrail/internal/store"
	"github.com/henomis/packtrail/invoker"
)

const singleTaskFlow = `
name: single
nodes:
  - {id: a, type: task, subject: "tasks.a.{execution_id}"}
`

// A graceful shutdown (ctx cancelled) must let work already in flight finish and
// settle within DrainTimeout, rather than aborting the invocation mid-call (which
// would redeliver and double-fire non-idempotent targets on a clean restart).
func TestGracefulDrainCompletesInflight(t *testing.T) {
	ctx := context.Background()
	srv := natstest.Start(t)
	n := names.New("")

	st, err := store.Open(ctx, srv.JS, n)
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	sch := scheduler.New(srv.JS, n)
	if err := sch.EnsureStream(ctx); err != nil {
		t.Fatalf("scheduler: %v", err)
	}

	flow, err := dsl.Parse([]byte(singleTaskFlow))
	if err != nil {
		t.Fatalf("flow: %v", err)
	}

	var startedOnce sync.Once

	started := make(chan struct{})
	release := make(chan struct{})

	inv := invoker.Func(func(c context.Context, _ invoker.Request) (invoker.Result, error) {
		startedOnce.Do(func() { close(started) })
		// Block until released or the (detached) invocation context is cancelled.
		// A graceful drain must NOT cancel this context, so the release path wins.
		select {
		case <-release:
			return invoker.Result{Status: invoker.StatusOK}, nil
		case <-c.Done():
			return invoker.Result{}, c.Err()
		}
	})

	eng, err := New(inv, st, sch, testSignals(t, st), map[string]*dsl.Flow{flow.Name: flow}, Config{DrainTimeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("engine: %v", err)
	}

	runCtx, cancelRun := context.WithCancel(ctx)

	done := make(chan struct{})

	go func() { _ = eng.Run(runCtx); close(done) }()

	id, err := eng.Start(ctx, flow.Name, nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("invocation never started")
	}

	// Begin graceful shutdown while the invocation is in flight.
	cancelRun()

	// Run must NOT have returned yet — it is draining the in-flight item.
	select {
	case <-done:
		t.Fatal("Run returned before in-flight work drained")
	case <-time.After(200 * time.Millisecond):
	}

	// Release the invocation; the drain should now let it settle and Run return.
	close(release)

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after drain")
	}

	ex, err := st.Get(ctx, id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if ex.Status != store.StatusCompleted {
		t.Fatalf("exec status = %q (err %q), want completed — in-flight work was not drained", ex.Status, ex.Error)
	}
}

// When in-flight work exceeds DrainTimeout, the drain must give up bounded: it
// cancels the stragglers and Run returns instead of blocking forever.
func TestGracefulDrainBoundedByTimeout(t *testing.T) {
	ctx := context.Background()
	srv := natstest.Start(t)
	n := names.New("")

	st, err := store.Open(ctx, srv.JS, n)
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	sch := scheduler.New(srv.JS, n)
	if err := sch.EnsureStream(ctx); err != nil {
		t.Fatalf("scheduler: %v", err)
	}

	flow, err := dsl.Parse([]byte(singleTaskFlow))
	if err != nil {
		t.Fatalf("flow: %v", err)
	}

	var startedOnce sync.Once

	started := make(chan struct{})

	inv := invoker.Func(func(c context.Context, _ invoker.Request) (invoker.Result, error) {
		startedOnce.Do(func() { close(started) })
		<-c.Done() // never finishes on its own; only the drain abort unblocks it

		return invoker.Result{}, c.Err()
	})

	eng, err := New(inv, st, sch, testSignals(t, st), map[string]*dsl.Flow{flow.Name: flow}, Config{DrainTimeout: 300 * time.Millisecond})
	if err != nil {
		t.Fatalf("engine: %v", err)
	}

	runCtx, cancelRun := context.WithCancel(ctx)

	done := make(chan struct{})

	go func() { _ = eng.Run(runCtx); close(done) }()

	if _, err = eng.Start(ctx, flow.Name, nil); err != nil {
		t.Fatalf("start: %v", err)
	}

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("invocation never started")
	}

	cancelRun()

	// Run must return within roughly the drain timeout, not hang on the straggler.
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return; drain was not bounded by DrainTimeout")
	}
}
