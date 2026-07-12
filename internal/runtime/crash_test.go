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
	"sync/atomic"
	"testing"
	"time"

	"github.com/henomis/packtrail/internal/dsl"
	"github.com/henomis/packtrail/internal/names"
	"github.com/henomis/packtrail/internal/natstest"
	"github.com/henomis/packtrail/internal/scheduler"
	"github.com/henomis/packtrail/internal/store"
	"github.com/henomis/packtrail/invoker/natstask"
	"github.com/henomis/packtrail/pkg/protocol"
)

// TestCrashDuringFanoutResumesFromPersistedBranches is the §12 acceptance test:
// one instance is "killed" mid-fanout (one branch already completed and
// persisted, the others in flight); a second instance acquires the lease, takes
// over, and completes the join without losing or recomputing finished branches.
func TestCrashDuringFanoutResumesFromPersistedBranches(t *testing.T) {
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

	flow, err := dsl.Parse([]byte(fanFlow("all")))
	if err != nil {
		t.Fatalf("flow: %v", err)
	}

	flows := map[string]*dsl.Flow{flow.Name: flow}

	// Short lease/ack so takeover happens quickly in the test.
	cfg := Config{LeaseTTL: time.Second, AckWait: 2 * time.Second}

	engineA, err := New(natstask.New(srv.NC, n.Prefix), st, sch, testSignals(t, st), flows, cfg)
	if err != nil {
		t.Fatalf("engine A: %v", err)
	}

	engineB, err := New(natstask.New(srv.NC, n.Prefix), st, sch, testSignals(t, st), flows, cfg)
	if err != nil {
		t.Fatalf("engine B: %v", err)
	}

	// Task workers (shared, independent of the engines).
	var xCalls atomic.Int32

	release := make(chan struct{})
	h := &harness{nc: srv.NC, prefix: n.Prefix, store: st, engine: engineA}
	h.serve(t, "tasks.start.*", passthrough)
	h.serve(t, "tasks.x.*", func(_ context.Context, req protocol.TaskRequest) (protocol.TaskResponse, error) {
		xCalls.Add(1)
		return protocol.TaskResponse{Status: protocol.StatusOK, Payload: req.Payload}, nil
	})
	// y and z block until released, so they are still in flight when A is killed.
	blocking := func(_ context.Context, req protocol.TaskRequest) (protocol.TaskResponse, error) {
		<-release
		return protocol.TaskResponse{Status: protocol.StatusOK, Payload: req.Payload}, nil
	}
	h.serve(t, "tasks.y.*", blocking)
	h.serve(t, "tasks.z.*", blocking)

	var doneCalled atomic.Bool

	h.serve(t, "tasks.done.*", func(_ context.Context, req protocol.TaskRequest) (protocol.TaskResponse, error) {
		doneCalled.Store(true)
		return protocol.TaskResponse{Status: protocol.StatusOK, Payload: req.Payload}, nil
	})

	// Start only engine A.
	ctxA, killA := context.WithCancel(ctx)
	go func() { _ = engineA.Run(ctxA) }()

	id, err := engineA.Start(ctx, "fan", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	// Wait until branch x has completed and been persisted on A.
	if !waitFor(5*time.Second, func() bool {
		ex, getErr := st.Get(ctx, id)
		return getErr == nil && ex.Branches["x"].Status == store.BranchCompleted
	}) {
		t.Fatal("branch x never completed on A")
	}

	// Kill A mid-fanout (y, z still in flight and not yet persisted).
	killA()

	// Now start engine B, which must take over via the lease and ack-wait
	// redelivery, then release y and z so they can complete under B.
	ctxB, stopB := context.WithCancel(ctx)
	t.Cleanup(stopB)

	go func() { _ = engineB.Run(ctxB) }()

	close(release)

	ex := h.waitStatus(t, id, store.StatusCompleted, 20*time.Second)
	for _, b := range []string{"x", "y", "z"} {
		if ex.Branches[b].Status != store.BranchCompleted {
			t.Errorf("branch %s = %q after takeover, want completed", b, ex.Branches[b].Status)
		}
	}

	if !doneCalled.Load() {
		t.Error("done not reached after takeover")
	}

	if got := xCalls.Load(); got != 1 {
		t.Errorf("branch x ran %d times, want 1 (completed work must not be recomputed)", got)
	}
}

func waitFor(within time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}

		time.Sleep(20 * time.Millisecond)
	}

	return false
}
