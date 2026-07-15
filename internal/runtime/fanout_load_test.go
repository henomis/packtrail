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
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/henomis/packtrail/internal/dsl"
	"github.com/henomis/packtrail/internal/names"
	"github.com/henomis/packtrail/internal/natstest"
	"github.com/henomis/packtrail/internal/scheduler"
	"github.com/henomis/packtrail/internal/store"
	"github.com/henomis/packtrail/invoker"
)

// wideFanFlow builds a flow with n parallel branches that all settle through a
// single fanin (join_policy: all). Each branch is a task routed to the in-process
// "mem" invoker so it settles instantly and with no network hop — the worst case
// for single-execution-key CAS contention, since all n branch goroutines race to
// write the same execution document at once.
func wideFanFlow(n int) string {
	var b strings.Builder

	b.WriteString("name: wide\nnodes:\n")

	branches := make([]string, n)
	for i := range branches {
		branches[i] = fmt.Sprintf("b%d", i)
	}

	b.WriteString("  - {id: fo, type: fanout, branches: [")
	b.WriteString(strings.Join(branches, ","))
	b.WriteString("]}\n")

	for _, id := range branches {
		b.WriteString("  - {id: ")
		b.WriteString(id)
		b.WriteString(", type: task, invoker: mem, target: t}\n")
	}

	b.WriteString("  - {id: join, type: fanin, wait_for: [")
	b.WriteString(strings.Join(branches, ","))
	b.WriteString("], join_policy: all}\n")
	b.WriteString("edges:\n  - {from: fo, to: join}\n")

	return b.String()
}

// memHarness wires an engine whose task/branch nodes are served by a synchronous
// in-process invoker (kind "mem"). Unlike the nats-task harness, branch results
// do not serialize behind a single subscription, so concurrency is maximal.
func memHarness(t *testing.T, flowYAML string, inv invoker.Invoker) (*store.Store, *Engine) {
	t.Helper()

	ctx := context.Background()
	srv := natstest.Start(t)
	n := names.New("")

	st, err := store.Open(ctx, srv.JS, n)
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	sch := scheduler.New(srv.JS, n)
	if err = sch.EnsureStream(ctx); err != nil {
		t.Fatalf("scheduler: %v", err)
	}

	flow, err := dsl.Parse([]byte(flowYAML))
	if err != nil {
		t.Fatalf("flow: %v", err)
	}

	eng, err := New(inv, st, sch, testSignals(t, st), map[string]*dsl.Flow{flow.Name: flow}, Config{})
	if err != nil {
		t.Fatalf("engine: %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)

	go func() { _ = eng.Run(runCtx) }()

	return st, eng
}

// TestWideFanoutContention load-tests wide fanout at increasing widths under the
// race detector, measuring CAS-conflict retry pressure on the single execution
// key. It asserts every branch completes (no Mutate exhaustion) and reports the
// observed conflict count and wall-clock per width.
func TestWideFanoutContention(t *testing.T) {
	if testing.Short() {
		t.Skip("wide-fanout load test skipped in -short")
	}

	reg := invoker.NewRegistry()
	if err := reg.Register("mem", invoker.Func(func(_ context.Context, _ invoker.Request) (invoker.Result, error) {
		return invoker.Result{Status: invoker.StatusOK, Payload: json.RawMessage(`{"ok":true}`)}, nil
	})); err != nil {
		t.Fatalf("register invoker: %v", err)
	}

	for _, width := range []int{50, 100, 200} {
		t.Run(fmt.Sprintf("width=%d", width), func(t *testing.T) {
			st, eng := memHarness(t, wideFanFlow(width), reg)

			before := st.CASConflicts()
			start := time.Now()

			id, err := eng.Start(context.Background(), "wide", nil)
			if err != nil {
				t.Fatalf("start: %v", err)
			}

			ex := pollStatus(t, st, id, store.StatusCompleted, 30*time.Second)
			elapsed := time.Since(start)
			conflicts := st.CASConflicts() - before

			completed := 0

			for _, bs := range ex.Branches {
				if bs.Status == store.BranchCompleted {
					completed++
				}
			}

			if completed != width {
				t.Fatalf("only %d/%d branches completed (Mutate may have exhausted retries)", completed, width)
			}

			t.Logf("width=%d: completed in %s, %d CAS-conflict retries (%.1f per branch)",
				width, elapsed.Round(time.Millisecond), conflicts, float64(conflicts)/float64(width))
		})
	}
}

// waitFor polls until the execution reaches status or the deadline elapses.
func pollStatus(t *testing.T, st *store.Store, id, status string, within time.Duration) *store.Execution {
	t.Helper()

	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		ex, err := st.Get(context.Background(), id)
		if err == nil && ex.Status == status {
			return ex
		}

		time.Sleep(10 * time.Millisecond)
	}

	ex, _ := st.Get(context.Background(), id)
	if ex != nil {
		t.Fatalf("exec %s: status=%q err=%q, want %q", id, ex.Status, ex.Error, status)
	}

	t.Fatalf("exec %s never reached %q", id, status)

	return nil
}
