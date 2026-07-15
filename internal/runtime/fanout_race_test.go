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
	"testing"
	"time"

	"github.com/henomis/packtrail/internal/dsl"
	"github.com/henomis/packtrail/internal/names"
	"github.com/henomis/packtrail/internal/natstest"
	"github.com/henomis/packtrail/internal/scheduler"
	"github.com/henomis/packtrail/internal/store"
	"github.com/henomis/packtrail/invoker"
)

const fanRaceFlow = `
version: "1.0"
name: fan-race
nodes:
  - {id: fo, type: fanout, branches: [fast, slow]}
  - {id: fast, type: task, subject: "x"}
  - {id: slow, type: task, subject: "y"}
  - {id: join, type: fanin, wait_for: [fast, slow], join_policy: all}
edges:
  - {from: fo, to: join}
`

// fanRaceInvoker reproduces the completion-beats-park race deterministically:
// the "fast" branch settles its own async completion *before* even returning
// StatusPending — so the branch is already completed (and its fanin_eval already
// dropped as stale) while the "slow" synchronous branch is still running and the
// fanout has not yet parked the execution at the fanin.
type fanRaceInvoker struct {
	eng *Engine // set after New (the engine needs the invoker first)
}

func (f *fanRaceInvoker) Invoke(ctx context.Context, req invoker.Request) (invoker.Result, error) {
	switch req.NodeID {
	case "fast":
		if err := f.eng.CompleteActivity(ctx, req.ExecutionID, req.NodeID, req.Attempt,
			invoker.Result{Status: invoker.StatusOK, Payload: json.RawMessage(`{"fast":true}`)}); err != nil {
			return invoker.Result{Status: invoker.StatusError, Error: err.Error()}, nil
		}

		return invoker.Result{Status: invoker.StatusPending}, nil
	case "slow":
		time.Sleep(300 * time.Millisecond) // hold the fanout's wg.Wait open past the completion

		return invoker.Result{Status: invoker.StatusOK, Payload: json.RawMessage(`{"slow":true}`)}, nil
	default:
		return invoker.Result{Status: invoker.StatusError, Error: "unexpected node " + req.NodeID}, nil
	}
}

// TestFanoutCompletionBeatsPark verifies an execution is not stranded when the
// last outstanding async branch completes before the fanout parks at the fanin:
// the post-park fanin_eval must pick up the already-settled branches and finish
// the flow. Without the post-park eval this execution waits forever.
func TestFanoutCompletionBeatsPark(t *testing.T) {
	ctx := context.Background()
	srv := natstest.Start(t)

	st, err := store.Open(ctx, srv.JS, names.New(""))
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	sch := scheduler.New(srv.JS, names.New(""))
	if err = sch.EnsureStream(ctx); err != nil {
		t.Fatalf("scheduler: %v", err)
	}

	flow, err := dsl.Parse([]byte(fanRaceFlow))
	if err != nil {
		t.Fatalf("flow: %v", err)
	}

	inv := &fanRaceInvoker{}

	eng, err := New(inv, st, sch, testSignals(t, st), map[string]*dsl.Flow{flow.Name: flow}, Config{})
	if err != nil {
		t.Fatalf("engine: %v", err)
	}

	inv.eng = eng

	runCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)

	go func() { _ = eng.Run(runCtx) }()

	id, err := eng.Start(ctx, "fan-race", json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	// The join has no successor, so a satisfied join completes the execution.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ex, getErr := st.Get(ctx, id)
		if getErr == nil && ex.Status == store.StatusCompleted {
			for _, b := range []string{"fast", "slow"} {
				if ex.Branches[b].Status != store.BranchCompleted {
					t.Fatalf("branch %s = %q, want completed", b, ex.Branches[b].Status)
				}
			}

			return
		}

		time.Sleep(15 * time.Millisecond)
	}

	ex, _ := st.Get(ctx, id)
	t.Fatalf("execution stranded: status=%q node=%q branches=%+v (completion-beats-park race)",
		ex.Status, ex.CurrentNode, ex.Branches)
}
