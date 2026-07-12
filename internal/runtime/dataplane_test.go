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

	"github.com/henomis/packtrail/internal/store"
	"github.com/henomis/packtrail/invoker"
)

// TestOutputOverwrittenOnRerun covers the data-before-control crash ordering: a
// task's output landed in the data plane but the process died before the
// control-plane advance committed. The re-run (here via the stall watchdog)
// invokes the node again and its put overwrites the orphaned entry
// idempotently — the flow completes with the re-run's output and no duplicate
// Outputs entry.
func TestOutputOverwrittenOnRerun(t *testing.T) {
	ctx := context.Background()

	inv := invoker.Func(func(context.Context, invoker.Request) (invoker.Result, error) {
		return invoker.Result{Status: invoker.StatusOK, Payload: json.RawMessage(`{"run":"second"}`)}, nil
	})

	st, eng := watchdogHarness(t, inv)

	// The crashed first run's orphaned output: written to the data plane, never
	// committed to the control plane (no Outputs entry, no advance).
	if err := st.PutPayload(ctx, store.OutputKey("rerun-1", "work"), json.RawMessage(`{"run":"first"}`)); err != nil {
		t.Fatalf("put orphan output: %v", err)
	}

	exec := &store.Execution{
		ID: "rerun-1", FlowName: "sig-redrive",
		Status: store.StatusRunning, CurrentNode: "work",
	}
	if _, err := st.Create(ctx, exec); err != nil {
		t.Fatalf("create: %v", err)
	}

	time.Sleep(20 * time.Millisecond)

	redriven, err := eng.RedriveStalled(ctx, "rerun-1", staleEnough)
	if err != nil || !redriven {
		t.Fatalf("redrive: redriven=%v err=%v, want true/nil", redriven, err)
	}

	waitStatus(t, st, "rerun-1", store.StatusCompleted)

	doc, err := eng.Results(ctx, "rerun-1")
	if err != nil {
		t.Fatalf("results: %v", err)
	}

	if got := parseCtx(t, doc); string(got.Results["work"]) != `{"run":"second"}` {
		t.Fatalf("results.work = %s, want the re-run's output", got.Results["work"])
	}

	if ex, _ := st.Get(ctx, "rerun-1"); len(ex.Outputs) != 1 {
		t.Fatalf("Outputs = %v, want exactly [work]", ex.Outputs)
	}
}
