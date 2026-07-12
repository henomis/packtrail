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
	"testing"

	"github.com/henomis/packtrail/internal/dsl"
	"github.com/henomis/packtrail/internal/names"
	"github.com/henomis/packtrail/internal/natstest"
	"github.com/henomis/packtrail/internal/scheduler"
	"github.com/henomis/packtrail/internal/store"
	"github.com/henomis/packtrail/invoker"
)

// TestAdvanceClearsActivityStash: an unconsumed early-completion stash belongs
// to the node being left. Attempts reset to 0 on advance and task cycles are
// legal, so a surviving stash for (node, 0) would satisfy a later revisit of
// that node with the OLD result instead of invoking it. advanceTo must drop it.
func TestAdvanceClearsActivityStash(t *testing.T) {
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

	flow, err := dsl.Parse([]byte(signalRedriveFlow)) // gate -> work
	if err != nil {
		t.Fatalf("flow: %v", err)
	}

	inv := invoker.Func(func(context.Context, invoker.Request) (invoker.Result, error) {
		return invoker.Result{Status: invoker.StatusOK}, nil
	})

	eng, err := New(inv, st, sch, testSignals(t, st), map[string]*dsl.Flow{flow.Name: flow}, Config{})
	if err != nil {
		t.Fatalf("engine: %v", err)
	}

	exec := &store.Execution{
		ID: "stash-1", FlowName: "sig-redrive",
		Status: store.StatusRunning, CurrentNode: "gate",
		// payload moved to the data plane
		Activity: &store.ActivityResult{Node: "work", Attempt: 0, Status: string(invoker.StatusOK)},
	}
	if _, err = st.Create(ctx, exec); err != nil {
		t.Fatalf("create: %v", err)
	}

	if err = eng.advanceTo(ctx, "stash-1", "gate", "work", nil); err != nil {
		t.Fatalf("advanceTo: %v", err)
	}

	ex, err := st.Get(ctx, "stash-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if ex.Activity != nil {
		t.Fatalf("Activity stash survived the advance: %+v (would satisfy node %q without invoking)",
			ex.Activity, ex.Activity.Node)
	}
}

// TestConsumeSignalClearsActivityStash: same contract on the signal-consumption
// transition (consumeSignal is a pure function over the document).
func TestConsumeSignalClearsActivityStash(t *testing.T) {
	ex := &store.Execution{
		ID: "stash-2", Status: store.StatusWaiting, CurrentNode: "gate", WaitSignal: "go",
		Signals:  map[string]bool{"go": true},
		Activity: &store.ActivityResult{Node: "work", Attempt: 0, Status: string(invoker.StatusOK)},
	}

	consumeSignal(ex, "go", "work")

	if ex.Activity != nil {
		t.Fatalf("Activity stash survived signal consumption: %+v", ex.Activity)
	}

	if ex.Status != store.StatusRunning || ex.CurrentNode != "work" {
		t.Fatalf("unexpected post-consume state: status=%q node=%q", ex.Status, ex.CurrentNode)
	}
}
