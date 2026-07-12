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

// TestOutboxSchedFlushDrivesRetry covers the sched-kind outbox flush: a task
// retry commits its attempt bump together with the scheduled next delivery
// (transactional outbox), so a crash before the flush leaves the timer durably
// on the document. The re-flush routes it through the Message Scheduler at the
// committed absolute fire time and the retry runs.
//
// The test manufactures the post-crash state — attempt bumped, the scheduled
// advance still in the outbox, nothing published — and asserts a re-flush (via
// the stall watchdog) completes the flow.
func TestOutboxSchedFlushDrivesRetry(t *testing.T) {
	ctx := context.Background()

	inv := invoker.Func(func(context.Context, invoker.Request) (invoker.Result, error) {
		return invoker.Result{Status: invoker.StatusOK, Payload: json.RawMessage(`{"done":true}`)}, nil
	})

	st, eng := signalHarness(t, inv)

	runCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)

	go func() { _ = eng.Run(runCtx) }()

	item, err := json.Marshal(workItem{ExecID: "sched-crash-1", Kind: kindAdvance})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	// RetryAt in the past: the backoff elapsed while nothing was published.
	retryAt := time.Now().Add(-time.Second).UTC()

	exec := &store.Execution{
		ID: "sched-crash-1", FlowName: "sig-redrive",
		Status: store.StatusRunning, CurrentNode: "work", Attempt: 1,
		RetryAt: retryAt,
	}
	exec.AppendSched(item, retryAt)

	if _, err = st.Create(ctx, exec); err != nil {
		t.Fatalf("create: %v", err)
	}

	time.Sleep(20 * time.Millisecond) // age past the tiny stall threshold

	redriven, err := eng.RedriveStalled(ctx, "sched-crash-1", time.Millisecond)
	if err != nil || !redriven {
		t.Fatalf("redrive: redriven=%v err=%v, want true/nil", redriven, err)
	}

	// The scheduler fires past-due schedules at its next tick (~1s), the fired
	// consumer re-injects the advance, and the retry completes the flow.
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if ex, getErr := st.Get(ctx, "sched-crash-1"); getErr == nil && ex.Status == store.StatusCompleted {
			return
		}

		time.Sleep(25 * time.Millisecond)
	}

	ex, _ := st.Get(ctx, "sched-crash-1")
	t.Fatalf("retry never ran after sched-outbox re-flush: status=%q node=%q attempt=%d outbox=%d",
		ex.Status, ex.CurrentNode, ex.Attempt, len(ex.Outbox))
}
