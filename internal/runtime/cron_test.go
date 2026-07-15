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
	"strings"
	"testing"
	"time"

	"github.com/henomis/packtrail/pkg/protocol"
)

// TestScheduleFlowCron verifies a recurring cron schedule auto-starts executions.
func TestScheduleFlowCron(t *testing.T) {
	h := newHarness(t, linearFlow, Config{})
	started := make(chan string, 4)

	h.serve(t, "tasks.a.*", func(_ context.Context, req protocol.TaskRequest) (protocol.TaskResponse, error) {
		started <- req.ExecutionID
		return protocol.TaskResponse{Status: protocol.StatusOK, Payload: req.Payload}, nil
	})
	h.serve(t, "tasks.b.*", passthrough)

	// Every second (6-field cron: sec min hour dom mon dow).
	if err := h.engine.ScheduleFlow(context.Background(), "tick", "linear", "* * * * * *", json.RawMessage(`{"cron":true}`)); err != nil {
		t.Fatalf("schedule: %v", err)
	}

	select {
	case <-started:
		// An execution was auto-started by the cron schedule.
	case <-time.After(5 * time.Second):
		t.Fatal("cron did not start an execution within 5s")
	}
}

// TestCronFiredIdempotent verifies a redelivered cron tick (same fired id) starts
// exactly one execution, while a distinct firing (different fired id) starts its
// own — so a fired-schedule redelivery cannot duplicate the scheduled work.
func TestCronFiredIdempotent(t *testing.T) {
	h := newHarness(t, linearFlow, Config{})
	h.serve(t, "tasks.a.*", passthrough)
	h.serve(t, "tasks.b.*", passthrough)

	ctx := context.Background()
	handle := h.engine.handleFired(ctx)

	// Two deliveries of the SAME firing (same fired id) must collapse to one
	// execution; a distinct firing (different fired id) starts a second.
	for _, firedID := range []string{"42", "42", "43"} {
		if err := handle("start.linear", json.RawMessage(`{}`), firedID); err != nil {
			t.Fatalf("handle fired %q: %v", firedID, err)
		}
	}

	keys, err := h.store.ListExecutionKeys(ctx)
	if err != nil {
		t.Fatalf("list executions: %v", err)
	}

	got := 0

	for _, k := range keys {
		if strings.HasPrefix(k, "cron-") {
			got++
		}
	}

	if got != 2 {
		t.Fatalf("cron-started executions = %d, want 2 (one per distinct firing)", got)
	}
}
