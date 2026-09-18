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
	"sync/atomic"
	"testing"
	"time"

	"github.com/henomis/packtrail/internal/store"
	"github.com/henomis/packtrail/pkg/protocol"
)

// TestReleasedBySurvivesResume: resuming the node a signal released re-runs the
// same visit, so it must see the same released_by as the first run.
func TestReleasedBySurvivesResume(t *testing.T) {
	h := newHarness(t, signalFlow("24h"), Config{})
	h.serve(t, "tasks.start.*", passthrough)
	h.serve(t, "tasks.fallback.*", passthrough)

	seen := make(chan string, 2)

	var calls atomic.Int32

	h.serve(t, "tasks.after.*", func(_ context.Context, req protocol.TaskRequest) (protocol.TaskResponse, error) {
		seen <- parseCtx(t, req.Payload).ReleasedBy

		if calls.Add(1) == 1 {
			return protocol.TaskResponse{Status: protocol.StatusError, Error: "boom"}, nil
		}

		return protocol.TaskResponse{Status: protocol.StatusOK, Payload: json.RawMessage(`{}`)}, nil
	})

	ctx := context.Background()

	id, err := h.engine.Start(ctx, "sig", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	h.waitStatus(t, id, store.StatusWaiting, 5*time.Second)

	if err = h.engine.Signal(ctx, id, "approval", json.RawMessage(`{}`)); err != nil {
		t.Fatalf("signal: %v", err)
	}

	if got := <-seen; got != "approval" {
		t.Fatalf("first run released_by = %q, want approval", got)
	}

	h.waitStatus(t, id, store.StatusFailed, 5*time.Second)

	if err = h.engine.Resume(ctx, id); err != nil {
		t.Fatalf("resume: %v", err)
	}

	select {
	case got := <-seen:
		if got != "approval" {
			t.Fatalf("resumed run released_by = %q, want approval (same node visit)", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("resumed node was never invoked")
	}
}
