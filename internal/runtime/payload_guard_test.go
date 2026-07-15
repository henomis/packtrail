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
	"github.com/henomis/packtrail/pkg/protocol"
)

// TestTaskNonObjectResultAllowedSync: node outputs are namespaced per node
// (results.<node> in the assembled context) and never merge into a shared
// root, so any JSON shape is a legal task result — the old object-only rule is
// gone with the merged-payload model.
func TestTaskNonObjectResultAllowedSync(t *testing.T) {
	h := newHarness(t, linearFlow, Config{})
	h.serve(t, "tasks.a.*", func(_ context.Context, _ protocol.TaskRequest) (protocol.TaskResponse, error) {
		return protocol.TaskResponse{Status: protocol.StatusOK, Payload: json.RawMessage(`[1,2,3]`)}, nil
	})
	h.serve(t, "tasks.b.*", passthrough)

	id, err := h.engine.Start(context.Background(), "linear", json.RawMessage(`{"v":1}`))
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	h.waitStatus(t, id, store.StatusCompleted, 5*time.Second)

	doc, err := h.engine.Results(context.Background(), id)
	if err != nil {
		t.Fatalf("results: %v", err)
	}

	if got := parseCtx(t, doc); string(got.Results["a"]) != `[1,2,3]` {
		t.Fatalf("results.a = %s, want the array output verbatim", got.Results["a"])
	}
}

// TestTaskNonObjectResultAllowedAsync: same contract on the CompleteActivity
// path (both paths settle through settleTask).
func TestTaskNonObjectResultAllowedAsync(t *testing.T) {
	h := newAsyncHarness(t, asyncLinearFlow)
	ctx := context.Background()

	id, err := h.engine.Start(ctx, "async-linear", json.RawMessage(`{"v":1}`))
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	_ = h.nextReq(t)
	h.waitStatus(t, id, store.StatusWaiting)

	if completeErr := h.engine.CompleteActivity(ctx, id, "a", 0,
		invoker.Result{Status: invoker.StatusOK, Payload: json.RawMessage(`"just a string"`)}); completeErr != nil {
		t.Fatalf("complete: %v", completeErr)
	}

	_ = h.nextReq(t) // b dispatched: the string output did not fail the flow

	if got := h.results(t, id); string(got.Results["a"]) != `"just a string"` {
		t.Fatalf("results.a = %s, want the string output verbatim", got.Results["a"])
	}
}
