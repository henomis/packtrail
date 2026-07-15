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
	"sync/atomic"
	"testing"
	"time"

	"github.com/henomis/packtrail/internal/store"
	"github.com/henomis/packtrail/invoker"
	"github.com/henomis/packtrail/pkg/protocol"
)

// TestStartWithIDIsIdempotent verifies that repeated StartWithID calls with the
// same caller-supplied id produce exactly one execution that runs exactly once —
// including a retry issued after the first run has completed.
func TestStartWithIDIsIdempotent(t *testing.T) {
	h := newHarness(t, linearFlow, Config{})

	var aCalls atomic.Int32

	h.serve(t, "tasks.a.*", func(_ context.Context, req protocol.TaskRequest) (protocol.TaskResponse, error) {
		aCalls.Add(1)
		return protocol.TaskResponse{Status: protocol.StatusOK, Payload: req.Payload}, nil
	})
	h.serve(t, "tasks.b.*", passthrough)

	ctx := context.Background()

	const key = "order-12345"

	id1, err := h.engine.StartWithID(ctx, key, "linear", json.RawMessage(`{"v":1}`))
	if err != nil {
		t.Fatalf("first start: %v", err)
	}

	// A retry (e.g. the caller timed out) with the same key and the same
	// arguments must not duplicate.
	id2, err := h.engine.StartWithID(ctx, key, "linear", json.RawMessage(`{"v":1}`))
	if err != nil {
		t.Fatalf("retry start: %v", err)
	}

	if id1 != key || id2 != key {
		t.Fatalf("ids = %q, %q; want both %q", id1, id2, key)
	}

	h.waitStatus(t, key, store.StatusCompleted, 5*time.Second)

	// First-write wins: the input is the first call's.
	doc, err := h.engine.Results(ctx, key)
	if err != nil {
		t.Fatalf("results: %v", err)
	}

	if got := parseCtx(t, doc); string(got.Input) != `{"v":1}` {
		t.Fatalf("input = %s, want first-call value {\"v\":1}", got.Input)
	}

	// A late retry after completion is still a no-op (does not re-run the flow).
	if id3, lateErr := h.engine.StartWithID(ctx, key, "linear", json.RawMessage(`{"v":1}`)); lateErr != nil || id3 != key {
		t.Fatalf("late retry = %q, %v; want %q, nil", id3, lateErr, key)
	}

	// Give any erroneous re-dispatch a chance to land, then assert single run.
	time.Sleep(200 * time.Millisecond)

	if n := aCalls.Load(); n != 1 {
		t.Fatalf("start task ran %d times, want exactly 1", n)
	}
}

// TestStartWithIDValidatesID rejects ids that are empty or contain characters
// unsafe as a NATS subject token / KV key.
func TestStartWithIDValidatesID(t *testing.T) {
	h := newHarness(t, linearFlow, Config{})
	ctx := context.Background()

	for _, bad := range []string{"", "has space", "dotted.id", "wild*card", "tooupstream>"} {
		if _, err := h.engine.StartWithID(ctx, bad, "linear", nil); err == nil {
			t.Errorf("StartWithID(%q) = nil error, want rejection", bad)
		}
	}

	// A well-formed id is accepted.
	if _, err := h.engine.StartWithID(ctx, "good-id_1", "linear", nil); err != nil {
		t.Fatalf("StartWithID(good-id_1): %v", err)
	}
}

func TestExecIDValidationAcrossEntryPoints(t *testing.T) {
	h := newHarness(t, linearFlow, Config{})
	ctx := context.Background()

	for _, bad := range []string{"", "has space", "dotted.id", "wild*card", "tooupstream>"} {
		if _, err := h.engine.Results(ctx, bad); err == nil {
			t.Errorf("Results(%q) = nil error, want rejection", bad)
		}

		if err := h.engine.CompleteActivity(ctx, bad, "a", 0, invoker.Result{Status: invoker.StatusOK}); err == nil {
			t.Errorf("CompleteActivity(%q) = nil error, want rejection", bad)
		}

		if err := h.engine.Resume(ctx, bad); err == nil {
			t.Errorf("Resume(%q) = nil error, want rejection", bad)
		}

		if err := h.engine.Cancel(ctx, bad, "x"); err == nil {
			t.Errorf("Cancel(%q) = nil error, want rejection", bad)
		}
	}
}

// TestStartRejectsNonObjectPayload verifies the execution payload must be a JSON
// object: a non-object (array/scalar/null) is rejected up front rather than
// silently dropped at the first fanin or signal merge, while an object — and an
// empty/nil payload (defaulted to {}) — is accepted.
func TestStartRejectsNonObjectPayload(t *testing.T) {
	h := newHarness(t, linearFlow, Config{})
	ctx := context.Background()

	for i, bad := range []string{`[1,2,3]`, `"hello"`, `42`, `true`, `null`, `{`, `not-json`} {
		// Both entry points share the same start path; check each rejects (the
		// StartWithID id is well-formed so the rejection is the payload, not the id).
		if _, err := h.engine.Start(ctx, "linear", json.RawMessage(bad)); err == nil {
			t.Errorf("Start(%s) = nil error, want rejection", bad)
		}

		if _, err := h.engine.StartWithID(ctx, fmt.Sprintf("ok-id-%d", i), "linear", json.RawMessage(bad)); err == nil {
			t.Errorf("StartWithID(%s) = nil error, want rejection", bad)
		}
	}

	// An object payload, and an empty/nil payload, are accepted.
	for _, ok := range []json.RawMessage{json.RawMessage(`{"v":1}`), json.RawMessage(`{}`), nil} {
		if _, err := h.engine.Start(ctx, "linear", ok); err != nil {
			t.Errorf("Start(%s) = %v, want accepted", ok, err)
		}
	}
}
