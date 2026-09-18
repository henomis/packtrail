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
	"errors"
	"testing"
	"time"

	"github.com/henomis/packtrail/internal/store"
)

// Cancel answers three different questions with "the hot bucket has no such
// key", and they must not collapse into one another:
//
//	already terminal        → nil   (idempotent, the documented guarantee)
//	terminal and archived   → nil   (same thing, just swept out of the hot bucket)
//	no such execution       → error (the caller is addressing nothing)
//
// The middle case is why cancelAbsent exists: Mutate reads the hot bucket only,
// so an archived execution reports ErrNotFound from exactly the same code path
// as an id that was never real. Without the archive lookup, making the third
// case an error would have made the second one an error too — turning a
// documented no-op into a failure for any deployment with archival enabled.

// TestCancelArchivedExecutionIsNoOp: a terminal execution swept into the cold
// archive is still an execution, and cancelling it is the same no-op it was
// before the sweep.
func TestCancelArchivedExecutionIsNoOp(t *testing.T) {
	h := newHarness(t, linearFlow, Config{})
	ctx := context.Background()

	if err := h.store.EnableArchive(ctx, time.Hour); err != nil {
		t.Fatalf("enable archive: %v", err)
	}

	done := &store.Execution{ID: "arch-cancel", FlowName: "linear", Status: store.StatusCompleted}
	if _, err := h.store.Create(ctx, done); err != nil {
		t.Fatalf("create: %v", err)
	}

	moved, err := h.store.ArchiveTerminal(ctx)
	if err != nil || moved != 1 {
		t.Fatalf("archive: moved=%d err=%v, want 1/nil", moved, err)
	}

	if err = h.engine.Cancel(ctx, "arch-cancel", "too late"); err != nil {
		t.Fatalf("cancel archived: %v, want nil (terminal no-op)", err)
	}

	// And the archived record is untouched: a cancel must not rewrite the
	// outcome of an execution that already completed.
	ex, ok, err := h.store.ArchivedExecution(ctx, "arch-cancel")
	if err != nil || !ok {
		t.Fatalf("archived lookup: ok=%v err=%v", ok, err)
	}

	if ex.Status != store.StatusCompleted || ex.Error != "" {
		t.Fatalf("archived execution mutated: status=%q error=%q", ex.Status, ex.Error)
	}
}

// TestCancelUnknownWithArchiveEnabled: with archival on, an id that exists in
// neither bucket must still be an error — the archive lookup is a way to
// recognise a real execution, not a way to excuse every unknown id.
func TestCancelUnknownWithArchiveEnabled(t *testing.T) {
	h := newHarness(t, linearFlow, Config{})
	ctx := context.Background()

	if err := h.store.EnableArchive(ctx, time.Hour); err != nil {
		t.Fatalf("enable archive: %v", err)
	}

	if err := h.engine.Cancel(ctx, "never-existed", "x"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("cancel unknown: %v, want store.ErrNotFound", err)
	}
}

// TestCancelTerminalExecutionStillNoOp: the idempotence guarantee itself, kept
// under test next to the change that narrowed it — a completed execution still
// in the hot bucket is a no-op, not a not-found.
func TestCancelTerminalExecutionStillNoOp(t *testing.T) {
	h := newHarness(t, linearFlow, Config{})
	ctx := context.Background()

	done := &store.Execution{ID: "hot-terminal", FlowName: "linear", Status: store.StatusCompleted}
	if _, err := h.store.Create(ctx, done); err != nil {
		t.Fatalf("create: %v", err)
	}

	if err := h.engine.Cancel(ctx, "hot-terminal", "too late"); err != nil {
		t.Fatalf("cancel terminal: %v, want nil (idempotent no-op)", err)
	}
}
