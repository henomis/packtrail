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

	"github.com/henomis/packtrail/internal/store"
)

// A work item whose current node no longer exists in the flow (e.g. after a
// graph edit) must settle the execution to failed within a bounded number of
// deliveries instead of Nak-looping forever and burning a concurrency slot.
func TestDeadLetterUnknownNode(t *testing.T) {
	h := newHarness(t, linearFlow, Config{})

	ctx := context.Background()

	ex := &store.Execution{
		ID:          "exec-ghost-node",
		FlowName:    "linear",
		CurrentNode: "does-not-exist",
		Status:      store.StatusRunning,
	}
	if _, err := h.store.Create(ctx, ex); err != nil {
		t.Fatalf("create: %v", err)
	}

	if err := h.engine.enqueue(ctx, ex.ID, workItem{Kind: kindAdvance}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	got := h.waitStatus(t, ex.ID, store.StatusFailed, 3*time.Second)
	if !strings.Contains(got.Error, "does-not-exist") {
		t.Fatalf("failure reason = %q, want it to mention the unknown node", got.Error)
	}
}

// A work item for an execution whose flow is no longer registered must also
// dead-letter to failed rather than loop.
func TestDeadLetterUnknownFlow(t *testing.T) {
	h := newHarness(t, linearFlow, Config{})

	ctx := context.Background()

	ex := &store.Execution{
		ID:          "exec-ghost-flow",
		FlowName:    "removed-flow",
		CurrentNode: "a",
		Status:      store.StatusRunning,
	}
	if _, err := h.store.Create(ctx, ex); err != nil {
		t.Fatalf("create: %v", err)
	}

	if err := h.engine.enqueue(ctx, ex.ID, workItem{Kind: kindAdvance}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	got := h.waitStatus(t, ex.ID, store.StatusFailed, 3*time.Second)
	if !strings.Contains(got.Error, "removed-flow") {
		t.Fatalf("failure reason = %q, want it to mention the unknown flow", got.Error)
	}
}

// Dead-lettering a poisoned work item must also leave a durable, queryable trace
// in the dead-letter stream — not only fail the execution and log a line — so an
// operator can see what was dropped and why.
func TestDeadLetterEmitsDurableRecord(t *testing.T) {
	h := newHarness(t, linearFlow, Config{})

	ctx := context.Background()

	ex := &store.Execution{
		ID:          "exec-dlq-trace",
		FlowName:    "linear",
		CurrentNode: "vanished-node",
		Status:      store.StatusRunning,
	}
	if _, err := h.store.Create(ctx, ex); err != nil {
		t.Fatalf("create: %v", err)
	}

	if err := h.engine.enqueue(ctx, ex.ID, workItem{Kind: kindAdvance}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	h.waitStatus(t, ex.ID, store.StatusFailed, 3*time.Second)

	// The dead-letter record lands just before Term; poll briefly for it.
	deadline := time.Now().Add(2 * time.Second)

	for {
		recent, err := h.store.RecentDeadLetters(ctx, 50)
		if err != nil {
			t.Fatalf("recent dead-letters: %v", err)
		}

		for _, dl := range recent {
			if dl.Key == ex.ID && dl.Kind == store.DeadLetterWork {
				if !strings.Contains(dl.Reason, "vanished-node") {
					t.Fatalf("dead-letter reason = %q, want it to mention the unknown node", dl.Reason)
				}

				return // found it
			}
		}

		if time.Now().After(deadline) {
			t.Fatalf("no work dead-letter record for %s within deadline", ex.ID)
		}

		time.Sleep(20 * time.Millisecond)
	}
}

// An unknown work kind is non-retryable too: it dead-letters the execution.
func TestDeadLetterUnknownKind(t *testing.T) {
	h := newHarness(t, linearFlow, Config{})

	ctx := context.Background()

	ex := &store.Execution{
		ID:          "exec-bad-kind",
		FlowName:    "linear",
		CurrentNode: "a",
		Status:      store.StatusRunning,
	}
	if _, err := h.store.Create(ctx, ex); err != nil {
		t.Fatalf("create: %v", err)
	}

	if err := h.engine.enqueue(ctx, ex.ID, workItem{Kind: "bogus-kind"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	got := h.waitStatus(t, ex.ID, store.StatusFailed, 3*time.Second)
	if !strings.Contains(got.Error, "bogus-kind") {
		t.Fatalf("failure reason = %q, want it to mention the unknown kind", got.Error)
	}
}

// TestFlushOutboxDropsUnknownKind: an outbox item of a kind this binary does not
// recognize (a newer writer in a mixed-version fleet) is cleared from the outbox
// so it cannot poison it forever, and a durable dead-letter trace is recorded so
// the drop is observable rather than only logged (F-034).
func TestFlushOutboxDropsUnknownKind(t *testing.T) {
	h := newHarness(t, linearFlow, Config{})
	ctx := context.Background()

	exec := &store.Execution{
		ID:          "unknown-outbox-kind",
		FlowName:    "linear",
		CurrentNode: "a",
		Status:      store.StatusRunning,
		Outbox:      []store.OutboxItem{{Kind: "bogus-future-kind", Item: json.RawMessage(`{}`), Seq: 1}},
	}
	if _, err := h.store.Create(ctx, exec); err != nil {
		t.Fatalf("create: %v", err)
	}

	before := h.store.DeadLetters()

	if err := h.engine.flushOutbox(ctx, exec); err != nil {
		t.Fatalf("flushOutbox: %v", err)
	}

	got, err := h.store.Get(ctx, exec.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if len(got.Outbox) != 0 {
		t.Fatalf("unknown outbox item not cleared: %v", got.Outbox)
	}

	if h.store.DeadLetters() <= before {
		t.Fatalf("no dead-letter trace emitted for the dropped outbox item")
	}
}
