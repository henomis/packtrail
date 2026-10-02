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

package acceptance

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/event"
	"github.com/henomis/packtrail/worker"
)

// TestG503UpdateIsSynchronous: Update writes channels of a waiting execution
// and returns the state right after the write; the same update id applies
// once; rejections come back as errors (not dead letters); the task that runs
// next sees the update (G5-03).
func TestG503UpdateIsSynchronous(t *testing.T) {
	e := NewEnv(t, []string{`
name: review
channels: {notes: {reducer: append}}
nodes:
  - {id: approve, type: await, signal: approve, timeout: 1h, next: use}
  - {id: use, type: task, kind: reader}
`})

	var (
		mu   sync.Mutex
		seen string
	)

	e.Worker("reader", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		mu.Lock()
		seen = string(j.Context.Channels["notes"])
		mu.Unlock()

		return &worker.Result{Output: map[string]any{}}, nil
	})

	id := e.Start("review", nil)
	e.WaitStatus(id, packtrail.StatusWaiting)

	st, err := e.Client.Update(e.Ctx, id, map[string]any{"notes": "from a reviewer"},
		packtrail.WithUpdateID("u-1"))
	if err != nil {
		t.Fatal(err)
	}

	if string(st.Channels["notes"]) != `["from a reviewer"]` {
		t.Fatalf("state after update: %s", st.Channels["notes"])
	}

	// Retrying the same update is a no-op.
	if st, err = e.Client.Update(e.Ctx, id, map[string]any{"notes": "from a reviewer"},
		packtrail.WithUpdateID("u-1")); err != nil {
		t.Fatalf("retried update: %v", err)
	}

	if string(st.Channels["notes"]) != `["from a reviewer"]` {
		t.Fatalf("retried update applied twice: %s", st.Channels["notes"])
	}

	if _, err = e.Client.Update(e.Ctx, id, map[string]any{"nope": 1}); !errors.Is(err, packtrail.ErrInvalidArgument) {
		t.Fatalf("undeclared channel: %v", err)
	}

	if _, err = e.Client.Update(e.Ctx, "no-such", map[string]any{"notes": "x"}); !errors.Is(err, packtrail.ErrNotFound) {
		t.Fatalf("missing execution: %v", err)
	}

	if err = e.Client.Signal(e.Ctx, id, "approve", nil); err != nil {
		t.Fatal(err)
	}

	e.Completed(id)

	mu.Lock()
	got := seen
	mu.Unlock()

	if got != `["from a reviewer"]` {
		t.Fatalf("the next task saw notes %s", got)
	}

	if _, err = e.Client.Update(e.Ctx, id, map[string]any{"notes": "late"}); !errors.Is(err, packtrail.ErrTerminal) {
		t.Fatalf("update after the end: %v", err)
	}

	if dl, _ := e.Client.DeadLetters(e.Ctx, 10); len(dl) != 0 {
		t.Fatalf("rejected updates were dead-lettered: %+v", dl)
	}

	if c := countType(mustHistory(t, e, id), event.ChannelsUpdated); c != 1 {
		t.Fatalf("%d ChannelsUpdated events, want 1", c)
	}
}

// TestG503ForkWithEdits: a fork with writes changes the state at the fork
// point and continues from there; the re-run task sees the edit; a bad edit
// fails at the call (G5-03).
func TestG503ForkWithEdits(t *testing.T) {
	e := NewEnv(t, []string{`
name: draft
channels: {draft: {reducer: replace}}
nodes:
  - {id: write, type: task, kind: drafter, next: publish}
  - {id: publish, type: task, kind: drafter}
`})

	var (
		mu        sync.Mutex
		published = map[string]string{}
	)

	e.Worker("drafter", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		if j.Node == "write" {
			return &worker.Result{Output: map[string]any{}, Writes: map[string]any{"draft": "v1"}}, nil
		}

		mu.Lock()
		published[j.ExecID] = string(j.Context.Channels["draft"])
		mu.Unlock()

		return &worker.Result{Output: map[string]any{}}, nil
	})

	id := e.Start("draft", nil)
	e.Completed(id)

	var afterWrite uint64

	for _, ev := range mustHistory(t, e, id) {
		if d, ok := ev.Data.(*event.NodeDone); ok && d.Node == "write" {
			afterWrite = ev.Seq
		}
	}

	fork, err := e.Client.Fork(e.Ctx, id, afterWrite, packtrail.WithForkWrites(map[string]any{"draft": "edited"}))
	if err != nil {
		t.Fatal(err)
	}

	e.Completed(fork)

	mu.Lock()
	defer mu.Unlock()

	if published[id] != `"v1"` || published[fork] != `"edited"` {
		t.Fatalf("published %v", published)
	}

	if _, err = e.Client.Fork(e.Ctx, id, afterWrite,
		packtrail.WithForkWrites(map[string]any{"nope": 1})); !errors.Is(err, packtrail.ErrInvalidArgument) {
		t.Fatalf("bad fork edit: %v", err)
	}
}

func mustHistory(t *testing.T, e *Env, id string) []event.Event {
	t.Helper()

	evs, err := e.Client.History(e.Ctx, id)
	if err != nil {
		t.Fatal(err)
	}

	return evs
}
