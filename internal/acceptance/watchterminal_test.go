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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/event"
	"github.com/henomis/packtrail/worker"
)

const wtOne = `
name: one
nodes:
  - {id: a, type: task, kind: echo}
`

const wtFails = `
name: fails
nodes:
  - {id: a, type: task, kind: boom}
`

const wtAwait = `
name: hold
nodes:
  - {id: a, type: await, signal: go, timeout: 1h}
`

// ended reads n endings from ch, failing the test on a timeout or an early
// close, and checks their sequences strictly increase.
func ended(t *testing.T, ch <-chan packtrail.Ended, n int) []packtrail.Ended {
	t.Helper()

	out := make([]packtrail.Ended, 0, n)
	timeout := time.After(30 * time.Second)

	for len(out) < n {
		select {
		case e, ok := <-ch:
			if !ok {
				t.Fatalf("watch closed after %d of %d endings: %v", len(out), n, out)
			}

			if len(out) > 0 && e.Seq <= out[len(out)-1].Seq {
				t.Fatalf("sequence went from %d to %d", out[len(out)-1].Seq, e.Seq)
			}

			out = append(out, e)
		case <-timeout:
			t.Fatalf("got %d of %d endings: %v", len(out), n, out)
		}
	}

	return out
}

// quiet checks that nothing more arrives on ch for a while.
func quiet(t *testing.T, ch <-chan packtrail.Ended) {
	t.Helper()

	select {
	case e := <-ch:
		t.Fatalf("unexpected ending %+v", e)
	case <-time.After(500 * time.Millisecond):
	}
}

func byID(es []packtrail.Ended) map[string]packtrail.Ended {
	m := make(map[string]packtrail.Ended, len(es))
	for _, e := range es {
		m[e.ExecID] = e
	}

	return m
}

// TestWatchTerminalStatuses: each way an execution ends is reported once,
// with its status and the sequence of its terminal decision; an execution
// still waiting is not.
func TestWatchTerminalStatuses(t *testing.T) {
	e := NewEnv(t, []string{wtOne, wtFails, wtAwait})
	e.Worker("echo", Echo)
	e.Worker("boom", func(context.Context, *worker.Job) (*worker.Result, error) {
		return nil, worker.Permanent(errors.New("boom"))
	})

	ch, err := e.Client.WatchTerminal(e.Ctx, 0)
	if err != nil {
		t.Fatal(err)
	}

	ok := e.Start("one", nil)
	bad := e.Start("fails", nil)
	held := e.Start("hold", nil)
	gone := e.Start("hold", nil)
	e.WaitStatus(gone, packtrail.StatusWaiting)

	if err = e.Client.Cancel(e.Ctx, gone, "stop"); err != nil {
		t.Fatal(err)
	}

	got := byID(ended(t, ch, 3))
	want := map[string]packtrail.Status{
		ok: packtrail.StatusCompleted, bad: packtrail.StatusFailed, gone: packtrail.StatusCancelled,
	}

	for id, st := range want {
		if got[id].Status != st {
			t.Fatalf("%s: got %+v, want %s", id, got[id], st)
		}

		evs, herr := e.Client.History(e.Ctx, id)
		if herr != nil {
			t.Fatal(herr)
		}

		if last := evs[len(evs)-1]; !last.Type.Terminal() || last.Seq != got[id].Seq {
			t.Fatalf("%s: last event %s at %d, ended at %d", id, last.Type, last.Seq, got[id].Seq)
		}
	}

	if _, seen := got[held]; seen {
		t.Fatalf("waiting execution %s reported", held)
	}

	quiet(t, ch)
}

// TestWatchTerminalFromSeq: 0 starts after what is already in the log; a
// sequence replays from there, in order, and Seq+1 resumes without repeats.
// More endings than the channel buffers arrive intact (backpressure).
func TestWatchTerminalFromSeq(t *testing.T) {
	e := NewEnv(t, []string{wtOne})
	e.Worker("echo", Echo)

	const n = 100

	ids := make([]string, n)
	for i := range ids {
		ids[i] = e.Start("one", nil)
	}

	for _, id := range ids {
		e.Completed(id)
	}

	// Already ended: a watch from now sees none of them.
	now, err := e.Client.WatchTerminal(e.Ctx, 0)
	if err != nil {
		t.Fatal(err)
	}

	// From the start of the log, nothing is read until all are in the log.
	all, err := e.Client.WatchTerminal(e.Ctx, 1)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(200 * time.Millisecond)

	replayed := ended(t, all, n)
	if m := byID(replayed); len(m) != n {
		t.Fatalf("%d distinct executions, want %d", len(m), n)
	}

	quiet(t, all)
	quiet(t, now)

	// Resuming right after the 50th replays the other 50.
	rest, err := e.Client.WatchTerminal(e.Ctx, replayed[49].Seq+1)
	if err != nil {
		t.Fatal(err)
	}

	if got := ended(t, rest, n-50); got[0] != replayed[50] || got[len(got)-1] != replayed[n-1] {
		t.Fatalf("resume got %+v … %+v", got[0], got[len(got)-1])
	}

	quiet(t, rest)

	// The watch from now sees a new ending.
	id := e.Start("one", nil)
	if got := ended(t, now, 1); got[0].ExecID != id {
		t.Fatalf("got %+v, want %s", got[0], id)
	}
}

// TestWatchTerminalAcrossServerRestart: endings before and after a NATS
// restart all arrive, once each, in order.
func TestWatchTerminalAcrossServerRestart(t *testing.T) {
	e := NewEnv(t, []string{wtOne})
	e.Worker("echo", Echo)

	ch, err := e.Client.WatchTerminal(e.Ctx, 0)
	if err != nil {
		t.Fatal(err)
	}

	before := e.Start("one", nil)
	if got := ended(t, ch, 1); got[0].ExecID != before {
		t.Fatalf("got %+v, want %s", got[0], before)
	}

	e.S.Restart(t)

	after := make(map[string]bool)

	for range 5 {
		after[e.Start("one", nil)] = true
	}

	got := ended(t, ch, len(after))
	for _, en := range got {
		if !after[en.ExecID] {
			t.Fatalf("unexpected ending %+v", en)
		}

		delete(after, en.ExecID)
	}

	quiet(t, ch)
}

// TestWatchTerminalOffloadedDecision: a terminal decision whose body went to
// the blob store is reported from its headers alone, even with the blob gone.
func TestWatchTerminalOffloadedDecision(t *testing.T) {
	e := NewEnv(t, []string{wtOne}, packtrail.WithBlobThreshold(1024))
	e.Worker("echo", func(_ context.Context, _ *worker.Job) (*worker.Result, error) {
		return &worker.Result{Output: map[string]any{"big": strings.Repeat("x", 8192)}}, nil
	})

	id := e.Start("one", nil)
	e.Completed(id)

	evs, err := e.Client.History(e.Ctx, id)
	if err != nil {
		t.Fatal(err)
	}

	seq := evs[len(evs)-1].Seq

	s, err := e.S.JS.Stream(e.Ctx, "packtrail-events")
	if err != nil {
		t.Fatal(err)
	}

	raw, err := s.GetMsg(e.Ctx, seq)
	if err != nil {
		t.Fatal(err)
	}

	name := raw.Header.Get("Pt-Blob")
	if name == "" || len(raw.Data) != 0 {
		t.Fatalf("terminal decision not offloaded: header %q, body %d bytes", name, len(raw.Data))
	}

	store, err := e.S.JS.ObjectStore(e.Ctx, "packtrail-blobs")
	if err != nil {
		t.Fatal(err)
	}

	if err = store.Delete(e.Ctx, name); err != nil {
		t.Fatal(err)
	}

	ch, err := e.Client.WatchTerminal(e.Ctx, seq)
	if err != nil {
		t.Fatal(err)
	}

	got := ended(t, ch, 1)
	if want := (packtrail.Ended{ExecID: id, Status: packtrail.StatusCompleted, Seq: seq}); got[0] != want {
		t.Fatalf("got %+v, want %+v", got[0], want)
	}
}

// TestWatchTerminalSubflow: a subflow child ends like any execution, before
// its parent.
func TestWatchTerminalSubflow(t *testing.T) {
	e := NewEnv(t, []string{m2Parent, m2Child})
	e.Worker("echo", Echo)

	childID := make(chan string, 1)

	e.Worker("child-work", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		childID <- j.ExecID

		return &worker.Result{}, nil
	})

	ch, err := e.Client.WatchTerminal(e.Ctx, 0)
	if err != nil {
		t.Fatal(err)
	}

	parent := e.Start("parent", map[string]any{"child": map[string]any{}})
	child := <-childID

	got := ended(t, ch, 2)
	if got[0].ExecID != child || got[1].ExecID != parent ||
		got[0].Status != packtrail.StatusCompleted || got[1].Status != packtrail.StatusCompleted {
		t.Fatalf("got %+v, want child %s then parent %s", got, child, parent)
	}

	quiet(t, ch)
}

const wtLoop = `
name: loop
start: step
nodes:
  - {id: step, type: task, kind: echo, next: again}
  - {id: again, type: choice, rules: [{when: "visits.step < 30", to: step}, {default: true, to: done}]}
  - {id: done, type: task, kind: echo}
`

// TestWatchTerminalContinueAsNew: continuing as new is not an ending; the
// execution is reported once, when it really ends.
func TestWatchTerminalContinueAsNew(t *testing.T) {
	e := NewEnv(t, []string{wtLoop}, packtrail.WithHistoryLimit(20))
	e.Worker("echo", Echo)

	ch, err := e.Client.WatchTerminal(e.Ctx, 0)
	if err != nil {
		t.Fatal(err)
	}

	id := e.Start("loop", nil)
	e.Completed(id)

	evs, err := e.Client.History(e.Ctx, id)
	if err != nil {
		t.Fatal(err)
	}

	if n := countType(evs, event.ExecutionContinued); n == 0 {
		t.Fatal("the execution never continued as new")
	}

	if got := ended(t, ch, 1); got[0].ExecID != id || got[0].Status != packtrail.StatusCompleted {
		t.Fatalf("got %+v, want %s completed", got[0], id)
	}

	quiet(t, ch)
}

// TestWatchTerminalCancel: ending ctx closes the channel, also while the
// watch waits to deliver an ending nobody reads.
func TestWatchTerminalCancel(t *testing.T) {
	e := NewEnv(t, []string{wtOne})
	e.Worker("echo", Echo)

	for _, blocked := range []bool{false, true} {
		t.Run(fmt.Sprintf("blocked=%v", blocked), func(t *testing.T) {
			ctx, cancel := context.WithCancel(e.Ctx)
			defer cancel()

			ch, err := e.Client.WatchTerminal(ctx, 0)
			if err != nil {
				t.Fatal(err)
			}

			if blocked {
				// Fill the buffer and one more, unread.
				ids := make([]string, 70)
				for i := range ids {
					ids[i] = e.Start("one", nil)
				}

				for _, id := range ids {
					e.Completed(id)
				}

				time.Sleep(200 * time.Millisecond)
			}

			cancel()

			deadline := time.After(5 * time.Second)

			for {
				select {
				case _, ok := <-ch:
					if !ok {
						return
					}
				case <-deadline:
					t.Fatal("channel not closed after cancel")
				}
			}
		})
	}
}

// TestWatchAcrossServerRestart: a NATS restart while an execution waits does
// not end its watch; the events after it arrive once each, in order, through
// the terminal one.
func TestWatchAcrossServerRestart(t *testing.T) {
	e := NewEnv(t, []string{wtAwait})

	id := e.Start("hold", nil)
	e.WaitStatus(id, packtrail.StatusWaiting)

	ch, err := e.Client.Watch(e.Ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}

	var seen []event.Event

	next := func() (event.Event, bool) {
		select {
		case ev, ok := <-ch:
			return ev, ok
		case <-time.After(30 * time.Second):
			t.Fatalf("no event after %d", len(seen))

			return event.Event{}, false
		}
	}

	// Everything up to the await.
	for {
		ev, ok := next()
		if !ok {
			t.Fatal("watch closed before the restart")
		}

		seen = append(seen, ev)

		if ev.Type == event.AwaitStarted {
			break
		}
	}

	e.S.Restart(t)

	if err = e.Client.Signal(e.Ctx, id, "go", nil); err != nil {
		t.Fatal(err)
	}

	for {
		ev, ok := next()
		if !ok {
			break
		}

		if last := seen[len(seen)-1]; ev.Seq < last.Seq {
			t.Fatalf("event %s at %d after %d", ev.Type, ev.Seq, last.Seq)
		}

		seen = append(seen, ev)
	}

	if last := seen[len(seen)-1]; last.Type != event.ExecutionCompleted {
		t.Fatalf("watch closed on %s, not on the terminal event", last.Type)
	}

	evs, err := e.Client.History(e.Ctx, id)
	if err != nil {
		t.Fatal(err)
	}

	if len(seen) != len(evs) {
		t.Fatalf("watched %d events, history holds %d", len(seen), len(evs))
	}
}
