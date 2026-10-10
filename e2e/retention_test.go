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

package e2e_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/event"
	"github.com/henomis/packtrail/worker"
)

// receiptFlow is archived two seconds after it ends; ledgerFlow is kept
// until archived by hand.
const receiptFlow = `
name: receipt
retention: 2s
search_attributes: {customer: input.customer}
nodes:
  - {id: gate, type: await, signal: go, timeout: 1h, next: issue}
  - {id: issue, type: task, kind: issue}
`

const ledgerFlow = `
name: ledger
nodes:
  - {id: issue, type: task, kind: issue}
`

// TestRetentionAndArchive lets finished executions age out under a retention
// policy and archives another by hand. An archived execution stays readable
// — state, history, time travel, the index — refuses every change, can be
// forked, and is never started again under its id; one still running is
// never archived.
func TestRetentionAndArchive(t *testing.T) {
	cl := newCluster(t, []string{receiptFlow, ledgerFlow})

	cl.worker("issue", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		return &worker.Result{Output: map[string]any{"number": "R-" + j.ExecID}}, nil
	})

	prefix := fmt.Sprintf("rcpt%d-", time.Now().UnixNano())
	done := make([]string, 3, 4) //nolint:mnd // the ledger joins them.
	history := map[string][]event.Event{}

	for i := range done {
		done[i] = cl.start("receipt", map[string]any{"customer": "kim"},
			packtrail.WithExecutionID(fmt.Sprintf("%s%d", prefix, i)))

		if err := cl.c.Signal(cl.ctx, done[i], "go", nil); err != nil {
			t.Fatal(err)
		}
	}

	open := cl.start("receipt", map[string]any{"customer": "kim"})
	cl.parkedAt(open, "gate")

	for _, id := range done {
		if st := cl.wait(id); st.Status != packtrail.StatusCompleted {
			t.Fatalf("%s: %s", id, st.Status)
		}

		history[id] = cl.check(id).events
	}

	ledger := cl.start("ledger", nil)
	cl.wait(ledger)
	history[ledger] = cl.check(ledger).events

	if err := cl.liveEngines()[0].Archive(cl.ctx, open); err == nil {
		t.Fatal("a running execution was archived by hand")
	}

	if err := cl.liveEngines()[0].Archive(cl.ctx, ledger); err != nil {
		t.Fatal(err)
	}

	archived := append(done, ledger)

	for _, id := range archived {
		cl.eventually(id+" archived", func() bool {
			st, err := cl.c.Get(cl.ctx, id)

			return err == nil && st.Archived
		})
	}

	for _, id := range archived {
		st, err := cl.c.Get(cl.ctx, id)
		if err != nil || st.Status != packtrail.StatusCompleted || st.Results["issue"] == nil {
			t.Fatalf("archived %s: %+v %v", id, st, err)
		}

		// Readable as before: the full history, any past state, outputs.
		evs, err := cl.c.History(cl.ctx, id)
		if err != nil || len(evs) != len(history[id]) {
			t.Fatalf("archived %s: %d events (%v), %d before", id, len(evs), err, len(history[id]))
		}

		for i, ev := range evs {
			if ev.Index != history[id][i].Index || ev.Type != history[id][i].Type || ev.Seq != history[id][i].Seq {
				t.Fatalf("archived %s: event %d is %s#%d, was %s#%d", id, i, ev.Type, ev.Index,
					history[id][i].Type, history[id][i].Index)
			}
		}

		if past, perr := cl.c.StateAt(cl.ctx, id, evs[0].Seq); perr != nil || past.Status.Terminal() {
			t.Fatalf("archived %s: state at its start %+v %v", id, past, perr)
		}

		if outs, oerr := cl.c.OutputHistory(cl.ctx, id, "issue"); oerr != nil || len(outs) != 1 {
			t.Fatalf("archived %s: outputs %d %v", id, len(outs), oerr)
		}

		// Nothing changes it.
		for what, err := range map[string]error{
			"cancel": cl.c.Cancel(cl.ctx, id, "late"),
			"signal": cl.c.Signal(cl.ctx, id, "go", nil),
			"resume": cl.c.Resume(cl.ctx, id, "issue", "x"),
		} {
			if !errors.Is(err, packtrail.ErrArchived) {
				t.Fatalf("%s of archived %s: %v, want ErrArchived", what, id, err)
			}
		}

		if _, err = cl.c.Update(cl.ctx, id, map[string]any{}); err == nil {
			t.Fatalf("update of archived %s accepted", id)
		}
	}

	// Still in the index, flagged.
	cl.eventually("archived receipts listed", func() bool {
		l, err := cl.c.List(cl.ctx, packtrail.ListFilter{Flow: "receipt", Attr: "customer", Value: "kim"})
		if err != nil {
			return false
		}

		n := 0

		for _, s := range l {
			if s.Archived {
				n++
			}
		}

		return len(l) == len(done)+1 && n == len(done)
	})

	// Its id is spent: starting it again is a duplicate.
	if _, err := cl.c.Start(cl.ctx, "receipt", nil, packtrail.WithExecutionID(done[0])); err != nil &&
		!errors.Is(err, packtrail.ErrArchived) {
		t.Fatalf("restart of archived id: %v", err)
	}

	if st, _ := cl.c.Get(cl.ctx, done[0]); !st.Archived || st.Status != packtrail.StatusCompleted {
		t.Fatalf("archived id restarted: %+v", st)
	}

	// A fork of an archived execution is a new, live one.
	fork, err := cl.c.Fork(cl.ctx, done[1], history[done[1]][0].Seq)
	if err != nil {
		t.Fatal(err)
	}

	cl.parkedAt(fork, "gate")

	if err = cl.c.Signal(cl.ctx, fork, "go", nil); err != nil {
		t.Fatal(err)
	}

	if st := cl.wait(fork); st.Status != packtrail.StatusCompleted || st.ForkedFrom != done[1] {
		t.Fatalf("fork of archived: %s from %q", st.Status, st.ForkedFrom)
	}

	cl.check(fork)

	// The one still waiting outlived the retention untouched.
	if st, _ := cl.c.Get(cl.ctx, open); st.Archived || st.Status != packtrail.StatusWaiting {
		t.Fatalf("running receipt: %+v", st)
	}
}

const ticketFlow = `
name: ticket
retention: 5s
on_expire: delete
search_attributes: {customer: input.customer}
nodes:
  - {id: gate, type: await, signal: go, timeout: 1h, next: issue}
  - {id: issue, type: task, kind: issue}
`

// skewedClock is an engine clock a test can move forward: an execution is
// only deleted once the deduplication window (ten minutes) has passed since
// it finished, and the test jumps past it instead of waiting.
type skewedClock struct{ offset atomic.Int64 }

func (c *skewedClock) now() time.Time { return time.Now().Add(time.Duration(c.offset.Load())) }

func (c *skewedClock) pastDedupWindow() { c.offset.Add(int64(11 * time.Minute)) }

// TestRetentionAndDelete lets finished executions of a flow with
// on_expire: delete age out, and deletes others by hand. A deleted execution
// is gone for every reader — state, history, the index — and for every
// writer; one still running is never deleted, and neither is one that has
// just finished.
func TestRetentionAndDelete(t *testing.T) {
	clock := &skewedClock{}
	cl := newCluster(t, []string{ticketFlow, ledgerFlow}, packtrail.WithClock(clock.now))

	cl.worker("issue", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		return &worker.Result{Output: map[string]any{"number": "T-" + j.ExecID}}, nil
	})

	prefix := fmt.Sprintf("tkt%d-", time.Now().UnixNano())
	done := make([]string, 3)

	for i := range done {
		done[i] = cl.start("ticket", map[string]any{"customer": "kim"},
			packtrail.WithExecutionID(fmt.Sprintf("%s%d", prefix, i)))

		if err := cl.c.Signal(cl.ctx, done[i], "go", nil); err != nil {
			t.Fatal(err)
		}
	}

	open := cl.start("ticket", map[string]any{"customer": "kim"})
	cl.parkedAt(open, "gate")

	for _, id := range done {
		if st := cl.wait(id); st.Status != packtrail.StatusCompleted {
			t.Fatalf("%s: %s", id, st.Status)
		}
	}

	kept := cl.start("ledger", nil)
	cl.wait(kept)

	archived := cl.start("ledger", nil)
	cl.wait(archived)

	eng := cl.liveEngines()[0]

	if err := eng.Archive(cl.ctx, archived); err != nil {
		t.Fatal(err)
	}

	// By hand: not a running one, not one that has just finished, not one
	// that never existed.
	for id, what := range map[string]string{open: "running", kept: "just finished", archived: "just archived"} {
		if err := eng.Delete(cl.ctx, id); !errors.Is(err, packtrail.ErrInvalidArgument) {
			t.Fatalf("delete of a %s execution: %v, want ErrInvalidArgument", what, err)
		}
	}

	if err := eng.Delete(cl.ctx, "no-such-execution"); !errors.Is(err, packtrail.ErrNotFound) {
		t.Fatalf("delete of a missing execution: %v, want ErrNotFound", err)
	}

	clock.pastDedupWindow()

	for _, id := range []string{kept, archived} {
		if err := eng.Delete(cl.ctx, id); err != nil {
			t.Fatalf("delete %s: %v", id, err)
		}
	}

	// The tickets age out by themselves; the ledgers were deleted by hand.
	for _, id := range append(slices.Clone(done), kept, archived) {
		cl.eventually(id+" deleted", func() bool {
			_, err := cl.c.Get(cl.ctx, id)

			return errors.Is(err, packtrail.ErrNotFound)
		})

		for what, err := range map[string]error{
			"cancel": cl.c.Cancel(cl.ctx, id, "late"),
			"signal": cl.c.Signal(cl.ctx, id, "go", nil),
			"resume": cl.c.Resume(cl.ctx, id, "issue", "x"),
		} {
			if !errors.Is(err, packtrail.ErrNotFound) {
				t.Fatalf("%s of deleted %s: %v, want ErrNotFound", what, id, err)
			}
		}

		if _, err := cl.c.History(cl.ctx, id); !errors.Is(err, packtrail.ErrNotFound) {
			t.Fatalf("history of deleted %s: %v, want ErrNotFound", id, err)
		}

		if _, err := cl.c.Fork(cl.ctx, id, 1); err == nil {
			t.Fatalf("fork of deleted %s accepted", id)
		}
	}

	// Out of the index too: only the open ticket is left.
	cl.eventually("deleted tickets unlisted", func() bool {
		l, err := cl.c.List(cl.ctx, packtrail.ListFilter{Flow: "ticket", Attr: "customer", Value: "kim"})

		return err == nil && len(l) == 1 && l[0].ExecID == open
	})

	cl.eventually("deleted ledgers unlisted", func() bool {
		l, err := cl.c.List(cl.ctx, packtrail.ListFilter{Flow: "ledger"})

		return err == nil && len(l) == 0
	})

	// Nothing was archived on the way.
	if n := eng.Metrics(cl.ctx).Archived; n > 1 {
		t.Fatalf("archived metric = %d: only the ledger archived by hand should count", n)
	}

	// The one still waiting outlived its retention untouched, and finishes.
	if st, err := cl.c.Get(cl.ctx, open); err != nil || st.Status != packtrail.StatusWaiting {
		t.Fatalf("running ticket: %+v %v", st, err)
	}

	if err := cl.c.Signal(cl.ctx, open, "go", nil); err != nil {
		t.Fatal(err)
	}

	if st := cl.wait(open); st.Status != packtrail.StatusCompleted {
		t.Fatalf("open ticket: %s", st.Status)
	}

	cl.check(open)
}
