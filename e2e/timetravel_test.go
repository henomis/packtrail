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
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/event"
)

// TestRerunAfterFix: an order compensated because its card was declined is
// rerun from the charge once the "bug" is fixed: the rerun ships, the
// original is untouched.
func TestRerunAfterFix(t *testing.T) {
	cl := newCluster(t, []string{orderFlow, invoiceFlow})
	orderWorkers(cl)

	o := order{
		ID: fmt.Sprintf("rerun%d", time.Now().UnixNano()), Customer: "fay", Amount: 80, Card: "declined",
		Items: []item{{SKU: "A", Price: 1}, {SKU: "B", Price: 2}},
	}
	oc := orderCase{name: "declined", o: o, want: compensated, failedAt: "settle"}

	id := cl.runOrder(oc)
	cl.verifyOrder(oc, id)

	fixedCards.Store(o.ID, true)

	rerun, err := cl.c.Rerun(cl.ctx, id, "charge")
	if err != nil {
		t.Fatal(err)
	}

	st := cl.wait(rerun)
	f := cl.check(rerun)

	if st.Status != packtrail.StatusCompleted || st.ForkedFrom != id {
		t.Fatalf("rerun %s from %q: %s", st.Status, st.ForkedFrom, st.Error)
	}

	var out struct{ Status string }
	if err = st.Result(st.LastNode, &out); err != nil || out.Status != "shipped" {
		t.Fatalf("rerun ended on %s: %s", st.LastNode, st.Results[st.LastNode])
	}

	// The fork's history starts at the fork and validation did not run again.
	if f.events[0].Type != event.ExecutionForked || f.completed["validate"] != 0 {
		t.Fatalf("rerun history starts with %s and validates %d times", f.events[0].Type, f.completed["validate"])
	}

	// The source still shows its compensation.
	src := cl.wait(id)
	if src.LastNode != "compensate" {
		t.Fatalf("source changed: last node %s", src.LastNode)
	}
}

// TestForkWithWrites: an agent run is forked at a past decision with an
// edited channel; the fork continues from there with the edit, and the source
// ends as before.
func TestForkWithWrites(t *testing.T) {
	cl := newCluster(t, []string{fmt.Sprintf(agentFlow, 1_000_000)})
	agentWorkers(cl)

	id := cl.start("agent", agentTask{Steps: 6, AskAt: -1})
	src := cl.wait(id)
	cl.check(id)

	// Fork right after the third tool call.
	evs, err := cl.c.History(cl.ctx, id)
	if err != nil {
		t.Fatal(err)
	}

	var at uint64

	tools := 0

	for _, ev := range evs {
		if d, ok := ev.Data.(*event.NodeDone); ok && d.Node == "tool" {
			if tools++; tools == 3 {
				at = ev.Seq
			}
		}
	}

	fork, err := cl.c.Fork(cl.ctx, id, at, packtrail.WithForkWrites(map[string]any{"scratch": "injected"}))
	if err != nil {
		t.Fatal(err)
	}

	st := cl.wait(fork)
	cl.check(fork)

	var notes, srcNotes []string

	_ = st.Channel("scratch", &notes)
	_ = src.Channel("scratch", &srcNotes)

	if st.Status != packtrail.StatusCompleted || !slices.Equal(notes[:3], srcNotes[:3]) || notes[3] != "injected" ||
		len(notes) != len(srcNotes)+1 {
		t.Fatalf("fork %s scratch %v, source %v", st.Status, notes, srcNotes)
	}

	// Same visits as the source: it resumed where the source was.
	if !jsonEqual(mustJSON(st.Visits), mustJSON(src.Visits)) {
		t.Fatalf("fork visits %v, source %v", st.Visits, src.Visits)
	}

	if again := cl.wait(id); !jsonEqual(mustJSON(again.Channels), mustJSON(src.Channels)) {
		t.Fatal("the source changed after the fork")
	}
}

// TestUpdateWhileRunning: channels written from outside while an order runs
// are folded in; updates to a finished execution or an undeclared channel are
// refused.
func TestUpdateWhileRunning(t *testing.T) {
	cl := newCluster(t, []string{orderFlow, invoiceFlow})
	orderWorkers(cl)

	o := order{
		ID: fmt.Sprintf("upd%d", time.Now().UnixNano()), Customer: "gus", Amount: 10, Gated: true,
		Items: []item{{SKU: "A", Price: 3}, {SKU: "B", Price: 4}},
	}

	id := cl.start("order", o, packtrail.WithExecutionID(o.ID))
	cl.waitUntil(id, "running validation", func(st *packtrail.State) bool { return st.Tasks["validate"] != nil })

	st, err := cl.c.Update(cl.ctx, id, map[string]any{"total": 100, "log": "manual"}, packtrail.WithUpdateID("u1"))
	if err != nil {
		t.Fatal(err)
	}

	if !jsonEqual(st.Channels["total"], json.RawMessage(`100`)) {
		t.Fatalf("state after update: total %s", st.Channels["total"])
	}

	// The same update id applies once.
	if _, err = cl.c.Update(cl.ctx, id, map[string]any{"total": 100}, packtrail.WithUpdateID("u1")); err != nil {
		t.Fatal(err)
	}

	if _, err = cl.c.Update(cl.ctx, id, map[string]any{"nope": 1}); !errors.Is(err, packtrail.ErrInvalidArgument) {
		t.Fatalf("update of an undeclared channel: %v", err)
	}

	close(gate(id))

	st = cl.wait(id)
	f := cl.check(id)

	var total float64
	if err = st.Channel("total", &total); err != nil || total != 100+o.total() {
		t.Fatalf("total %s, want %g", st.Channels["total"], 100+o.total())
	}

	if f.count[event.ChannelsUpdated] != 1 {
		t.Fatalf("%d updates applied, want 1", f.count[event.ChannelsUpdated])
	}

	if _, err = cl.c.Update(cl.ctx, id, map[string]any{"total": 1}); !errors.Is(err, packtrail.ErrTerminal) {
		t.Fatalf("update of a finished execution: %v", err)
	}
}
