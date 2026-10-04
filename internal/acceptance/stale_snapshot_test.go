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
	"fmt"
	"testing"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/event"
	"github.com/henomis/packtrail/internal/names"
)

// TestStaleSnapshotIgnored: the snapshot save after a continuation
// fails (simulated: the previous continuation's snapshot stays), the purge
// succeeds. StateAt at a purged decision between the two continuations must
// not return the old snapshot's state.
func TestStaleSnapshotIgnored(t *testing.T) {
	e := NewEnv(t, []string{tickLoop}, packtrail.WithHistoryLimit(20))
	c := e.Client

	id := e.Start("ticks", nil)
	waitFor(e.Ctx, t, func() bool {
		st, err := c.Get(e.Ctx, id)

		return err == nil && st.Awaits["wait"] != nil
	})

	n := names.New(names.Default)

	kv, err := e.S.JS.KeyValue(e.Ctx, n.BucketSnapshots)
	if err != nil {
		t.Fatal(err)
	}

	tick := 0
	continued := func() []event.Event {
		evs, herr := c.History(e.Ctx, id)
		if herr != nil {
			t.Fatal(herr)
		}

		var out []event.Event

		for _, ev := range evs {
			if ev.Type == event.ExecutionContinued {
				out = append(out, ev)
			}
		}

		return out
	}
	signal := func() {
		if serr := c.Signal(e.Ctx, id, "tick", nil, packtrail.WithSignalID(fmt.Sprintf("t-%d", tick))); serr != nil {
			t.Fatal(serr)
		}

		tick++

		waitFor(e.Ctx, t, func() bool {
			st, gerr := c.Get(e.Ctx, id)

			return gerr == nil && st.Visits["wait"] == tick+1
		})
	}

	for len(continued()) < 1 {
		signal()
	}

	c1 := continued()[0].Seq

	var old []byte

	waitFor(e.Ctx, t, func() bool {
		ent, gerr := kv.Get(e.Ctx, id)
		if gerr != nil {
			return false
		}

		old = ent.Value()

		return true
	})

	for len(continued()) < 2 {
		signal()
	}

	c2 := continued()[1].Seq

	// A decision strictly between the two continuations, purged by the second.
	var target uint64

	hist, err := c.History(e.Ctx, id)
	if err != nil {
		t.Fatal(err)
	}

	for _, ev := range hist {
		if ev.DecisionEnd && ev.Seq > c1+2 && ev.Seq < c2 {
			target = ev.Seq

			break
		}
	}

	want, err := c.StateAt(e.Ctx, id, target)
	if err != nil {
		t.Fatal(err)
	}

	events, err := e.S.JS.Stream(e.Ctx, n.StreamEvents)
	if err != nil {
		t.Fatal(err)
	}

	if _, gerr := events.GetMsg(e.Ctx, target); gerr == nil {
		t.Skipf("decision %d not purged (dispatcher behind); precondition not met", target)
	}

	// The save after the second continuation failed: the first one's
	// snapshot is still there.
	if _, err = kv.Put(e.Ctx, id, old); err != nil {
		t.Fatal(err)
	}

	got, err := c.StateAt(e.Ctx, id, target)
	if err != nil {
		t.Fatalf("StateAt(%d): %v", target, err)
	}

	t.Logf("c1=%d c2=%d target=%d: want LastSeq %d visits %d, got LastSeq %d visits %d",
		c1, c2, target, want.LastSeq, want.Visits["wait"], got.LastSeq, got.Visits["wait"])

	if got.LastSeq != want.LastSeq || got.Visits["wait"] != want.Visits["wait"] {
		t.Fatalf("StateAt(%d) silently returned the state at %d", target, got.LastSeq)
	}
}
