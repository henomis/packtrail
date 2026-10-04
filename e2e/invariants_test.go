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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/event"
)

// facts are what the invariant check learned from an execution's history,
// for scenario-specific assertions.
type facts struct {
	st     *packtrail.State
	events []event.Event
	// count of events by type.
	count map[event.Type]int
	// completions by node (NodeCompleted).
	completed map[string]int
	// attempts that failed and were retried, by node.
	retried map[string]int
}

type attempt struct {
	key             string
	gen, attemptNum int
}

// check verifies the invariants of the event model on a
// finished execution and returns what it read. Every scenario calls it on
// every execution it creates.
func (cl *cluster) check(id string) *facts {
	cl.t.Helper()

	f, err := checkExecution(cl.ctx, cl.c, id)
	if err != nil {
		cl.t.Fatalf("invariant broken in %s: %v", id, err)
	}

	return f
}

//nolint:gocognit,gocyclo,cyclop,funlen,maintidx // one place for every invariant.
func checkExecution(ctx context.Context, c *packtrail.Client, id string) (*facts, error) {
	st, err := c.Get(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("get: %w", err)
	}

	evs, err := c.History(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("history: %w", err)
	}

	f := &facts{
		st: st, events: evs, count: map[event.Type]int{}, completed: map[string]int{}, retried: map[string]int{},
	}

	if len(evs) == 0 {
		return nil, fmt.Errorf("empty history")
	}

	// I1: a history starts with its creation and indexes are contiguous.
	if t := evs[0].Type; t != event.ExecutionStarted && t != event.ExecutionForked {
		return nil, fmt.Errorf("history starts with %s", t)
	}

	var (
		terminal  = -1
		scheduled = map[attempt]bool{}
		settled   = map[attempt]event.Type{}
		cancelled = map[string]bool{}
		entered   = map[string]int{}
		steps     int
		continued bool
	)

	for i, ev := range evs {
		if i > 0 && ev.Index != evs[i-1].Index+1 {
			return nil, fmt.Errorf("event %d (%s) has index %d after %d", i, ev.Type, ev.Index, evs[i-1].Index)
		}

		if i > 0 && ev.Seq < evs[i-1].Seq && ev.Type != event.ExecutionContinued {
			return nil, fmt.Errorf("event %d (%s) has sequence %d after %d", i, ev.Type, ev.Seq, evs[i-1].Seq)
		}

		f.count[ev.Type]++

		// I2: at most one terminal event, and nothing after it.
		if terminal >= 0 {
			return nil, fmt.Errorf("%s at index %d after the terminal event", ev.Type, ev.Index)
		}

		if ev.Type.Terminal() {
			terminal = i
		}

		switch d := ev.Data.(type) {
		case *event.Continued:
			continued = true
		case *event.Entered:
			entered[d.Node]++
			steps++
		case *event.Scheduled:
			a := attempt{d.Key, d.Generation, d.Attempt}
			if scheduled[a] {
				return nil, fmt.Errorf("%v scheduled twice", a)
			}

			scheduled[a] = true
		case *event.NodeDone:
			a := attempt{d.Key, d.Generation, d.Attempt}
			if err = settle(scheduled, settled, a, ev.Type); err != nil {
				return nil, err
			}

			if d.Key == d.Node {
				f.completed[d.Node]++
			}
		case *event.NodeFail:
			a := attempt{d.Key, d.Generation, d.Attempt}
			if err = settle(scheduled, settled, a, ev.Type); err != nil {
				return nil, err
			}

			if d.WillRetry {
				f.retried[d.Node]++
			}
		case *event.Interrupted:
			// An interrupt settles the attempt it was raised from.
			for a := range scheduled {
				if a.key == d.Key && a.gen == d.Generation {
					if _, done := settled[a]; !done {
						settled[a] = ev.Type
					}
				}
			}
		case *event.NodeCancel:
			cancelled[d.Key] = true
		}
	}

	// I3: status and log agree on whether it ended.
	if st.Status.Terminal() != (terminal >= 0) {
		return nil, fmt.Errorf("status %s but terminal event at %d", st.Status, terminal)
	}

	// I4: a completed execution leaves nothing open, and every attempt it
	// scheduled settled (or its instance was cancelled).
	if st.Status == packtrail.StatusCompleted {
		if len(st.Tasks)+len(st.Fans)+len(st.Maps)+len(st.Awaits)+len(st.Children) != 0 {
			return nil, fmt.Errorf("completed with open work: tasks %v fans %v maps %v awaits %v children %v",
				keys(st.Tasks), keys(st.Fans), keys(st.Maps), keys(st.Awaits), keys(st.Children))
		}

		for a := range scheduled {
			if _, ok := settled[a]; !ok && !cancelled[a.key] {
				return nil, fmt.Errorf("completed but attempt %v never settled", a)
			}
		}
	}

	// I5: visits and steps count the node entries (a fork inherits them).
	if evs[0].Type == event.ExecutionStarted {
		if st.Steps != steps {
			return nil, fmt.Errorf("steps %d, history enters %d nodes", st.Steps, steps)
		}

		for n, v := range st.Visits {
			if entered[n] != v {
				return nil, fmt.Errorf("visits[%s] = %d, history enters it %d times", n, v, entered[n])
			}
		}
	}

	// I6: the state right after the last decision is the current state.
	last := evs[len(evs)-1].Seq

	at, err := c.StateAt(ctx, id, last)
	if err != nil {
		return nil, fmt.Errorf("state at %d: %w", last, err)
	}

	if err = sameState(at, st); err != nil {
		return nil, fmt.Errorf("StateAt(last) differs from Get: %w", err)
	}

	// I6b: every past decision can be read back (time travel, and what a
	// dispatcher does after a cache miss), and its steps never go backwards.
	prevSteps := -1

	for _, ev := range evs {
		if !ev.DecisionEnd || ev.Type == event.ExecutionContinued {
			continue
		}

		past, perr := c.StateAt(ctx, id, ev.Seq)
		if perr != nil {
			return nil, fmt.Errorf("state at %d (%s #%d): %w", ev.Seq, ev.Type, ev.Index, perr)
		}

		if past.Steps < prevSteps {
			return nil, fmt.Errorf("state at %d has %d steps, fewer than the decision before (%d)", ev.Seq, past.Steps, prevSteps)
		}

		prevSteps = past.Steps
	}

	// I7: OutputHistory has one output per completion of a plain node.
	for n, k := range f.completed {
		outs, oerr := c.OutputHistory(ctx, id, n)
		if oerr != nil {
			return nil, fmt.Errorf("output history %s: %w", n, oerr)
		}

		if len(outs) != k {
			return nil, fmt.Errorf("output history of %s holds %d outputs, %d completions", n, len(outs), k)
		}
	}

	// I8: Watch replays the live log exactly as History has it.
	if !continued && st.Status.Terminal() && !st.Archived {
		if err = sameAsWatch(ctx, c, id, evs); err != nil {
			return nil, err
		}
	}

	return f, nil
}

func settle(scheduled map[attempt]bool, settled map[attempt]event.Type, a attempt, t event.Type) error {
	if !scheduled[a] {
		return fmt.Errorf("%s for %v, which was never scheduled", t, a)
	}

	if prev, dup := settled[a]; dup {
		return fmt.Errorf("%v settled twice: %s then %s", a, prev, t)
	}

	settled[a] = t

	return nil
}

func sameState(a, b *packtrail.State) error {
	if a.Status != b.Status || !jsonEqual(a.Output, b.Output) || a.Steps != b.Steps || a.LastNode != b.LastNode {
		return fmt.Errorf("status %s/%s output %s/%s steps %d/%d last %s/%s",
			a.Status, b.Status, a.Output, b.Output, a.Steps, b.Steps, a.LastNode, b.LastNode)
	}

	for name, pair := range map[string][2]any{
		"channels": {a.Channels, b.Channels}, "results": {a.Results, b.Results},
		"visits": {a.Visits, b.Visits}, "counters": {a.Counters, b.Counters},
	} {
		if !reflect.DeepEqual(normalize(pair[0]), normalize(pair[1])) {
			return fmt.Errorf("%s %v / %v", name, pair[0], pair[1])
		}
	}

	return nil
}

func sameAsWatch(ctx context.Context, c *packtrail.Client, id string, evs []event.Event) error {
	wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	ch, err := c.Watch(wctx, id, 0)
	if err != nil {
		return fmt.Errorf("watch: %w", err)
	}

	i := 0
	for ev := range ch {
		if i >= len(evs) || ev.Index != evs[i].Index || ev.Type != evs[i].Type || ev.Seq != evs[i].Seq {
			return fmt.Errorf("watch event %d is %s#%d@%d, history has %d events", i, ev.Type, ev.Index, ev.Seq, len(evs))
		}

		i++
	}

	if i != len(evs) {
		return fmt.Errorf("watch delivered %d of %d events", i, len(evs))
	}

	return nil
}

func normalize(v any) any {
	b, _ := json.Marshal(v) //nolint:errchkjson // test values always encode.

	var out any

	_ = json.Unmarshal(b, &out)

	if m, ok := out.(map[string]any); ok && len(m) == 0 {
		return nil
	}

	return out
}

func jsonEqual(a, b json.RawMessage) bool {
	if len(bytes.TrimSpace(a)) == 0 || len(bytes.TrimSpace(b)) == 0 {
		return len(bytes.TrimSpace(a)) == len(bytes.TrimSpace(b))
	}

	return reflect.DeepEqual(normalize(a), normalize(b))
}

func keys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}

	return out
}
