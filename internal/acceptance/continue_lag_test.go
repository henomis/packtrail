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
	"fmt"
	"testing"
	"time"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/internal/natstest"
)

const tickLoop = `
name: ticks
start: wait
nodes:
  - {id: wait, type: await, signal: tick, timeout: 1h, next: again}
  - {id: again, type: choice, rules: [{when: "visits.wait < 40", to: wait}, {default: true, to: done}]}
  - {id: done, type: task, kind: echo}
`

// TestContinueAsNewWhileDispatcherLags: an execution continued as new while
// its dispatcher is behind keeps the messages the dispatcher has not
// processed in front of the continuation. The dispatcher must still get
// through them: the execution then finishes (found by the e2e chaos run,
// where it stalled the whole partition).
func TestContinueAsNewWhileDispatcherLags(t *testing.T) {
	s := natstest.Start(t)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
	defer cancel()

	opts := []packtrail.Option{
		packtrail.WithFlowYAML([]byte(tickLoop)), packtrail.WithPartitions(1), packtrail.WithHistoryLimit(20),
	}

	run := func(extra ...packtrail.Option) (*packtrail.Engine, context.CancelFunc, chan struct{}) {
		eng, err := packtrail.New(s.Connect(t), append(append([]packtrail.Option{}, opts...), extra...)...)
		if err != nil {
			t.Fatal(err)
		}

		rctx, stop := context.WithCancel(ctx)
		done := make(chan struct{})

		go func() { defer close(done); _ = eng.Run(rctx) }()

		select {
		case <-eng.Ready():
		case <-ctx.Done():
			t.Fatal("engine never ready")
		}

		return eng, stop, done
	}

	// A full engine creates the dispatcher's consumer and processes the start.
	eng, stop, done := run()
	c := eng.Client()

	id, err := c.Start(ctx, "ticks", nil)
	if err != nil {
		t.Fatal(err)
	}

	waitFor(ctx, t, func() bool {
		st, gerr := c.Get(ctx, id)

		return gerr == nil && st.Awaits["wait"] != nil
	})

	stop()
	<-done

	// Commands only: the log grows past the history limit, the dispatcher's
	// ack floor stays where it was.
	_, stopCmds, doneCmds := run(packtrail.WithoutDispatcher())

	for i := range 39 {
		if err = c.Signal(ctx, id, "tick", nil, packtrail.WithSignalID(fmt.Sprintf("tick-%d", i))); err != nil {
			t.Fatal(err)
		}

		waitFor(ctx, t, func() bool {
			st, gerr := c.Get(ctx, id)

			return gerr == nil && st.Visits["wait"] == i+2
		})
	}

	evs, err := c.History(ctx, id)
	if err != nil {
		t.Fatal(err)
	}

	if countType(evs, "ExecutionContinued") == 0 {
		t.Fatal("the execution never continued as new")
	}

	// Every past decision can be read back, including the ones the
	// continuation left in front of itself for the dispatcher.
	for _, ev := range evs {
		if !ev.DecisionEnd {
			continue
		}

		if _, serr := c.StateAt(ctx, id, ev.Seq); serr != nil {
			t.Errorf("state at %d (%s #%d): %v", ev.Seq, ev.Type, ev.Index, serr)
		}
	}

	stopCmds()
	<-doneCmds

	// A dispatcher catches up through the leftovers; the last tick routes
	// to done, a task that needs it.
	e := &Env{T: t, S: s, Ctx: ctx, Client: c}
	_, stopAll, doneAll := run()

	defer func() { stopAll(); <-doneAll }()

	e.Worker("echo", Echo)

	if err = c.Signal(ctx, id, "tick", nil, packtrail.WithSignalID("tick-last")); err != nil {
		t.Fatal(err)
	}

	wctx, wcancel := context.WithTimeout(ctx, 20*time.Second)
	defer wcancel()

	st, err := c.Wait(wctx, id)
	if err != nil || st.Status != packtrail.StatusCompleted {
		t.Fatalf("execution did not finish after the dispatcher caught up: %+v %v", st, err)
	}
}

func waitFor(ctx context.Context, t *testing.T, cond func() bool) {
	t.Helper()

	for !cond() {
		if ctx.Err() != nil {
			t.Fatal("condition never met")
		}

		time.Sleep(10 * time.Millisecond)
	}
}
