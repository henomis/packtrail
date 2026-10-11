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

package engine

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/henomis/packtrail/event"
	"github.com/henomis/packtrail/flow"
	"github.com/henomis/packtrail/internal/cmd"
	"github.com/henomis/packtrail/internal/eventlog"
	"github.com/henomis/packtrail/internal/fold"
	"github.com/henomis/packtrail/internal/infra"
	"github.com/henomis/packtrail/internal/loader"
	"github.com/henomis/packtrail/internal/metrics"
	"github.com/henomis/packtrail/internal/names"
	"github.com/henomis/packtrail/internal/natstest"
	"github.com/henomis/packtrail/internal/registry"
	"github.com/henomis/packtrail/internal/snapshot"
	"github.com/henomis/packtrail/internal/statecache"
)

func newEngine(t *testing.T, maxEvents int) (*Engine, context.Context) {
	t.Helper()

	s := natstest.Start(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)

	in, err := infra.New(s.NC, names.New(""), nil)
	if err != nil {
		t.Fatal(err)
	}

	if err = in.Provision(ctx, 1); err != nil {
		t.Fatal(err)
	}

	ld := loader.New(in, eventlog.New(in), snapshot.New(in, 0), registry.New(in))

	def, err := flow.Parse([]byte(`
name: wide
nodes:
  - {id: f, type: fanout, branches: [a, b, c], next: j}
  - {id: a, type: task, kind: k}
  - {id: b, type: task, kind: k}
  - {id: c, type: task, kind: k}
  - {id: j, type: join}
`))
	if err != nil {
		t.Fatal(err)
	}

	if _, err = ld.Flows.Register(ctx, def); err != nil {
		t.Fatal(err)
	}

	return &Engine{In: in, Loader: ld, Cache: statecache.New(0), Metrics: &metrics.M{}, MaxEvents: maxEvents}, ctx
}

// TestOversizedDecisionFailsExecution: a decision over the events limit is
// never appended; the execution must fail cleanly instead of staying
// "running" while the command is retried and dead-lettered (F-04).
func TestOversizedDecisionFailsExecution(t *testing.T) {
	e, ctx := newEngine(t, 4)

	c, err := cmd.New("start.x", cmd.Start, "x", cmd.StartData{Flow: "wide"})
	if err != nil {
		t.Fatal(err)
	}

	if _, _, err = e.apply(ctx, c, ""); err != nil {
		t.Fatalf("apply: %v", err)
	}

	st, _, err := e.Loader.Load(ctx, "x", 0)
	if err != nil {
		t.Fatal(err)
	}

	if st.Status != fold.StatusFailed || st.Reason != event.ReasonDecisionTooLarge {
		t.Fatalf("status %s reason %s", st.Status, st.Reason)
	}
}

// TestCoreTriggerReady: once a core trigger reports ready, a message
// published from another connection reaches it (core NATS drops messages
// sent before the server knows the subscription).
func TestCoreTriggerReady(t *testing.T) {
	s := natstest.Start(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	in, err := infra.New(s.NC, names.New(""), nil)
	if err != nil {
		t.Fatal(err)
	}

	if err = in.Provision(ctx, 1); err != nil {
		t.Fatal(err)
	}

	e := &Engine{In: in}
	other := s.Connect(t)

	// Each round has its own subject, so a stopping round's subscription
	// cannot take the next round's message.
	for i := range 20 {
		def, perr := flow.Parse(fmt.Appendf(nil, "name: trig\ntriggers: [{subject: trig.core.%d}]\nnodes: [{id: a, type: task, kind: k}]", i))
		if perr != nil {
			t.Fatal(perr)
		}

		rctx, stop := context.WithCancel(ctx)
		ready := make(chan struct{})
		done := make(chan struct{})

		go func() {
			defer close(done)

			_ = e.RunTrigger(rctx, def, 0, def.Triggers[0], func() { close(ready) })
		}()

		<-ready

		if err = other.Publish(def.Triggers[0].Subject, []byte(`{}`)); err != nil {
			t.Fatal(err)
		}

		cmds, serr := in.JS.Stream(ctx, in.Names.StreamCmd)
		if serr != nil {
			t.Fatal(serr)
		}

		deadline := time.Now().Add(2 * time.Second)

		for {
			info, ierr := cmds.Info(ctx)
			if ierr == nil && info.State.Msgs == uint64(i+1) {
				break
			}

			if time.Now().After(deadline) {
				t.Fatalf("round %d: the trigger message was lost", i)
			}

			time.Sleep(5 * time.Millisecond)
		}

		stop()
		<-done
	}
}
