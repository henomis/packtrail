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

package packtrail_test

import (
	"context"
	"testing"
	"time"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/internal/natstest"
)

const observeFlow = `
version: "1.0"
name: observe-me
nodes:
  - {id: a, type: task, invoker: custom, target: agent-a}
  - id: route
    type: choice
    rules:
      - {when: 'input.x == 1', to: b}
      - {default: true, to: c}
  - {id: b, type: task, invoker: custom, target: agent-b}
  - {id: c, type: task, invoker: custom, target: agent-c}
edges:
  - {from: a, to: route}
`

func TestFlowRegistry(t *testing.T) {
	srv := natstest.Start(t)

	s, err := packtrail.New(srv.NC, packtrail.WithNamespace("obs"), packtrail.WithFlow([]byte(observeFlow)),
		packtrail.WithInvoker("custom", okInvoker()))
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	ctx := context.Background()

	names, err := s.ListFlows(ctx)
	if err != nil || len(names) != 1 || names[0] != "observe-me" {
		t.Fatalf("ListFlows = %v, %v; want [observe-me]", names, err)
	}

	g, err := s.FlowGraph(ctx, "observe-me")
	if err != nil {
		t.Fatalf("FlowGraph: %v", err)
	}

	if g.Name != "observe-me" || len(g.Nodes) != 4 || len(g.Edges) != 1 {
		t.Fatalf("graph = %+v, want 4 nodes / 1 edge", g)
	}
	// The choice node's rules are preserved for rendering.
	var choice *packtrail.GraphNode

	for i := range g.Nodes {
		if g.Nodes[i].ID == "route" {
			choice = &g.Nodes[i]
		}
	}

	if choice == nil || choice.Type != "choice" || len(choice.Rules) != 2 {
		t.Fatalf("choice node not rendered: %+v", choice)
	}

	if _, graphErr := s.FlowGraph(ctx, "nope"); graphErr != packtrail.ErrNotFound {
		t.Fatalf("FlowGraph(missing) err = %v, want ErrNotFound", graphErr)
	}
}

func TestWatchEvents(t *testing.T) {
	srv := natstest.Start(t)
	custom := packtrail.InvokerFunc(func(_ context.Context, _ packtrail.Request) (packtrail.Result, error) {
		return packtrail.Result{Status: packtrail.StatusOK, Payload: []byte(`{"x":1}`)}, nil
	})

	s, err := packtrail.New(srv.NC,
		packtrail.WithNamespace("obs2"),
		packtrail.WithFlow([]byte(observeFlow)),
		packtrail.WithInvoker("custom", custom),
	)
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = s.Run(ctx) }()

	events, err := s.WatchEvents(ctx)
	if err != nil {
		t.Fatalf("watch: %v", err)
	}

	id, err := s.Start(ctx, "observe-me", []byte(`{"x":1}`))
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	// Expect to observe this execution reach completion via the live feed.
	deadline := time.After(10 * time.Second)

	for {
		select {
		case ev := <-events:
			if ev.ExecID == id && ev.Status == packtrail.ExecCompleted {
				return
			}
		case <-deadline:
			t.Fatal("did not observe completion event")
		}
	}
}

// TestWatchEventsCancelUnderLoad cancels a watch while events are still being
// produced and the subscriber is NOT draining the channel — the shape that
// used to race a late consume callback into a send on the just-closed channel
// (cc.Stop does not wait for an executing callback). A panic there kills the
// process, so the test failing to complete IS the regression signal; the
// closed channel must also be observed.
func TestWatchEventsCancelUnderLoad(t *testing.T) {
	srv := natstest.Start(t)
	custom := packtrail.InvokerFunc(func(_ context.Context, _ packtrail.Request) (packtrail.Result, error) {
		return packtrail.Result{Status: packtrail.StatusOK, Payload: []byte(`{"x":1}`)}, nil
	})

	s, err := packtrail.New(srv.NC,
		packtrail.WithNamespace("obs4"),
		packtrail.WithFlow([]byte(observeFlow)),
		packtrail.WithInvoker("custom", custom),
	)
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	runCtx, stopRun := context.WithCancel(context.Background())
	defer stopRun()

	go func() { _ = s.Run(runCtx) }()

	watchCtx, cancelWatch := context.WithCancel(context.Background())

	events, err := s.WatchEvents(watchCtx)
	if err != nil {
		t.Fatalf("watch: %v", err)
	}

	// Produce a steady stream of transitions (3 events per execution) while
	// nobody reads: the channel buffer fills and callbacks block in their send.
	for range 30 {
		if _, startErr := s.Start(runCtx, "observe-me", []byte(`{"x":1}`)); startErr != nil {
			t.Fatalf("start: %v", startErr)
		}
	}

	time.Sleep(200 * time.Millisecond) // let events pile up mid-flight
	cancelWatch()

	// The channel must close; draining whatever was buffered must not hang.
	deadline := time.After(5 * time.Second)

	for {
		select {
		case _, ok := <-events:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("event channel never closed after cancel")
		}
	}
}
