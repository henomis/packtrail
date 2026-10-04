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
	"testing"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/worker"
)

const rerunRoute = `
name: route
channels: {mode: {reducer: replace}}
nodes:
  - {id: work, type: task, kind: echo, next: route}
  - id: route
    type: choice
    rules: [{when: "channels.mode == 'more'", to: more}, {default: true, to: $end}]
  - {id: more, type: task, kind: echo}
`

const rerunGate = `
name: gate
nodes:
  - {id: work, type: task, kind: slow, next: hold}
  - {id: hold, type: await, signal: go, timeout: 1h, next: after}
  - {id: after, type: task, kind: echo}
`

// TestRerunRefusesSettledNodes: a node that settled in the decision that
// entered it (a choice, a choice to $end, an await whose signal was buffered)
// cannot be rerun: Rerun fails with ErrInvalidArgument instead of forking
// past it. Its predecessor and a waiting await can be rerun.
func TestRerunRefusesSettledNodes(t *testing.T) {
	e := NewEnv(t, []string{rerunRoute, rerunGate})
	e.Worker("echo", Echo)

	release := make(chan struct{})

	e.Worker("slow", func(context.Context, *worker.Job) (*worker.Result, error) {
		<-release

		return &worker.Result{}, nil
	})

	route := e.Start("route", nil)
	e.Completed(route)

	for _, writes := range []map[string]any{nil, {"mode": "more"}} {
		if _, err := e.Client.Rerun(e.Ctx, route, "route", packtrail.WithForkWrites(writes)); !errors.Is(err,
			packtrail.ErrInvalidArgument) {
			t.Fatalf("rerun of a choice (writes %v): %v, want ErrInvalidArgument", writes, err)
		}
	}

	// The predecessor reruns, and an edit there re-routes the choice.
	fork, err := e.Client.Rerun(e.Ctx, route, "work", packtrail.WithForkWrites(map[string]any{"mode": "more"}))
	if err != nil {
		t.Fatal(err)
	}

	if st := e.Completed(fork); st.LastNode != "more" {
		t.Fatalf("rerun of work with mode=more ended at %s", st.LastNode)
	}

	// The signal arrives while work runs: buffered, consumed on entry.
	gate := e.Start("gate", nil)
	e.Eventually(func() bool {
		st, gerr := e.Client.Get(e.Ctx, gate)

		return gerr == nil && st.Tasks["work"] != nil
	}, func() string { return "work never scheduled" })

	if err = e.Client.Signal(e.Ctx, gate, "go", nil); err != nil {
		t.Fatal(err)
	}

	close(release)
	e.Completed(gate)

	if _, err = e.Client.Rerun(e.Ctx, gate, "hold"); !errors.Is(err, packtrail.ErrInvalidArgument) {
		t.Fatalf("rerun of an await that consumed a buffered signal: %v, want ErrInvalidArgument", err)
	}
}
