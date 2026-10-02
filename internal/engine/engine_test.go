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

	if _, err = e.apply(ctx, c, ""); err != nil {
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
