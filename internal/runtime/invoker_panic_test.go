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

package runtime

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/henomis/packtrail/internal/dsl"
	"github.com/henomis/packtrail/internal/names"
	"github.com/henomis/packtrail/internal/natstest"
	"github.com/henomis/packtrail/internal/scheduler"
	"github.com/henomis/packtrail/internal/store"
	"github.com/henomis/packtrail/invoker"
)

const panicFlow = `
name: panicflow
nodes:
  - {id: a, type: task, invoker: boom, target: x}
`

// TestSyncInvokerPanicFailsExecution verifies that a panic in a synchronous
// Invoker is recovered at e.invoke and fails just that execution (StatusError),
// rather than propagating up the work-consumer goroutine and crashing the engine.
// If the recover were absent the panic would take down the test process.
func TestSyncInvokerPanicFailsExecution(t *testing.T) {
	ctx := context.Background()
	srv := natstest.Start(t)
	n := names.New("")

	st, err := store.Open(ctx, srv.JS, n)
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	sch := scheduler.New(srv.JS, n)
	if err = sch.EnsureStream(ctx); err != nil {
		t.Fatalf("scheduler: %v", err)
	}

	flow, err := dsl.Parse([]byte(panicFlow))
	if err != nil {
		t.Fatalf("flow: %v", err)
	}

	reg := invoker.NewRegistry()
	reg.Register("boom", invoker.Func(func(context.Context, invoker.Request) (invoker.Result, error) {
		panic("boom in sync invoker")
	}))

	eng, err := New(reg, st, sch, testSignals(t, st), map[string]*dsl.Flow{flow.Name: flow}, Config{})
	if err != nil {
		t.Fatalf("engine: %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)

	go func() { _ = eng.Run(runCtx) }()

	id, err := eng.Start(ctx, "panicflow", json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ex, gErr := st.Get(ctx, id)
		if gErr == nil && ex.Status == store.StatusFailed {
			if ex.Error == "" {
				t.Fatal("expected a failure reason from the recovered panic")
			}

			return
		}

		time.Sleep(20 * time.Millisecond)
	}

	t.Fatal("execution did not fail within timeout (panic may have crashed the engine goroutine)")
}
