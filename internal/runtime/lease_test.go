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

// TestLostLeaseAbortsProcessing verifies the heartbeat's lost-lease detection:
// when this instance's lease is taken over mid-invocation (simulated by writing a
// foreign, unexpired lease), the heartbeat cancels the processing context so the
// in-flight invocation aborts — narrowing the at-least-once double-fire window.
func TestLostLeaseAbortsProcessing(t *testing.T) {
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

	const flowYAML = `
name: lease
nodes:
  - {id: a, type: task, invoker: block, target: t}
`

	flow, err := dsl.Parse([]byte(flowYAML))
	if err != nil {
		t.Fatalf("flow: %v", err)
	}

	started := make(chan struct{}, 1)
	cancelled := make(chan struct{}, 1)

	reg := invoker.NewRegistry()
	if err = reg.Register("block", invoker.Func(func(ic context.Context, _ invoker.Request) (invoker.Result, error) {
		select {
		case started <- struct{}{}:
		default:
		}

		<-ic.Done() // block until processing is cancelled (lost lease) or times out

		select {
		case cancelled <- struct{}{}:
		default:
		}

		return invoker.Result{Status: invoker.StatusError, Error: "aborted"}, ic.Err()
	})); err != nil {
		t.Fatalf("register invoker: %v", err)
	}

	eng, err := New(reg, st, sch, testSignals(t, st), map[string]*dsl.Flow{flow.Name: flow},
		Config{LeaseTTL: 200 * time.Millisecond, AckWait: 2 * time.Second})
	if err != nil {
		t.Fatalf("engine: %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)

	go func() { _ = eng.Run(runCtx) }()

	id, err := eng.Start(ctx, "lease", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("invocation never started")
	}

	// Steal the lease: write a foreign, unexpired owner directly into the leases
	// bucket. The next heartbeat renewal sees it and must cancel processing.
	kv, err := st.JS().KeyValue(ctx, n.BucketLeases)
	if err != nil {
		t.Fatalf("leases kv: %v", err)
	}

	foreign, err := json.Marshal(store.Lease{Owner: "thief", Expires: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatalf("marshal lease: %v", err)
	}

	if _, putErr := kv.Put(ctx, id, foreign); putErr != nil {
		t.Fatalf("steal lease: %v", putErr)
	}

	select {
	case <-cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("processing was not aborted after the lease was lost")
	}
}
