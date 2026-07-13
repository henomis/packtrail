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
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail/internal/dsl"
	"github.com/henomis/packtrail/internal/names"
	"github.com/henomis/packtrail/internal/natstest"
	"github.com/henomis/packtrail/internal/scheduler"
	"github.com/henomis/packtrail/internal/store"
	"github.com/henomis/packtrail/invoker"
)

// hbMsg satisfies jetstream.Msg for the heartbeat's InProgress call without
// touching NATS (the test severs the connection deliberately). Any other
// method panics via the nil embedded interface — the heartbeat must not use
// them.
type hbMsg struct{ jetstream.Msg }

func (hbMsg) InProgress() error { return nil }

type hbPanicMsg struct{ jetstream.Msg }

func (hbPanicMsg) InProgress() error { panic("boom in heartbeat InProgress") }

// TestHeartbeatAbortsOnPersistentRenewalErrors: when lease renewal keeps
// erroring (NATS unreachable) for a full TTL, the lease may have expired and
// been taken over while this instance runs blind. The heartbeat must assume
// the lease lost and abort processing, not keep running with no lease
// protection at all.
func TestHeartbeatAbortsOnPersistentRenewalErrors(t *testing.T) {
	ctx := context.Background()
	srv := natstest.Start(t)

	st, err := store.Open(ctx, srv.JS, names.New(""))
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	sch := scheduler.New(srv.JS, names.New(""))
	if err = sch.EnsureStream(ctx); err != nil {
		t.Fatalf("scheduler: %v", err)
	}

	flow, err := dsl.Parse([]byte(signalRedriveFlow))
	if err != nil {
		t.Fatalf("flow: %v", err)
	}

	inv := invoker.Func(func(context.Context, invoker.Request) (invoker.Result, error) {
		return invoker.Result{Status: invoker.StatusOK}, nil
	})

	eng, err := New(inv, st, sch, testSignals(t, st), map[string]*dsl.Flow{flow.Name: flow},
		Config{LeaseTTL: 300 * time.Millisecond})
	if err != nil {
		t.Fatalf("engine: %v", err)
	}

	// Sever the connection: every renewal from here on errors.
	srv.NC.Close()

	procCtx, onLost := context.WithCancel(ctx)
	t.Cleanup(onLost)

	hbCtx, stopHB := context.WithCancel(ctx)
	t.Cleanup(stopHB)

	go eng.heartbeat(hbCtx, onLost, hbMsg{}, "hb-exec")

	// Three ticks of 100ms must trip the abort; give generous slack.
	select {
	case <-procCtx.Done():
		// aborted as required
	case <-time.After(3 * time.Second):
		t.Fatal("heartbeat kept running through persistent renewal errors (double-fire window unbounded)")
	}
}

// TestHeartbeatRecoversInProgressPanic: a panic from msg.InProgress must not
// crash the engine process; heartbeat recovers and aborts processing via onLost.
func TestHeartbeatRecoversInProgressPanic(t *testing.T) {
	ctx := context.Background()
	srv := natstest.Start(t)

	st, err := store.Open(ctx, srv.JS, names.New(""))
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	sch := scheduler.New(srv.JS, names.New(""))
	if err = sch.EnsureStream(ctx); err != nil {
		t.Fatalf("scheduler: %v", err)
	}

	flow, err := dsl.Parse([]byte(signalRedriveFlow))
	if err != nil {
		t.Fatalf("flow: %v", err)
	}

	inv := invoker.Func(func(context.Context, invoker.Request) (invoker.Result, error) {
		return invoker.Result{Status: invoker.StatusOK}, nil
	})

	eng, err := New(inv, st, sch, testSignals(t, st), map[string]*dsl.Flow{flow.Name: flow},
		Config{LeaseTTL: 300 * time.Millisecond})
	if err != nil {
		t.Fatalf("engine: %v", err)
	}

	procCtx, onLost := context.WithCancel(ctx)
	t.Cleanup(onLost)

	hbCtx, stopHB := context.WithCancel(ctx)
	t.Cleanup(stopHB)

	go eng.heartbeat(hbCtx, onLost, hbPanicMsg{}, "hb-exec")

	select {
	case <-procCtx.Done():
		// recovered and aborted as required
	case <-time.After(3 * time.Second):
		t.Fatal("heartbeat did not recover from InProgress panic")
	}
}
