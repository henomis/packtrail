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

	"github.com/henomis/packtrail/internal/dsl"
	"github.com/henomis/packtrail/internal/names"
	"github.com/henomis/packtrail/internal/natstest"
	"github.com/henomis/packtrail/internal/scheduler"
	"github.com/henomis/packtrail/internal/store"
	"github.com/henomis/packtrail/invoker"
)

// TestMaxDeliverClampsNonPositive: the dead-letter discipline promises no
// message loops forever, so a negative (or zero) MaxDeliver must clamp to the
// default instead of silently disabling the cap.
func TestMaxDeliverClampsNonPositive(t *testing.T) {
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

	inv := invoker.Func(func(context.Context, invoker.Request) (invoker.Result, error) {
		return invoker.Result{Status: invoker.StatusOK}, nil
	})

	for _, n := range []int{-1, 0} {
		eng, newErr := New(inv, st, sch, testSignals(t, st), map[string]*dsl.Flow{}, Config{MaxDeliver: n})
		if newErr != nil {
			t.Fatalf("engine (MaxDeliver=%d): %v", n, newErr)
		}

		if eng.cfg.MaxDeliver != defaultMaxDeliver {
			t.Fatalf("MaxDeliver=%d clamped to %d, want %d", n, eng.cfg.MaxDeliver, defaultMaxDeliver)
		}
	}
}

// TestLeaseTTLClampsNonPositive ensures invalid/non-positive lease TTL values
// fall back to the safe default instead of reaching ticker setup unchanged.
func TestLeaseTTLClampsNonPositive(t *testing.T) {
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

	inv := invoker.Func(func(context.Context, invoker.Request) (invoker.Result, error) {
		return invoker.Result{Status: invoker.StatusOK}, nil
	})

	for _, d := range []time.Duration{-1 * time.Second, 0} {
		eng, newErr := New(inv, st, sch, testSignals(t, st), map[string]*dsl.Flow{}, Config{LeaseTTL: d})
		if newErr != nil {
			t.Fatalf("engine (LeaseTTL=%v): %v", d, newErr)
		}

		if eng.cfg.LeaseTTL != defaultLeaseTTL {
			t.Fatalf("LeaseTTL=%v clamped to %v, want %v", d, eng.cfg.LeaseTTL, defaultLeaseTTL)
		}
	}
}

// TestMaxConcurrencyClampsNonPositive ensures invalid/non-positive concurrency
// values fall back to default and never reach channel allocation unchanged.
func TestMaxConcurrencyClampsNonPositive(t *testing.T) {
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

	inv := invoker.Func(func(context.Context, invoker.Request) (invoker.Result, error) {
		return invoker.Result{Status: invoker.StatusOK}, nil
	})

	for _, n := range []int{-1, 0} {
		eng, newErr := New(inv, st, sch, testSignals(t, st), map[string]*dsl.Flow{}, Config{MaxConcurrency: n})
		if newErr != nil {
			t.Fatalf("engine (MaxConcurrency=%d): %v", n, newErr)
		}

		if eng.cfg.MaxConcurrency != defaultConcurrency {
			t.Fatalf("MaxConcurrency=%d clamped to %d, want %d", n, eng.cfg.MaxConcurrency, defaultConcurrency)
		}
	}
}
