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

// TestRedriveStalledWiring smoke-tests the public watchdog entry point: with
// the reconcile-active schedule and stall re-drive configured, a healthy
// deployment runs flows normally and a manual RedriveStalled pass over the
// index finds nothing to re-drive. (The stranded-state behavior itself is
// covered deterministically in internal/runtime/watchdog_test.go, where the
// store is reachable to manufacture stranded documents.)
func TestRedriveStalledWiring(t *testing.T) {
	srv := natstest.Start(t)

	custom := packtrail.InvokerFunc(func(_ context.Context, _ packtrail.Request) (packtrail.Result, error) {
		return packtrail.Result{Status: packtrail.StatusOK, Payload: []byte(`{"x":1}`)}, nil
	})

	s, err := packtrail.New(srv.NC,
		packtrail.WithNamespace("redrive"),
		packtrail.WithFlow([]byte(observeFlow)),
		packtrail.WithInvoker("custom", custom),
		packtrail.WithReconcileActive("* * * * * *"),
		packtrail.WithStallRedrive(100*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = s.Run(ctx) }()

	id, err := s.Start(ctx, "observe-me", []byte(`{"x":1}`))
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		ex, getErr := s.Get(ctx, id)
		if getErr == nil && ex.Status == packtrail.ExecCompleted {
			break
		}

		time.Sleep(20 * time.Millisecond)
	}

	// The flow completed and reconcile-active passes (with the watchdog) have
	// been firing every second throughout; a manual pass finds nothing stalled.
	redriven, err := s.RedriveStalled(ctx)
	if err != nil {
		t.Fatalf("redrive stalled: %v", err)
	}

	if redriven != 0 {
		t.Fatalf("redriven = %d, want 0 on a healthy deployment", redriven)
	}
}
