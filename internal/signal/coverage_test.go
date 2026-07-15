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

package signal_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/henomis/packtrail/internal/names"
	"github.com/henomis/packtrail/internal/natstest"
	"github.com/henomis/packtrail/internal/signal"
)

// TestConsumeHandlerErrorRedelivers verifies a handler error triggers a
// NakWithDelay and the signal is redelivered until the handler succeeds.
func TestConsumeHandlerErrorRedelivers(t *testing.T) {
	ctx, sigs := setup(t)

	var (
		calls atomic.Int32
		done  = make(chan struct{})
	)

	cc, err := sigs.Consume(ctx, "redeliver", 10, nil, func(_ context.Context, _ signal.Delivery) error {
		if calls.Add(1) == 1 {
			return context.DeadlineExceeded // fail first delivery
		}

		close(done)

		return nil
	})
	if err != nil {
		t.Fatalf("consume: %v", err)
	}

	t.Cleanup(cc.Stop)

	if err = sigs.Publish(ctx, "exec-r", "go", []byte("1")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case <-done:
		if got := calls.Load(); got < 2 {
			t.Fatalf("handler called %d times, want >= 2 (error then redelivery)", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("signal was not redelivered after handler error")
	}
}

func TestConsumeHandlerPanicDeadLetters(t *testing.T) {
	ctx, sigs := setup(t)
	done := make(chan struct{})

	var gotReason string

	cc, err := sigs.Consume(ctx, "panic", 10, func(execID, name, reason string, deliveries uint64) {
		if execID != "exec-p" || name != "go" {
			t.Errorf("dead-letter id = %q/%q, want exec-p/go", execID, name)
		}

		if deliveries == 0 {
			t.Error("dead-letter deliveries = 0, want metadata delivery count")
		}

		gotReason = reason

		close(done)
	}, func(_ context.Context, _ signal.Delivery) error {
		panic("boom")
	})
	if err != nil {
		t.Fatalf("consume: %v", err)
	}

	t.Cleanup(cc.Stop)

	if err = sigs.Publish(ctx, "exec-p", "go", []byte("1")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case <-done:
		if !strings.Contains(gotReason, "boom") {
			t.Fatalf("dead-letter reason = %q, want it to mention the panic", gotReason)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("panic was not dead-lettered")
	}
}

// TestEnsureStreamContextError drives EnsureStream's error-wrapping path with a
// cancelled context.
func TestEnsureStreamContextError(t *testing.T) {
	srv := natstest.Start(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	sigs := signal.New(srv.JS, names.New(""))
	if err := sigs.EnsureStream(ctx); err == nil {
		t.Fatal("EnsureStream with cancelled context succeeded, want error")
	}
}
