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

package scheduler_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/henomis/packtrail/internal/names"
	"github.com/henomis/packtrail/internal/natstest"
	"github.com/henomis/packtrail/internal/scheduler"
)

// TestCronFires installs a 6-field cron that fires every second and verifies the
// firing reaches ConsumeFired with the original key and payload.
func TestCronFires(t *testing.T) {
	ctx, sched, ch := setup(t)

	if err := sched.Cron(ctx, "tick", "exec-cron", "* * * * * *", []byte("beat")); err != nil {
		t.Fatalf("cron: %v", err)
	}

	select {
	case f := <-ch:
		if f.key != "exec-cron" {
			t.Fatalf("key = %q, want exec-cron", f.key)
		}

		if string(f.payload) != "beat" {
			t.Fatalf("payload = %q, want beat", f.payload)
		}
	case <-time.After(fireTimeout):
		t.Fatal("cron schedule did not fire within timeout")
	}
}

// TestConsumeFiredHandlerErrorRedelivers verifies that a handler error triggers a
// NakWithDelay and the message is redelivered until the handler succeeds.
func TestConsumeFiredHandlerErrorRedelivers(t *testing.T) {
	ctx := context.Background()
	srv := natstest.Start(t)

	sched, err := scheduler.New(ctx, srv.JS, names.New(""))
	if err != nil {
		t.Fatalf("scheduler: %v", err)
	}

	var (
		calls atomic.Int32
		done  = make(chan struct{})
	)

	cc, err := sched.ConsumeFired(ctx, "test-fired-err", func(string, []byte) error {
		// Fail the first delivery (Nak), succeed on redelivery (Ack).
		if calls.Add(1) == 1 {
			return context.DeadlineExceeded
		}

		close(done)

		return nil
	})
	if err != nil {
		t.Fatalf("consume fired: %v", err)
	}

	t.Cleanup(cc.Stop)

	if err := sched.After(ctx, "exec-err", time.Second, []byte("x")); err != nil {
		t.Fatalf("after: %v", err)
	}

	select {
	case <-done:
		if got := calls.Load(); got < 2 {
			t.Fatalf("handler called %d times, want >= 2 (error then redelivery)", got)
		}
	case <-time.After(fireTimeout):
		t.Fatal("message was not redelivered after handler error")
	}
}

// TestNewContextError drives New's error-wrapping path with a cancelled context.
func TestNewContextError(t *testing.T) {
	srv := natstest.Start(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := scheduler.New(ctx, srv.JS, names.New("")); err == nil {
		t.Fatal("New with cancelled context succeeded, want error")
	}
}
