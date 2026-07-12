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

	sched := scheduler.New(srv.JS, names.New(""))
	if err := sched.EnsureStream(ctx); err != nil {
		t.Fatalf("scheduler: %v", err)
	}

	var (
		calls atomic.Int32
		done  = make(chan struct{})
	)

	cc, err := sched.ConsumeFired(ctx, "test-fired-err", 10, nil, func(string, []byte) error {
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

	if afterErr := sched.After(ctx, "exec-err", time.Second, []byte("x")); afterErr != nil {
		t.Fatalf("after: %v", afterErr)
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

// terminalError is a non-retryable handler error. ConsumeFired detects it
// structurally (interface{ Terminal() bool }) — mirroring the runtime engine's
// terminalError for a cron start of a removed flow — and Terms it.
type terminalError struct{}

func (terminalError) Error() string  { return "terminal" }
func (terminalError) Terminal() bool { return true }

// TestConsumeFiredTerminalDeadLetters verifies a terminal handler error is Term'd
// on the first delivery (not redelivered): the handler is invoked exactly once.
func TestConsumeFiredTerminalDeadLetters(t *testing.T) {
	ctx := context.Background()
	srv := natstest.Start(t)

	sched := scheduler.New(srv.JS, names.New(""))
	if err := sched.EnsureStream(ctx); err != nil {
		t.Fatalf("scheduler: %v", err)
	}

	var calls atomic.Int32

	cc, err := sched.ConsumeFired(ctx, "test-fired-terminal", 10, nil, func(string, []byte) error {
		calls.Add(1)

		return terminalError{}
	})
	if err != nil {
		t.Fatalf("consume fired: %v", err)
	}

	t.Cleanup(cc.Stop)

	if afterErr := sched.After(ctx, "exec-term", time.Second, []byte("x")); afterErr != nil {
		t.Fatalf("after: %v", afterErr)
	}

	deadline := time.After(5 * time.Second)

	for calls.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("fired schedule never reached the handler")
		case <-time.After(50 * time.Millisecond):
		}
	}

	time.Sleep(2 * time.Second) // longer than firedNakDelay; a Nak would redeliver

	if got := calls.Load(); got != 1 {
		t.Fatalf("handler called %d times, want 1 (terminal error dead-lettered, not redelivered)", got)
	}
}

// TestEnsureStreamContextError drives EnsureStream's error-wrapping path with a
// cancelled context (New itself performs no I/O and cannot fail).
func TestEnsureStreamContextError(t *testing.T) {
	srv := natstest.Start(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := scheduler.New(srv.JS, names.New("")).EnsureStream(ctx); err == nil {
		t.Fatal("EnsureStream with cancelled context succeeded, want error")
	}
}
