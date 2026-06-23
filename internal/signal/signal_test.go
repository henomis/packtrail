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
	"testing"
	"time"

	"github.com/henomis/packtrail/internal/names"
	"github.com/henomis/packtrail/internal/natstest"
	"github.com/henomis/packtrail/internal/signal"
)

const deliverTimeout = 5 * time.Second

// setup starts an embedded server and a Signals with its stream ensured.
func setup(t *testing.T) (context.Context, *signal.Signals) {
	t.Helper()

	ctx := context.Background()
	srv := natstest.Start(t)

	sigs := signal.New(srv.JS, names.New(""))
	if err := sigs.EnsureStream(ctx); err != nil {
		t.Fatalf("ensure stream: %v", err)
	}

	return ctx, sigs
}

// consume starts a consumer that forwards every delivery onto the returned
// channel.
func consume(ctx context.Context, t *testing.T, sigs *signal.Signals, durable string) <-chan signal.Delivery {
	t.Helper()

	ch := make(chan signal.Delivery, 8)

	cc, err := sigs.Consume(ctx, durable, func(_ context.Context, d signal.Delivery) error {
		ch <- d
		return nil
	})
	if err != nil {
		t.Fatalf("consume: %v", err)
	}

	t.Cleanup(cc.Stop)

	return ch
}

// TestPublishConsumeRoundTrip verifies a published signal is delivered with its
// execution id, name, payload and a non-zero stream sequence.
func TestPublishConsumeRoundTrip(t *testing.T) {
	ctx, sigs := setup(t)
	ch := consume(ctx, t, sigs, "test-sig")

	if err := sigs.Publish(ctx, "exec-1", "approval", []byte(`{"ok":true}`)); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case d := <-ch:
		if d.ExecID != "exec-1" || d.Name != "approval" {
			t.Fatalf("delivery = %+v, want exec-1/approval", d)
		}

		if string(d.Payload) != `{"ok":true}` {
			t.Fatalf("payload = %s, want {\"ok\":true}", d.Payload)
		}

		if d.Seq == 0 {
			t.Fatal("expected a non-zero stream sequence for idempotency")
		}
	case <-time.After(deliverTimeout):
		t.Fatal("signal not delivered within timeout")
	}
}

// TestSequencesAreMonotonic verifies two signals to the same execution arrive
// with strictly increasing stream sequences (the basis for idempotent apply).
func TestSequencesAreMonotonic(t *testing.T) {
	ctx, sigs := setup(t)
	ch := consume(ctx, t, sigs, "test-seq")

	if err := sigs.Publish(ctx, "exec-2", "go", []byte("1")); err != nil {
		t.Fatalf("publish 1: %v", err)
	}

	if err := sigs.Publish(ctx, "exec-2", "go", []byte("2")); err != nil {
		t.Fatalf("publish 2: %v", err)
	}

	first := recv(t, ch)
	second := recv(t, ch)

	if second.Seq <= first.Seq {
		t.Fatalf("sequences not increasing: first=%d second=%d", first.Seq, second.Seq)
	}
}

// TestSubject verifies the subject layout used to route signals.
func TestSubject(t *testing.T) {
	_, sigs := setup(t)

	if got := sigs.Subject("exec-3", "approval"); got == "" {
		t.Fatal("Subject returned empty string")
	}
}

func recv(t *testing.T, ch <-chan signal.Delivery) signal.Delivery {
	t.Helper()

	select {
	case d := <-ch:
		return d
	case <-time.After(deliverTimeout):
		t.Fatal("signal not delivered within timeout")
		return signal.Delivery{}
	}
}
