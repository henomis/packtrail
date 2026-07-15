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
	"testing"
	"time"

	"github.com/henomis/packtrail/internal/names"
	"github.com/henomis/packtrail/internal/natstest"
	"github.com/henomis/packtrail/internal/signal"
)

// TestConsumeDeadLettersUnparseableSubject: a message on the signals stream
// whose subject lacks the "<exec>.<name>" shape can never be applied. It must
// be Term'd (not Nak-looped) — and it must leave a dead-letter trace via the
// sink rather than vanish silently.
func TestConsumeDeadLettersUnparseableSubject(t *testing.T) {
	ctx := context.Background()
	srv := natstest.Start(t)

	n := names.New("")

	sigs := signal.New(srv.JS, n)
	if err := sigs.EnsureStream(ctx); err != nil {
		t.Fatalf("ensure stream: %v", err)
	}

	type trace struct {
		execID, name, reason string
	}

	traces := make(chan trace, 1)

	cc, err := sigs.Consume(ctx, "bad-subject-test", 10,
		func(execID, name, reason string, _ uint64) {
			traces <- trace{execID, name, reason}
		},
		func(context.Context, signal.Delivery) error {
			t.Error("handler called for an unparseable subject")

			return nil
		})
	if err != nil {
		t.Fatalf("consume: %v", err)
	}

	t.Cleanup(cc.Stop)

	// One token after the prefix — no "<exec>.<name>" dot to split on.
	if _, err = srv.JS.Publish(ctx, n.SubjSignalPrefix+"lonelytoken", []byte(`{}`)); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case tr := <-traces:
		if !strings.Contains(tr.reason, "unparseable") {
			t.Fatalf("reason = %q, want mention of unparseable subject", tr.reason)
		}

		if !strings.Contains(tr.execID, "lonelytoken") {
			t.Fatalf("trace key = %q, want the offending subject", tr.execID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no dead-letter trace for the unparseable signal subject")
	}
}
