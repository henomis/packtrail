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

// TestHistoryTrace: with WithHistory enabled, an execution's step-by-step
// transition trace is durably queryable — ordered oldest-first, ending in the
// terminal status — independent of the short-retention events stream.
func TestHistoryTrace(t *testing.T) {
	srv := natstest.Start(t)

	s, err := packtrail.New(srv.NC,
		packtrail.WithNamespace("hist"),
		packtrail.WithFlow([]byte(observeFlow)),
		packtrail.WithInvoker("custom", okInvoker()),
		packtrail.WithHistory(24*time.Hour),
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

	trace, err := s.History(ctx, id, 0)
	if err != nil {
		t.Fatalf("history: %v", err)
	}

	// observe-me runs a → route → b (input.x == 1): at least the start record
	// and the terminal record, ordered, all for this execution.
	if len(trace) < 3 {
		t.Fatalf("trace has %d records, want >= 3 (start, transitions, terminal): %+v", len(trace), trace)
	}

	for i, ev := range trace {
		if ev.ExecID != id {
			t.Fatalf("record %d belongs to %q, want %q", i, ev.ExecID, id)
		}

		if i > 0 && ev.Revision < trace[i-1].Revision {
			t.Fatalf("trace out of order at %d: revision %d after %d", i, ev.Revision, trace[i-1].Revision)
		}
	}

	if first, last := trace[0], trace[len(trace)-1]; first.Node != "a" || last.Status != packtrail.ExecCompleted {
		t.Fatalf("trace endpoints = start@%q … %q, want start@a … completed", first.Node, last.Status)
	}

	// A different execution's trace is not mixed in.
	other, err := s.History(ctx, "no-such-exec", 0)
	if err != nil || len(other) != 0 {
		t.Fatalf("history of unknown id = %v, %v; want empty", other, err)
	}
}

// TestHistoryDisabled: without WithHistory, History returns nothing (and no
// history stream is created).
func TestHistoryDisabled(t *testing.T) {
	srv := natstest.Start(t)

	s, err := packtrail.New(srv.NC,
		packtrail.WithNamespace("nohist"),
		packtrail.WithFlow([]byte(observeFlow)),
		packtrail.WithInvoker("custom", okInvoker()),
	)
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	ctx := context.Background()

	trace, err := s.History(ctx, "whatever", 0)
	if err != nil || len(trace) != 0 {
		t.Fatalf("history disabled = %v, %v; want empty, nil", trace, err)
	}
}
