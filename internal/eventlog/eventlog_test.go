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

package eventlog

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/henomis/packtrail/event"
	"github.com/henomis/packtrail/internal/blob"
	"github.com/henomis/packtrail/internal/infra"
	"github.com/henomis/packtrail/internal/names"
	"github.com/henomis/packtrail/internal/natstest"
)

func newLog(t *testing.T) (*Log, context.Context) {
	t.Helper()

	s := natstest.Start(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	in, err := infra.New(s.NC, names.New(""), nil)
	if err != nil {
		t.Fatal(err)
	}

	if err = in.Provision(ctx, 1); err != nil {
		t.Fatal(err)
	}

	return New(in), ctx
}

func entered(from, n int) []event.Event {
	evs := make([]event.Event, n)
	for i := range evs {
		evs[i] = event.New(event.NodeEntered, &event.Entered{Node: "n"})
		evs[i].Index = from + i
		evs[i].CmdID = "c"
	}

	return evs
}

// TestDecisionIsOneMessage: a decision is stored as one message (G5-08); its
// events share the message's sequence and only the last ends the decision.
// The expected-sequence check still makes the log single-writer.
func TestDecisionIsOneMessage(t *testing.T) {
	l, ctx := newLog(t)

	seq1, err := l.Append(ctx, "x", 0, entered(1, 3))
	if err != nil {
		t.Fatal(err)
	}

	seq2, err := l.Append(ctx, "x", seq1, entered(4, 2))
	if err != nil {
		t.Fatal(err)
	}

	if seq2 != seq1+1 {
		t.Fatalf("two decisions took sequences %d and %d: not one message each", seq1, seq2)
	}

	if _, err = l.Append(ctx, "x", seq1, entered(6, 1)); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale expected sequence: %v, want ErrConflict", err)
	}

	evs, err := l.Read(ctx, "x", 1, 0)
	if err != nil {
		t.Fatal(err)
	}

	if len(evs) != 5 {
		t.Fatalf("read %d events, want 5", len(evs))
	}

	for i, ev := range evs {
		wantSeq, wantEnd := seq1, i == 2
		if i >= 3 {
			wantSeq, wantEnd = seq2, i == 4
		}

		if ev.Index != i+1 || ev.Seq != wantSeq || ev.DecisionEnd != wantEnd {
			t.Fatalf("event %d: index %d seq %d end %v", i, ev.Index, ev.Seq, ev.DecisionEnd)
		}
	}

	var decisions [][]event.Event

	if err = l.ScanDecisions(ctx, "x", seq2, func(d []event.Event) bool {
		decisions = append(decisions, d)

		return true
	}); err != nil {
		t.Fatal(err)
	}

	if len(decisions) != 1 || len(decisions[0]) != 2 || decisions[0][0].Index != 4 {
		t.Fatalf("scan from the second decision: %+v", decisions)
	}

	if _, err = l.Append(ctx, "y", 0, entered(1, MaxEvents+1)); err == nil {
		t.Fatal("a decision over MaxEvents was appended")
	}
}

// TestLargeDecisionIsClaimChecked: a decision larger than a NATS message is
// stored as one claim-checked message and read back whole.
func TestLargeDecisionIsClaimChecked(t *testing.T) {
	l, ctx := newLog(t)

	big, err := json.Marshal(map[string]string{"blob": strings.Repeat("x", 2<<20)})
	if err != nil {
		t.Fatal(err)
	}

	start := event.New(event.ExecutionStarted, &event.Started{ExecID: "x", Flow: "f", FlowHash: "h", Input: big})
	start.Index = 1

	evs := append([]event.Event{start}, entered(2, 2)...)

	seq, err := l.Append(ctx, "x", 0, evs)
	if err != nil {
		t.Fatal(err)
	}

	s, err := l.events(ctx)
	if err != nil {
		t.Fatal(err)
	}

	m, err := s.GetMsg(ctx, seq)
	if err != nil {
		t.Fatal(err)
	}

	if m.Header.Get(blob.Header) == "" || len(m.Data) != 0 {
		t.Fatalf("a %d-byte decision was not claim-checked", len(big))
	}

	if got := event.Types(m.Header); len(got) != 3 || got[0] != event.ExecutionStarted {
		t.Fatalf("types header %v", got)
	}

	got, err := l.Read(ctx, "x", 1, 0)
	if err != nil {
		t.Fatal(err)
	}

	if len(got) != 3 || !got[2].DecisionEnd {
		t.Fatalf("read %d events", len(got))
	}

	if d, ok := got[0].Data.(*event.Started); !ok || len(d.Input) != len(big) {
		t.Fatal("claim-checked input not restored")
	}
}
