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
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/internal/natstest"
	"github.com/henomis/packtrail/pkg/protocol"
)

// historyFlow loops work → gate → retry → work three times.
const historyFlow = `
name: history
nodes:
  - {id: start, type: task, subject: "tasks.hstart.{execution_id}"}
  - {id: work, type: task, subject: "tasks.hwork.{execution_id}"}
  - id: gate
    type: choice
    on_error: fail
    rules:
      - {when: 'visits.work >= 3', to: done}
      - {default: true, to: retry}
  - {id: retry, type: task, subject: "tasks.hretry.{execution_id}"}
  - {id: done, type: task, subject: "tasks.hdone.{execution_id}"}
edges:
  - {from: start, to: work}
  - {from: work, to: gate}
  - {from: retry, to: work}
`

// Results holds one output per node, so a loop's earlier attempts were
// overwritten — and "why did this run three times?" is answered by exactly
// those. They were always on disk; nothing could read them back.
func TestOutputHistoryReturnsEveryVisit(t *testing.T) {
	srv := natstest.Start(t)
	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	engine, err := packtrail.New(srv.NC,
		packtrail.WithNamespace("hist"),
		packtrail.WithFlow([]byte(historyFlow)),
	)
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	attempt := 0

	if err = engine.Handle(ctx, "tasks.hwork.*",
		func(_ context.Context, _ protocol.TaskRequest) (protocol.TaskResponse, error) {
			attempt++

			return protocol.TaskResponse{
				Status:  protocol.StatusOK,
				Payload: json.RawMessage(`{"attempt":` + strconv.Itoa(attempt) + `}`),
			}, nil
		}); err != nil {
		t.Fatalf("handle work: %v", err)
	}

	for _, subject := range []string{"tasks.hstart.*", "tasks.hretry.*", "tasks.hdone.*"} {
		if err = engine.Handle(ctx, subject,
			func(_ context.Context, _ protocol.TaskRequest) (protocol.TaskResponse, error) {
				return protocol.TaskResponse{Status: protocol.StatusOK, Payload: json.RawMessage(`{}`)}, nil
			}); err != nil {
			t.Fatalf("handle %s: %v", subject, err)
		}
	}

	go func() { _ = engine.Run(ctx) }()

	id, err := engine.Start(ctx, "history", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	waitFor(t, func() bool {
		ex, getErr := engine.Get(ctx, id)

		return getErr == nil && ex.Status == packtrail.ExecCompleted
	})

	history, err := engine.OutputHistory(ctx, id, "work")
	if err != nil {
		t.Fatalf("output history: %v", err)
	}

	if len(history) != 3 {
		t.Fatalf("history has %d entries, want 3 (one per visit)", len(history))
	}

	// Oldest first, so the loop reads as it ran.
	for i, rec := range history {
		want := `{"attempt":` + strconv.Itoa(i+1) + `}`
		if string(rec.Payload) != want {
			t.Errorf("entry %d = %s, want %s", i, rec.Payload, want)
		}

		if rec.Node != "work" {
			t.Errorf("entry %d node = %q, want work", i, rec.Node)
		}

		if rec.At.IsZero() {
			t.Errorf("entry %d has no timestamp", i)
		}
	}

	// Exactly one is the version the flow committed, and it is the last.
	current := 0

	for _, rec := range history {
		if rec.Current {
			current++
		}
	}

	if current != 1 || !history[len(history)-1].Current {
		t.Errorf("current entries = %d (last current: %v), want exactly the newest",
			current, history[len(history)-1].Current)
	}

	// Results still shows only the committed one, unchanged.
	doc, err := engine.Results(ctx, id)
	if err != nil {
		t.Fatalf("results: %v", err)
	}

	in, err := packtrail.DecodeContext(doc)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if string(in.Results["work"]) != `{"attempt":3}` {
		t.Errorf("results[work] = %s, want the last attempt", in.Results["work"])
	}
}

func TestOutputHistoryRejectsBadIdentifiers(t *testing.T) {
	srv := natstest.Start(t)

	engine, err := packtrail.New(srv.NC, packtrail.WithNamespace("hist2"))
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	if _, err = engine.OutputHistory(context.Background(), "bad id", "work"); err == nil ||
		!strings.Contains(err.Error(), "execution id") {
		t.Errorf("err = %v, want a rejected execution id", err)
	}

	if _, err = engine.OutputHistory(context.Background(), "exec-1", "bad node"); err == nil ||
		!strings.Contains(err.Error(), "node id") {
		t.Errorf("err = %v, want a rejected node id", err)
	}
}

// A node that never ran has no history, which is not an error.
func TestOutputHistoryOfAnUnvisitedNode(t *testing.T) {
	srv := natstest.Start(t)

	engine, err := packtrail.New(srv.NC, packtrail.WithNamespace("hist3"))
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	history, err := engine.OutputHistory(context.Background(), "exec-nothing", "work")
	if err != nil {
		t.Fatalf("output history: %v", err)
	}

	if len(history) != 0 {
		t.Errorf("history = %v, want empty", history)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}

		time.Sleep(20 * time.Millisecond)
	}

	t.Fatal("condition not met in time")
}
