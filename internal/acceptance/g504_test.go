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

package acceptance

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/worker"
)

// TestG504ProgressStreams: workers publish progress while they run (a task,
// then the items of a map); a client subscribed before the start receives
// every message in order per job, and the stream closes when the execution
// finishes. Nothing is stored (G5-04).
func TestG504ProgressStreams(t *testing.T) {
	e := NewEnv(t, []string{`
name: stream
nodes:
  - {id: write, type: task, kind: streamer, next: each}
  - {id: each, type: map, kind: streamer, over: input.items}
`})

	e.Worker("streamer", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		n := 3
		if j.Node == "each" {
			n = 1
		}

		for i := 1; i <= n; i++ {
			if err := j.Progress(map[string]any{"node": j.Node, "i": i}); err != nil {
				return nil, err
			}
		}

		return &worker.Result{Output: map[string]any{"done": true}}, nil
	})

	const id = "progress-1"

	ch, err := e.Client.Progress(e.Ctx, id)
	if err != nil {
		t.Fatal(err)
	}

	e.Start("stream", map[string]any{"items": []int{10, 20}}, packtrail.WithExecutionID(id))

	var got []packtrail.Progress

	timeout := time.After(10 * time.Second)

loop:
	for {
		select {
		case p, ok := <-ch:
			if !ok {
				break loop
			}

			got = append(got, p)
		case <-timeout:
			t.Fatalf("stream did not close; got %d messages", len(got))
		}
	}

	byKey := map[string][]packtrail.Progress{}
	for _, p := range got {
		byKey[p.Key] = append(byKey[p.Key], p)
	}

	if len(got) != 5 || len(byKey["write"]) != 3 || len(byKey["each#0"]) != 1 || len(byKey["each#1"]) != 1 {
		t.Fatalf("messages by key: %v", keysOf(byKey))
	}

	for i, p := range byKey["write"] {
		want := fmt.Sprintf(`{"i":%d,"node":"write"}`, i+1)
		if p.Seq != int64(i+1) || p.Node != "write" || p.ExecID != id || p.Attempt != 1 || string(p.Data) != want {
			t.Fatalf("write message %d: %+v", i, p)
		}
	}

	if st := e.Completed(id); st.Status != packtrail.StatusCompleted {
		t.Fatal(st.Status)
	}

	evs, err := e.Client.History(e.Ctx, id)
	if err != nil {
		t.Fatal(err)
	}

	for _, ev := range evs {
		if b, _ := json.Marshal(ev.Data); json.Valid(b) && containsProgress(b) {
			t.Fatalf("progress leaked into the log: %s %s", ev.Type, b)
		}
	}
}

// TestG504ProgressEndsWithContext: the stream also closes when its context
// ends, while the execution keeps running (G5-04).
func TestG504ProgressEndsWithContext(t *testing.T) {
	e := NewEnv(t, []string{`
name: parked
nodes:
  - {id: wait, type: await, signal: go, timeout: 1h}
`})

	id := e.Start("parked", nil)
	e.WaitStatus(id, packtrail.StatusWaiting)

	ctx, cancel := context.WithCancel(e.Ctx)

	ch, err := e.Client.Progress(ctx, id)
	if err != nil {
		t.Fatal(err)
	}

	cancel()

	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("unexpected message")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stream did not close with its context")
	}
}

func keysOf(m map[string][]packtrail.Progress) map[string]int {
	out := map[string]int{}
	for k, v := range m {
		out[k] = len(v)
	}

	return out
}

func containsProgress(b []byte) bool {
	var v map[string]any
	if json.Unmarshal(b, &v) != nil {
		return false
	}

	_, ok := v["i"]

	return ok
}
