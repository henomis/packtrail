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
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/event"
	"github.com/henomis/packtrail/internal/blob"
	"github.com/henomis/packtrail/worker"
)

// TestReviewG508TransientFailureInsideADecision: the dispatcher fails after
// running part of a decision's effects (the fan-out's first job is out, the
// second needs the result-cache bucket, which is gone). The whole decision is
// retried once the bucket is back: the execution completes, nothing is
// quarantined, and the job already published does not run twice (G5-08).
func TestReviewG508TransientFailureInsideADecision(t *testing.T) {
	e := NewEnv(t, []string{`
name: packed
nodes:
  - {id: split, type: fanout, branches: [a, b], next: join}
  - {id: a, type: task, kind: plain}
  - {id: b, type: task, kind: plain, cache: {ttl: 1h}}
  - {id: join, type: join, policy: all}
`})

	var (
		mu   sync.Mutex
		runs = map[string]int{}
	)

	e.Worker("plain", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		mu.Lock()
		runs[j.Node]++
		mu.Unlock()

		return &worker.Result{Output: map[string]any{"node": j.Node}}, nil
	})

	if err := e.S.JS.DeleteKeyValue(e.Ctx, "packtrail-cache"); err != nil {
		t.Fatal(err)
	}

	id := e.Start("packed", nil)

	// The first job of the decision ran; the decision itself is stuck.
	e.Eventually(func() bool {
		mu.Lock()
		defer mu.Unlock()

		return runs["a"] == 1
	}, func() string { return "branch a never ran" })

	time.Sleep(3 * time.Second) // a few failed deliveries

	mu.Lock()
	b := runs["b"]
	mu.Unlock()

	if b != 0 {
		t.Fatal("branch b ran without its cache bucket")
	}

	if err := attach(t, e).Provision(e.Ctx, testPartitions); err != nil {
		t.Fatal(err)
	}

	e.Completed(id)

	mu.Lock()
	defer mu.Unlock()

	if runs["a"] != 1 || runs["b"] != 1 {
		t.Fatalf("runs %v: a retried decision re-ran or skipped a job", runs)
	}

	if q, err := e.Client.Quarantined(e.Ctx); err != nil || len(q) != 0 {
		t.Fatalf("quarantined after a transient failure: %v %v", q, err)
	}
}

// TestReviewG508LateDecisionBlob: a claim-checked decision whose body the
// dispatcher cannot read yet (a lagging replica) is retried, not quarantined,
// and dispatched once the body is readable (G5-08, R3 note).
func TestReviewG508LateDecisionBlob(t *testing.T) {
	e := NewEnv(t, []string{m1Linear})
	e.Worker("echo", Echo)

	e.KillEngines()
	e.StartEngine(packtrail.WithoutDispatcher())

	id := e.Start("linear", map[string]any{"big": strings.Repeat("x", 2<<20)})

	in := attach(t, e)

	events, err := e.S.JS.Stream(e.Ctx, "packtrail-events")
	if err != nil {
		t.Fatal(err)
	}

	var name string

	e.Eventually(func() bool {
		m, lerr := events.GetLastMsgForSubject(e.Ctx, in.EventSubject(id))
		if lerr == nil {
			name = m.Header.Get(blob.Header)
		}

		return name != ""
	}, func() string { return "the start decision was not claim-checked" })

	obs, err := e.S.JS.ObjectStore(e.Ctx, "packtrail-blobs")
	if err != nil {
		t.Fatal(err)
	}

	body, err := obs.GetBytes(e.Ctx, name)
	if err != nil {
		t.Fatal(err)
	}

	if err = obs.Delete(e.Ctx, name); err != nil {
		t.Fatal(err)
	}

	e.KillEngines()
	e.StartEngine()

	time.Sleep(1500 * time.Millisecond) // at least one failed delivery

	if _, err = obs.PutBytes(e.Ctx, name, body); err != nil {
		t.Fatal(err)
	}

	e.Completed(id)

	if q, qerr := e.Client.Quarantined(e.Ctx); qerr != nil || len(q) != 0 {
		t.Fatalf("quarantined: %v %v", q, qerr)
	}
}

// TestReviewG508StateAtNeverSplitsADecision: every point in time is the end
// of a decision. StateAt at any stored sequence folds whole decisions, and a
// fork cut at a sequence keeps every event of that decision (G5-08).
func TestReviewG508StateAtNeverSplitsADecision(t *testing.T) {
	e := NewEnv(t, []string{m3Story})
	e.Worker("step", stepWorker(nil))

	id := e.Start("story", map[string]any{"tag": "!"})
	e.Completed(id)

	evs, err := e.Client.History(e.Ctx, id)
	if err != nil {
		t.Fatal(err)
	}

	upTo := map[uint64]int{} // sequence -> events stored up to it
	for i, ev := range evs {
		upTo[ev.Seq] = i + 1

		if ev.DecisionEnd != (i == len(evs)-1 || evs[i+1].Seq != ev.Seq) {
			t.Fatalf("event %d (%s): decision end %v", i, ev.Type, ev.DecisionEnd)
		}
	}

	if len(upTo) == len(evs) {
		t.Fatal("every decision holds one event: the test proves nothing")
	}

	for seq, n := range upTo {
		st, serr := e.Client.StateAt(e.Ctx, id, seq)
		if serr != nil {
			t.Fatal(serr)
		}

		if st.Events != n {
			t.Fatalf("state at %d folds %d events, want %d", seq, st.Events, n)
		}
	}

	var afterA uint64

	for _, ev := range evs {
		if d, ok := ev.Data.(*event.NodeDone); ok && d.Node == "a" {
			afterA = ev.Seq
		}
	}

	fork, err := e.Client.Fork(e.Ctx, id, afterA, packtrail.WithForkID("story-cut"))
	if err != nil {
		t.Fatal(err)
	}

	st := e.Completed(fork)
	if string(st.Channels["log"]) != `["a!","b!","c!"]` {
		t.Fatalf("fork log %s", st.Channels["log"])
	}

	if c := countEvents(t, e, fork, event.NodeCompleted, "a"); c != 0 {
		t.Fatal("the fork re-ran a node completed in the decision it was cut at")
	}
}

// TestReviewG508WaitRetriesFailedReads: a read that fails while waiting (here
// every read times out; on a loaded cluster, a 5 s API timeout) does not end
// Wait: only the caller's context does (G5-08).
func TestReviewG508WaitRetriesFailedReads(t *testing.T) {
	e := NewEnv(t, []string{`
name: parked
nodes:
  - {id: wait, type: await, signal: go, timeout: 1h}
`})

	id := e.Start("parked", nil)
	e.WaitStatus(id, packtrail.StatusWaiting)

	slow, err := packtrail.NewClient(e.NC(), packtrail.WithClientReadTimeout(time.Nanosecond))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(e.Ctx, 2*time.Second)
	defer cancel()

	start := time.Now()

	if _, err = slow.Wait(ctx, id); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wait: %v, want the caller's deadline", err)
	}

	if took := time.Since(start); took < 1900*time.Millisecond {
		t.Fatalf("wait gave up after %v on a failed read", took)
	}
}
