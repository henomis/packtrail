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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/internal/names"
	"github.com/henomis/packtrail/worker"
)

// TestReviewF01LongSemaphoreWaitDoesNotFail: jobs queued behind a per-key
// limit must wait as long as needed, not burn delivery attempts (F-01).
func TestReviewF01LongSemaphoreWaitDoesNotFail(t *testing.T) {
	e := NewEnv(t, []string{`
name: serial
nodes:
  - {id: work, type: task, kind: serial, concurrency: {key: input.tenant, max: 1}}
`})
	e.Worker("serial", func(context.Context, *worker.Job) (*worker.Result, error) {
		time.Sleep(time.Second)

		return &worker.Result{}, nil
	}, worker.WithConcurrency(8), worker.WithMaxDeliver(2))

	ids := make([]string, 0, 6)
	for range 6 {
		ids = append(ids, e.Start("serial", map[string]any{"tenant": "same"}))
	}

	for _, id := range ids {
		e.Completed(id) // the last one waits ~5s for its slot
	}
}

// TestReviewF02WorkerProcessesAddCapacity: concurrency is per process, so two
// processes with concurrency 2 run 4 jobs at once (F-02).
func TestReviewF02WorkerProcessesAddCapacity(t *testing.T) {
	e := NewEnv(t, []string{`
name: wide
nodes:
  - {id: w, type: task, kind: wide}
`})

	var (
		mu         sync.Mutex
		running    int
		peak       int
		inProgress atomic.Int32
	)

	h := func(context.Context, *worker.Job) (*worker.Result, error) {
		inProgress.Add(1)
		mu.Lock()
		running++
		peak = max(peak, running)
		mu.Unlock()

		time.Sleep(700 * time.Millisecond)

		mu.Lock()
		running--
		mu.Unlock()

		return &worker.Result{}, nil
	}

	e.Worker("wide", h, worker.WithConcurrency(2))
	e.Worker("wide", h, worker.WithConcurrency(2))

	ids := make([]string, 0, 8)
	for range 8 {
		ids = append(ids, e.Start("wide", nil))
	}

	for _, id := range ids {
		e.Completed(id)
	}

	if peak < 4 {
		t.Fatalf("peak concurrency %d with two processes of concurrency 2", peak)
	}

	_ = packtrail.StatusCompleted
}

// TestReviewF05SlotLeaseRenewedForLongJobs: a job longer than the slot lease
// keeps its slot (the lease is renewed), so the limit holds (F-05).
func TestReviewF05SlotLeaseRenewedForLongJobs(t *testing.T) {
	e := NewEnv(t, []string{`
name: long
nodes:
  - {id: work, type: task, kind: long, concurrency: {key: input.tenant, max: 1}}
`})

	var running, peak atomic.Int32

	e.Worker("long", func(context.Context, *worker.Job) (*worker.Result, error) {
		n := running.Add(1)

		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}

		time.Sleep(4 * time.Second) // lease = 3 × 1s ack wait

		running.Add(-1)

		return &worker.Result{}, nil
	}, worker.WithConcurrency(4), worker.WithAckWait(time.Second))

	a := e.Start("long", map[string]any{"tenant": "t"})
	b := e.Start("long", map[string]any{"tenant": "t"})

	e.Completed(a)
	e.Completed(b)

	if peak.Load() != 1 {
		t.Fatalf("peak %d: the slot expired under a long job", peak.Load())
	}
}

// TestReviewF03BadEventQuarantinesOnlyItsExecution: an event the dispatcher
// cannot process must not stall the partition: the execution is quarantined
// (dead letter + index mark) and the others keep running (F-03).
func TestReviewF03BadEventQuarantinesOnlyItsExecution(t *testing.T) {
	e := NewEnv(t, []string{strings.Replace(m2Await, "%s", "1h", 1)})
	e.Worker("echo", Echo)
	e.Worker("slow-echo", Echo)

	bad := e.Start("approval", nil, packtrail.WithExecutionID("bad-0"))
	e.WaitStatus(bad, packtrail.StatusWaiting)

	// Corrupt the log: an event of a type no reader knows.
	subj := names.New("").EventSubject(names.Partition(bad, testPartitions), bad)
	if _, err := e.S.JS.Publish(e.Ctx, subj, []byte(`{"type":"Bogus","v":1,"i":99}`)); err != nil {
		t.Fatal(err)
	}

	// Another execution in the same partition must still complete.
	var other string

	for i := 0; ; i++ {
		id := "other-" + strconv.Itoa(i)
		if names.Partition(id, testPartitions) == names.Partition(bad, testPartitions) {
			other = id

			break
		}
	}

	e.Start("approval", nil, packtrail.WithExecutionID(other))
	e.WaitStatus(other, packtrail.StatusWaiting)

	if err := e.Client.Signal(e.Ctx, other, "approve", nil); err != nil {
		t.Fatal(err)
	}

	e.Completed(other)

	e.Eventually(func() bool {
		l, _ := e.Client.List(e.Ctx, packtrail.ListFilter{Flow: "approval"})
		for _, s := range l {
			if s.ExecID == bad && s.Quarantined {
				return true
			}
		}

		return false
	}, func() string { return "bad execution not marked quarantined" })

	if q, err := e.Client.Quarantined(e.Ctx); err != nil || len(q) != 1 || q[0] != bad {
		t.Fatalf("quarantined %v %v", q, err)
	}

	dl, err := e.Client.DeadLetters(e.Ctx, 10)
	if err != nil || len(dl) == 0 || dl[0].Key != bad {
		t.Fatalf("dead letters %+v %v", dl, err)
	}
}

// TestReviewF06LateDispatcherSkipsCancelledWork: a dispatcher that is behind
// must not dispatch jobs for tasks that are no longer active at the head of
// the log (F-06).
func TestReviewF06LateDispatcherSkipsCancelledWork(t *testing.T) {
	e := NewEnv(t, []string{m1Linear}, packtrail.WithoutDispatcher())

	var calls atomic.Int32

	e.Worker("echo", func(ctx context.Context, j *worker.Job) (*worker.Result, error) {
		calls.Add(1)

		return Echo(ctx, j)
	})

	id := e.Start("linear", nil)
	e.Eventually(func() bool {
		st, err := e.Client.Get(e.Ctx, id)

		return err == nil && st.Tasks["a"] != nil
	}, func() string { return "task a never scheduled" })

	if err := e.Client.Cancel(e.Ctx, id, "changed my mind"); err != nil {
		t.Fatal(err)
	}

	e.WaitStatus(id, packtrail.StatusCancelled)

	// Now a dispatcher catches up on the whole history.
	late, err := packtrail.New(e.NC(), packtrail.WithFlowYAML([]byte(m1Linear)), packtrail.WithoutCommands(),
		packtrail.WithPartitions(testPartitions))
	if err != nil {
		t.Fatal(err)
	}

	ctx, stop := context.WithCancel(e.Ctx)
	defer stop()

	go func() { _ = late.Run(ctx) }()

	e.Eventually(func() bool {
		l, lerr := e.Client.List(e.Ctx, packtrail.ListFilter{Status: packtrail.StatusCancelled})

		return lerr == nil && len(l) == 1
	}, func() string { return "late dispatcher did not catch up" })

	time.Sleep(500 * time.Millisecond)

	if calls.Load() != 0 {
		t.Fatalf("a cancelled task was dispatched %d time(s)", calls.Load())
	}
}
