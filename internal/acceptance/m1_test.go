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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/event"
	"github.com/henomis/packtrail/worker"
)

const m1Linear = `
name: linear
nodes:
  - {id: a, type: task, kind: echo, next: b}
  - {id: b, type: task, kind: echo, next: c}
  - {id: c, type: task, kind: echo}
`

func countEvents(t *testing.T, e *Env, id string, typ event.Type, node string) int {
	t.Helper()

	evs, err := e.Client.History(e.Ctx, id)
	if err != nil {
		t.Fatal(err)
	}

	n := 0

	for _, ev := range evs {
		if ev.Type != typ {
			continue
		}

		switch d := ev.Data.(type) {
		case *event.NodeDone:
			if node == "" || d.Node == node {
				n++
			}
		case *event.Entered:
			if node == "" || d.Node == node {
				n++
			}
		default:
			n++
		}
	}

	return n
}

func TestM1Linear(t *testing.T) {
	e := NewEnv(t, []string{m1Linear})
	e.Worker("echo", Echo)

	id := e.Start("linear", map[string]any{"q": "hello"})
	st := e.Completed(id)

	if st.LastNode != "c" || st.Visits["a"] != 1 || len(st.Results) != 3 {
		t.Fatalf("state %+v", st)
	}
}

func TestM1Choice(t *testing.T) {
	e := NewEnv(t, []string{`
name: choice
nodes:
  - {id: score, type: task, kind: score, next: route}
  - id: route
    type: choice
    rules:
      - {when: "results.score.value > 50", to: high}
      - {default: true, to: low}
  - {id: high, type: task, kind: echo}
  - {id: low, type: task, kind: echo}
`})
	e.Worker("echo", Echo)
	e.Worker("score", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		var in struct{ V int }

		_ = j.Input(&in)

		return &worker.Result{Output: map[string]any{"value": in.V}}, nil
	})

	hi := e.Completed(e.Start("choice", map[string]any{"v": 90}))
	lo := e.Completed(e.Start("choice", map[string]any{"v": 10}))

	if hi.LastNode != "high" || lo.LastNode != "low" {
		t.Fatalf("routes: %s %s", hi.LastNode, lo.LastNode)
	}
}

const m1Fan = `
name: fan-%s
nodes:
  - {id: split, type: fanout, branches: [a, b, c], next: join}
  - {id: a, type: task, kind: branch}
  - {id: b, type: task, kind: branch}
  - {id: c, type: task, kind: branch}
  - {id: join, type: join, policy: '%s', next: after}
  - {id: after, type: task, kind: echo}
`

// branchWorker completes branches in the order given by delays and fails the
// ones listed in failing.
func branchWorker(delays map[string]time.Duration, failing map[string]bool) worker.Handler {
	return func(ctx context.Context, j *worker.Job) (*worker.Result, error) {
		select {
		case <-time.After(delays[j.Node]):
		case <-ctx.Done():
			return nil, ctx.Err()
		}

		if failing[j.Node] {
			return nil, worker.Permanent(errors.New("branch failed"))
		}

		return &worker.Result{Output: map[string]any{"branch": j.Node}}, nil
	}
}

func TestM1FanoutPolicies(t *testing.T) {
	e := NewEnv(t, []string{
		fmt.Sprintf(m1Fan, "all", "all"), fmt.Sprintf(m1Fan, "any", "any"), fmt.Sprintf(m1Fan, "q", "quorum:2"),
	})
	e.Worker("echo", Echo)
	// Reordered completions: c first, a last (I-02).
	e.Worker("branch", branchWorker(map[string]time.Duration{"a": 300 * time.Millisecond, "b": 150 * time.Millisecond},
		map[string]bool{}), worker.WithConcurrency(8))

	for _, f := range []string{"fan-all", "fan-any", "fan-q"} {
		st := e.Completed(e.Start(f, nil))
		if st.LastNode != "after" {
			t.Fatalf("%s: last node %s", f, st.LastNode)
		}
	}
}

func TestM1FanoutFailures(t *testing.T) {
	e := NewEnv(t, []string{fmt.Sprintf(m1Fan, "all", "all"), fmt.Sprintf(m1Fan, "any", "any")})
	e.Worker("echo", Echo)
	e.Worker("branch", branchWorker(map[string]time.Duration{"a": 200 * time.Millisecond},
		map[string]bool{"b": true, "c": true}), worker.WithConcurrency(8))

	all := e.Wait(e.Start("fan-all", nil))
	if all.Status != packtrail.StatusFailed || all.Reason != event.ReasonJoin {
		t.Fatalf("all: %s %s", all.Status, all.Reason)
	}

	anyS := e.Completed(e.Start("fan-any", nil))
	if anyS.Branches["a"] != "completed" {
		t.Fatalf("any: branches %v", anyS.Branches)
	}
}

func TestM1RetryAndTerminalFailure(t *testing.T) {
	e := NewEnv(t, []string{`
name: retry
nodes:
  - id: flaky
    type: task
    kind: flaky
    retry: {max_attempts: 3, backoff: fixed, delay: 100ms}
`, `
name: doomed
nodes:
  - id: d
    type: task
    kind: doomed
    retry: {max_attempts: 2, backoff: fixed, delay: 100ms}
`})

	var calls atomic.Int32

	e.Worker("flaky", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		calls.Add(1)

		if j.Attempt < 3 {
			return nil, errors.New("transient")
		}

		return &worker.Result{Output: map[string]any{"attempt": j.Attempt}}, nil
	})
	e.Worker("doomed", func(context.Context, *worker.Job) (*worker.Result, error) {
		return nil, errors.New("always")
	})

	e.Completed(e.Start("retry", nil))

	if calls.Load() != 3 {
		t.Fatalf("calls = %d", calls.Load())
	}

	st := e.Wait(e.Start("doomed", nil))
	if st.Status != packtrail.StatusFailed || st.FailedNode != "d" {
		t.Fatalf("doomed: %+v", st)
	}
}

func TestM1PermanentErrorDoesNotRetry(t *testing.T) {
	e := NewEnv(t, []string{`
name: perm
nodes:
  - {id: p, type: task, kind: perm, retry: {max_attempts: 5}}
`})

	var calls atomic.Int32

	e.Worker("perm", func(context.Context, *worker.Job) (*worker.Result, error) {
		calls.Add(1)

		return nil, worker.Permanent(errors.New("bad input"))
	})

	if st := e.Wait(e.Start("perm", nil)); st.Status != packtrail.StatusFailed || calls.Load() != 1 {
		t.Fatalf("status %s calls %d (I-11)", st.Status, calls.Load())
	}
}

func TestM1TimeoutRetries(t *testing.T) {
	e := NewEnv(t, []string{`
name: slow
nodes:
  - {id: s, type: task, kind: slow, timeout: 1s, retry: {max_attempts: 2, backoff: fixed, delay: 100ms}}
`})
	e.Worker("slow", func(ctx context.Context, j *worker.Job) (*worker.Result, error) {
		if j.Attempt == 1 {
			select {
			case <-time.After(3 * time.Second):
			case <-ctx.Done():
			}
		}

		return &worker.Result{Output: map[string]any{"attempt": j.Attempt}}, nil
	}, worker.WithConcurrency(2))

	st := e.Completed(e.Start("slow", nil))
	if string(st.Results["s"]) != `{"attempt":2}` {
		t.Fatalf("results %s: the late attempt 1 must be ignored (I-18)", st.Results["s"])
	}
}

func TestM1CancelDuringTask(t *testing.T) {
	e := NewEnv(t, []string{m1Linear})

	started := make(chan string, 1)
	release := make(chan struct{})

	e.Worker("echo", func(ctx context.Context, j *worker.Job) (*worker.Result, error) {
		started <- j.ExecID

		<-release

		return Echo(ctx, j)
	})

	id := e.Start("linear", nil)

	<-started

	if err := e.Client.Cancel(e.Ctx, id, "user request"); err != nil {
		t.Fatal(err)
	}

	st := e.WaitStatus(id, packtrail.StatusCancelled)

	close(release)

	time.Sleep(300 * time.Millisecond)

	if again, _ := e.Client.Get(e.Ctx, id); again.Status != packtrail.StatusCancelled || st.Reason != "user request" {
		t.Fatalf("cancel lost or rewound (I-01): %s", again.Status)
	}
}

func TestM1EngineKilledAtEveryStep(t *testing.T) {
	e := NewEnv(t, []string{m1Linear})

	var mu sync.Mutex

	e.Worker("echo", func(ctx context.Context, j *worker.Job) (*worker.Result, error) {
		mu.Lock()
		e.RestartEngine()
		mu.Unlock()

		return Echo(ctx, j)
	})

	st := e.Completed(e.Start("linear", nil))

	for _, n := range []string{"a", "b", "c"} {
		if c := countEvents(t, e, st.ExecID, event.NodeCompleted, n); c != 1 {
			t.Fatalf("node %s completed %d times", n, c)
		}
	}
}

func TestM1NATSRestart(t *testing.T) {
	e := NewEnv(t, []string{m1Linear})

	var once sync.Once

	e.Worker("echo", func(ctx context.Context, j *worker.Job) (*worker.Result, error) {
		if j.Node == "b" {
			once.Do(func() { e.S.Restart(e.T) })
		}

		return Echo(ctx, j)
	})

	e.Completed(e.Start("linear", nil))
}

func TestM1DuplicateCompletionIgnored(t *testing.T) {
	e := NewEnv(t, []string{m1Linear})

	dupNC := e.NC()

	e.Worker("echo", func(ctx context.Context, j *worker.Job) (*worker.Result, error) {
		if j.Node == "a" {
			// A second, independent completion of the same attempt (another
			// worker that also got the job): it must be absorbed (I-18).
			publishDuplicateComplete(t, dupNC, j)
		}

		return Echo(ctx, j)
	})

	st := e.Completed(e.Start("linear", nil))
	if c := countEvents(t, e, st.ExecID, event.NodeCompleted, "a"); c != 1 {
		t.Fatalf("node a completed %d times", c)
	}
}

func TestM1StartIdempotent(t *testing.T) {
	e := NewEnv(t, []string{m1Linear})
	e.Worker("echo", Echo)

	id := e.Start("linear", nil, packtrail.WithExecutionID("order-42"))
	id2 := e.Start("linear", nil, packtrail.WithExecutionID("order-42"))

	if id != "order-42" || id2 != id {
		t.Fatal("ids differ")
	}

	e.Completed(id)

	// Even after completion a duplicate start changes nothing (I-03).
	e.Start("linear", nil, packtrail.WithExecutionID("order-42"))
	time.Sleep(300 * time.Millisecond)

	if c := countEvents(t, e, id, event.ExecutionStarted, ""); c != 1 {
		t.Fatalf("started %d times", c)
	}
}

func TestM1ValidationAndMissingExecutions(t *testing.T) {
	e := NewEnv(t, []string{m1Linear})
	c := e.Client

	for _, id := range []string{"a.b", "*", ">", "", "a b"} {
		if _, err := c.Get(e.Ctx, id); !errors.Is(err, packtrail.ErrInvalidArgument) {
			t.Errorf("Get(%q) = %v (I-19)", id, err)
		}

		if err := c.Cancel(e.Ctx, id, ""); !errors.Is(err, packtrail.ErrInvalidArgument) {
			t.Errorf("Cancel(%q) = %v", id, err)
		}
	}

	if err := c.Signal(e.Ctx, "nope", "s", nil); !errors.Is(err, packtrail.ErrNotFound) {
		t.Fatalf("signal to missing execution = %v (I-09)", err)
	}

	if err := c.Cancel(e.Ctx, "nope", ""); !errors.Is(err, packtrail.ErrNotFound) {
		t.Fatalf("cancel of missing execution = %v (I-17)", err)
	}

	if _, err := c.Start(e.Ctx, "no-such-flow", nil); !errors.Is(err, packtrail.ErrUnknownFlow) {
		t.Fatalf("start unknown flow = %v", err)
	}

	if _, err := c.Start(e.Ctx, "linear", []int{1}); !errors.Is(err, packtrail.ErrInvalidArgument) {
		t.Fatalf("non-object input = %v (I-04)", err)
	}
}

func TestM1WorkerPanicIsRecovered(t *testing.T) {
	e := NewEnv(t, []string{`
name: panicky
nodes:
  - {id: p, type: task, kind: panicky, retry: {max_attempts: 2, backoff: fixed, delay: 50ms}}
`})

	e.Worker("panicky", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		if j.Attempt == 1 {
			panic("boom")
		}

		return &worker.Result{}, nil
	})

	e.Completed(e.Start("panicky", nil)) // I-21: the worker survived the panic
}

func TestM1ConcurrentEngines(t *testing.T) {
	e := NewEnv(t, []string{m1Linear, fmt.Sprintf(m1Fan, "all", "all")})
	e.StartEngine()
	e.StartEngine()
	e.Worker("echo", Echo, worker.WithConcurrency(16))
	e.Worker("branch", branchWorker(nil, nil), worker.WithConcurrency(16))

	const n = 30

	ids := make([]string, 0, 2*n)
	for i := range n {
		ids = append(ids, e.Start("linear", map[string]any{"i": i}), e.Start("fan-all", nil))
	}

	for _, id := range ids {
		st := e.Completed(id)
		if c := countEvents(t, e, id, event.NodeCompleted, ""); c != len(st.Results)-boolInt(st.Flow == "fan-all") {
			t.Fatalf("%s: %d completions for %d results", id, c, len(st.Results))
		}
	}
}

func boolInt(b bool) int {
	if b {
		return 1
	}

	return 0
}

func TestM1InvalidCommandIsDeadLettered(t *testing.T) {
	e := NewEnv(t, []string{m1Linear})
	e.Worker("echo", Echo)

	// A command for an execution that never existed can never succeed: it is
	// dead-lettered instead of redelivered forever (I-11).
	publishRawCommand(t, e, "ghost", `{"id":"x1","type":"signal","exec_id":"ghost","data":{"name":"s"}}`)

	e.Eventually(func() bool {
		dl, err := e.Client.DeadLetters(e.Ctx, 10)

		return err == nil && len(dl) > 0
	}, func() string { return "no dead letter for an undecodable command" })
}
