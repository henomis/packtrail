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
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/event"
	"github.com/henomis/packtrail/worker"
)

const m2Await = `
name: approval
nodes:
  - {id: prepare, type: task, kind: slow-echo, next: wait}
  - {id: wait, type: await, signal: approve, timeout: %s, on_timeout: expired, next: done}
  - {id: done, type: task, kind: echo}
  - {id: expired, type: task, kind: echo}
`

func slowEcho(d time.Duration) worker.Handler {
	return func(ctx context.Context, j *worker.Job) (*worker.Result, error) {
		select {
		case <-time.After(d):
		case <-ctx.Done():
		}

		return Echo(ctx, j)
	}
}

func TestM2SignalAfterAwait(t *testing.T) {
	e := NewEnv(t, []string{strings.Replace(m2Await, "%s", "1h", 1)})
	e.Worker("echo", Echo)
	e.Worker("slow-echo", Echo)

	id := e.Start("approval", nil)
	e.WaitStatus(id, packtrail.StatusWaiting)

	if err := e.Client.Signal(e.Ctx, id, "approve", map[string]any{"by": "ana"}); err != nil {
		t.Fatal(err)
	}

	st := e.Completed(id)
	if st.LastNode != "done" || string(st.Signals["approve"]) != `{"by":"ana"}` {
		t.Fatalf("state %+v", st)
	}
}

func TestM2SignalBeforeAwaitIsBuffered(t *testing.T) {
	e := NewEnv(t, []string{strings.Replace(m2Await, "%s", "1h", 1)})
	e.Worker("echo", Echo)
	e.Worker("slow-echo", slowEcho(500*time.Millisecond))

	id := e.Start("approval", nil)
	e.WaitStatus(id, packtrail.StatusRunning)

	// The execution is still in "prepare": the signal is early (I-09). Sending
	// it twice with the same id delivers it once.
	for range 2 {
		if err := e.Client.Signal(e.Ctx, id, "approve", "early", packtrail.WithSignalID("sig-1")); err != nil {
			t.Fatal(err)
		}
	}

	st := e.Completed(id)
	if st.LastNode != "done" || countEvents(t, e, id, event.SignalReceived, "") != 1 {
		t.Fatalf("last %s", st.LastNode)
	}
}

func TestM2AwaitTimeoutRoutes(t *testing.T) {
	e := NewEnv(t, []string{strings.Replace(m2Await, "%s", "1s", 1)})
	e.Worker("echo", Echo)
	e.Worker("slow-echo", Echo)

	st := e.Completed(e.Start("approval", nil))
	if st.LastNode != "expired" {
		t.Fatalf("last node %s", st.LastNode)
	}
}

func TestM2InterruptResumeAcrossRestart(t *testing.T) {
	e := NewEnv(t, []string{`
name: review
nodes:
  - {id: draft, type: task, kind: drafter, next: publish}
  - {id: publish, type: task, kind: echo}
`})
	e.Worker("echo", Echo)
	e.Worker("drafter", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		var answer string

		resumed, err := j.Resumed(&answer)
		if err != nil {
			return nil, worker.Permanent(err)
		}

		if !resumed {
			return nil, worker.Interrupt(map[string]any{"question": "ship it?"})
		}

		return &worker.Result{Output: map[string]any{"answer": answer}}, nil
	})

	id := e.Start("review", nil)
	st := e.WaitStatus(id, packtrail.StatusWaiting)

	if string(st.Tasks["draft"].Interrupt) != `{"question":"ship it?"}` {
		t.Fatalf("interrupt payload %s", st.Tasks["draft"].Interrupt)
	}

	e.RestartEngine()

	if err := e.Client.Resume(e.Ctx, id, "draft", "yes"); err != nil {
		t.Fatal(err)
	}

	st = e.Completed(id)
	if string(st.Results["draft"]) != `{"answer":"yes"}` {
		t.Fatalf("draft %s", st.Results["draft"])
	}
}

// TestM2ResumedJobSeesInterruptPayload: every attempt of a resumed job gets
// the payload it interrupted with, claim-checked when large; a rerun of the
// node starts without it.
func TestM2ResumedJobSeesInterruptPayload(t *testing.T) {
	e := NewEnv(t, []string{`
name: ask
nodes:
  - id: draft
    type: task
    kind: asker
    retry: {max_attempts: 2, delay: 50ms}
`})

	size := 2 * 1024 * 1024
	if raceEnabled {
		size = 1536 * 1024
	}

	question := strings.Repeat("q", size)

	var bad atomic.Value

	e.Worker("asker", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		var asked struct{ Question string }

		interrupted, err := j.Interrupted(&asked)
		if err != nil {
			return nil, worker.Permanent(err)
		}

		resumed, _ := j.Resumed(nil)

		switch {
		case resumed != interrupted:
			bad.Store("resumed " + strconv.FormatBool(resumed) + ", interrupted " + strconv.FormatBool(interrupted))
		case !resumed:
			return nil, worker.Interrupt(map[string]any{"question": question})
		case asked.Question != question:
			bad.Store("attempt " + strconv.Itoa(j.Attempt) + ": payload of " + strconv.Itoa(len(asked.Question)))
		case j.Attempt == 1:
			return nil, errors.New("transient")
		}

		return &worker.Result{Output: map[string]any{"asked": len(asked.Question)}}, nil
	}, worker.WithAckWait(10*time.Second))

	id := e.Start("ask", nil)
	e.WaitStatus(id, packtrail.StatusWaiting)

	if err := e.Client.Resume(e.Ctx, id, "draft", "yes"); err != nil {
		t.Fatal(err)
	}

	st := e.Completed(id)

	if v := bad.Load(); v != nil {
		t.Fatal(v)
	}

	if string(st.Results["draft"]) != `{"asked":`+strconv.Itoa(size)+`}` {
		t.Fatalf("draft %s", st.Results["draft"])
	}

	re, err := e.Client.Rerun(e.Ctx, id, "draft")
	if err != nil {
		t.Fatal(err)
	}

	e.WaitStatus(re, packtrail.StatusWaiting)

	if v := bad.Load(); v != nil {
		t.Fatal(v)
	}
}

// TestM2UsageOfFailedAndInterruptedRuns: worker.WithUsage counts the spend of
// runs that fail or interrupt, and a budget exceeded by an interrupt fails the
// execution at that node instead of pausing it.
func TestM2UsageOfFailedAndInterruptedRuns(t *testing.T) {
	e := NewEnv(t, []string{`
name: spend
budget: {tokens: 35}
nodes:
  - id: agent
    type: task
    kind: spender
    retry: {max_attempts: 3, delay: 50ms}
`})
	e.Worker("spender", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		var in struct{ Cost float64 }

		_ = j.Input(&in)
		usage := map[string]float64{"tokens": in.Cost}

		if resumed, _ := j.Resumed(nil); resumed {
			return &worker.Result{Usage: usage}, nil
		}

		if j.Attempt == 1 {
			return nil, fmt.Errorf("llm: %w", worker.WithUsage(errors.New("schema mismatch"), usage))
		}

		return nil, worker.WithUsage(worker.Interrupt("which color?"), usage)
	})

	// 10 (failed) + 10 (interrupted) + 10 (completed).
	id := e.Start("spend", map[string]any{"cost": 10})
	st := e.WaitStatus(id, packtrail.StatusWaiting)

	if st.Counters["tokens"] != 20 {
		t.Fatalf("tokens while paused %g", st.Counters["tokens"])
	}

	if err := e.Client.Resume(e.Ctx, id, "agent", "red"); err != nil {
		t.Fatal(err)
	}

	if st = e.Completed(id); st.Counters["tokens"] != 30 {
		t.Fatalf("tokens %g", st.Counters["tokens"])
	}

	// 20 (failed) + 20 (interrupted) > 35: no question is asked.
	st = e.Wait(e.Start("spend", map[string]any{"cost": 20}))
	if st.Status != packtrail.StatusFailed || st.Reason != event.ReasonBudget || st.FailedNode != "agent" {
		t.Fatalf("status %s reason %s node %q", st.Status, st.Reason, st.FailedNode)
	}
}

func TestM2DynamicEdges(t *testing.T) {
	e := NewEnv(t, []string{`
name: router
nodes:
  - {id: decide, type: task, kind: router, dynamic: [left, right], next: right}
  - {id: left, type: task, kind: echo}
  - {id: right, type: task, kind: echo}
`})
	e.Worker("echo", Echo)
	e.Worker("router", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		var in struct{ Go string }

		_ = j.Input(&in)

		return &worker.Result{Next: in.Go}, nil
	})

	if st := e.Completed(e.Start("router", map[string]any{"go": "left"})); st.LastNode != "left" {
		t.Fatalf("dynamic route: %s", st.LastNode)
	}

	if st := e.Completed(e.Start("router", nil)); st.LastNode != "right" {
		t.Fatalf("static fallback: %s", st.LastNode)
	}

	if st := e.Wait(e.Start("router", map[string]any{"go": "nowhere"})); st.Status != packtrail.StatusFailed {
		t.Fatal("undeclared dynamic target must fail")
	}
}

func TestM2Map(t *testing.T) {
	e := NewEnv(t, []string{`
name: mapper
channels: {total: {reducer: sum}}
nodes:
  - {id: each, type: map, kind: square, over: input.items, max_parallel: 8}
`})
	e.Worker("square", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		var n float64
		if err := j.Item(&n); err != nil {
			return nil, worker.Permanent(err)
		}

		return &worker.Result{Output: map[string]any{"sq": n * n}, Writes: map[string]any{"total": n}}, nil
	}, worker.WithConcurrency(16))

	for _, size := range []int{0, 1, 1000} {
		items := make([]int, size)
		for i := range items {
			items[i] = i + 1
		}

		st := e.Completed(e.Start("mapper", map[string]any{"items": items}))

		var out []map[string]float64

		_ = json.Unmarshal(st.Results["each"], &out)

		if len(out) != size || (size > 0 && out[size-1]["sq"] != float64(size*size)) {
			t.Fatalf("size %d: %d results", size, len(out))
		}

		if size > 0 {
			if got, want := JSON(t, st.Channels["total"]), float64(size*(size+1)/2); got != want {
				t.Fatalf("size %d: total %v, want %v", size, got, want)
			}
		}
	}
}

const m2Parent = `
name: parent
nodes:
  - {id: sub, type: subflow, flow: child, input: "input.child", next: after}
  - {id: after, type: task, kind: echo}
`

const m2Child = `
name: child
nodes:
  - {id: work, type: task, kind: child-work}
`

func TestM2SubflowCompletes(t *testing.T) {
	e := NewEnv(t, []string{m2Parent, m2Child})
	e.Worker("echo", Echo)
	e.Worker("child-work", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		var in struct{ N int }

		_ = j.Input(&in)

		return &worker.Result{Output: map[string]any{"double": in.N * 2}, Usage: map[string]float64{"units": 3}}, nil
	})

	st := e.Completed(e.Start("parent", map[string]any{"child": map[string]any{"n": 21}}))
	if string(st.Results["sub"]) != `{"double":42}` || st.Counters["units"] != 3 {
		t.Fatalf("sub %s counters %v", st.Results["sub"], st.Counters)
	}
}

// TestM2ChoiceEndEndsOnlyTheChild: a choice to $end in a child execution
// completes the child; the parent continues past the subflow.
func TestM2ChoiceEndEndsOnlyTheChild(t *testing.T) {
	e := NewEnv(t, []string{m2Parent, `
name: child
nodes:
  - {id: work, type: task, kind: child-work, next: check}
  - id: check
    type: choice
    rules: [{default: true, to: $end}]
`})
	e.Worker("echo", Echo)
	e.Worker("child-work", func(context.Context, *worker.Job) (*worker.Result, error) {
		return &worker.Result{Output: map[string]any{"done": true}}, nil
	})

	st := e.Completed(e.Start("parent", map[string]any{"child": map[string]any{}}))
	if string(st.Results["sub"]) != `{"done":true}` || st.Results["after"] == nil {
		t.Fatalf("results %v", st.Results)
	}
}

func TestM2ChildCancelledWithParent(t *testing.T) {
	e := NewEnv(t, []string{m2Parent, m2Child})

	block := make(chan struct{})
	childID := make(chan string, 1)

	e.Worker("child-work", func(ctx context.Context, j *worker.Job) (*worker.Result, error) {
		childID <- j.ExecID

		select {
		case <-block:
		case <-ctx.Done():
		}

		return &worker.Result{}, nil
	})

	id := e.Start("parent", map[string]any{"child": map[string]any{}})
	child := <-childID

	if err := e.Client.Cancel(e.Ctx, id, "stop"); err != nil {
		t.Fatal(err)
	}

	e.WaitStatus(id, packtrail.StatusCancelled)
	cst := e.WaitStatus(child, packtrail.StatusCancelled)

	close(block)

	if cst.Parent == nil || cst.Parent.ExecID != id {
		t.Fatalf("child parent ref %+v", cst.Parent)
	}
}

func TestM2LargePayloadClaimCheck(t *testing.T) {
	e := NewEnv(t, []string{m1Linear})

	size := 10 * 1024 * 1024
	if raceEnabled {
		size = 3 * 1024 * 1024
	}

	big := strings.Repeat("x", size)

	e.Worker("echo", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		var in struct{ Blob string }
		if err := j.Input(&in); err != nil || len(in.Blob) != len(big) {
			return nil, worker.Permanent(errors.New("input not resolved"))
		}

		return &worker.Result{Output: map[string]any{"len": len(in.Blob), "echo": in.Blob}}, nil
	}, worker.WithAckWait(10*time.Second))

	st := e.Completed(e.Start("linear", map[string]any{"blob": big}))

	var out struct {
		Len  int
		Echo string
	}

	_ = json.Unmarshal(st.Results["c"], &out)

	if out.Len != len(big) || len(out.Echo) != len(big) {
		t.Fatalf("output len %d (I-12, I-20)", out.Len)
	}
}

func TestM2CronSchedule(t *testing.T) {
	e := NewEnv(t, []string{m1Linear})

	var runs atomic.Int32

	e.Worker("echo", func(ctx context.Context, j *worker.Job) (*worker.Result, error) {
		if j.Node == "c" {
			runs.Add(1)
		}

		return Echo(ctx, j)
	})

	if err := e.Client.Schedule(e.Ctx, "every-second", "linear", "@every 1s", map[string]any{"cron": true}); err != nil {
		t.Fatal(err)
	}

	scheds, err := e.Client.Schedules(e.Ctx)
	if err != nil || len(scheds) != 1 || scheds[0].Cron != "@every 1s" {
		t.Fatalf("schedules %+v %v", scheds, err)
	}

	e.Eventually(func() bool { return runs.Load() >= 2 }, func() string { return "cron did not fire twice" })

	if err = e.Client.Unschedule(e.Ctx, "every-second"); err != nil {
		t.Fatal(err)
	}

	var list []packtrail.Summary

	e.Eventually(func() bool {
		list, err = e.Client.List(e.Ctx, packtrail.ListFilter{Flow: "linear", Status: packtrail.StatusCompleted})

		return err == nil && len(list) >= 2
	}, func() string { return "cron executions not indexed" })

	if !strings.HasPrefix(list[0].ExecID, "every-second-") {
		t.Fatalf("list %+v", list)
	}

	if err = e.Client.Schedule(e.Ctx, "bad", "linear", "@every 10ms", nil); !errors.Is(err,
		packtrail.ErrInvalidArgument) {
		t.Fatalf("invalid cron accepted: %v (I-22)", err)
	}
}
