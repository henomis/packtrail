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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/event"
	"github.com/henomis/packtrail/worker"
)

// blocker is a handler that blocks until its job context ends and records why,
// per node and attempt.
type blocker struct {
	mu      sync.Mutex
	started map[string]chan struct{}
	causes  map[string]error
	calls   atomic.Int32
}

func newBlocker() *blocker {
	return &blocker{started: map[string]chan struct{}{}, causes: map[string]error{}}
}

func (b *blocker) ch(name string) chan struct{} {
	b.mu.Lock()
	defer b.mu.Unlock()

	c, ok := b.started[name]
	if !ok {
		c = make(chan struct{})
		b.started[name] = c
	}

	return c
}

// block runs as the job named name: it signals start, waits for its context
// and records the cause.
func (b *blocker) block(ctx context.Context, name string) (*worker.Result, error) {
	b.calls.Add(1)
	close(b.ch(name))

	<-ctx.Done()

	b.mu.Lock()
	b.causes[name] = context.Cause(ctx)
	b.mu.Unlock()

	return nil, errors.New("stopped")
}

func (b *blocker) waitStarted(t *testing.T, name string) {
	t.Helper()

	select {
	case <-b.ch(name):
	case <-time.After(10 * time.Second):
		t.Fatalf("job %s never started", name)
	}
}

// waitCause waits until the job named name was stopped with ErrCancelled.
func (b *blocker) waitCause(t *testing.T, e *Env, name string) {
	t.Helper()

	e.Eventually(func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()

		return errors.Is(b.causes[name], worker.ErrCancelled)
	}, func() string {
		b.mu.Lock()
		defer b.mu.Unlock()

		return "job " + name + " not cancelled: " + errString(b.causes[name])
	})
}

func errString(err error) string {
	if err == nil {
		return "still running"
	}

	return err.Error()
}

const g502Slow = `
name: slow
nodes:
  - {id: work, type: task, kind: slow}
`

// TestG502CancelStopsRunningJob: cancelling an execution cancels the context
// of the job a worker is running, with cause worker.ErrCancelled; the job is
// acked, not redelivered, and no result is sent (G5-02).
func TestG502CancelStopsRunningJob(t *testing.T) {
	e := NewEnv(t, []string{g502Slow})
	b := newBlocker()

	e.Worker("slow", func(ctx context.Context, _ *worker.Job) (*worker.Result, error) {
		return b.block(ctx, "work")
	})

	id := e.Start("slow", nil)

	b.waitStarted(t, "work")

	if err := e.Client.Cancel(e.Ctx, id, "user"); err != nil {
		t.Fatal(err)
	}

	b.waitCause(t, e, "work")
	e.WaitStatus(id, packtrail.StatusCancelled)

	time.Sleep(time.Second) // a redelivery would call the handler again

	if n := b.calls.Load(); n != 1 {
		t.Fatalf("handler called %d times, want 1", n)
	}

	if c := countEvents(t, e, id, event.NodeFailed, "work"); c != 0 {
		t.Fatal("a cancelled job must not report a result")
	}
}

// TestG502SettledJoinStopsLosingBranch: a join with policy any settles on the
// first branch; the branch still running is stopped (G5-02).
func TestG502SettledJoinStopsLosingBranch(t *testing.T) {
	e := NewEnv(t, []string{`
name: race
nodes:
  - {id: split, type: fanout, branches: [fast, slow], next: join}
  - {id: fast, type: task, kind: racer}
  - {id: slow, type: task, kind: racer}
  - {id: join, type: join, policy: any}
`})
	b := newBlocker()

	e.Worker("racer", func(ctx context.Context, j *worker.Job) (*worker.Result, error) {
		if j.Node == "fast" {
			b.waitStarted(t, "slow") // let the slow branch be running first

			return &worker.Result{Output: map[string]any{"won": true}}, nil
		}

		return b.block(ctx, "slow")
	}, worker.WithConcurrency(2))

	id := e.Start("race", nil)
	e.Completed(id)
	b.waitCause(t, e, "slow")
}

// TestG502TimedOutAttemptIsStopped: an attempt that timed out is stopped when
// the engine retries it, so two attempts do not run side by side; and no timer
// fires early (G5-02).
func TestG502TimedOutAttemptIsStopped(t *testing.T) {
	e := NewEnv(t, []string{`
name: tmo
nodes:
  - {id: work, type: task, kind: tmo, timeout: 1s, retry: {max_attempts: 2, delay: 100ms}}
`})
	b := newBlocker()

	e.Worker("tmo", func(ctx context.Context, j *worker.Job) (*worker.Result, error) {
		if j.Attempt == 1 {
			return b.block(ctx, "attempt1")
		}

		return &worker.Result{Output: map[string]any{"attempt": j.Attempt}}, nil
	}, worker.WithConcurrency(2))

	id := e.Start("tmo", nil)

	b.waitStarted(t, "attempt1")
	b.waitCause(t, e, "attempt1")
	e.Completed(id)

	// No timer fires before its time: @at used to be truncated to the
	// second, so this 1 s timeout fired up to a second early.
	evs, err := e.Client.History(e.Ctx, id)
	if err != nil {
		t.Fatal(err)
	}

	due := map[string]time.Time{}

	for _, ev := range evs {
		switch d := ev.Data.(type) {
		case *event.Timer:
			due[d.ID] = d.At
		case *event.Fired:
			if ev.Time.Before(due[d.ID]) {
				t.Fatalf("timer %s fired at %s, due %s", d.ID, ev.Time, due[d.ID])
			}
		}
	}
}

// TestG502LiveCheckWithoutStopNotice: with no dispatcher to send the stop
// notice, the running job still stops: it sees the execution ended at the
// head of the log (G5-02 backstop).
func TestG502LiveCheckWithoutStopNotice(t *testing.T) {
	e := NewEnv(t, []string{g502Slow})
	b := newBlocker()

	e.Worker("slow", func(ctx context.Context, _ *worker.Job) (*worker.Result, error) {
		return b.block(ctx, "work")
	}, worker.WithLiveCheck(300*time.Millisecond))

	id := e.Start("slow", nil)

	b.waitStarted(t, "work")

	e.KillEngines()
	e.StartEngine(packtrail.WithoutDispatcher())

	if err := e.Client.Cancel(e.Ctx, id, "user"); err != nil {
		t.Fatal(err)
	}

	b.waitCause(t, e, "work")
}
