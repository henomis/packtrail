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

package e2e_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/event"
	"github.com/henomis/packtrail/worker"
)

// reminderFlow waits for an answer; without one it nudges, through a flaky
// channel retried after a delay. Both the await timeout and the retry delay
// are durable timers.
const reminderFlow = `
name: reminder
nodes:
  - {id: wait, type: await, signal: answer, timeout: %s, on_timeout: nudge, next: thanks}
  - {id: thanks, type: task, kind: note}
  - {id: nudge, type: task, kind: nudge, retry: {max_attempts: 3, delay: %s}}
`

func reminderWorkers(cl *cluster) {
	cl.worker("note", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		var a struct{ By string }
		if err := j.Signal("answer", &a); err != nil {
			return nil, worker.Permanent(err)
		}

		return &worker.Result{Output: map[string]any{"thanked": a.By}}, nil
	})

	// nudge fails its first two attempts: two retry timers.
	cl.worker("nudge", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		if j.Attempt < 3 {
			return nil, errors.New("sms gateway down")
		}

		return &worker.Result{Output: map[string]any{"nudged": true}}, nil
	})
}

// TestTimersFireAfterOutage stops every engine while executions wait on
// durable timers, lets the deadlines pass with nothing running, then brings
// engines back. Every timeout fires exactly once and takes its route, every
// retry delay elapses, and answers sent during the outage, before the
// deadline, win over the timeout.
func TestTimersFireAfterOutage(t *testing.T) {
	for _, restartNATS := range []bool{false, true} {
		t.Run(fmt.Sprintf("nats-restart=%v", restartNATS), func(t *testing.T) {
			// The answers are sent within the timeout; the outage outlasts it.
			const timeout, outage = 4 * time.Second, 6 * time.Second

			cl := newCluster(t, []string{fmt.Sprintf(reminderFlow, timeout, "500ms")})
			cl.startEngine()
			reminderWorkers(cl)

			ids := make([]string, 10)
			for i := range ids {
				ids[i] = cl.start("reminder", nil)
			}

			for _, id := range ids {
				cl.waitUntil(id, "waiting for an answer", func(st *packtrail.State) bool { return st.Awaits["wait"] != nil })
			}

			cl.stopEngines()

			// Half of them are answered while no engine runs.
			answered := map[string]bool{}

			for _, id := range ids[:len(ids)/2] {
				if err := cl.c.Signal(cl.ctx, id, "answer", map[string]any{"by": "ops"}); err != nil {
					t.Fatal(err)
				}

				answered[id] = true
			}

			if restartNATS {
				cl.s.Restart(t)
			}

			time.Sleep(outage)

			cl.startEngine()
			cl.startEngine()

			for _, id := range ids {
				st := cl.wait(id)
				f := cl.check(id)

				if st.Status != packtrail.StatusCompleted {
					t.Fatalf("%s: %s (%s: %s)", id, st.Status, st.Reason, st.Error)
				}

				if answered[id] {
					if st.LastNode != "thanks" || f.count[event.AwaitTimedOut] != 0 || f.count[event.SignalReceived] != 1 {
						t.Fatalf("%s answered during the outage ended at %s: %v", id, st.LastNode, f.count)
					}

					continue
				}

				if st.LastNode != "nudge" || f.count[event.AwaitTimedOut] != 1 || f.retried["nudge"] != 2 {
					t.Fatalf("%s ended at %s, timeouts %d, nudge retries %d", id, st.LastNode,
						f.count[event.AwaitTimedOut], f.retried["nudge"])
				}
			}
		})
	}
}

// TestRetryTimersSurviveOutage stops every engine while retry delays are
// pending: the retries run once engines are back, exactly as many as the
// policy allows.
func TestRetryTimersSurviveOutage(t *testing.T) {
	cl := newCluster(t, []string{fmt.Sprintf(reminderFlow, "100ms", "3s")})
	reminderWorkers(cl)

	ids := make([]string, 6)
	for i := range ids {
		ids[i] = cl.start("reminder", nil)
	}

	// Each nudge failed once: its retry timer is pending.
	for _, id := range ids {
		cl.waitUntil(id, "retrying its nudge", func(st *packtrail.State) bool {
			task := st.Tasks["nudge"]

			return task != nil && task.Attempt == 2
		})
	}

	cl.stopEngines()
	cl.s.Restart(t)
	time.Sleep(4 * time.Second)
	cl.startEngine()

	for _, id := range ids {
		st := cl.wait(id)
		f := cl.check(id)

		if st.Status != packtrail.StatusCompleted || st.LastNode != "nudge" || f.retried["nudge"] != 2 {
			t.Fatalf("%s: %s at %s, nudge retries %d (%s)", id, st.Status, st.LastNode, f.retried["nudge"], st.Error)
		}
	}
}
