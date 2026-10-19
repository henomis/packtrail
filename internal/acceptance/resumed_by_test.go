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
	"github.com/henomis/packtrail/worker"
)

// TestResumedBy: WithResumedBy reaches every attempt of the resumed run as
// Job.Context.ResumedBy and is kept on the NodeScheduled event; an actor over
// the limit is refused before anything is sent.
func TestResumedBy(t *testing.T) {
	e := NewEnv(t, []string{`
name: review
nodes:
  - id: draft
    type: task
    kind: reviewer
    retry: {max_attempts: 2, delay: 50ms}
`})

	var (
		mu   sync.Mutex
		seen []string
	)

	e.Worker("reviewer", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		if resumed, _ := j.Resumed(nil); !resumed {
			return nil, worker.Interrupt(map[string]any{"review": "please"})
		}

		mu.Lock()

		seen = append(seen, j.Context.ResumedBy)
		mu.Unlock()

		if j.Attempt == 1 {
			return nil, errors.New("transient")
		}

		return &worker.Result{Output: map[string]any{"ok": true}}, nil
	}, worker.WithAckWait(10*time.Second))

	id := e.Start("review", nil)
	e.WaitStatus(id, packtrail.StatusWaiting)

	long := packtrail.WithResumedBy(strings.Repeat("x", 257))
	if err := e.Client.Resume(e.Ctx, id, "draft", "yes", long); !errors.Is(err, packtrail.ErrInvalidArgument) {
		t.Fatalf("over-long actor: %v", err)
	}

	if err := e.Client.Resume(e.Ctx, id, "draft", "yes", packtrail.WithResumedBy("alice")); err != nil {
		t.Fatal(err)
	}

	e.Completed(id)

	mu.Lock()
	got := strings.Join(seen, ",")
	mu.Unlock()

	if got != "alice,alice" {
		t.Fatalf("attempts saw resumed_by %q", got)
	}

	evs, err := e.Client.History(e.Ctx, id)
	if err != nil {
		t.Fatal(err)
	}

	var recorded []string

	for _, ev := range evs {
		if s, ok := ev.Data.(*event.Scheduled); ok && s.ResumedBy != "" {
			recorded = append(recorded, s.ResumedBy)
		}
	}

	if strings.Join(recorded, ",") != "alice,alice" {
		t.Fatalf("history resumed_by %v", recorded)
	}
}
