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

package worker

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail/event"
	"github.com/henomis/packtrail/internal/wire"
)

// ErrCancelled is the cause of a job context cancelled because the work is no
// longer wanted: its execution ended (cancelled, failed, completed elsewhere),
// its task was cancelled (a join settled, a map aborted), or its attempt timed
// out and was retried. Check it with context.Cause(ctx). The worker drops a
// cancelled job's result — it would be stale — and acks the job.
var ErrCancelled = errors.New("worker: job cancelled")

// DefaultLiveCheck is how often a running job checks the head of its
// execution's log, in case a stop notice was missed.
const DefaultLiveCheck = 5 * time.Second

// WithLiveCheck sets how often a running job checks whether its execution
// ended (default 5s); 0 disables the check, leaving only stop notices.
func WithLiveCheck(d time.Duration) Option {
	return func(w *Worker) error {
		if d < 0 {
			return errors.New("worker: live check interval must not be negative")
		}

		w.liveCheck = d

		return nil
	}
}

// runningJob is a job whose handler is running: what a stop notice matches.
type runningJob struct {
	key        string
	generation int
	attempt    int
	cancel     context.CancelCauseFunc
}

// jobs tracks the running jobs of a worker process by execution.
type jobs struct {
	mu   sync.Mutex
	byID map[string][]*runningJob
}

func (j *jobs) add(execID string, r *runningJob) {
	j.mu.Lock()
	defer j.mu.Unlock()

	if j.byID == nil {
		j.byID = map[string][]*runningJob{}
	}

	j.byID[execID] = append(j.byID[execID], r)
}

func (j *jobs) remove(execID string, r *runningJob) {
	j.mu.Lock()
	defer j.mu.Unlock()

	list := slices.DeleteFunc(j.byID[execID], func(x *runningJob) bool { return x == r })
	if len(list) == 0 {
		delete(j.byID, execID)

		return
	}

	j.byID[execID] = list
}

// stop cancels the running jobs that s names.
func (j *jobs) stop(s wire.Stop) {
	j.mu.Lock()
	defer j.mu.Unlock()

	for _, r := range j.byID[s.ExecID] {
		if s.Matches(r.key, r.generation, r.attempt) {
			r.cancel(ErrCancelled)
		}
	}
}

// subscribeStops follows the dispatcher's stop notices (one subscription per
// worker process).
func (w *Worker) subscribeStops() (*nats.Subscription, error) {
	return w.nc.Subscribe(w.in.Names.StopSubjects(), func(m *nats.Msg) {
		var s wire.Stop
		if err := json.Unmarshal(m.Data, &s); err != nil {
			return
		}

		w.running.stop(s)
	})
}

// watchLive cancels the job when the head of its execution's log shows the
// execution ended: the backstop for a stop notice core NATS did not deliver.
// It returns a function that ends the watch.
func (w *Worker) watchLive(ctx context.Context, wj wire.Job, cancel context.CancelCauseFunc) func() {
	if w.liveCheck <= 0 {
		return func() {}
	}

	done := make(chan struct{})

	go func() {
		t := time.NewTicker(w.liveCheck)
		defer t.Stop()

		missing := 0

		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-t.C:
			}

			ended, gone, err := w.executionEnded(ctx, wj.ExecID)
			if err != nil {
				continue
			}

			// An empty log means archived, unless a lagging replica answered
			// (I-59): it must persist.
			if gone {
				missing++
			} else {
				missing = 0
			}

			if ended || missing >= 2 { //nolint:mnd // two checks in a row.
				cancel(ErrCancelled)

				return
			}
		}
	}()

	return func() { close(done) }
}

// executionEnded reads the last decision of execID: ended when it holds a
// terminal event, gone when the log is empty (archived).
func (w *Worker) executionEnded(ctx context.Context, execID string) (ended, gone bool, err error) {
	s, err := w.eventsStream(ctx)
	if err != nil {
		return false, false, err
	}

	m, err := s.GetLastMsgForSubject(ctx, w.in.EventSubject(execID))
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return false, true, nil
	}

	if err != nil {
		return false, false, err
	}

	return slices.ContainsFunc(event.Types(m.Header), event.Type.Terminal), false, nil
}

func (w *Worker) eventsStream(ctx context.Context) (jetstream.Stream, error) {
	w.streamMu.Lock()
	defer w.streamMu.Unlock()

	if w.events != nil {
		return w.events, nil
	}

	s, err := w.in.JS.Stream(ctx, w.in.Names.StreamEvents)
	if err != nil {
		return nil, err
	}

	w.events = s

	return s, nil
}
