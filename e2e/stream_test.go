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
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/event"
	"github.com/henomis/packtrail/worker"
)

const renderFlow = `
name: render
nodes:
  - {id: frames, type: task, kind: renderer, next: publish}
  - {id: publish, type: task, kind: publisher}
`

const frameCount = 10

// collect drains ch until it closes.
func collect[T any](cl *cluster, what string, ch <-chan T) <-chan []T {
	out := make(chan []T, 1)

	go func() {
		var got []T

		for {
			select {
			case v, ok := <-ch:
				if !ok {
					out <- got

					return
				}

				got = append(got, v)
			case <-cl.ctx.Done():
				cl.t.Errorf("%s never closed (%d received)", what, len(got))

				out <- got

				return
			}
		}
	}()

	return out
}

// TestStreamingAcrossCrash follows a job that streams progress while its
// worker process crashes half way and NATS restarts. The client's progress
// stream carries on with the redelivered job and closes when the execution
// ends; the live event stream matches the history exactly.
func TestStreamingAcrossCrash(t *testing.T) {
	cl := newCluster(t, []string{renderFlow})

	half := make(chan struct{})

	renderer := func(ctx context.Context, j *worker.Job) (*worker.Result, error) {
		n := frameCount
		if j.Deliveries == 1 {
			n = frameCount / 2
		}

		for i := 1; i <= n; i++ {
			if err := j.Progress(map[string]any{"frame": i}); err != nil {
				return nil, err
			}

			time.Sleep(20 * time.Millisecond)
		}

		if j.Deliveries == 1 {
			// Crashed here by the test: never answers.
			close(half)
			<-ctx.Done()

			return nil, ctx.Err()
		}

		return &worker.Result{Output: map[string]any{"frames": n}}, nil
	}

	first := cl.worker("renderer", renderer)
	cl.worker("publisher", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		var r struct{ Frames int }
		if err := j.Result("frames", &r); err != nil {
			return nil, worker.Permanent(err)
		}

		return &worker.Result{Output: map[string]any{"published": r.Frames}}, nil
	})

	id := fmt.Sprintf("render-%d", time.Now().UnixNano())

	// Progress is not stored: subscribe before the execution starts.
	prog, err := cl.c.Progress(cl.ctx, id)
	if err != nil {
		t.Fatal(err)
	}

	progress := collect(cl, "progress", prog)

	cl.start("render", nil, packtrail.WithExecutionID(id))

	watch, err := cl.c.Watch(cl.ctx, id, 0)
	if err != nil {
		t.Fatal(err)
	}

	live := collect(cl, "watch", watch)

	select {
	case <-half:
	case <-cl.ctx.Done():
		t.Fatal("the first delivery never streamed half its frames")
	}

	first.crash()
	cl.s.Restart(t)
	// Let the client resubscribe before the redelivered job streams.
	time.Sleep(500 * time.Millisecond)
	cl.worker("renderer", renderer)

	st := cl.wait(id)
	f := cl.check(id)

	var pub struct{ Published int }
	if st.Status != packtrail.StatusCompleted || st.Result("publish", &pub) != nil || pub.Published != frameCount {
		t.Fatalf("%s: %s published %s", id, st.Status, st.Results["publish"])
	}

	// The stream: half the frames of the crashed delivery, then every frame
	// of the redelivery, in order.
	msgs := <-progress

	frames := make([]int, len(msgs))

	for i, m := range msgs {
		var d struct{ Frame int }
		if json.Unmarshal(m.Data, &d) != nil || m.ExecID != id || m.Node != "frames" {
			t.Fatalf("progress message %+v", m)
		}

		frames[i] = d.Frame
	}

	if len(frames) < frameCount {
		t.Fatalf("progress frames %v", frames)
	}

	tail := frames[len(frames)-frameCount:]
	for i, fr := range tail {
		if fr != i+1 {
			t.Fatalf("the redelivered job streamed %v (all: %v)", tail, frames)
		}
	}

	for i, fr := range frames[:len(frames)-frameCount] {
		if fr != i+1 || fr > frameCount/2 {
			t.Fatalf("the crashed delivery streamed %v (all: %v)", frames[:len(frames)-frameCount], frames)
		}
	}

	// The live event stream is the history, and ended with it.
	evs := <-live
	if len(evs) != len(f.events) || !evs[len(evs)-1].Type.Terminal() {
		t.Fatalf("watch delivered %d events, history has %d", len(evs), len(f.events))
	}

	for i, ev := range evs {
		h := f.events[i]
		if ev.Index != h.Index || ev.Type != h.Type || ev.Seq != h.Seq {
			t.Fatalf("watch event %d is %s#%d@%d, history has %s#%d@%d", i, ev.Type, ev.Index, ev.Seq,
				h.Type, h.Index, h.Seq)
		}
	}

	// The crash cost a redelivery, not a retry.
	if f.retried["frames"] != 0 || f.count[event.NodeScheduled] != 2 {
		t.Fatalf("frames retried %d, %d jobs scheduled", f.retried["frames"], f.count[event.NodeScheduled])
	}
}
