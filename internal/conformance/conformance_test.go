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

package conformance

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/internal/natstest"
	"github.com/henomis/packtrail/worker"
)

const flows = `
name: conf-task
channels: {log: {reducer: append}, total: {reducer: sum}}
nodes:
  - id: t
    type: task
    kind: echo
    timeout: 20s
    retry: {max_attempts: 3, backoff: fixed, delay: 50ms}
    dynamic: [left]
    next: right
  - {id: left, type: task, kind: echo}
  - {id: right, type: task, kind: echo}
`

const mapFlow = `
name: conf-map
nodes:
  - {id: m, type: map, kind: echo, over: input.items, max_parallel: 3}
`

type suite struct {
	t      *testing.T
	ctx    context.Context //nolint:containedctx // test-scoped.
	client *packtrail.Client
}

func setup(t *testing.T) *suite {
	t.Helper()

	s := natstest.Start(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)

	eng, err := packtrail.New(s.Connect(t), packtrail.WithFlowYAML([]byte(flows)), packtrail.WithFlowYAML([]byte(mapFlow)),
		packtrail.WithPartitions(2))
	if err != nil {
		t.Fatal(err)
	}

	if err = eng.Init(ctx); err != nil {
		t.Fatal(err)
	}

	runCtx, stop := context.WithCancel(ctx)
	t.Cleanup(stop)

	go func() { _ = eng.Run(runCtx) }()

	startWorker(runCtx, t, s)

	return &suite{t: t, ctx: ctx, client: eng.Client()}
}

func startWorker(ctx context.Context, t *testing.T, s *natstest.Server) {
	t.Helper()

	if command := os.Getenv("PT_CONFORMANCE_WORKER"); command != "" {
		cmd := exec.CommandContext(ctx, "sh", "-c", command) //nolint:gosec // operator-supplied test command.

		cmd.Env = append(os.Environ(), "NATS_URL="+s.URL(), "PACKTRAIL_NAMESPACE=packtrail")
		cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr

		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })

		return
	}

	w, err := worker.New(s.Connect(t), Kind, Echo, worker.WithAckWait(2*time.Second), worker.WithConcurrency(4))
	if err != nil {
		t.Fatal(err)
	}

	go func() { _ = w.Run(ctx) }()
}

func (s *suite) run(flowName string, input any) *packtrail.State {
	s.t.Helper()

	id, err := s.client.Start(s.ctx, flowName, input)
	if err != nil {
		s.t.Fatal(err)
	}

	st, err := s.client.Wait(s.ctx, id)
	if err != nil {
		s.t.Fatal(err)
	}

	return st
}

func field(t *testing.T, raw json.RawMessage, key string) string {
	t.Helper()

	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("output %s: %v", raw, err)
	}

	return string(m[key])
}

func TestConformance(t *testing.T) {
	s := setup(t)

	t.Run("output", func(t *testing.T) {
		st := s.run("conf-task", map[string]any{"q": 1})
		if st.Status != packtrail.StatusCompleted || field(t, st.Results["t"], "input") != `{"q":1}` {
			t.Fatalf("%s %s", st.Status, st.Results["t"])
		}
	})

	t.Run("writes, next and usage", func(t *testing.T) {
		st := s.run("conf-task", map[string]any{
			"writes": map[string]any{"log": "x", "total": 2}, "next": map[string]string{"t": "left"}, "usage": map[string]float64{"u": 3},
		})
		if st.Status != packtrail.StatusCompleted || st.LastNode != "left" || st.Counters["u"] < 3 || !strings.Contains(string(st.Channels["log"]), `"x"`) {
			t.Fatalf("last %s counters %v channels %v", st.LastNode, st.Counters, st.Channels)
		}
	})

	t.Run("retryable failure", func(t *testing.T) {
		st := s.run("conf-task", map[string]any{"fail_until": map[string]int{"t": 3}})
		if st.Status != packtrail.StatusCompleted || field(t, st.Results["t"], "attempt") != "3" {
			t.Fatalf("%s %s", st.Status, st.Results["t"])
		}
	})

	t.Run("permanent failure", func(t *testing.T) {
		if st := s.run("conf-task", map[string]any{"permanent": true}); st.Status != packtrail.StatusFailed {
			t.Fatalf("status %s", st.Status)
		}
	})

	t.Run("interrupt and resume", func(t *testing.T) {
		id, err := s.client.Start(s.ctx, "conf-task", map[string]any{"interrupt": "t"})
		if err != nil {
			t.Fatal(err)
		}

		deadline := time.Now().Add(20 * time.Second)

		for {
			st, lerr := s.client.Get(s.ctx, id)
			if lerr == nil && st.Status == packtrail.StatusWaiting {
				break
			}

			if time.Now().After(deadline) {
				b, _ := json.Marshal(st)
				t.Fatalf("never waiting: %v %s", lerr, b)
			}

			time.Sleep(20 * time.Millisecond)
		}

		if err = s.client.Resume(s.ctx, id, "t", "go on"); err != nil {
			t.Fatal(err)
		}

		wctx, cancel := context.WithTimeout(s.ctx, 15*time.Second)
		defer cancel()

		st, err := s.client.Wait(wctx, id)
		if err != nil {
			evs, _ := s.client.History(s.ctx, id)
			for _, ev := range evs {
				b, _ := json.Marshal(ev)
				t.Log(string(b))
			}

			t.Fatal(err)
		}

		if field(t, st.Results["t"], "resume") != `"go on"` {
			t.Fatalf("%v %s", err, st.Results["t"])
		}
	})

	t.Run("map items", func(t *testing.T) {
		st := s.run("conf-map", map[string]any{"items": []string{"a", "b", "c", "d"}})
		if st.Status != packtrail.StatusCompleted || !strings.Contains(string(st.Results["m"]), `"item":"d"`) {
			t.Fatalf("%s %s", st.Status, st.Results["m"])
		}
	})

	t.Run("claim-checked payload", func(t *testing.T) {
		big := strings.Repeat("z", 2*1024*1024)

		st := s.run("conf-task", map[string]any{"blob": big})
		if st.Status != packtrail.StatusCompleted || !strings.Contains(string(st.Results["t"]), big) {
			t.Fatalf("status %s", st.Status)
		}
	})

	t.Run("heartbeat keeps a long job", func(t *testing.T) {
		st := s.run("conf-task", map[string]any{"sleep_ms": 5000})
		if st.Status != packtrail.StatusCompleted || field(t, st.Results["t"], "attempt") != "1" {
			t.Fatalf("a job longer than the ack wait was redelivered: %s %s", st.Status, st.Results["t"])
		}
	})
}
