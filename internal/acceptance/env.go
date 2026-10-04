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

// Package acceptance is packtrail's black-box acceptance suite: every test
// drives a real engine, real workers and a real embedded nats-server through
// the public API only, with fault injection (engine kill and restart, NATS
// restart, duplicate and reordered completions). Each scenario pins a
// guaranteed behaviour.
package acceptance

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/flow"
	"github.com/henomis/packtrail/internal/natstest"
	"github.com/henomis/packtrail/worker"
)

// Env is one test deployment.
type Env struct {
	T      *testing.T
	S      *natstest.Server
	Ctx    context.Context //nolint:containedctx // test-scoped context.
	Client *packtrail.Client
	// Engine is the first engine started.
	Engine *packtrail.Engine

	opts []packtrail.Option

	mu      sync.Mutex
	engines []*running
	workers []*running
}

type running struct {
	stop context.CancelFunc
	done chan struct{}
}

// testPartitions is the partition count of every test deployment.
const testPartitions = 4

// DefaultTimeout bounds every acceptance test.
const DefaultTimeout = 60 * time.Second

// NewEnv starts NATS, provisions the namespace with the given flows and
// starts one engine.
func NewEnv(t *testing.T, flows []string, opts ...packtrail.Option) *Env {
	t.Helper()

	s := natstest.Start(t)
	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
	t.Cleanup(cancel)

	base := make([]packtrail.Option, 0, 2+len(flows)+len(opts))
	base = append(base, packtrail.WithPartitions(testPartitions), packtrail.WithDrainTimeout(2*time.Second))

	for _, f := range flows {
		base = append(base, packtrail.WithFlowYAML([]byte(f)))
	}

	e := &Env{T: t, S: s, Ctx: ctx, opts: append(base, opts...)}
	eng := e.StartEngine()
	e.Client = eng.Client()
	e.Engine = eng

	t.Cleanup(e.stopAll)

	return e
}

// StartEngine starts another engine process (own connection).
func (e *Env) StartEngine(extra ...packtrail.Option) *packtrail.Engine {
	e.T.Helper()

	eng, err := packtrail.New(e.S.Connect(e.T), append(append([]packtrail.Option{}, e.opts...), extra...)...)
	if err != nil {
		e.T.Fatal(err)
	}

	if err = eng.Init(e.Ctx); err != nil {
		e.T.Fatal(err)
	}

	r := e.spawn(func(ctx context.Context) { _ = eng.Run(ctx) })

	e.mu.Lock()
	e.engines = append(e.engines, r)
	e.mu.Unlock()

	return eng
}

func (e *Env) spawn(fn func(context.Context)) *running {
	ctx, stop := context.WithCancel(e.Ctx)
	r := &running{stop: stop, done: make(chan struct{})}

	go func() {
		defer close(r.done)

		fn(ctx)
	}()

	return r
}

// KillEngines stops every running engine (gracefully: in-flight commands
// drain) and waits for them.
func (e *Env) KillEngines() {
	e.mu.Lock()
	engines := e.engines
	e.engines = nil
	e.mu.Unlock()

	for _, r := range engines {
		r.stop()
		<-r.done
	}
}

// RestartEngine replaces every engine with a fresh one.
func (e *Env) RestartEngine() {
	e.KillEngines()
	e.StartEngine()
}

func (e *Env) stopAll() {
	e.KillEngines()

	e.mu.Lock()
	ws := e.workers
	e.workers = nil
	e.mu.Unlock()

	for _, r := range ws {
		r.stop()
		<-r.done
	}
}

// Worker starts a worker for kind.
func (e *Env) Worker(kind string, h worker.Handler, opts ...worker.Option) {
	e.T.Helper()

	opts = append([]worker.Option{worker.WithDrainTimeout(time.Second), worker.WithAckWait(2 * time.Second)},
		opts...)

	w, err := worker.New(e.S.Connect(e.T), kind, h, opts...)
	if err != nil {
		e.T.Fatal(err)
	}

	r := e.spawn(func(ctx context.Context) { _ = w.Run(ctx) })

	e.mu.Lock()
	e.workers = append(e.workers, r)
	e.mu.Unlock()
}

// Echo is a handler returning {"node": <node>, "input": <input>}.
func Echo(_ context.Context, j *worker.Job) (*worker.Result, error) {
	return &worker.Result{Output: map[string]any{"node": j.Node, "input": j.Context.Input}}, nil
}

// Start starts flowName and returns the execution id.
func (e *Env) Start(flowName string, input any, opts ...packtrail.StartOption) string {
	e.T.Helper()

	id, err := e.Client.Start(e.Ctx, flowName, input, opts...)
	if err != nil {
		e.T.Fatal(err)
	}

	return id
}

// Wait waits for a terminal state.
func (e *Env) Wait(id string) *packtrail.State {
	e.T.Helper()

	wctx, cancel := context.WithTimeout(e.Ctx, DefaultTimeout/2)
	defer cancel()

	st, err := e.Client.Wait(wctx, id)
	if err != nil {
		cur, gerr := e.Client.Get(context.Background(), id)
		if gerr == nil {
			b, _ := json.Marshal(cur) //nolint:errchkjson // diagnostics only
			e.T.Fatalf("wait %s: %v\nstate: %s\n%s", id, err, b, e.debugStreams())
		}

		e.T.Fatalf("wait %s: %v (get: %v)", id, err, gerr)
	}

	return st
}

// WaitStatus waits until the execution has status want.
func (e *Env) WaitStatus(id string, want packtrail.Status) *packtrail.State {
	e.T.Helper()

	var st *packtrail.State

	e.Eventually(func() bool {
		var err error

		st, err = e.Client.Get(e.Ctx, id)

		return err == nil && st.Status == want
	}, func() string { return fmt.Sprintf("execution %s never reached %s (last: %+v)", id, want, st) })

	return st
}

// Eventually polls cond until it holds or the test context ends.
func (e *Env) Eventually(cond func() bool, msg func() string) {
	e.T.Helper()

	for !cond() {
		select {
		case <-e.Ctx.Done():
			e.T.Fatal(msg())
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// Completed waits and asserts a completed execution.
func (e *Env) Completed(id string) *packtrail.State {
	e.T.Helper()

	st := e.Wait(id)
	if st.Status != packtrail.StatusCompleted {
		e.T.Fatalf("execution %s: status %s (%s: %s)", id, st.Status, st.Reason, st.Error)
	}

	return st
}

// Register registers a flow at runtime.
func (e *Env) Register(yaml string) string {
	e.T.Helper()

	def, err := flow.Parse([]byte(yaml))
	if err != nil {
		e.T.Fatal(err)
	}

	h, err := e.Client.Register(e.Ctx, def)
	if err != nil {
		e.T.Fatal(err)
	}

	return h
}

// NC returns a fresh connection.
func (e *Env) NC() *nats.Conn { return e.S.Connect(e.T) }

// JSON decodes raw into a generic value.
func JSON(t *testing.T, raw json.RawMessage) any {
	t.Helper()

	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("json %s: %v", raw, err)
	}

	return v
}

func (e *Env) debugStreams() string {
	ctx := context.Background()
	out := ""

	if path := os.Getenv("PT_STACK_DUMP"); path != "" {
		buf := make([]byte, 64<<20)
		n := runtime.Stack(buf, true)
		_ = os.WriteFile(path, buf[:n], 0o600) //nolint:gosec // test-only debug dump path.
	}

	for _, name := range []string{"packtrail-cmd", "packtrail-work", "packtrail-events"} {
		s, err := e.S.JS.Stream(ctx, name)
		if err != nil {
			continue
		}

		info, _ := s.Info(ctx)
		out += fmt.Sprintf("stream %s: msgs=%d last=%d\n", name, info.State.Msgs, info.State.LastSeq)

		for c := range s.ListConsumers(ctx).Info() {
			out += fmt.Sprintf("  consumer %s: pending=%d ackpending=%d redelivered=%d waiting=%d "+
				"delivered=%d ackfloor=%d pins=%+v\n",
				c.Name, c.NumPending, c.NumAckPending, c.NumRedelivered, c.NumWaiting, c.Delivered.Stream,
				c.AckFloor.Stream, c.PriorityGroups)
		}
	}

	return out
}
