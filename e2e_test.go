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

package packtrail_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/internal/natstest"
	"github.com/henomis/packtrail/worker"
)

const linearFlow = `
name: lin
nodes:
  - {id: a, type: task, kind: upper, next: b}
  - {id: b, type: task, kind: upper}
`

func TestEndToEndLinear(t *testing.T) {
	s := natstest.Start(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	eng, err := packtrail.New(s.NC, packtrail.WithFlowYAML([]byte(linearFlow)), packtrail.WithPartitions(2))
	if err != nil {
		t.Fatal(err)
	}

	if err = eng.Init(ctx); err != nil {
		t.Fatal(err)
	}

	runCtx, stop := context.WithCancel(ctx)
	defer stop()

	go func() { _ = eng.Run(runCtx) }()

	w, err := worker.New(s.Connect(t), "upper", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		return &worker.Result{Output: map[string]any{"node": j.Node}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	go func() { _ = w.Run(runCtx) }()

	id, err := eng.Client().Start(ctx, "lin", map[string]any{"x": 1})
	if err != nil {
		t.Fatal(err)
	}

	st, err := eng.Client().Wait(ctx, id)
	if err != nil {
		t.Fatal(err)
	}

	if st.Status != packtrail.StatusCompleted || string(st.Output) != `{"node":"b"}` {
		t.Fatalf("state = %s %s %s", st.Status, st.Output, st.Error)
	}
}

const gatedFlow = `
name: gated
nodes:
  - {id: w, type: await, signal: go, timeout: 1h, next: a}
  - {id: a, type: task, kind: upper}
`

// TestDriveRightAfterStart drives executions the moment Start returns, waits
// for a point inside one, and decodes what it produced.
func TestDriveRightAfterStart(t *testing.T) {
	s := natstest.Start(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	eng, err := packtrail.New(s.NC, packtrail.WithFlowYAML([]byte(gatedFlow)), packtrail.WithPartitions(2))
	if err != nil {
		t.Fatal(err)
	}

	if err = eng.Init(ctx); err != nil {
		t.Fatal(err)
	}

	c := eng.Client()

	// No engine runs yet: Start cannot complete, but its command is kept.
	short, cancelShort := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancelShort()

	if _, err = c.Start(short, "gated", nil, packtrail.WithExecutionID("early")); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("start without an engine: %v", err)
	}

	runCtx, stop := context.WithCancel(ctx)
	defer stop()

	go func() { _ = eng.Run(runCtx) }()

	w, err := worker.New(s.Connect(t), "upper", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		return &worker.Result{Output: map[string]any{"node": j.Node}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	go func() { _ = w.Run(runCtx) }()

	// Retrying with the same id returns the execution the first call started.
	if id, serr := c.Start(ctx, "gated", nil, packtrail.WithExecutionID("early")); serr != nil || id != "early" {
		t.Fatalf("retried start: %q %v", id, serr)
	}

	id, err := c.Start(ctx, "gated", map[string]any{"x": 1})
	if err != nil {
		t.Fatal(err)
	}

	if err = c.Signal(ctx, id, "go", map[string]any{"by": "test"}); err != nil {
		t.Fatalf("signal right after start: %v", err)
	}

	gone, err := c.Start(ctx, "gated", nil)
	if err != nil {
		t.Fatal(err)
	}

	if err = c.Cancel(ctx, gone, "changed my mind"); err != nil {
		t.Fatalf("cancel right after start: %v", err)
	}

	// The early execution parks at its await.
	if _, err = c.WaitUntil(ctx, "early", func(st *packtrail.State) bool { return st.Awaits["w"] != nil }); err != nil {
		t.Fatal(err)
	}

	// A condition the execution ends without meeting.
	st, err := c.WaitUntil(ctx, gone, func(st *packtrail.State) bool { return st.Results["a"] != nil })
	if !errors.Is(err, packtrail.ErrTerminal) || st == nil || st.Status != packtrail.StatusCancelled {
		t.Fatalf("wait until on a cancelled execution: %v %+v", err, st)
	}

	if st, err = c.Wait(ctx, id); err != nil || st.Status != packtrail.StatusCompleted {
		t.Fatalf("wait: %v %+v", err, st)
	}

	var (
		in     struct{ X int }
		out    struct{ Node string }
		signal struct{ By string }
	)

	if err = st.DecodeInput(&in); err != nil || in.X != 1 {
		t.Fatalf("input %+v %v", in, err)
	}

	if err = st.Result("a", &out); err != nil || out.Node != "a" {
		t.Fatalf("result %+v %v", out, err)
	}

	if err = st.DecodeOutput(&out); err != nil || out.Node != "a" {
		t.Fatalf("output %+v %v", out, err)
	}

	if err = st.Signal("go", &signal); err != nil || signal.By != "test" {
		t.Fatalf("signal %+v %v", signal, err)
	}

	if err = st.Result("nope", &out); !errors.Is(err, packtrail.ErrNoValue) {
		t.Fatalf("result of a missing node: %v", err)
	}

	if err = st.Channel("none", &out); !errors.Is(err, packtrail.ErrNoValue) {
		t.Fatalf("missing channel: %v", err)
	}
}
