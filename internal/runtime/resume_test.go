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

package runtime

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/henomis/packtrail/internal/dsl"
	"github.com/henomis/packtrail/internal/names"
	"github.com/henomis/packtrail/internal/natstest"
	"github.com/henomis/packtrail/internal/scheduler"
	"github.com/henomis/packtrail/internal/store"
	"github.com/henomis/packtrail/invoker"
)

// flakyInvoker fails until ok is set, then succeeds.
type flakyInvoker struct{ ok atomic.Bool }

func (f *flakyInvoker) Invoke(_ context.Context, _ invoker.Request) (invoker.Result, error) {
	if f.ok.Load() {
		return invoker.Result{Status: invoker.StatusOK, Payload: []byte(`{"done":true}`)}, nil
	}

	return invoker.Result{Status: invoker.StatusError, Error: "boom"}, nil
}

const resumeFlow = `
version: "1.0"
name: resume-me
nodes:
  - {id: a, type: task, subject: "x"}
edges: []
`

func TestResumeFailedExecution(t *testing.T) {
	srv := natstest.Start(t)
	ctx := context.Background()

	st, err := store.Open(ctx, srv.JS, names.New(""))
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	sch := scheduler.New(srv.JS, names.New(""))
	if err = sch.EnsureStream(ctx); err != nil {
		t.Fatalf("scheduler: %v", err)
	}

	flow, _ := dsl.Parse([]byte(resumeFlow))
	inv := &flakyInvoker{}

	eng, err := New(inv, st, sch, testSignals(t, st), map[string]*dsl.Flow{flow.Name: flow}, Config{})
	if err != nil {
		t.Fatalf("engine: %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() { _ = eng.Run(runCtx) }()

	waitFor := func(id, status string) *store.Execution {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			ex, getErr := st.Get(ctx, id)
			if getErr == nil && ex.Status == status {
				return ex
			}

			time.Sleep(15 * time.Millisecond)
		}

		t.Fatalf("exec %s never reached %q", id, status)

		return nil
	}

	id, err := eng.Start(ctx, "resume-me", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	waitFor(id, store.StatusFailed)

	// Resuming the failed execution re-runs node a; now the invoker succeeds.
	inv.ok.Store(true)

	if resumeErr := eng.Resume(ctx, id); resumeErr != nil {
		t.Fatalf("resume: %v", resumeErr)
	}

	waitFor(id, store.StatusCompleted)

	doc, err := eng.Results(ctx, id)
	if err != nil {
		t.Fatalf("results: %v", err)
	}

	if got := parseCtx(t, doc); string(got.Results["a"]) != `{"done":true}` {
		t.Errorf("results.a = %s, want {\"done\":true}", got.Results["a"])
	}

	// Resuming a non-failed (now completed) execution is an error.
	if resumeErr := eng.Resume(ctx, id); resumeErr == nil {
		t.Error("expected error resuming a completed execution")
	}
	// Resuming an unknown execution returns ErrNotFound.
	if resumeErr := eng.Resume(ctx, "exec-nope"); !errors.Is(resumeErr, store.ErrNotFound) {
		t.Errorf("resume unknown = %v, want ErrNotFound", resumeErr)
	}
}
