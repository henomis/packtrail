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
	"encoding/json"
	"testing"
	"time"

	"github.com/henomis/packtrail/internal/dsl"
	"github.com/henomis/packtrail/internal/names"
	"github.com/henomis/packtrail/internal/natstest"
	"github.com/henomis/packtrail/internal/scheduler"
	"github.com/henomis/packtrail/internal/signal"
	"github.com/henomis/packtrail/internal/store"
	"github.com/henomis/packtrail/invoker"
)

// pendingInvoker records every dispatch and always reports StatusPending, so the
// test drives completion explicitly via CompleteActivity.
type pendingInvoker struct{ reqs chan invoker.Request }

func (p *pendingInvoker) Invoke(_ context.Context, req invoker.Request) (invoker.Result, error) {
	p.reqs <- req
	return invoker.Result{Status: invoker.StatusPending}, nil
}

type blockingResultInvoker struct {
	reqs    chan invoker.Request
	release chan invoker.Result
}

func newBlockingResultInvoker() *blockingResultInvoker {
	return &blockingResultInvoker{
		reqs:    make(chan invoker.Request, 1),
		release: make(chan invoker.Result, 1),
	}
}

func (b *blockingResultInvoker) Invoke(ctx context.Context, req invoker.Request) (invoker.Result, error) {
	b.reqs <- req

	select {
	case res := <-b.release:
		return res, nil
	case <-ctx.Done():
		return invoker.Result{}, ctx.Err()
	}
}

// testSignals builds a Signals with its stream ensured, as New requires.
func testSignals(t *testing.T, st *store.Store) *signal.Signals {
	t.Helper()

	sigs := signal.New(st.JS(), st.Names())
	if err := sigs.EnsureStream(context.Background()); err != nil {
		t.Fatalf("signals stream: %v", err)
	}

	return sigs
}

// asyncHarness wires an engine with a pendingInvoker.
type asyncHarness struct {
	store  *store.Store
	engine *Engine
	inv    *pendingInvoker
}

func newAsyncHarness(t *testing.T, flowYAML string, moreFlows ...string) *asyncHarness {
	t.Helper()
	return newAsyncHarnessCfg(t, flowYAML, Config{}, moreFlows...)
}

func newAsyncHarnessCfg(t *testing.T, flowYAML string, cfg Config, moreFlows ...string) *asyncHarness {
	t.Helper()

	ctx := context.Background()
	srv := natstest.Start(t)

	st, err := store.Open(ctx, srv.JS, names.New(""))
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	sch := scheduler.New(srv.JS, names.New(""))
	if err = sch.EnsureStream(ctx); err != nil {
		t.Fatalf("scheduler: %v", err)
	}

	flows := map[string]*dsl.Flow{}

	for _, y := range append([]string{flowYAML}, moreFlows...) {
		flow, parseErr := dsl.Parse([]byte(y))
		if parseErr != nil {
			t.Fatalf("flow: %v", parseErr)
		}

		flows[flow.Name] = flow
	}

	inv := &pendingInvoker{reqs: make(chan invoker.Request, 16)}

	eng, err := New(inv, st, sch, testSignals(t, st), flows, cfg)
	if err != nil {
		t.Fatalf("engine: %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)

	go func() { _ = eng.Run(runCtx) }()

	return &asyncHarness{store: st, engine: eng, inv: inv}
}

func newAsyncEngineWithInvoker(
	t *testing.T, flowYAML string, inv invoker.Invoker,
) (*Engine, *store.Store, *dsl.Flow) {
	t.Helper()

	ctx := context.Background()
	srv := natstest.Start(t)

	st, err := store.Open(ctx, srv.JS, names.New(""))
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	sch := scheduler.New(srv.JS, names.New(""))
	if err = sch.EnsureStream(ctx); err != nil {
		t.Fatalf("scheduler: %v", err)
	}

	flow, err := dsl.Parse([]byte(flowYAML))
	if err != nil {
		t.Fatalf("flow: %v", err)
	}

	eng, err := New(inv, st, sch, testSignals(t, st), map[string]*dsl.Flow{flow.Name: flow}, Config{})
	if err != nil {
		t.Fatalf("engine: %v", err)
	}

	return eng, st, flow
}

func (h *asyncHarness) nextReq(t *testing.T) invoker.Request {
	t.Helper()

	select {
	case r := <-h.inv.reqs:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("no dispatch received")
		return invoker.Request{}
	}
}

// ctxDoc is the unpacked shape of an assembled invocation context — what an
// Invoker's Request.Payload (and Engine.Results) carries.
type ctxDoc struct {
	Input   json.RawMessage            `json:"input"`
	Results map[string]json.RawMessage `json:"results"`
	Signals map[string]json.RawMessage `json:"signals"`
}

// parseCtx unpacks an assembled context document.
func parseCtx(t *testing.T, doc json.RawMessage) ctxDoc {
	t.Helper()

	var c ctxDoc
	if err := json.Unmarshal(doc, &c); err != nil {
		t.Fatalf("context doc %s: %v", doc, err)
	}

	return c
}

// results assembles the execution's data-plane view via the engine.
func (h *asyncHarness) results(t *testing.T, id string) ctxDoc {
	t.Helper()

	doc, err := h.engine.Results(context.Background(), id)
	if err != nil {
		t.Fatalf("results %s: %v", id, err)
	}

	return parseCtx(t, doc)
}

func (h *asyncHarness) get(t *testing.T, id string) *store.Execution {
	t.Helper()

	ex, err := h.store.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("get %s: %v", id, err)
	}

	return ex
}

func (h *asyncHarness) waitStatus(t *testing.T, id, status string) *store.Execution {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ex, err := h.store.Get(context.Background(), id)
		if err == nil && ex.Status == status {
			return ex
		}

		time.Sleep(15 * time.Millisecond)
	}

	t.Fatalf("exec %s never reached %q", id, status)

	return nil
}

const asyncLinearFlow = `
version: "1.0"
name: async-linear
nodes:
  - {id: a, type: task, subject: "x"}
  - {id: b, type: task, subject: "y"}
edges:
  - {from: a, to: b}
`

const asyncSingleFlow = `
version: "1.0"
name: async-single
nodes:
  - {id: a, type: task, subject: "x"}
edges: []
`

// TestAsyncTaskParksAndCompletes drives a two-node async flow: each task reports
// Pending (engine parks, freeing the slot), the test completes it, the payload
// threads forward, and the flow finishes.
func TestAsyncTaskParksAndCompletes(t *testing.T) {
	h := newAsyncHarness(t, asyncLinearFlow)
	ctx := context.Background()

	id, err := h.engine.Start(ctx, "async-linear", json.RawMessage(`{"n":0}`))
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	// Node a is dispatched and the execution parks as waiting.
	ra := h.nextReq(t)
	if ra.NodeID != "a" || ra.Attempt != 0 {
		t.Fatalf("first dispatch = %+v, want node a attempt 0", ra)
	}

	h.waitStatus(t, id, store.StatusWaiting)

	// Complete a; engine advances to b with a's payload.
	if completeErr := h.engine.CompleteActivity(ctx, id, "a", 0, invoker.Result{Status: invoker.StatusOK, Payload: json.RawMessage(`{"n":1}`)}); completeErr != nil {
		t.Fatalf("complete a: %v", completeErr)
	}

	rb := h.nextReq(t)
	if rb.NodeID != "b" {
		t.Fatalf("second dispatch = %+v, want node b", rb)
	}

	// b sees a's output under results.a and the start input untouched.
	bCtx := parseCtx(t, rb.Payload)
	if string(bCtx.Results["a"]) != `{"n":1}` || string(bCtx.Input) != `{"n":0}` {
		t.Fatalf("b context = %s, want results.a={\"n\":1} input={\"n\":0}", rb.Payload)
	}

	// Complete b; flow finishes.
	if completeErr := h.engine.CompleteActivity(ctx, id, "b", 0, invoker.Result{Status: invoker.StatusOK, Payload: json.RawMessage(`{"n":2}`)}); completeErr != nil {
		t.Fatalf("complete b: %v", completeErr)
	}

	h.waitStatus(t, id, store.StatusCompleted)

	if got := h.results(t, id); string(got.Results["b"]) != `{"n":2}` {
		t.Fatalf("final results.b = %s, want {\"n\":2}", got.Results["b"])
	}
}

// TestCompleteActivityIdempotent verifies a duplicate or stale completion is a
// no-op (does not double-advance).
func TestCompleteActivityIdempotent(t *testing.T) {
	h := newAsyncHarness(t, asyncLinearFlow)
	ctx := context.Background()
	id, _ := h.engine.Start(ctx, "async-linear", nil)
	_ = h.nextReq(t)
	h.waitStatus(t, id, store.StatusWaiting)

	res := invoker.Result{Status: invoker.StatusOK, Payload: json.RawMessage(`{"k":1}`)}
	if err := h.engine.CompleteActivity(ctx, id, "a", 0, res); err != nil {
		t.Fatalf("complete: %v", err)
	}

	_ = h.nextReq(t) // b dispatched

	// Duplicate completion of a (now at b) must be ignored.
	if err := h.engine.CompleteActivity(ctx, id, "a", 0, res); err != nil {
		t.Fatalf("duplicate complete: %v", err)
	}

	ex := h.get(t, id)
	if ex.CurrentNode != "b" {
		t.Fatalf("after duplicate, current node = %q, want b", ex.CurrentNode)
	}
}

func TestCompleteActivityRejectsStaleGenerationAfterResume(t *testing.T) {
	h := newAsyncHarness(t, asyncSingleFlow)
	ctx := context.Background()

	id, err := h.engine.Start(ctx, "async-single", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	firstReq := h.nextReq(t)
	if firstReq.NodeID != "a" || firstReq.Generation == 0 || firstReq.Attempt != 0 {
		t.Fatalf("first dispatch = %+v, want node a generation >0 attempt 0", firstReq)
	}

	h.waitStatus(t, id, store.StatusWaiting)

	if err = h.engine.CompleteActivityWithGeneration(ctx, id, "a", firstReq.Generation, 0,
		invoker.Result{Status: invoker.StatusError, Error: "boom"}); err != nil {
		t.Fatalf("fail first generation: %v", err)
	}

	h.waitStatus(t, id, store.StatusFailed)

	if err = h.engine.Resume(ctx, id); err != nil {
		t.Fatalf("resume: %v", err)
	}

	secondReq := h.nextReq(t)
	if secondReq.NodeID != "a" || secondReq.Generation <= firstReq.Generation || secondReq.Attempt != 0 {
		t.Fatalf("second dispatch = %+v, want newer generation attempt 0", secondReq)
	}

	h.waitStatus(t, id, store.StatusWaiting)

	if err = h.engine.CompleteActivityWithGeneration(ctx, id, "a", firstReq.Generation, 0,
		invoker.Result{Status: invoker.StatusOK, Payload: json.RawMessage(`{"stale":true}`)}); err != nil {
		t.Fatalf("complete stale generation: %v", err)
	}

	ex := h.get(t, id)
	if ex.Status != store.StatusWaiting || ex.NodeGeneration != secondReq.Generation {
		t.Fatalf("after stale completion = %+v, want still waiting on generation %d", ex, secondReq.Generation)
	}

	if err = h.engine.CompleteActivityWithGeneration(ctx, id, "a", secondReq.Generation, 0,
		invoker.Result{Status: invoker.StatusOK, Payload: json.RawMessage(`{"fresh":true}`)}); err != nil {
		t.Fatalf("complete fresh generation: %v", err)
	}

	h.waitStatus(t, id, store.StatusCompleted)

	if got := h.results(t, id); string(got.Results["a"]) != `{"fresh":true}` {
		t.Fatalf("results.a = %s, want fresh generation output", got.Results["a"])
	}
}

func TestStepTaskIgnoresStaleGenerationAfterInvoke(t *testing.T) {
	tests := []struct {
		name string
		res  invoker.Result
	}{
		{
			name: "pending",
			res:  invoker.Result{Status: invoker.StatusPending},
		},
		{
			name: "success",
			res:  invoker.Result{Status: invoker.StatusOK, Payload: json.RawMessage(`{"stale":true}`)},
		},
		{
			name: "error",
			res:  invoker.Result{Status: invoker.StatusError, Error: "stale failure"},
		},
		{
			name: "retry",
			res:  invoker.Result{Status: invoker.StatusRetry, Error: "stale retry"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			inv := newBlockingResultInvoker()
			eng, st, flow := newAsyncEngineWithInvoker(t, asyncSingleFlow, inv)
			exec := &store.Execution{
				ID:             "stale-invoke-" + tt.name,
				FlowName:       flow.Name,
				Status:         store.StatusRunning,
				CurrentNode:    "a",
				NodeGeneration: 1,
				Attempt:        0,
			}

			if _, err := st.Create(ctx, exec); err != nil {
				t.Fatalf("create: %v", err)
			}

			errCh := make(chan error, 1)
			go func() {
				errCh <- eng.stepTask(ctx, flow, flow.Node("a"), exec)
			}()

			var req invoker.Request
			select {
			case req = <-inv.reqs:
			case <-time.After(5 * time.Second):
				t.Fatal("no dispatch received")
			}

			if req.Generation != 1 || req.Attempt != 0 {
				t.Fatalf("request = %+v, want generation 1 attempt 0", req)
			}

			if _, err := st.Mutate(ctx, exec.ID, func(ex *store.Execution) error {
				ex.NodeGeneration = 2
				ex.Status = store.StatusRunning
				ex.CurrentNode = "a"
				ex.Attempt = 0

				return nil
			}); err != nil {
				t.Fatalf("move generation: %v", err)
			}

			inv.release <- tt.res

			select {
			case err := <-errCh:
				if err != nil {
					t.Fatalf("stepTask: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("stepTask did not return")
			}

			got, err := st.Get(ctx, exec.ID)
			if err != nil {
				t.Fatalf("get: %v", err)
			}

			if got.Status != store.StatusRunning ||
				got.CurrentNode != "a" ||
				got.NodeGeneration != 2 ||
				got.Attempt != 0 ||
				got.Activity != nil ||
				!got.RetryAt.IsZero() ||
				len(got.Outputs) != 0 {
				t.Fatalf("after stale %s result = %+v, want untouched generation 2 visit", tt.name, got)
			}
		})
	}
}

func TestAdvanceWorkItemRejectsStaleGeneration(t *testing.T) {
	ctx := context.Background()
	inv := &pendingInvoker{reqs: make(chan invoker.Request, 2)}
	eng, st, flow := newAsyncEngineWithInvoker(t, asyncSingleFlow, inv)

	exec := &store.Execution{
		ID:             "stale-work-generation",
		FlowName:       flow.Name,
		Status:         store.StatusRunning,
		CurrentNode:    "a",
		NodeGeneration: 2,
		Attempt:        0,
	}
	if _, err := st.Create(ctx, exec); err != nil {
		t.Fatalf("create: %v", err)
	}

	if err := eng.processAdvance(ctx, flow, exec, workItem{
		Kind: kindAdvance, Node: "a", Generation: 1, Attempt: 0,
	}); err != nil {
		t.Fatalf("stale processAdvance: %v", err)
	}

	select {
	case req := <-inv.reqs:
		t.Fatalf("stale generation work dispatched: %+v", req)
	case <-time.After(300 * time.Millisecond):
	}

	fresh, err := st.Get(ctx, exec.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if err = eng.processAdvance(ctx, flow, fresh, workItem{
		Kind: kindAdvance, Node: "a", Generation: 2, Attempt: 0,
	}); err != nil {
		t.Fatalf("fresh processAdvance: %v", err)
	}

	select {
	case req := <-inv.reqs:
		if req.Generation != 2 || req.Attempt != 0 {
			t.Fatalf("fresh dispatch = %+v, want generation 2 attempt 0", req)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fresh generation work did not dispatch")
	}
}

// TestStaleAdvanceDoesNotInvokeDuringAsyncWait covers redelivered advance work
// after an async task has parked. The scoped work item still matches the node and
// attempt, but StatusWaiting fences it so it cannot dispatch a duplicate activity.
func TestStaleAdvanceDoesNotInvokeDuringAsyncWait(t *testing.T) {
	h := newAsyncHarness(t, asyncLinearFlow)
	ctx := context.Background()

	id, err := h.engine.Start(ctx, "async-linear", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	_ = h.nextReq(t)
	h.waitStatus(t, id, store.StatusWaiting)

	if err = h.engine.enqueue(ctx, id, workItem{Kind: kindAdvance, Node: "a", Attempt: 0}); err != nil {
		t.Fatalf("enqueue stale advance: %v", err)
	}

	select {
	case dup := <-h.inv.reqs:
		t.Fatalf("stale advance dispatched duplicate activity: %+v", dup)
	case <-time.After(300 * time.Millisecond):
	}
}

const asyncFanFlow = `
version: "1.0"
name: async-fan
nodes:
  - {id: fo, type: fanout, branches: [x, y]}
  - {id: x, type: task, subject: "x"}
  - {id: y, type: task, subject: "y"}
  - {id: join, type: fanin, wait_for: [x, y], join_policy: "all"}
  - {id: done, type: task, subject: "d"}
edges:
  - {from: fo, to: join}
  - {from: join, to: done}
`

// TestAsyncFanout dispatches two async branches, completes each via
// CompleteActivity, and verifies the join is satisfied and the flow advances.
func TestAsyncFanout(t *testing.T) {
	h := newAsyncHarness(t, asyncFanFlow)
	ctx := context.Background()

	id, err := h.engine.Start(ctx, "async-fan", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	// Both branches dispatched; execution parks waiting at the join.
	r1, r2 := h.nextReq(t), h.nextReq(t)

	got := map[string]bool{r1.NodeID: true, r2.NodeID: true}
	if !got["x"] || !got["y"] {
		t.Fatalf("branch dispatches = %v, want x and y", got)
	}

	h.waitStatus(t, id, store.StatusWaiting)

	// Complete both branches.
	for _, b := range []string{"x", "y"} {
		if completeErr := h.engine.CompleteActivity(ctx, id, b, 0, invoker.Result{Status: invoker.StatusOK, Payload: json.RawMessage(`{"branch":"` + b + `"}`)}); completeErr != nil {
			t.Fatalf("complete %s: %v", b, completeErr)
		}
	}

	// Join satisfied → advance to done.
	rd := h.nextReq(t)
	if rd.NodeID != "done" {
		t.Fatalf("after join, dispatch = %q, want done", rd.NodeID)
	}

	if completeErr := h.engine.CompleteActivity(ctx, id, "done", 0, invoker.Result{Status: invoker.StatusOK, Payload: json.RawMessage(`{"final":true}`)}); completeErr != nil {
		t.Fatalf("complete done: %v", completeErr)
	}

	ex := h.waitStatus(t, id, store.StatusCompleted)
	for _, b := range []string{"x", "y"} {
		if ex.Branches[b].Status != store.BranchCompleted {
			t.Errorf("branch %s = %q, want completed", b, ex.Branches[b].Status)
		}
	}
}

func TestFanoutResumeRefreshesStaleBranchGenerations(t *testing.T) {
	ctx := context.Background()
	inv := &pendingInvoker{reqs: make(chan invoker.Request, 4)}
	eng, st, flow := newAsyncEngineWithInvoker(t, asyncFanFlow, inv)

	exec := &store.Execution{
		ID:             "fanout-resume-generation",
		FlowName:       flow.Name,
		Status:         store.StatusFailed,
		CurrentNode:    "fo",
		NodeGeneration: 1,
		Outputs:        []string{"y"},
		OutputVersions: map[string]string{"y": "old"},
		Branches: map[string]store.BranchState{
			"x": {NodeID: "x", Status: store.BranchPending, Generation: 1},
			"y": {NodeID: "y", Status: store.BranchCompleted, Generation: 1},
		},
	}

	if err := st.PutPayload(ctx, store.OutputVersionKey(exec.ID, "y", "old"), json.RawMessage(`{"old":true}`)); err != nil {
		t.Fatalf("put old output: %v", err)
	}

	if _, err := st.Create(ctx, exec); err != nil {
		t.Fatalf("create: %v", err)
	}

	if err := eng.Resume(ctx, exec.ID); err != nil {
		t.Fatalf("resume: %v", err)
	}

	resumed, err := st.Get(ctx, exec.ID)
	if err != nil {
		t.Fatalf("get resumed: %v", err)
	}

	resumeGeneration := resumed.NodeGeneration
	if resumeGeneration <= exec.NodeGeneration {
		t.Fatalf("resume generation = %d, want > %d", resumeGeneration, exec.NodeGeneration)
	}

	if err = eng.CompleteActivityWithGeneration(ctx, exec.ID, "x", exec.NodeGeneration, 0,
		invoker.Result{Status: invoker.StatusOK, Payload: json.RawMessage(`{"stale":true}`)}); err != nil {
		t.Fatalf("stale completion: %v", err)
	}

	afterStale, err := st.Get(ctx, exec.ID)
	if err != nil {
		t.Fatalf("get after stale: %v", err)
	}

	if afterStale.Branches["x"].Status != store.BranchPending {
		t.Fatalf("stale completion settled branch x: %+v", afterStale.Branches["x"])
	}

	if err = eng.stepFanout(ctx, flow, flow.Node("fo"), afterStale); err != nil {
		t.Fatalf("stepFanout: %v", err)
	}

	r1, r2 := <-inv.reqs, <-inv.reqs

	got := map[string]invoker.Request{r1.NodeID: r1, r2.NodeID: r2}
	for _, branch := range []string{"x", "y"} {
		req, ok := got[branch]
		if !ok {
			t.Fatalf("dispatches = %v, missing branch %s", got, branch)
		}

		if req.Generation != resumeGeneration || req.Attempt != 0 {
			t.Fatalf("branch %s request = %+v, want generation %d attempt 0", branch, req, resumeGeneration)
		}
	}

	refreshed, err := st.Get(ctx, exec.ID)
	if err != nil {
		t.Fatalf("get refreshed: %v", err)
	}

	for _, branch := range []string{"x", "y"} {
		bs := refreshed.Branches[branch]
		if bs.Status != store.BranchPending || bs.Generation != resumeGeneration || bs.Attempt != 0 {
			t.Fatalf("branch %s = %+v, want pending generation %d attempt 0", branch, bs, resumeGeneration)
		}
	}

	if len(refreshed.Outputs) != 0 || refreshed.OutputVersion("y") != "" {
		t.Fatalf("refreshed fanout kept stale outputs: outputs=%v versions=%v",
			refreshed.Outputs, refreshed.OutputVersions)
	}

	doc, err := eng.Results(ctx, exec.ID)
	if err != nil {
		t.Fatalf("results: %v", err)
	}

	if results := parseCtx(t, doc).Results; results["y"] != nil {
		t.Fatalf("results.y = %s, want stale branch output hidden", results["y"])
	}
}

// TestAsyncBranchRetryDoesNotBlock is the non-blocking counterpart to the
// (intentionally) slot-blocking synchronous branch retry documented on runBranch:
// an async branch that asks to retry re-dispatches at the next attempt while the
// execution stays parked (waiting) the whole time — so the retry never occupies
// an engine slot. (asyncFanRetryFlow is defined in coverage_test.go.)
func TestAsyncBranchRetryDoesNotBlock(t *testing.T) {
	h := newAsyncHarness(t, asyncFanRetryFlow)
	ctx := context.Background()

	id, err := h.engine.Start(ctx, "async-fan-retry", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	// Both branches dispatch async; the execution parks waiting at the join,
	// freeing its slot rather than holding one across the branches' lifetimes.
	drainBranchDispatches(t, h)
	h.waitStatus(t, id, store.StatusWaiting)

	// Branch x asks to retry; it re-dispatches at attempt 1...
	if retryErr := h.engine.CompleteActivity(ctx, id, "x", 0, invoker.Result{Status: invoker.StatusRetry, Error: "transient"}); retryErr != nil {
		t.Fatalf("retry x: %v", retryErr)
	}

	if rx := h.nextReq(t); rx.NodeID != "x" || rx.Attempt != 1 {
		t.Fatalf("re-dispatch = %+v, want branch x attempt 1", rx)
	}

	// ...and the execution is still parked, proving the branch retry held no slot.
	if ex := h.get(t, id); ex.Status != store.StatusWaiting {
		t.Fatalf("status during branch retry = %q, want waiting (no slot held)", ex.Status)
	}

	// Settle both branches; the join is satisfied and the flow advances to done.
	if completeErr := h.engine.CompleteActivity(ctx, id, "x", 1, invoker.Result{Status: invoker.StatusOK, Payload: json.RawMessage(`{"branch":"x"}`)}); completeErr != nil {
		t.Fatalf("complete x: %v", completeErr)
	}

	if completeErr := h.engine.CompleteActivity(ctx, id, "y", 0, invoker.Result{Status: invoker.StatusOK, Payload: json.RawMessage(`{"branch":"y"}`)}); completeErr != nil {
		t.Fatalf("complete y: %v", completeErr)
	}

	if rd := h.nextReq(t); rd.NodeID != "done" {
		t.Fatalf("after join, dispatch = %q, want done", rd.NodeID)
	}

	if completeErr := h.engine.CompleteActivity(ctx, id, "done", 0, invoker.Result{Status: invoker.StatusOK}); completeErr != nil {
		t.Fatalf("complete done: %v", completeErr)
	}

	ex := h.waitStatus(t, id, store.StatusCompleted)
	for _, b := range []string{"x", "y"} {
		if ex.Branches[b].Status != store.BranchCompleted {
			t.Errorf("branch %s = %q, want completed", b, ex.Branches[b].Status)
		}
	}
}

const asyncFanSubsetRetryFlow = `
version: "1.0"
name: async-fan-subset-retry
nodes:
  - {id: fo, type: fanout, branches: [x, y, z]}
  - {id: x, type: task, subject: "x"}
  - {id: y, type: task, subject: "y"}
  - {id: z, type: task, subject: "z", retry: {max_attempts: 3}}
  - {id: join, type: fanin, wait_for: [x, y], join_policy: "all"}
  - {id: done, type: task, subject: "d"}
edges:
  - {from: fo, to: join}
  - {from: join, to: done}
`

func TestAsyncBranchRetryForNonWaitedBranch(t *testing.T) {
	h := newAsyncHarnessCfg(t, asyncFanSubsetRetryFlow, Config{RetryBaseDelay: 20 * time.Millisecond})
	ctx := context.Background()

	id, err := h.engine.Start(ctx, "async-fan-subset-retry", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	got := map[string]bool{}

	for range 3 {
		req := h.nextReq(t)
		got[req.NodeID] = true
	}

	if !got["x"] || !got["y"] || !got["z"] {
		t.Fatalf("branch dispatches = %v, want x/y/z", got)
	}

	h.waitStatus(t, id, store.StatusWaiting)

	if err = h.engine.CompleteActivity(ctx, id, "z", 0,
		invoker.Result{Status: invoker.StatusRetry, Error: "transient"}); err != nil {
		t.Fatalf("retry z: %v", err)
	}

	retry := h.nextReq(t)
	if retry.NodeID != "z" || retry.Attempt != 1 {
		t.Fatalf("redispatch = %+v, want branch z attempt 1", retry)
	}
}

const asyncRetryFlow = `
version: "1.0"
name: async-retry
nodes:
  - {id: a, type: task, subject: "x", retry: {max_attempts: 3}}
edges: []
`

// TestAsyncTaskRetry verifies a retry completion re-dispatches with the next
// attempt only after its backoff deadline, then a success finishes the flow.
func TestAsyncTaskRetry(t *testing.T) {
	h := newAsyncHarness(t, asyncRetryFlow)
	ctx := context.Background()
	id, _ := h.engine.Start(ctx, "async-retry", nil)

	r0 := h.nextReq(t)
	if r0.Attempt != 0 {
		t.Fatalf("attempt = %d, want 0", r0.Attempt)
	}

	h.waitStatus(t, id, store.StatusWaiting)

	// Ask for a retry; engine schedules attempt 1 behind the retry deadline.
	if err := h.engine.CompleteActivity(ctx, id, "a", 0, invoker.Result{Status: invoker.StatusRetry, Error: "transient"}); err != nil {
		t.Fatalf("retry: %v", err)
	}

	select {
	case early := <-h.inv.reqs:
		t.Fatalf("retry dispatched before its backoff deadline: %+v", early)
	case <-time.After(150 * time.Millisecond):
	}

	r1 := h.nextReq(t)
	if r1.Attempt != 1 {
		t.Fatalf("retry attempt = %d, want 1", r1.Attempt)
	}

	// Now succeed.
	if err := h.engine.CompleteActivity(ctx, id, "a", 1, invoker.Result{Status: invoker.StatusOK, Payload: json.RawMessage(`{"ok":true}`)}); err != nil {
		t.Fatalf("complete: %v", err)
	}

	h.waitStatus(t, id, store.StatusCompleted)

	if got := h.results(t, id); string(got.Results["a"]) != `{"ok":true}` {
		t.Fatalf("results.a = %s, want {\"ok\":true}", got.Results["a"])
	}
}
