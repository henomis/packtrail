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

package fold

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/henomis/packtrail/event"
	"github.com/henomis/packtrail/flow"
	"github.com/henomis/packtrail/internal/cmd"
)

var t0 = time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)

// h drives one execution through Decide and checks after every command that
// folding the whole log from scratch reproduces the decided state.
type h struct {
	t    *testing.T
	def  *flow.Flow
	st   *State
	log  []event.Event
	now  time.Time
	n    int
	last []event.Event
}

func newH(t *testing.T, yaml string) *h {
	t.Helper()

	def, err := flow.Parse([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}

	return &h{t: t, def: def, st: New("e1"), now: t0}
}

func (x *h) do(typ cmd.Type, data any) []event.Event {
	x.t.Helper()
	x.n++

	return x.doID(fmt.Sprintf("c%d", x.n), typ, data)
}

func (x *h) doID(id string, typ cmd.Type, data any) []event.Event {
	x.t.Helper()

	c, err := cmd.New(id, typ, "e1", data)
	if err != nil {
		x.t.Fatal(err)
	}

	evs, err := Decide(x.def, x.st, c, x.now)
	if err != nil {
		x.t.Fatalf("decide %s: %v", typ, err)
	}

	x.log = append(x.log, evs...)
	x.last = evs
	x.checkFold()

	return evs
}

func (x *h) checkFold() {
	x.t.Helper()

	// Round-trip the log through the codec: what is stored is what is folded.
	evs := make([]event.Event, 0, len(x.log))

	for _, ev := range x.log {
		b, err := event.Encode(ev)
		if err != nil {
			x.t.Fatal(err)
		}

		d, err := event.Decode(b)
		if err != nil {
			x.t.Fatal(err)
		}

		evs = append(evs, d)
	}

	re, err := Fold(x.def, "e1", evs)
	if err != nil {
		x.t.Fatalf("fold: %v", err)
	}

	a, _ := json.Marshal(x.st)
	b, _ := json.Marshal(re)

	if string(a) != string(b) {
		x.t.Fatalf("fold(log) != decided state\nfold:    %s\ndecided: %s", b, a)
	}
}

func (x *h) start(input string) {
	x.t.Helper()
	x.do(cmd.Start, cmd.StartData{Flow: x.def.Name, Input: json.RawMessage(input)})
}

func (x *h) task(key string) *Task {
	x.t.Helper()

	t := x.st.Tasks[key]
	if t == nil {
		x.t.Fatalf("task %q not active; tasks=%v status=%s", key, keys(x.st.Tasks), x.st.Status)
	}

	return t
}

func keys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}

	slices.Sort(out)

	return out
}

//nolint:unparam // the events are used by some tests.
func (x *h) ok(key, output string) []event.Event {
	x.t.Helper()

	t := x.task(key)

	return x.do(cmd.Complete, cmd.CompleteData{
		TaskRef: cmd.TaskRef{Key: key, Generation: t.Generation, Attempt: t.Attempt},
		Output:  json.RawMessage(output),
	})
}

//nolint:unparam // the events are used by some tests.
func (x *h) okWith(key string, data cmd.CompleteData) []event.Event {
	x.t.Helper()

	t := x.task(key)
	data.TaskRef = cmd.TaskRef{Key: key, Generation: t.Generation, Attempt: t.Attempt}

	return x.do(cmd.Complete, data)
}

//nolint:unparam // the events are used by some tests.
func (x *h) failT(key string, retryable bool) []event.Event {
	x.t.Helper()

	t := x.task(key)

	return x.do(cmd.Fail, cmd.FailData{
		TaskRef: cmd.TaskRef{Key: key, Generation: t.Generation, Attempt: t.Attempt}, Error: "boom", Retryable: retryable,
	})
}

//nolint:unparam // the events are used by some tests.
func (x *h) fire(purpose string) []event.Event {
	x.t.Helper()

	for _, id := range keys(x.st.Timers) {
		if x.st.Timers[id].Purpose == purpose {
			return x.do(cmd.Timer, cmd.TimerData{TimerID: id})
		}
	}

	x.t.Fatalf("no %s timer", purpose)

	return nil
}

func (x *h) status(want Status) {
	x.t.Helper()

	if x.st.Status != want {
		x.t.Fatalf("status = %s (%s %s), want %s", x.st.Status, x.st.Reason, x.st.Error, want)
	}
}

func types(evs []event.Event) []event.Type {
	out := make([]event.Type, len(evs))
	for i, e := range evs {
		out[i] = e.Type
	}

	return out
}

const linear = `
name: lin
nodes:
  - {id: a, type: task, kind: k, next: b}
  - {id: b, type: task, kind: k}
`

func TestLinear(t *testing.T) {
	x := newH(t, linear)
	x.start(`{"x":1}`)
	x.status(StatusRunning)

	if got := types(x.last); !slices.Equal(got, []event.Type{
		event.ExecutionStarted, event.NodeEntered,
		event.NodeScheduled,
	}) {
		t.Fatalf("start events = %v", got)
	}

	x.ok("a", `{"r":1}`)
	x.ok("b", `{"r":2}`)
	x.status(StatusCompleted)

	if string(x.st.Output) != `{"r":2}` || x.st.LastNode != "b" || x.st.Visits["a"] != 1 {
		t.Fatalf("output %s last %s", x.st.Output, x.st.LastNode)
	}
}

func TestStartIdempotentAndValidates(t *testing.T) {
	x := newH(t, linear)
	x.start(`{}`)

	if evs := x.do(cmd.Start, cmd.StartData{Flow: "lin"}); len(evs) != 0 {
		t.Fatal("second start must be a no-op (I-03)")
	}

	other, _ := cmd.New("s2", cmd.Start, "e1", cmd.StartData{Flow: "other"})
	if evs, err := Decide(x.def, x.st, other, t0); len(evs) != 0 || !errors.Is(err, ErrConflict) ||
		!errors.Is(err, ErrRejected) {
		t.Fatalf("start of another flow on an existing id = %d events, %v", len(evs), err)
	}

	y := newH(t, linear)
	c, _ := cmd.New("s", cmd.Start, "e1", cmd.StartData{Flow: "lin", Input: json.RawMessage(`[1]`)})

	if _, err := Decide(y.def, y.st, c, t0); err == nil {
		t.Fatal("non-object input accepted")
	}
}

func TestCommandOnMissingExecution(t *testing.T) {
	x := newH(t, linear)
	c, _ := cmd.New("c", cmd.Signal, "e1", cmd.SignalData{Name: "s"})

	if _, err := Decide(x.def, x.st, c, t0); err == nil {
		t.Fatal("signal to a missing execution must fail (I-09)")
	}
}

func TestStaleAndDuplicateCompletionsIgnored(t *testing.T) {
	x := newH(t, linear)
	x.start(`{}`)
	a := *x.task("a")
	x.ok("a", `{}`)

	// Duplicate delivery of the same completion (I-18).
	evs := x.do(cmd.Complete, cmd.CompleteData{TaskRef: cmd.TaskRef{
		Key: "a", Generation: a.Generation,
		Attempt: a.Attempt,
	}})
	if len(evs) != 0 {
		t.Fatalf("duplicate completion produced %v", types(evs))
	}

	// Wrong attempt for the active task.
	b := x.task("b")
	if stale := x.do(cmd.Complete, cmd.CompleteData{TaskRef: cmd.TaskRef{
		Key: "b", Generation: b.Generation,
		Attempt: 9,
	}}); len(stale) != 0 {
		t.Fatal("stale attempt accepted")
	}
}

func TestOutputMustBeObject(t *testing.T) {
	x := newH(t, linear)
	x.start(`{}`)
	x.ok("a", `[1,2]`)
	x.status(StatusFailed)

	if x.st.Reason != event.ReasonInvalidOutput {
		t.Fatalf("reason = %s", x.st.Reason)
	}
}

const retrying = `
name: r
nodes:
  - id: a
    type: task
    kind: k
    timeout: 10s
    retry: {max_attempts: 3, backoff: exponential, delay: 1s}
`

func TestRetryThenSucceed(t *testing.T) {
	x := newH(t, retrying)
	x.start(`{}`)
	x.failT("a", true)

	if x.task("a").Status != TaskRetrying {
		t.Fatal("task should be retrying")
	}

	// A late completion of the failed attempt is ignored.
	if evs := x.do(cmd.Complete, cmd.CompleteData{TaskRef: cmd.TaskRef{
		Key: "a", Generation: x.task("a").Generation,
		Attempt: 1,
	}}); len(evs) != 0 {
		t.Fatal("completion of a failed attempt accepted")
	}

	x.fire(event.TimerRetry)

	if x.task("a").Attempt != 2 || x.task("a").Status != TaskScheduled {
		t.Fatalf("attempt = %+v", x.task("a"))
	}

	x.ok("a", `{}`)
	x.status(StatusCompleted)
}

func TestRetryExhaustedAndTerminalErrors(t *testing.T) {
	x := newH(t, retrying)
	x.start(`{}`)
	x.failT("a", true)
	x.fire(event.TimerRetry)
	x.failT("a", true)
	x.fire(event.TimerRetry)
	x.failT("a", true)
	x.status(StatusFailed)

	y := newH(t, retrying)
	y.start(`{}`)
	y.failT("a", false) // terminal error: no retry (I-11)
	y.status(StatusFailed)
}

func TestTimeoutRetriesAndStaleTimerIgnored(t *testing.T) {
	x := newH(t, retrying)
	x.start(`{}`)

	var timeoutID string

	for id, tm := range x.st.Timers {
		if tm.Purpose == event.TimerTimeout {
			timeoutID = id
		}
	}

	x.fire(event.TimerTimeout)

	if x.task("a").Status != TaskRetrying {
		t.Fatal("timeout should schedule a retry")
	}

	if evs := x.do(cmd.Timer, cmd.TimerData{TimerID: timeoutID}); len(evs) != 0 {
		t.Fatal("fired timer fired twice")
	}

	x.fire(event.TimerRetry)
	x.ok("a", `{}`)

	// The timeout timer of the successful attempt is still pending; firing it
	// after completion is a no-op.
	x.status(StatusCompleted)
}

func TestCancel(t *testing.T) {
	x := newH(t, linear)
	x.start(`{}`)
	a := *x.task("a")
	x.do(cmd.Cancel, cmd.CancelData{Reason: "user"})
	x.status(StatusCancelled)

	// A completion after cancel is ignored: no rewind (I-01).
	if evs := x.do(cmd.Complete, cmd.CompleteData{TaskRef: cmd.TaskRef{
		Key: "a", Generation: a.Generation,
		Attempt: 1,
	}}); len(evs) != 0 {
		t.Fatal("completion after cancel accepted")
	}

	if evs := x.do(cmd.Cancel, cmd.CancelData{}); len(evs) != 0 {
		t.Fatal("cancel of a terminal execution must be a no-op")
	}
}

const choiceFlow = `
name: ch
nodes:
  - {id: a, type: task, kind: k, next: c}
  - id: c
    type: choice
    rules:
      - {when: "results.a.score > 5", to: hi}
      - {default: true, to: lo}
  - {id: hi, type: task, kind: k}
  - {id: lo, type: task, kind: k}
`

func TestChoice(t *testing.T) {
	for _, tc := range []struct{ out, want string }{
		{`{"score":9}`, "hi"}, {`{"score":1}`, "lo"}, {`{}`, "lo"},
	} {
		x := newH(t, choiceFlow)
		x.start(`{}`)
		x.ok("a", tc.out)
		x.task(tc.want)
	}
}

// evalOpt is evaluator-optimizer: generate, evaluate, and a choice that loops
// back or ends the execution.
const evalOpt = `
name: eo
start: gen
nodes:
  - {id: gen, type: task, kind: k, next: eval}
  - {id: eval, type: task, kind: k, next: check}
  - id: check
    type: choice
    rules: [{when: "results.eval.pass", to: $end}, {default: true, to: gen}]
`

// TestChoiceEnd: a choice rule to $end completes the execution. $end is not a
// node: no NodeEntered, no step, no visit. The default output is the result of
// the last task before the choice (here the evaluator's verdict); an output
// expression picks the generation instead.
func TestChoiceEnd(t *testing.T) {
	for _, tc := range []struct{ output, want string }{
		{"", `{"pass":true}`},
		{`output: "results.gen"`, `{"v":2}`},
	} {
		x := newH(t, evalOpt+tc.output)
		x.start(`{}`)
		x.ok("gen", `{"v":1}`)
		x.ok("eval", `{"pass":false}`)
		x.ok("gen", `{"v":2}`)

		evs := x.ok("eval", `{"pass":true}`)
		x.status(StatusCompleted)

		if string(x.st.Output) != tc.want {
			t.Fatalf("output = %s, want %s", x.st.Output, tc.want)
		}

		if got := types(evs); !slices.Equal(got, []event.Type{
			event.NodeCompleted, event.NodeEntered, event.ChoiceEvaluated, event.ExecutionCompleted,
		}) {
			t.Fatalf("events %v", got)
		}

		if _, ok := x.st.Visits[flow.End]; ok || x.st.Steps != 6 || x.st.LastNode != "eval" {
			t.Fatalf("visits %v steps %d last %s", x.st.Visits, x.st.Steps, x.st.LastNode)
		}
	}
}

// TestChoiceEndAfterAwait: after an await, last_node is the await, so ending
// from the choice that follows returns the signal payload by default.
func TestChoiceEndAfterAwait(t *testing.T) {
	x := newH(t, `
name: ap
start: draft
nodes:
  - {id: draft, type: task, kind: k, next: approve}
  - {id: approve, type: await, signal: approval, timeout: 1h, next: check}
  - id: check
    type: choice
    rules: [{when: "signals.approval.ok", to: $end}, {default: true, to: draft}]
`)
	x.start(`{}`)
	x.ok("draft", `{"text":"x"}`)
	x.do(cmd.Signal, cmd.SignalData{Name: "approval", Payload: json.RawMessage(`{"ok":true}`)})
	x.status(StatusCompleted)

	if string(x.st.Output) != `{"ok":true}` || x.st.LastNode != "approve" {
		t.Fatalf("output %s last %s", x.st.Output, x.st.LastNode)
	}
}

func TestChoiceOnErrorFail(t *testing.T) {
	x := newH(t, `
name: ch
nodes:
  - {id: a, type: task, kind: k, next: c}
  - id: c
    type: choice
    on_error: fail
    rules:
      - {when: "results.a.x.y > 5", to: lo}
      - {default: true, to: lo}
  - {id: lo, type: task, kind: k}
`)
	x.start(`{}`)
	x.ok("a", `{}`)
	x.status(StatusFailed)
}

const fanFlow = `
name: fan
nodes:
  - {id: f, type: fanout, branches: [a, b, c], next: j}
  - {id: a, type: task, kind: k}
  - {id: b, type: task, kind: k}
  - {id: c, type: task, kind: k}
  - {id: j, type: join, policy: %s, next: z}
  - {id: z, type: task, kind: k}
`

func TestFanoutAll(t *testing.T) {
	x := newH(t, fmt.Sprintf(fanFlow, "all"))
	x.start(`{}`)

	if len(x.st.Tasks) != 3 {
		t.Fatalf("tasks = %v", keys(x.st.Tasks))
	}

	// Completions in any order; the join settles exactly once (I-02).
	x.ok("c", `{"v":3}`)
	x.ok("a", `{"v":1}`)
	x.ok("b", `{"v":2}`)
	x.task("z")

	var out map[string]any

	_ = json.Unmarshal(x.st.Results["j"], &out)

	if len(out) != 3 || x.st.Branches["b"] != BranchCompleted {
		t.Fatalf("join output %s branches %v", x.st.Results["j"], x.st.Branches)
	}
}

func TestFanoutAllFailsFast(t *testing.T) {
	x := newH(t, fmt.Sprintf(fanFlow, "all"))
	x.start(`{}`)
	x.ok("a", `{}`)
	x.failT("b", false)
	x.status(StatusFailed)

	if x.st.Reason != event.ReasonJoin {
		t.Fatalf("reason = %s", x.st.Reason)
	}
}

func TestFanoutAnyCancelsRest(t *testing.T) {
	x := newH(t, fmt.Sprintf(fanFlow, "any"))
	x.start(`{}`)
	b := *x.task("b")
	x.failT("a", false)
	x.ok("c", `{}`)
	x.task("z")

	if _, active := x.st.Tasks["b"]; active {
		t.Fatal("pending branch not cancelled")
	}

	if evs := x.do(cmd.Complete, cmd.CompleteData{TaskRef: cmd.TaskRef{
		Key: "b", Generation: b.Generation,
		Attempt: 1,
	}}); len(evs) != 0 {
		t.Fatal("cancelled branch completion accepted")
	}
}

func TestFanoutQuorum(t *testing.T) {
	x := newH(t, fmt.Sprintf(fanFlow, "'quorum:2'"))
	x.start(`{}`)
	x.ok("a", `{}`)
	x.failT("b", false)

	if _, ok := x.st.Tasks["z"]; ok {
		t.Fatal("quorum settled too early")
	}

	x.ok("c", `{}`)
	x.task("z")

	y := newH(t, fmt.Sprintf(fanFlow, "'quorum:2'"))
	y.start(`{}`)
	y.failT("a", false)
	y.failT("b", false)
	y.status(StatusFailed) // quorum unreachable
}

func TestFanoutRevisitUsesFreshState(t *testing.T) {
	x := newH(t, `
name: loop
start: f
nodes:
  - {id: f, type: fanout, branches: [a], next: j}
  - {id: a, type: task, kind: k}
  - {id: j, type: join, next: c}
  - id: c
    type: choice
    rules: [{when: "visits.f < 2", to: f}, {default: true, to: done}]
  - {id: done, type: task, kind: k}
`)
	x.start(`{}`)
	g1 := x.task("a").Generation
	x.ok("a", `{}`)

	if x.task("a").Generation == g1 {
		t.Fatal("revisit must use a new generation")
	}

	x.ok("a", `{}`)
	x.task("done")
}

const awaitFlow = `
name: aw
nodes:
  - {id: w, type: await, signal: approval, timeout: 1h, next: z}
  - {id: z, type: task, kind: k}
`

func TestAwaitSignalAfter(t *testing.T) {
	x := newH(t, awaitFlow)
	x.start(`{}`)
	x.status(StatusWaiting)
	x.do(cmd.Signal, cmd.SignalData{Name: "approval", Payload: json.RawMessage(`{"ok":true}`)})
	x.task("z")

	if string(x.st.Signals["approval"]) != `{"ok":true}` || len(x.st.Timers) != 0 {
		t.Fatalf("signals %v timers %v", x.st.Signals, x.st.Timers)
	}
}

func TestEarlySignalBufferedAndDeduplicated(t *testing.T) {
	x := newH(t, `
name: aw
nodes:
  - {id: a, type: task, kind: k, next: w}
  - {id: w, type: await, signal: approval, timeout: 1h, next: z}
  - {id: z, type: task, kind: k}
`)
	x.start(`{}`)
	x.doID("sig-1", cmd.Signal, cmd.SignalData{Name: "approval"})

	if evs := x.doID("sig-1", cmd.Signal, cmd.SignalData{Name: "approval"}); len(evs) != 0 {
		t.Fatal("duplicate signal id accepted")
	}

	if len(x.st.Buffered) != 1 {
		t.Fatal("early signal not buffered (I-09)")
	}

	x.ok("a", `{}`)
	x.task("z")
}

func TestAwaitTimeout(t *testing.T) {
	x := newH(t, awaitFlow)
	x.start(`{}`)
	x.fire(event.TimerAwait)
	x.status(StatusFailed)

	y := newH(t, `
name: aw
nodes:
  - {id: w, type: await, signal: s, timeout: 1h, on_timeout: late, next: z}
  - {id: z, type: task, kind: k}
  - {id: late, type: task, kind: k}
`)
	y.start(`{}`)
	y.fire(event.TimerAwait)
	y.task("late")
}

func TestInterruptResume(t *testing.T) {
	x := newH(t, linear)
	x.start(`{}`)
	a := *x.task("a")
	x.do(cmd.Interrupt, cmd.InterruptData{
		TaskRef: cmd.TaskRef{Key: "a", Generation: a.Generation, Attempt: 1},
		Payload: json.RawMessage(`{"q":"approve?"}`),
	})
	x.status(StatusWaiting)
	x.do(cmd.Resume, cmd.ResumeData{Node: "a", Value: json.RawMessage(`"yes"`)})

	b := x.task("a")
	if b.Generation == a.Generation || string(b.Resume) != `"yes"` {
		t.Fatalf("resume = %+v", b)
	}

	if evs := x.do(cmd.Resume, cmd.ResumeData{Node: "a"}); len(evs) != 0 {
		t.Fatal("second resume accepted")
	}

	x.ok("a", `{}`)
	x.task("b")
}

func TestDynamicNext(t *testing.T) {
	src := `
name: dyn
nodes:
  - {id: a, type: task, kind: k, dynamic: [x, y], next: y}
  - {id: x, type: task, kind: k}
  - {id: y, type: task, kind: k}
`
	x := newH(t, src)
	x.start(`{}`)
	x.okWith("a", cmd.CompleteData{Next: "x"})
	x.task("x")

	y := newH(t, src)
	y.start(`{}`)
	y.okWith("a", cmd.CompleteData{Next: "zzz"})
	y.status(StatusFailed)
}

func TestMap(t *testing.T) {
	x := newH(t, `
name: m
channels: {total: {reducer: sum}}
nodes:
  - {id: m, type: map, kind: k, over: input.items, max_parallel: 2, next: z}
  - {id: z, type: task, kind: k}
`)
	x.start(`{"items":[10,20,30]}`)

	if got := keys(x.st.Tasks); !slices.Equal(got, []string{"m#0", "m#1"}) {
		t.Fatalf("window = %v", got)
	}

	if string(x.task("m#1").Item) != "20" {
		t.Fatal("item not passed")
	}

	x.okWith("m#1", cmd.CompleteData{
		Output: json.RawMessage(`{"i":1}`),
		Writes: map[string]json.RawMessage{"total": json.RawMessage("2")},
	})
	x.task("m#2")
	x.okWith("m#0", cmd.CompleteData{
		Output: json.RawMessage(`{"i":0}`),
		Writes: map[string]json.RawMessage{"total": json.RawMessage("1")},
	})
	x.ok("m#2", `{"i":2}`)
	x.task("z")

	if string(x.st.Results["m"]) != `[{"i":0},{"i":1},{"i":2}]` || string(x.st.Channels["total"]) != "3" {
		t.Fatalf("results %s total %s", x.st.Results["m"], x.st.Channels["total"])
	}
}

func TestMapEmpty(t *testing.T) {
	x := newH(t, `
name: m
nodes:
  - {id: m, type: map, kind: k, over: input.items}
`)
	x.start(`{"items":[]}`)
	x.status(StatusCompleted)
}

func TestChannelsAndReducers(t *testing.T) {
	x := newH(t, `
name: ch
channels:
  notes: {reducer: append}
  meta: {reducer: merge}
  n: {reducer: sum, default: 5}
  last: {}
nodes:
  - {id: a, type: task, kind: k, next: b}
  - {id: b, type: task, kind: k}
`)
	x.start(`{}`)
	x.okWith("a", cmd.CompleteData{Writes: map[string]json.RawMessage{
		"notes": json.RawMessage(`"x"`), "meta": json.RawMessage(`{"a":1}`), "n": json.RawMessage(`1`),
		"last": json.RawMessage(`"a"`),
	}})
	x.okWith("b", cmd.CompleteData{Writes: map[string]json.RawMessage{
		"notes": json.RawMessage(`["y","z"]`), "meta": json.RawMessage(`{"b":2}`), "n": json.RawMessage(`1`),
		"last": json.RawMessage(`"b"`),
	}})
	x.status(StatusCompleted)

	want := `{"last":"b","meta":{"a":1,"b":2},"n":7,"notes":["x","y","z"]}`
	if string(x.st.Output) != want {
		t.Fatalf("output = %s", x.st.Output)
	}
}

// TestOutputExpression: the flow's output expression picks what a completed
// execution returns — a subset of channels or the last node's result even when
// channels exist; channels left out stay in the state. Anything but an object
// or null fails the execution.
func TestOutputExpression(t *testing.T) {
	cases := []struct {
		name, output, want string
		status             Status
	}{
		{"channel subset", "{answer: channels.answer, score: channels.score}", `{"answer":"42","score":3}`, StatusCompleted},
		{"last result despite channels", "results[last_node]", `{"r":1}`, StatusCompleted},
		{"named result", "results.a", `{"r":1}`, StatusCompleted},
		{"nil", "nil", ``, StatusCompleted},
		{"not an object", "channels.answer", ``, StatusFailed},
		{"eval error", "channels.answer.x.y", ``, StatusFailed},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			x := newH(t, fmt.Sprintf(`
name: ch
channels:
  memory: {reducer: append}
  answer: {}
  score: {reducer: sum, default: 3}
output: %q
nodes:
  - {id: a, type: task, kind: k}
`, tc.output))
			x.start(`{}`)
			x.okWith("a", cmd.CompleteData{Output: json.RawMessage(`{"r":1}`), Writes: map[string]json.RawMessage{
				"memory": json.RawMessage(`"secret"`), "answer": json.RawMessage(`"42"`),
			}})
			x.status(tc.status)

			if string(x.st.Output) != tc.want {
				t.Fatalf("output = %s, want %s", x.st.Output, tc.want)
			}

			if string(x.st.Channels["memory"]) != `["secret"]` {
				t.Fatalf("memory = %s", x.st.Channels["memory"])
			}
		})
	}
}

func TestInvalidWriteFailsNode(t *testing.T) {
	x := newH(t, `
name: ch
channels: {n: {reducer: sum}}
nodes:
  - {id: a, type: task, kind: k}
`)
	x.start(`{}`)
	x.okWith("a", cmd.CompleteData{Writes: map[string]json.RawMessage{"n": json.RawMessage(`"x"`)}})
	x.status(StatusFailed)
}

func TestBudgetAndMaxSteps(t *testing.T) {
	x := newH(t, `
name: b
budget: {units: 10}
nodes:
  - {id: a, type: task, kind: k, next: b}
  - {id: b, type: task, kind: k}
`)
	x.start(`{}`)
	x.okWith("a", cmd.CompleteData{Usage: map[string]float64{"units": 11}})
	x.status(StatusFailed)

	if x.st.Reason != event.ReasonBudget {
		t.Fatalf("reason = %s", x.st.Reason)
	}

	y := newH(t, `
name: loop
max_steps: 5
start: a
nodes:
  - {id: a, type: task, kind: k, next: c}
  - id: c
    type: choice
    rules: [{default: true, to: a}]
`)
	y.start(`{}`)

	for y.st.Status == StatusRunning {
		y.ok("a", `{}`)
	}

	y.status(StatusFailed)

	if y.st.Reason != event.ReasonMaxSteps {
		t.Fatalf("reason = %s", y.st.Reason)
	}
}

func TestOutputSchemaRetries(t *testing.T) {
	x := newH(t, `
name: s
nodes:
  - id: a
    type: task
    kind: k
    retry: {max_attempts: 2}
    output_schema: {type: object, required: [text]}
`)
	x.start(`{}`)
	x.ok("a", `{"nope":1}`)
	x.fire(event.TimerRetry)

	if x.task("a").Attempt != 2 {
		t.Fatal("schema violation should retry")
	}

	x.ok("a", `{"text":"ok"}`)
	x.status(StatusCompleted)
}

func TestSubflow(t *testing.T) {
	x := newH(t, `
name: p
nodes:
  - {id: s, type: subflow, flow: child, input: "input.sub", next: z}
  - {id: z, type: task, kind: k}
`)
	x.start(`{"sub":{"q":1}}`)

	var child *ChildState
	for _, c := range x.st.Children {
		child = c
	}

	if child == nil || child.Policy != flow.ParentCloseCancel {
		t.Fatal("child not started")
	}

	x.do(cmd.Cancel, cmd.CancelData{})

	if cc := x.log[len(x.log)-1].Data.(*event.Cancelled).CancelChildren; len(cc) != 1 || cc[0] != child.ChildID {
		t.Fatalf("cancel children = %v", cc)
	}

	y := newH(t, `
name: p
nodes:
  - {id: s, type: subflow, flow: child, next: z}
  - {id: z, type: task, kind: k}
`)
	y.start(`{}`)

	var id string
	for _, c := range y.st.Children {
		id = c.ChildID
	}

	y.do(cmd.ChildDone, cmd.ChildDoneData{
		ChildID: id, Status: "completed", Output: json.RawMessage(`{"r":1}`),
		Counters: map[string]float64{"u": 2},
	})
	y.task("z")

	if y.st.Counters["u"] != 2 || string(y.st.Results["s"]) != `{"r":1}` {
		t.Fatal("child result not folded")
	}
}

func TestForkReschedulesInFlight(t *testing.T) {
	x := newH(t, linear)
	x.start(`{}`)
	x.ok("a", `{"r":1}`)

	src, _ := x.st.Clone()
	fork := New("e2")

	evs, err := DecideFork(x.def, fork, src, "e1", 9, "fork-1", nil, t0)
	if err != nil {
		t.Fatal(err)
	}

	if evs[0].Type != event.ExecutionForked || fork.ExecID != "e2" || fork.ForkedFrom != "e1" {
		t.Fatalf("fork = %v", types(evs))
	}

	if fork.Tasks["b"] == nil || fork.Tasks["b"].Generation == x.st.Tasks["b"].Generation {
		t.Fatal("in-flight task not re-dispatched with a fresh generation")
	}

	re, err := Fold(x.def, "e2", evs)
	if err != nil {
		t.Fatal(err)
	}

	a, _ := json.Marshal(re)
	b, _ := json.Marshal(fork)

	if string(a) != string(b) {
		t.Fatal("fold(fork events) != fork state")
	}
}

func TestReduce(t *testing.T) {
	cases := []struct{ r, cur, delta, want string }{
		{"replace", `1`, `2`, `2`},
		{"append", ``, `1`, `[1]`},
		{"append", `[1]`, `[2,3]`, `[1,2,3]`},
		{"merge", ``, `{"a":1}`, `{"a":1}`},
		{"merge", `{"a":1}`, `{"a":2,"b":3}`, `{"a":2,"b":3}`},
		{"sum", ``, `2`, `2`},
		{"sum", `2.5`, `2`, `4.5`},
	}
	for _, c := range cases {
		got, err := Reduce(c.r, json.RawMessage(c.cur), json.RawMessage(c.delta))
		if err != nil || string(got) != c.want {
			t.Errorf("Reduce(%s,%s,%s) = %s, %v", c.r, c.cur, c.delta, got, err)
		}
	}

	for _, c := range [][3]string{
		{"merge", `[1]`, `{}`},
		{"merge", ``, `1`},
		{"sum", `"a"`, `1`},
		{"sum", ``, `"x"`},
		{"append", `{}`, `1`},
		{"max", ``, `1`},
	} {
		if _, err := Reduce(c[0], json.RawMessage(c[1]), json.RawMessage(c[2])); err == nil {
			t.Errorf("Reduce(%v) accepted", c)
		}
	}
}

func TestAbortFailsRunningExecution(t *testing.T) {
	x := newH(t, linear)
	x.start(`{}`)

	evs, err := Abort(x.def, x.st, nil, "c-big", event.ReasonDecisionTooLarge, "too big", t0)
	if err != nil || len(evs) != 1 || evs[0].Type != event.ExecutionFailed {
		t.Fatalf("abort = %v, %v", types(evs), err)
	}

	x.log = append(x.log, evs...)
	x.checkFold()
	x.status(StatusFailed)

	if _, err = Abort(x.def, New("missing"), nil, "c", "r", "m", t0); err == nil {
		t.Fatal("abort of a missing execution without its first event must fail")
	}
}

// TestOnFailureRoutesTask: a task that fails permanently (retries exhausted)
// routes to its on_failure instead of failing the execution; the handler sees
// errors.<node> and last_node, and a later success of the node clears the
// error (G5-01).
func TestOnFailureRoutesTask(t *testing.T) {
	x := newH(t, `
name: saga
nodes:
  - {id: reserve, type: task, kind: k, next: charge}
  - {id: charge, type: task, kind: k, retry: {max_attempts: 2}, on_failure: refund, next: done}
  - {id: refund, type: task, kind: k, next: retry}
  - {id: retry, type: choice, rules: [{when: "visits.charge < 2", to: charge}, {default: true, to: done}]}
  - {id: done, type: task, kind: k}
`)
	x.start(`{}`)
	x.ok("reserve", `{"r":1}`)
	x.failT("charge", true) // attempt 1: retried after the backoff
	x.fire(event.TimerRetry)
	x.failT("charge", true) // attempt 2: exhausted → refund

	x.status(StatusRunning)
	x.task("refund")

	ne, ok := x.st.Errors["charge"]
	if !ok || ne.Error != "boom" || ne.Reason != event.ReasonError || ne.Attempt != 2 {
		t.Fatalf("errors.charge = %+v", x.st.Errors)
	}

	if x.st.LastNode != "charge" {
		t.Fatalf("last_node = %q, want the failed node", x.st.LastNode)
	}

	view := x.st.ContextView(x.task("refund"))
	if view.Errors["charge"].Error != "boom" || string(view.Results["reserve"]) != `{"r":1}` {
		t.Fatalf("handler context: errors %+v results %v", view.Errors, view.Results)
	}

	x.ok("refund", `{}`)
	x.ok("charge", `{"paid":true}`) // the choice looped back once; now it succeeds

	if _, still := x.st.Errors["charge"]; still {
		t.Fatal("a success of the node must clear its error")
	}

	x.ok("done", `{}`)
	x.status(StatusCompleted)
}

// TestOnFailureNotTakenForExecutionLevelStops: budget, max_steps and a
// cancel still end the execution; only node failures route (G5-01).
func TestOnFailureNotTakenForExecutionLevelStops(t *testing.T) {
	x := newH(t, `
name: b
budget: {cost: 1}
nodes:
  - {id: a, type: task, kind: k, on_failure: h}
  - {id: h, type: task, kind: k}
`)
	x.start(`{}`)
	x.okWith("a", cmd.CompleteData{Output: json.RawMessage(`{}`), Usage: map[string]float64{"cost": 5}})
	x.status(StatusFailed)

	if x.st.Reason != event.ReasonBudget {
		t.Fatalf("reason %s, want budget", x.st.Reason)
	}

	y := newH(t, `
name: c
nodes:
  - {id: a, type: task, kind: k, on_failure: h}
  - {id: h, type: task, kind: k}
`)
	y.start(`{}`)
	y.failT("a", false) // a permanent error routes
	y.task("h")
	y.do(cmd.Cancel, cmd.CancelData{Reason: "stop"})
	y.status(StatusCancelled)
}

// TestOnFailureAbortsMap: one map item failing permanently cancels the items
// in flight, closes the map and routes; errors.<map> names the item (G5-01).
func TestOnFailureAbortsMap(t *testing.T) {
	x := newH(t, `
name: m
nodes:
  - {id: m, type: map, kind: k, over: input.items, max_parallel: 2, on_failure: h, next: z}
  - {id: h, type: task, kind: k}
  - {id: z, type: task, kind: k}
`)
	x.start(`{"items":[1,2,3]}`)
	x.ok("m#0", `{}`)

	stale := *x.task("m#2")

	evs := x.failT("m#1", false)
	if got := types(evs); !slices.Equal(got, []event.Type{
		event.NodeFailed, event.NodeCancelled, event.MapAborted, event.NodeEntered, event.NodeScheduled,
	}) {
		t.Fatalf("events %v", got)
	}

	if len(x.st.Maps) != 0 || x.st.Tasks["m#2"] != nil {
		t.Fatalf("map still open: maps %v tasks %v", keys(x.st.Maps), keys(x.st.Tasks))
	}

	if ne := x.st.Errors["m"]; ne.Index == nil || *ne.Index != 1 {
		t.Fatalf("errors.m = %+v", x.st.Errors)
	}

	// The cancelled item's late completion is stale.
	if evs = x.do(cmd.Complete, cmd.CompleteData{
		TaskRef: cmd.TaskRef{Key: "m#2", Generation: stale.Generation, Attempt: stale.Attempt},
		Output:  json.RawMessage(`{}`),
	}); len(evs) != 0 {
		t.Fatalf("stale completion produced %v", types(evs))
	}

	x.ok("h", `{}`)
	x.status(StatusCompleted)
}

// TestOnFailureRoutesJoinAndSubflow: a join whose policy is not met and a
// failed subflow both route to their on_failure (G5-01).
func TestOnFailureRoutesJoinAndSubflow(t *testing.T) {
	x := newH(t, `
name: j
nodes:
  - {id: split, type: fanout, branches: [a, b], next: join}
  - {id: a, type: task, kind: k}
  - {id: b, type: task, kind: k}
  - {id: join, type: join, policy: all, on_failure: compensate, next: z}
  - {id: compensate, type: task, kind: k}
  - {id: z, type: task, kind: k}
`)
	x.start(`{}`)
	x.ok("a", `{}`)
	x.failT("b", false)
	x.task("compensate")

	if x.st.Errors["join"].Reason != event.ReasonJoin || x.st.Errors["b"].Error != "boom" {
		t.Fatalf("errors %+v", x.st.Errors)
	}

	y := newH(t, `
name: p
nodes:
  - {id: s, type: subflow, flow: child, on_failure: h, next: z}
  - {id: h, type: task, kind: k}
  - {id: z, type: task, kind: k}
`)
	y.start(`{}`)

	var id string
	for _, c := range y.st.Children {
		id = c.ChildID
	}

	y.do(cmd.ChildDone, cmd.ChildDoneData{ChildID: id, Status: "failed", Error: "child broke"})
	y.task("h")

	if ne := y.st.Errors["s"]; ne.Error != "child broke" || ne.Reason != event.ReasonChild {
		t.Fatalf("errors.s = %+v", y.st.Errors)
	}
}

// TestContinueIsTransparent: continuing as new changes nothing but the log:
// the carried state is the state, in-flight work goes on, and folding the
// log from the continuation alone gives the same state (G5-07).
func TestContinueIsTransparent(t *testing.T) {
	x := newH(t, linear)
	x.start(`{"k":1}`)
	x.ok("a", `{"a":1}`)

	before, _ := json.Marshal(x.st)
	seg := event.Segment{Object: "e1.seg.1", FirstIndex: 1, LastIndex: x.st.Events, FirstSeq: 1, LastSeq: 2}

	evs, err := Continue(x.def, x.st, seg, "cont", x.now)
	if err != nil || len(evs) != 1 || evs[0].Type != event.ExecutionContinued {
		t.Fatalf("continue: %v %v", types(evs), err)
	}

	x.log = append(x.log, evs...)
	x.checkFold()

	if x.st.Base != evs[0].Index || len(x.st.Segments) != 1 || x.st.Segments[0] != seg {
		t.Fatalf("base %d segments %+v", x.st.Base, x.st.Segments)
	}

	// Everything else is unchanged.
	after := *x.st
	after.Base, after.Segments, after.Events = 0, nil, after.Events-1

	var want State

	_ = json.Unmarshal(before, &want)
	want.init()

	a, _ := json.Marshal(after)
	w, _ := json.Marshal(&want)

	if string(a) != string(w) {
		t.Fatalf("state changed:\n got %s\nwant %s", a, w)
	}

	// The live log alone (from the continuation) folds to the same state.
	live, err := Fold(x.def, "e1", evs)
	if err != nil {
		t.Fatal(err)
	}

	l, _ := json.Marshal(live)
	s, _ := json.Marshal(x.st)

	if string(l) != string(s) {
		t.Fatalf("live log folds differently:\n live %s\n full %s", l, s)
	}

	x.ok("b", `{}`) // the task in flight across the continuation completes
	x.status(StatusCompleted)

	if evs, _ = Continue(x.def, x.st, seg, "late", x.now); len(evs) != 0 {
		t.Fatal("a finished execution must not continue")
	}
}

// TestSignalIDsAreBounded: deduplication ids are forgotten after the window
// (by event time, deterministically) and capped by count (G5-07).
func TestSignalIDsAreBounded(t *testing.T) {
	x := newH(t, `
name: s
nodes:
  - {id: w, type: await, signal: go, timeout: 72h}
`)
	x.start(`{}`)
	x.doID("sig-old", cmd.Signal, cmd.SignalData{Name: "other"})

	x.now = x.now.Add(25 * time.Hour)
	x.doID("sig-new", cmd.Signal, cmd.SignalData{Name: "other"})

	if _, ok := x.st.SignalIDs["sig-old"]; ok {
		t.Fatal("an id older than the window was kept")
	}

	if _, ok := x.st.SignalIDs["sig-new"]; !ok {
		t.Fatal("a recent id was forgotten")
	}

	s := New("cap")
	base := t0

	for i := range maxSignalIDs + 5 {
		rememberID(s.SignalIDs, fmt.Sprintf("id-%05d", i), base.Add(time.Duration(i)*time.Millisecond))
	}

	if len(s.SignalIDs) != maxSignalIDs {
		t.Fatalf("%d ids kept, want %d", len(s.SignalIDs), maxSignalIDs)
	}

	if _, ok := s.SignalIDs["id-00004"]; ok {
		t.Fatal("the oldest ids must go first")
	}
}

// TestForkOfContinuedDropsSegments: a fork's history starts at the fork, so it
// does not inherit the source's segments (G5-07).
func TestForkOfContinuedDropsSegments(t *testing.T) {
	x := newH(t, linear)
	x.start(`{}`)

	seg := event.Segment{Object: "e1.seg.1", FirstIndex: 1, LastIndex: x.st.Events}

	evs, err := Continue(x.def, x.st, seg, "cont", x.now)
	if err != nil || len(evs) != 1 {
		t.Fatal(err)
	}

	fork := New("f1")

	out, err := DecideFork(x.def, fork, x.st, "e1", 9, "fork", nil, x.now)
	if err != nil || len(out) == 0 {
		t.Fatal(err)
	}

	if fork.Base != 0 || len(fork.Segments) != 0 {
		t.Fatalf("fork base %d segments %v", fork.Base, fork.Segments)
	}
}

// TestUpdateWritesChannels: an update folds its writes through the reducers,
// once per update id; bad writes are invalid; a finished execution rejects it
// (G5-03).
func TestUpdateWritesChannels(t *testing.T) {
	x := newH(t, `
name: u
channels: {notes: {reducer: append}, total: {reducer: sum}}
nodes:
  - {id: w, type: await, signal: go, timeout: 1h}
`)
	x.start(`{}`)

	writes := map[string]json.RawMessage{"notes": json.RawMessage(`"hi"`), "total": json.RawMessage(`2`)}

	evs := x.doID("upd-1", cmd.Update, cmd.UpdateData{Writes: writes})
	if got := types(evs); !slices.Equal(got, []event.Type{event.ChannelsUpdated}) {
		t.Fatalf("events %v", got)
	}

	if evs = x.doID("upd-1", cmd.Update, cmd.UpdateData{Writes: writes}); len(evs) != 0 {
		t.Fatal("the same update applied twice")
	}

	if string(x.st.Channels["notes"]) != `["hi"]` || string(x.st.Channels["total"]) != "2" {
		t.Fatalf("channels %s %s", x.st.Channels["notes"], x.st.Channels["total"])
	}

	bad := func(id string, w map[string]json.RawMessage, want error) {
		t.Helper()

		c, err := cmd.New(id, cmd.Update, "e1", cmd.UpdateData{Writes: w})
		if err != nil {
			t.Fatal(err)
		}

		if _, err = Decide(x.def, x.st, c, x.now); !errors.Is(err, want) {
			t.Fatalf("%s: %v, want %v", id, err, want)
		}
	}

	bad("upd-undeclared", map[string]json.RawMessage{"nope": json.RawMessage(`1`)}, ErrInvalid)
	bad("upd-type", map[string]json.RawMessage{"total": json.RawMessage(`"x"`)}, ErrInvalid)
	bad("upd-empty", nil, ErrInvalid)

	x.do(cmd.Cancel, cmd.CancelData{})
	bad("upd-late", writes, ErrRejected)
}

// TestForkWithWrites: a fork's edits land in its first decision, before the
// work in flight is dispatched again, so the re-dispatched task sees them
// (G5-03).
func TestForkWithWrites(t *testing.T) {
	x := newH(t, `
name: fw
channels: {draft: {reducer: replace}}
nodes:
  - {id: a, type: task, kind: k, next: b}
  - {id: b, type: task, kind: k}
`)
	x.start(`{}`)
	x.okWith("a", cmd.CompleteData{Output: json.RawMessage(`{}`), Writes: map[string]json.RawMessage{
		"draft": json.RawMessage(`"v1"`),
	}})

	fork := New("f1")
	writes := map[string]json.RawMessage{"draft": json.RawMessage(`"edited"`)}

	evs, err := DecideFork(x.def, fork, x.st, "e1", 9, "fork-w", writes, t0)
	if err != nil {
		t.Fatal(err)
	}

	got := types(evs)
	if len(got) < 3 || got[0] != event.ExecutionForked || got[1] != event.ChannelsUpdated ||
		!slices.Contains(got, event.NodeScheduled) {
		t.Fatalf("events %v", got)
	}

	if string(fork.Channels["draft"]) != `"edited"` || string(x.st.Channels["draft"]) != `"v1"` {
		t.Fatalf("fork %s source %s", fork.Channels["draft"], x.st.Channels["draft"])
	}

	if _, err = DecideFork(x.def, New("f2"), x.st, "e1", 9, "fork-bad",
		map[string]json.RawMessage{"nope": json.RawMessage(`1`)}, t0); !errors.Is(err, ErrInvalid) {
		t.Fatalf("bad fork writes: %v", err)
	}
}
