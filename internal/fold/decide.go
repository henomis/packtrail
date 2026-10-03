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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"time"

	"github.com/henomis/packtrail/event"
	"github.com/henomis/packtrail/flow"
	"github.com/henomis/packtrail/internal/cmd"
	"github.com/henomis/packtrail/internal/names"
)

// Decision errors. Both mean the command can never produce events: the engine
// dead-letters it. A command that is merely stale (an old attempt, a duplicate)
// returns no events and no error.
var (
	ErrNotFound = errors.New("fold: execution does not exist")
	ErrInvalid  = errors.New("fold: invalid command")
	// ErrRejected is a well-formed command the execution refuses in its
	// current state (an update after it finished): an answer, not poison.
	ErrRejected = errors.New("fold: command rejected")
)

// decider produces the events of one command. Every event is applied to the
// state as soon as it is emitted, so later decisions in the same command see
// it, and folding the emitted events reproduces exactly the decided state.
type decider struct {
	def   *flow.Flow
	st    *State
	now   time.Time
	cmdID string
	out   []event.Event
}

// Decide returns the events produced by command c on state st, applying them to
// st. def is the flow definition the execution is (or, for start, will be)
// bound to. On error st may be partially updated and must be discarded.
func Decide(def *flow.Flow, st *State, c cmd.Command, now time.Time) ([]event.Event, error) {
	d := &decider{def: def, st: st, now: now.UTC(), cmdID: c.ID}

	if c.Type == cmd.Start {
		return d.out, d.start(c)
	}

	if !st.Exists() {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, st.ExecID)
	}

	if st.Status.Terminal() {
		if c.Type == cmd.Update {
			return nil, fmt.Errorf("%w: execution %s is %s", ErrRejected, st.ExecID, st.Status)
		}

		return nil, nil
	}

	var err error

	switch c.Type { //nolint:exhaustive // start is handled above; fork and archive never reach the fold.
	case cmd.Complete:
		err = decode(c, func(p *cmd.CompleteData) error { return d.complete(p) })
	case cmd.Fail:
		err = decode(c, func(p *cmd.FailData) error { return d.failTask(p) })
	case cmd.Interrupt:
		err = decode(c, func(p *cmd.InterruptData) error { return d.interrupt(p) })
	case cmd.Resume:
		err = decode(c, func(p *cmd.ResumeData) error { return d.resume(p) })
	case cmd.Signal:
		err = decode(c, func(p *cmd.SignalData) error { return d.signal(p) })
	case cmd.Cancel:
		err = decode(c, func(p *cmd.CancelData) error { return d.cancel(p) })
	case cmd.Timer:
		err = decode(c, func(p *cmd.TimerData) error { return d.timer(p) })
	case cmd.ChildDone:
		err = decode(c, func(p *cmd.ChildDoneData) error { return d.childDone(p) })
	case cmd.Update:
		err = decode(c, func(p *cmd.UpdateData) error { return d.update(p.Writes) })
	default:
		return nil, fmt.Errorf("%w: %s is not decided by fold", ErrInvalid, c.Type)
	}

	return d.out, err
}

func decode[T any](c cmd.Command, fn func(*T) error) error {
	var p T
	if err := c.Payload(&p); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalid, err)
	}

	return fn(&p)
}

// DecideFork returns the events that create execution newID as a fork of src
// (the state of execution from at sequence seq): ExecutionForked carrying the
// source state, then a re-dispatch of everything that was in flight at that
// point under fresh generations.
func DecideFork(def *flow.Flow, st, src *State, from string, seq uint64, cmdID string,
	writes map[string]json.RawMessage, now time.Time,
) ([]event.Event, error) {
	if st.Exists() {
		return nil, nil
	}

	b, err := json.Marshal(src)
	if err != nil {
		return nil, err
	}

	d := &decider{def: def, st: st, now: now.UTC(), cmdID: cmdID}
	if err = d.emit(event.ExecutionForked, &event.Forked{ExecID: st.ExecID, From: from, Seq: seq, State: b}); err != nil {
		return nil, err
	}

	if len(writes) > 0 {
		if st.Status.Terminal() {
			return nil, fmt.Errorf("%w: cannot edit a fork of a finished execution", ErrInvalid)
		}

		if err = CheckWrites(def, st, writes); err != nil {
			return nil, fmt.Errorf("%w: fork writes: %w", ErrInvalid, err)
		}

		// The edit lands before anything in flight is dispatched again.
		if err = d.emit(event.ChannelsUpdated, &event.Update{ID: cmdID, Writes: writes}); err != nil {
			return nil, err
		}
	}

	if st.Status.Terminal() {
		return d.out, nil
	}

	return d.out, d.rekick()
}

// CheckWrites reports whether writes fit def's channels in state st: every
// channel declared and every value accepted by its reducer.
func CheckWrites(def *flow.Flow, st *State, writes map[string]json.RawMessage) error {
	for _, name := range slices.Sorted(maps.Keys(writes)) {
		ch, ok := def.Channels[name]
		if !ok {
			return fmt.Errorf("write to undeclared channel %q", name)
		}

		if _, err := Reduce(ch.ReducerOrDefault(), st.Channels[name], writes[name]); err != nil {
			return fmt.Errorf("write to channel %q: %w", name, err)
		}
	}

	return nil
}

// update writes channels from outside the graph, once per update id.
func (d *decider) update(writes map[string]json.RawMessage) error {
	if len(writes) == 0 {
		return fmt.Errorf("%w: update without writes", ErrInvalid)
	}

	if _, seen := d.st.UpdateIDs[d.cmdID]; seen {
		return nil // the same update delivered again
	}

	if err := CheckWrites(d.def, d.st, writes); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalid, err)
	}

	return d.emit(event.ChannelsUpdated, &event.Update{ID: d.cmdID, Writes: writes})
}

// Continue returns the decision that continues execution st as new under the
// same id once its history was archived as seg: one ExecutionContinued
// carrying the current state (plus seg in its segments). Behaviour is
// unchanged — channels, results, counters, visits, in-flight work and timers
// all carry over; only the live log restarts from here.
func Continue(def *flow.Flow, st *State, seg event.Segment, cmdID string, now time.Time) ([]event.Event, error) {
	if !st.Exists() || st.Status.Terminal() {
		return nil, nil
	}

	b, err := json.Marshal(st)
	if err != nil {
		return nil, err
	}

	carried, err := Unmarshal(b)
	if err != nil {
		return nil, err
	}

	carried.Segments = append(carried.Segments, seg)

	if b, err = json.Marshal(carried); err != nil {
		return nil, err
	}

	d := &decider{def: def, st: st, now: now.UTC(), cmdID: cmdID}
	if err = d.emit(event.ExecutionContinued, &event.Continued{State: b, Segment: seg}); err != nil {
		return nil, err
	}

	return d.out, nil
}

// Abort returns the events that fail execution st outright with reason and
// msg: used when a decision cannot be stored (e.g. it exceeds the decision
// events limit). For an execution that does not exist yet, first (its
// ExecutionStarted or ExecutionForked) is recorded before the failure, so the
// failure is visible like any other.
func Abort(def *flow.Flow, st *State, first *event.Event, cmdID, reason, msg string,
	now time.Time,
) ([]event.Event, error) {
	d := &decider{def: def, st: st, now: now.UTC(), cmdID: cmdID}

	if !st.Exists() {
		if first == nil {
			return nil, fmt.Errorf("%w: cannot abort a missing execution", ErrInvalid)
		}

		if err := d.emit(first.Type, first.Data); err != nil {
			return nil, err
		}
	}

	if st.Status.Terminal() {
		return d.out, nil
	}

	return d.out, d.fail(reason, msg, "")
}

func (d *decider) emit(t event.Type, data any) error {
	ev := event.New(t, data)
	ev.Time = d.now
	ev.CmdID = d.cmdID
	ev.Index = d.st.Events + 1

	if t == event.ExecutionForked {
		ev.Index = 1
	}

	if err := d.st.Apply(d.def, ev); err != nil {
		return err
	}

	d.out = append(d.out, ev)

	return nil
}

func (d *decider) alloc() int { return d.st.N + 1 }

// ---------------------------------------------------------------------------
// Start and routing

func (d *decider) start(c cmd.Command) error {
	if d.st.Exists() {
		return nil // idempotent start (I-03)
	}

	var p cmd.StartData
	if err := c.Payload(&p); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalid, err)
	}

	input := p.Input
	if isNull(input) {
		input = json.RawMessage(`{}`)
	}

	if !isObject(input) {
		return fmt.Errorf("%w: input must be a JSON object", ErrInvalid)
	}

	hash, err := d.def.Hash()
	if err != nil {
		return err
	}

	started := &event.Started{
		ExecID: d.st.ExecID, Flow: d.def.Name, FlowHash: hash, Input: input, Parent: p.Parent,
		Attrs: d.attrs(input),
	}

	if err = d.emit(event.ExecutionStarted, started); err != nil {
		return err
	}

	return d.enter(d.def.StartNode())
}

func (d *decider) attrs(input json.RawMessage) map[string]string {
	if len(d.def.SearchAttributes) == 0 {
		return nil
	}

	var in any

	_ = json.Unmarshal(input, &in)

	env := map[string]any{"input": in}
	out := map[string]string{}

	for _, k := range slices.Sorted(maps.Keys(d.def.SearchAttributes)) {
		v, err := d.def.AttrProgram(k).Eval(context.Background(), env)
		if err != nil || v == nil {
			continue
		}

		out[k] = fmt.Sprint(v)
	}

	return out
}

// enter visits node id.
func (d *decider) enter(id string) error {
	n := d.def.Node(id)
	if n == nil {
		return d.fail(event.ReasonError, fmt.Sprintf("unknown node %q", id), id)
	}

	if d.st.Steps+1 > d.def.MaxStepsOrDefault() {
		return d.fail(event.ReasonMaxSteps,
			fmt.Sprintf("recursion limit reached: %d node entries (max_steps)", d.def.MaxStepsOrDefault()), id)
	}

	if err := d.emit(event.NodeEntered, &event.Entered{Node: id}); err != nil {
		return err
	}

	switch n.Type {
	case flow.NodeTask:
		return d.schedule(n, n.ID, "", 0, nil, nil, 0, 1)
	case flow.NodeChoice:
		return d.choose(n)
	case flow.NodeFanout:
		return d.fanout(n)
	case flow.NodeAwait:
		return d.await(n)
	case flow.NodeMap:
		return d.mapStart(n)
	case flow.NodeSubflow:
		return d.subflow(n)
	default:
		return d.fail(event.ReasonError, fmt.Sprintf("node %q of type %s cannot be entered", id, n.Type), id)
	}
}

// advance leaves node n towards dyn (a dynamic successor) or its static next;
// with neither, the execution completes.
func (d *decider) advance(n *flow.Node, dyn string) error {
	next := dyn
	if next == "" {
		next = n.Next
	}

	if next == "" {
		return d.complete0()
	}

	return d.enter(next)
}

// complete0 completes the execution. Its output is the flow's output
// expression when set; otherwise every channel, or the last node's result when
// the flow declares no channels.
func (d *decider) complete0() error {
	out := d.st.Results[d.st.LastNode]

	switch {
	case d.def.OutputProgram() != nil:
		v, err := d.def.OutputProgram().Eval(context.Background(), d.st.Env(nil))
		if err != nil {
			return d.fail(event.ReasonExpression, fmt.Sprintf("output: %v", err), "")
		}

		if out, err = json.Marshal(v); err != nil {
			return d.fail(event.ReasonExpression, fmt.Sprintf("output: %v", err), "")
		}

		if isNull(out) {
			out = nil
		} else if !isObject(out) {
			return d.fail(event.ReasonExpression, "output must be an object or nil", "")
		}
	case len(d.def.Channels) > 0:
		b, err := json.Marshal(d.st.Channels)
		if err != nil {
			return err
		}

		out = b
	}

	return d.emit(event.ExecutionCompleted, &event.Completed{Output: out})
}

func (d *decider) cancelChildren() []string {
	var out []string

	for _, node := range slices.Sorted(maps.Keys(d.st.Children)) {
		if c := d.st.Children[node]; c.Policy != flow.ParentCloseAbandon {
			out = append(out, c.ChildID)
		}
	}

	return out
}

// ownedChildren lists the running children of fanout or map owner that must
// be cancelled now that it settled: all but the abandoned ones.
func (d *decider) ownedChildren(owner string) []string {
	var out []string

	for _, key := range slices.Sorted(maps.Keys(d.st.Children)) {
		if c := d.st.Children[key]; c.Owner == owner && c.Policy != flow.ParentCloseAbandon {
			out = append(out, c.ChildID)
		}
	}

	return out
}

func (d *decider) fail(reason, msg, node string) error {
	return d.emit(event.ExecutionFailed, &event.Failed{
		Error: msg, Reason: reason, Node: node, CancelChildren: d.cancelChildren(),
	})
}

func (d *decider) cancel(p *cmd.CancelData) error {
	return d.emit(event.ExecutionCancelled, &event.Cancelled{Reason: p.Reason, CancelChildren: d.cancelChildren()})
}

// ---------------------------------------------------------------------------
// Tasks

// schedule emits one attempt of a task instance. gen 0 allocates a fresh
// generation.
func (d *decider) schedule(n *flow.Node, key, owner string, index int, item, resume json.RawMessage,
	gen, attempt int,
) error {
	if gen == 0 {
		gen = d.alloc()
	}

	err := d.emit(event.NodeScheduled, &event.Scheduled{
		Key: key, Node: n.ID, Kind: n.Kind, Index: index, Generation: gen, Attempt: attempt, Owner: owner,
		Item: item, Resume: resume,
	})
	if err != nil {
		return err
	}

	if n.Timeout > 0 {
		return d.timerAt(event.TimerTimeout, n.Timeout.D(), key, n.ID, gen, attempt)
	}

	return nil
}

func (d *decider) timerAt(purpose string, after time.Duration, key, node string, gen, attempt int) error {
	nr := d.alloc()

	return d.emit(event.TimerScheduled, &event.Timer{
		ID: "t" + strconv.Itoa(nr), N: nr, At: d.now.Add(after), Purpose: purpose, Key: key, Node: node,
		Generation: gen, Attempt: attempt,
	})
}

// current returns the active task instance addressed by ref, or nil when the
// reference is stale (another attempt, a newer generation, a settled task).
func (d *decider) current(ref cmd.TaskRef, statuses ...string) *Task {
	t := d.st.Tasks[ref.Key]
	if t == nil || t.Generation != ref.Generation || t.Attempt != ref.Attempt {
		return nil
	}

	if !slices.Contains(statuses, t.Status) {
		return nil
	}

	return t
}

func (d *decider) complete(p *cmd.CompleteData) error {
	t := d.current(p.TaskRef, TaskScheduled)
	if t == nil {
		return nil
	}

	n := d.def.Node(t.Node)

	output := p.Output
	if isNull(output) {
		output = nil
	}

	if problem, retryable := d.checkCompletion(n, t, output, p); problem != "" {
		reason := event.ReasonInvalidOutput

		return d.taskFailed(t, problem, retryable, reason, p.Usage)
	}

	err := d.emit(event.NodeCompleted, &event.NodeDone{
		Key: t.Key, Node: t.Node, Generation: t.Generation, Attempt: t.Attempt, Output: output, Writes: p.Writes,
		Next: p.Next, Usage: p.Usage, Cached: p.Cached, CacheKey: p.CacheKey,
	})
	if err != nil {
		return err
	}

	if over := d.overBudget(); over != "" {
		return d.fail(event.ReasonBudget, over, t.Node)
	}

	return d.afterSettle(t, n, p.Next)
}

// checkCompletion validates a completion. It returns a problem description and
// whether retrying could fix it: a schema mismatch may (the worker's output
// varies), a structurally impossible completion may not.
func (d *decider) checkCompletion(n *flow.Node, t *Task, output json.RawMessage,
	p *cmd.CompleteData,
) (string, bool) {
	// I-04: an output is a JSON object (or absent).
	if output != nil && !isObject(output) {
		return "output must be a JSON object", false
	}

	if err := CheckWrites(d.def, d.st, p.Writes); err != nil {
		return err.Error(), false
	}

	if p.Next != "" && (t.Owner != "" || !n.AllowsDynamic(p.Next)) {
		return fmt.Sprintf("next %q is not a declared dynamic successor of %q", p.Next, n.ID), false
	}

	if output != nil {
		if err := n.ValidateOutput(output); err != nil {
			return err.Error(), true
		}
	}

	return "", false
}

func (d *decider) overBudget() string { return d.overBudgetWith(nil) }

// overBudgetWith reports the first budget the counters exceed once usage is
// added to them.
func (d *decider) overBudgetWith(usage map[string]float64) string {
	for _, k := range slices.Sorted(maps.Keys(d.def.Budget)) {
		if v := d.st.Counters[k] + usage[k]; v > d.def.Budget[k] {
			return fmt.Sprintf("counter %q = %g exceeds budget %g", k, v, d.def.Budget[k])
		}
	}

	return ""
}

// afterSettle continues after a task instance settled.
func (d *decider) afterSettle(t *Task, n *flow.Node, dyn string) error {
	if d.st.Status.Terminal() {
		return nil
	}

	if _, isFan := d.st.Fans[t.Owner]; isFan {
		return d.evalJoin(t.Owner)
	}

	if _, isMap := d.st.Maps[t.Owner]; isMap && t.Node == t.Owner {
		return d.mapProgress(n)
	}

	return d.advance(n, dyn)
}

func (d *decider) failTask(p *cmd.FailData) error {
	t := d.current(p.TaskRef, TaskScheduled)
	if t == nil {
		return nil
	}

	reason := p.Reason
	if reason == "" {
		reason = event.ReasonError
	}

	return d.taskFailed(t, p.Error, p.Retryable, reason, p.Usage)
}

// taskFailed records a failed attempt and the usage it reported: retry when
// allowed, otherwise settle the instance as failed and let its owner react. A
// usage that exceeds the budget fails the execution instead.
func (d *decider) taskFailed(t *Task, msg string, retryable bool, reason string, usage map[string]float64) error {
	n := d.def.Node(t.Node)
	over := d.overBudgetWith(usage)
	willRetry := retryable && t.Attempt < n.MaxAttempts() && over == ""

	// Copy what we need: Apply removes a settled task.
	tc := *t

	err := d.emit(event.NodeFailed, &event.NodeFail{
		Key: tc.Key, Node: tc.Node, Generation: tc.Generation, Attempt: tc.Attempt, Error: msg, Reason: reason,
		WillRetry: willRetry, Usage: usage,
	})
	if err != nil {
		return err
	}

	if over != "" {
		return d.fail(event.ReasonBudget, over, tc.Node)
	}

	if willRetry {
		delay := n.Backoff(tc.Attempt + 1)
		if delay <= 0 {
			return d.schedule(n, tc.Key, tc.Owner, tc.Index, tc.Item, tc.Resume, tc.Generation, tc.Attempt+1)
		}

		return d.timerAt(event.TimerRetry, delay, tc.Key, tc.Node, tc.Generation, tc.Attempt+1)
	}

	if _, isFan := d.st.Fans[tc.Owner]; isFan {
		return d.evalJoin(tc.Owner)
	}

	if n.OnFailure != "" {
		if _, isMap := d.st.Maps[tc.Owner]; isMap && tc.Node == tc.Owner {
			return d.abortMap(n, tc.Index)
		}

		return d.enter(n.OnFailure)
	}

	return d.fail(reason, fmt.Sprintf("node %q failed: %s", tc.Node, msg), tc.Node)
}

// abortMap closes map n after its item index failed permanently: the items
// still in flight are cancelled (their late completions become stale) and the
// execution continues at the map's on_failure.
func (d *decider) abortMap(n *flow.Node, index int) error {
	for _, k := range slices.Sorted(maps.Keys(d.st.Tasks)) {
		if t := d.st.Tasks[k]; t.Owner == n.ID {
			err := d.emit(event.NodeCancelled, &event.NodeCancel{Key: k, Node: t.Node, Reason: "map item failed"})
			if err != nil {
				return err
			}
		}
	}

	abort := &event.MapAbort{Node: n.ID, Index: index, CancelChildren: d.ownedChildren(n.ID)}
	if err := d.emit(event.MapAborted, abort); err != nil {
		return err
	}

	return d.enter(n.OnFailure)
}

func (d *decider) interrupt(p *cmd.InterruptData) error {
	t := d.current(p.TaskRef, TaskScheduled)
	if t == nil {
		return nil
	}

	node := t.Node

	err := d.emit(event.NodeInterrupted, &event.Interrupted{
		Key: t.Key, Node: node, Generation: t.Generation, Payload: p.Payload, Usage: p.Usage,
	})
	if err != nil {
		return err
	}

	// Nobody is asked a question the budget can no longer pay for.
	if over := d.overBudget(); over != "" {
		return d.fail(event.ReasonBudget, over, node)
	}

	return nil
}

func (d *decider) resume(p *cmd.ResumeData) error {
	var t *Task

	for _, k := range slices.Sorted(maps.Keys(d.st.Tasks)) {
		c := d.st.Tasks[k]
		if c.Status == TaskInterrupted && (k == p.Key || (p.Key == "" && c.Node == p.Node)) {
			t = c

			break
		}
	}

	if t == nil {
		return nil // nothing interrupted there: a duplicate or late resume
	}

	n := d.def.Node(t.Node)

	return d.schedule(n, t.Key, t.Owner, t.Index, t.Item, p.Value, 0, 1)
}

// ---------------------------------------------------------------------------
// Timers

func (d *decider) timer(p *cmd.TimerData) error {
	tm := d.st.Timers[p.TimerID]
	if tm == nil {
		return nil // already fired or obsolete
	}

	tc := *tm

	if err := d.emit(event.TimerFired, &event.Fired{ID: tc.ID}); err != nil {
		return err
	}

	switch tc.Purpose {
	case event.TimerRetry:
		t := d.st.Tasks[tc.Key]
		if t == nil || t.Generation != tc.Generation || t.Status != TaskRetrying || t.Attempt != tc.Attempt-1 {
			return nil
		}

		return d.schedule(d.def.Node(t.Node), t.Key, t.Owner, t.Index, t.Item, t.Resume, t.Generation, tc.Attempt)
	case event.TimerTimeout:
		t := d.current(cmd.TaskRef{Key: tc.Key, Generation: tc.Generation, Attempt: tc.Attempt}, TaskScheduled)
		if t == nil {
			return nil
		}

		return d.taskFailed(t, "attempt timed out", true, event.ReasonTimeout, nil)
	case event.TimerAwait:
		return d.awaitTimeout(tc)
	default:
		return nil
	}
}

// ---------------------------------------------------------------------------
// Choice

func (d *decider) choose(n *flow.Node) error {
	env := d.st.Env(nil)
	progs := n.RulePrograms()
	def := -1

	for i, r := range n.Rules {
		if r.Default {
			def = i

			continue
		}

		ok, err := progs[i].Match(context.Background(), env)
		if err != nil {
			if n.OnError == flow.OnErrorFail {
				if e := d.emit(event.ChoiceEvaluated, &event.Choice{Node: n.ID, Rule: i, Error: err.Error()}); e != nil {
					return e
				}

				return d.fail(event.ReasonExpression, err.Error(), n.ID)
			}

			continue // missing optional field = no match
		}

		if ok {
			return d.route(n, i, r.To)
		}
	}

	return d.route(n, def, n.Rules[def].To)
}

// route records the choice and follows it: into node to, or, for flow.End,
// to completion. Nothing else is in flight when a choice runs (a choice is
// never a fan-out branch), so completing here strands no work.
func (d *decider) route(n *flow.Node, rule int, to string) error {
	if err := d.emit(event.ChoiceEvaluated, &event.Choice{Node: n.ID, Rule: rule, To: to}); err != nil {
		return err
	}

	if to == flow.End {
		return d.complete0()
	}

	return d.enter(to)
}

// ---------------------------------------------------------------------------
// Fan-out / join

func (d *decider) fanout(n *flow.Node) error {
	join := d.def.JoinOf(n.ID)

	if err := d.emit(event.FanoutStarted, &event.Fanout{Node: n.ID, Join: join, Branches: n.Branches}); err != nil {
		return err
	}

	for _, b := range n.Branches {
		if err := d.emit(event.NodeEntered, &event.Entered{Node: b}); err != nil {
			return err
		}

		bn := d.def.Node(b)

		var err error
		if bn.Type == flow.NodeSubflow {
			err = d.startChild(bn, b, n.ID, 0, nil)
		} else {
			err = d.schedule(bn, b, n.ID, 0, nil, nil, 0, 1)
		}

		if err != nil || d.st.Status.Terminal() {
			return err
		}
	}

	return nil
}

// evalJoin settles the join of fan-out fan when its policy is decided: enough
// successes, or too many failures for the policy to still be met. Branches
// still in flight are cancelled (their late completions become stale).
func (d *decider) evalJoin(fan string) error {
	f := d.st.Fans[fan]
	join := d.def.Node(f.Join)
	waits := d.def.WaitFor(join.ID)

	var succeeded, failed []string

	pending := 0

	for _, b := range waits {
		switch f.Branches[b] {
		case BranchCompleted:
			succeeded = append(succeeded, b)
		case BranchFailed, BranchCancelled:
			failed = append(failed, b)
		default:
			pending++
		}
	}

	need := len(waits)

	switch kind, q := join.JoinKind(); kind {
	case flow.JoinAny:
		need = 1
	case flow.JoinQuorum:
		need = q
	}

	ok := len(succeeded) >= need
	if !ok && len(succeeded)+pending >= need {
		return nil // still undecided
	}

	return d.closeJoin(fan, join, succeeded, failed, ok)
}

func (d *decider) closeJoin(fan string, join *flow.Node, succeeded, failed []string, ok bool) error {
	cancel := d.ownedChildren(fan)

	for _, k := range slices.Sorted(maps.Keys(d.st.Tasks)) {
		if t := d.st.Tasks[k]; t.Owner == fan {
			err := d.emit(event.NodeCancelled, &event.NodeCancel{Key: k, Node: t.Node, Reason: "join settled"})
			if err != nil {
				return err
			}
		}
	}

	outputs := map[string]json.RawMessage{}
	for _, b := range succeeded {
		outputs[b] = d.st.Results[b]
	}

	out, err := json.Marshal(outputs)
	if err != nil {
		return err
	}

	if err = d.emit(event.NodeEntered, &event.Entered{Node: join.ID}); err != nil {
		return err
	}

	err = d.emit(event.JoinCompleted, &event.Join{
		Node: join.ID, Fanout: fan, Succeeded: succeeded, Failed: failed, OK: ok, Output: out,
		CancelChildren: cancel,
	})
	if err != nil {
		return err
	}

	if !ok {
		if join.OnFailure != "" {
			return d.enter(join.OnFailure)
		}

		return d.fail(event.ReasonJoin, fmt.Sprintf("join %q failed: branches %v did not succeed", join.ID, failed),
			join.ID)
	}

	return d.advance(join, "")
}

// ---------------------------------------------------------------------------
// Await / signals

func (d *decider) await(n *flow.Node) error {
	nr := d.alloc()
	id := "t" + strconv.Itoa(nr)

	if err := d.emit(event.AwaitStarted, &event.Await{Node: n.ID, Signal: n.Signal, TimerID: id}); err != nil {
		return err
	}

	err := d.emit(event.TimerScheduled, &event.Timer{
		ID: id, N: nr, At: d.now.Add(n.Timeout.D()), Purpose: event.TimerAwait, Node: n.ID,
	})
	if err != nil {
		return err
	}

	// A signal that arrived before the await is consumed now (I-09).
	return d.consume(n)
}

// consume delivers the oldest buffered signal of n's name to await node n.
func (d *decider) consume(n *flow.Node) error {
	i := slices.IndexFunc(d.st.Buffered, func(b Buffered) bool { return b.Name == n.Signal })
	if i < 0 {
		return nil
	}

	b := d.st.Buffered[i]

	if err := d.emit(event.SignalConsumed, &event.SignalUse{Node: n.ID, Name: b.Name, ID: b.ID}); err != nil {
		return err
	}

	return d.advance(n, "")
}

func (d *decider) signal(p *cmd.SignalData) error {
	if err := names.CheckToken("signal name", p.Name); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalid, err)
	}

	if _, seen := d.st.SignalIDs[d.cmdID]; seen {
		return nil // duplicate delivery of the same signal
	}

	if err := d.emit(event.SignalReceived, &event.Signal{Name: p.Name, ID: d.cmdID, Payload: p.Payload}); err != nil {
		return err
	}

	for _, node := range slices.Sorted(maps.Keys(d.st.Awaits)) {
		if d.st.Awaits[node].Signal == p.Name {
			return d.consume(d.def.Node(node))
		}
	}

	return nil
}

func (d *decider) awaitTimeout(tm event.Timer) error {
	a := d.st.Awaits[tm.Node]
	if a == nil || a.TimerID != tm.ID {
		return nil
	}

	n := d.def.Node(tm.Node)

	if err := d.emit(event.AwaitTimedOut, &event.AwaitTimeout{Node: n.ID, To: n.OnTimeout}); err != nil {
		return err
	}

	if n.OnTimeout == "" {
		return d.fail(event.ReasonAwaitTimeout, fmt.Sprintf("await %q: no signal %q before timeout", n.ID, n.Signal),
			n.ID)
	}

	return d.enter(n.OnTimeout)
}

// ---------------------------------------------------------------------------
// Map

func (d *decider) mapStart(n *flow.Node) error {
	v, err := n.OverProgram().Eval(context.Background(), d.st.Env(nil))
	if err != nil {
		return d.fail(event.ReasonExpression, fmt.Sprintf("map %q: over: %v", n.ID, err), n.ID)
	}

	list, ok := v.([]any)
	if !ok && v != nil {
		return d.fail(event.ReasonExpression, fmt.Sprintf("map %q: over must evaluate to a list, got %T", n.ID, v), n.ID)
	}

	items := make([]json.RawMessage, len(list))

	for i, it := range list {
		b, lerr := json.Marshal(it)
		if lerr != nil {
			return lerr
		}

		items[i] = b
	}

	if err = d.emit(event.MapStarted, &event.Map{Node: n.ID, Items: items, MaxParallel: n.MaxParallel}); err != nil {
		return err
	}

	return d.mapProgress(n)
}

// mapProgress schedules map items up to max_parallel and completes the map
// when every item settled.
func (d *decider) mapProgress(n *flow.Node) error {
	m := d.st.Maps[n.ID]

	if m.Done == len(m.Items) {
		if err := d.emit(event.MapCompleted, &event.MapDone{Node: n.ID}); err != nil {
			return err
		}

		return d.advance(n, "")
	}

	limit := m.MaxParallel
	if limit <= 0 {
		limit = flow.DefaultMapParallel
	}

	for m.Next < len(m.Items) && m.Next-m.Done < limit {
		i := m.Next
		key := n.ID + "#" + strconv.Itoa(i)

		var err error
		if n.Flow != "" {
			err = d.startChild(n, key, n.ID, i, m.Items[i])
		} else {
			err = d.schedule(n, key, n.ID, i, m.Items[i], nil, 0, 1)
		}

		if err != nil || d.st.Status.Terminal() {
			return err
		}
	}

	return nil
}

// ---------------------------------------------------------------------------
// Subflows

func (d *decider) subflow(n *flow.Node) error {
	return d.startChild(n, n.ID, "", 0, nil)
}

// startChild starts the child execution of instance key of node n: a
// subflow node, a fan-out branch (owner is the fanout) or a map item (owner
// is the map; item and index are what the input expression sees, and the
// item itself is the input without one).
func (d *decider) startChild(n *flow.Node, key, owner string, index int, item json.RawMessage) error {
	input := d.st.Input
	what := fmt.Sprintf("subflow %q", n.ID)
	isItem := owner == n.ID

	var view *Task
	if isItem {
		view = &Task{Node: n.ID, Owner: n.ID, Index: index, Item: item}
		what = fmt.Sprintf("map %q: item %d", n.ID, index)
		input = item
	}

	if p := n.InputProgram(); p != nil {
		v, err := p.Eval(context.Background(), d.st.Env(view))
		if err != nil {
			return d.fail(event.ReasonExpression, fmt.Sprintf("%s: input: %v", what, err), n.ID)
		}

		if input, err = json.Marshal(v); err != nil {
			return d.fail(event.ReasonExpression, fmt.Sprintf("%s: input: %v", what, err), n.ID)
		}
	}

	if !isObject(input) {
		hint := ""
		if isItem && n.InputProgram() == nil {
			hint = " (map the item to one with input)"
		}

		return d.fail(event.ReasonExpression, fmt.Sprintf("%s: input must be an object%s", what, hint), n.ID)
	}

	nr := d.alloc()

	c := &event.Child{
		Node: n.ID, ChildID: ChildID(d.st.ExecID, nr), ChildN: nr, Flow: n.Flow, Input: input, Policy: n.ParentClose(),
		Owner: owner, Index: index,
	}
	if key != n.ID {
		c.Key = key
	}

	return d.emit(event.ChildStarted, c)
}

// ChildID derives the id of a subflow execution: deterministic, so a
// redelivered start is idempotent, and always a valid token.
func ChildID(parent string, n int) string {
	id := parent + "-c" + strconv.Itoa(n)
	if names.ValidToken(id) {
		return id
	}

	sum := sha256.Sum256([]byte(parent))

	return hex.EncodeToString(sum[:12]) + "-c" + strconv.Itoa(n)
}

func (d *decider) childDone(p *cmd.ChildDoneData) error {
	var (
		key string
		c   *ChildState
	)

	for k, cs := range d.st.Children {
		if cs.ChildID == p.ChildID {
			key, c = k, cs
		}
	}

	if c == nil {
		return nil // stale: settled, cancelled or no longer wanted
	}

	cc := *c
	node := cc.NodeOf(key)

	done := &event.ChildDone{
		Node: node, ChildID: p.ChildID, Status: p.Status, Output: p.Output, Error: p.Error, Counters: p.Counters,
	}
	if key != node {
		done.Key = key
	}

	if err := d.emit(event.ChildCompleted, done); err != nil {
		return err
	}

	if over := d.overBudget(); over != "" {
		return d.fail(event.ReasonBudget, over, node)
	}

	n := d.def.Node(node)
	ok := p.Status == string(StatusCompleted)

	if _, isFan := d.st.Fans[cc.Owner]; isFan {
		return d.evalJoin(cc.Owner)
	}

	if _, isMap := d.st.Maps[cc.Owner]; isMap && cc.Owner == node {
		if ok {
			return d.mapProgress(n)
		}

		if n.OnFailure != "" {
			return d.abortMap(n, cc.Index)
		}

		return d.fail(event.ReasonChild, fmt.Sprintf("map %q: item %d (%s) %s: %s", node, cc.Index, p.ChildID,
			p.Status, p.Error), node)
	}

	if !ok {
		if n.OnFailure != "" {
			return d.enter(n.OnFailure)
		}

		return d.fail(event.ReasonChild, fmt.Sprintf("subflow %q (%s) %s: %s", node, p.ChildID, p.Status, p.Error),
			node)
	}

	return d.advance(n, "")
}

// ---------------------------------------------------------------------------
// Fork

// rekick re-dispatches everything in flight in a freshly forked state: tasks
// get new generations, awaits new timers, subflows new children.
func (d *decider) rekick() error {
	for _, k := range slices.Sorted(maps.Keys(d.st.Tasks)) {
		t := d.st.Tasks[k]
		if t.Status == TaskInterrupted {
			continue
		}

		if err := d.schedule(d.def.Node(t.Node), t.Key, t.Owner, t.Index, t.Item, t.Resume, 0, 1); err != nil {
			return err
		}
	}

	for _, node := range slices.Sorted(maps.Keys(d.st.Awaits)) {
		if err := d.await(d.def.Node(node)); err != nil {
			return err
		}
	}

	for _, key := range slices.Sorted(maps.Keys(d.st.Children)) {
		c := d.st.Children[key]

		var item json.RawMessage
		if m := d.st.Maps[c.Owner]; m != nil && c.Index < len(m.Items) {
			item = m.Items[c.Index]
		}

		if err := d.startChild(d.def.Node(c.NodeOf(key)), key, c.Owner, c.Index, item); err != nil {
			return err
		}
	}

	return nil
}

func isObject(b json.RawMessage) bool {
	t := bytes.TrimSpace(b)

	return len(t) > 0 && t[0] == '{' && json.Valid(t)
}

// IsObject reports whether b is a JSON object.
func IsObject(b json.RawMessage) bool { return isObject(b) }
