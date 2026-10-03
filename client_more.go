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

package packtrail

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail/event"
	"github.com/henomis/packtrail/internal/engine"
	"github.com/henomis/packtrail/internal/names"
	"github.com/henomis/packtrail/internal/sched"
	"github.com/henomis/packtrail/internal/store"
	"github.com/henomis/packtrail/internal/wire"
)

// IsTerminal reports whether ev ends an execution.
func IsTerminal(ev event.Event) bool { return ev.Type.Terminal() }

// Watch streams the events of an execution from sequence from (0 = the
// beginning), as they are appended: LangGraph's "updates" stream mode. The
// channel closes after the terminal event or when ctx ends.
func (c *Client) Watch(ctx context.Context, execID string, from uint64) (<-chan event.Event, error) {
	if err := checkExecID(execID); err != nil {
		return nil, err
	}

	if err := c.attach(ctx); err != nil {
		return nil, err
	}

	cfg := jetstream.OrderedConsumerConfig{FilterSubjects: []string{c.in.EventSubject(execID)}}
	if from > 0 {
		cfg.DeliverPolicy = jetstream.DeliverByStartSequencePolicy
		cfg.OptStartSeq = from
	}

	cons, err := c.orderedConsumer(ctx, cfg)
	if err != nil {
		return nil, err
	}

	it, err := cons.Messages()
	if err != nil {
		return nil, err
	}

	out := make(chan event.Event, watchBuffer)

	go func() {
		defer close(out)
		defer it.Stop()

		go func() {
			<-ctx.Done()
			it.Stop()
		}()

		for {
			msg, lerr := it.Next()
			if lerr != nil {
				return
			}

			evs, lerr := c.ld.Log.Decode(ctx, msg)
			if lerr != nil {
				c.in.Logger.Warn("packtrail: watch decode", "lerr", lerr)

				return
			}

			if !sendDecision(ctx, out, evs) {
				return
			}
		}
	}()

	return out, nil
}

// orderedConsumer creates an ordered consumer of the events stream. The
// request gets the read timeout: one sent as the server goes away is never
// answered, and without a deadline it would wait forever. The consumer
// itself survives reconnects.
func (c *Client) orderedConsumer(ctx context.Context, cfg jetstream.OrderedConsumerConfig) (jetstream.Consumer, error) {
	rctx, cancel := context.WithTimeout(ctx, c.in.ReadTimeout)
	defer cancel()

	return c.in.JS.OrderedConsumer(rctx, c.in.Names.StreamEvents, cfg)
}

// sendDecision sends the events of one decision to out. It returns false
// when the watch must end: ctx is done or the execution finished.
func sendDecision(ctx context.Context, out chan<- event.Event, evs []event.Event) bool {
	for _, ev := range evs {
		select {
		case out <- ev:
		case <-ctx.Done():
			return false
		}

		if IsTerminal(ev) {
			return false
		}
	}

	return true
}

const watchBuffer = 64

// Ended reports that an execution finished.
type Ended struct {
	ExecID string
	// Status is completed, failed or cancelled; Get has the reason and error.
	Status Status
	// Seq is the stream sequence of the terminal decision: pass Seq+1 to
	// WatchTerminal to resume after it.
	Seq uint64
}

// WatchTerminal streams the end of every execution in the namespace (subflow
// children included) from stream sequence fromSeq, or from now when fromSeq
// is 0: one consumer for all executions instead of a Wait per execution. It
// reads only event headers, never bodies. Endings arrive in log order, once
// each, across reconnects (the ordered consumer resumes after the last
// message it delivered); a slow reader slows the watch instead of losing
// endings. The channel closes when ctx ends or the connection is closed.
func (c *Client) WatchTerminal(ctx context.Context, fromSeq uint64) (<-chan Ended, error) {
	if err := c.attach(ctx); err != nil {
		return nil, err
	}

	next := fromSeq
	if next == 0 {
		var err error
		if next, err = c.eventsEnd(ctx); err != nil {
			return nil, err
		}
	}

	cons, err := c.orderedConsumer(ctx, jetstream.OrderedConsumerConfig{
		FilterSubjects: []string{c.in.Names.EventsSubjects()},
		DeliverPolicy:  jetstream.DeliverByStartSequencePolicy,
		OptStartSeq:    next,
		HeadersOnly:    true,
	})
	if err != nil {
		return nil, err
	}

	it, err := cons.Messages()
	if err != nil {
		return nil, err
	}

	out := make(chan Ended, watchBuffer)

	go func() {
		defer close(out)
		defer it.Stop()

		stop := context.AfterFunc(ctx, it.Stop)
		defer stop()

		for {
			msg, lerr := it.Next()
			if lerr != nil {
				return
			}

			md, lerr := msg.Metadata()
			if lerr != nil {
				continue
			}

			ended, ok := terminalOf(msg, md.Sequence.Stream)
			if !ok {
				continue
			}

			select {
			case out <- ended:
			case <-ctx.Done():
				return
			}
		}
	}()

	return out, nil
}

// eventsEnd returns the sequence the next event will get.
func (c *Client) eventsEnd(ctx context.Context) (uint64, error) {
	s, err := c.in.JS.Stream(ctx, c.in.Names.StreamEvents)
	if err != nil {
		return 0, err
	}

	info, err := s.Info(ctx)
	if err != nil {
		return 0, err
	}

	return info.State.LastSeq + 1, nil
}

// terminalOf reads the end of an execution from the headers of a decision.
func terminalOf(msg jetstream.Msg, seq uint64) (Ended, bool) {
	for _, t := range event.Types(msg.Headers()) {
		if !t.Terminal() {
			continue
		}

		subject := msg.Subject()

		return Ended{ExecID: subject[strings.LastIndexByte(subject, '.')+1:], Status: endedStatus(t), Seq: seq}, true
	}

	return Ended{}, false
}

// endedStatus is the status a terminal event type leaves an execution in.
func endedStatus(t event.Type) Status {
	switch t { //nolint:exhaustive // called with terminal types only.
	case event.ExecutionCompleted:
		return StatusCompleted
	case event.ExecutionFailed:
		return StatusFailed
	default:
		return StatusCancelled
	}
}

// Progress is an intermediate result a worker published while running a node
// (worker.Job.Progress). It is never stored: only listeners receive it.
type Progress struct {
	ExecID     string `json:"exec_id"`
	Node       string `json:"node"`
	Key        string `json:"key"`
	Generation int    `json:"generation"`
	Attempt    int    `json:"attempt"`
	// Seq counts the messages of one job attempt from 1: a gap means one was
	// lost (delivery is best effort).
	Seq  int64           `json:"seq"`
	Time time.Time       `json:"time"`
	Data json.RawMessage `json:"data,omitempty"`
}

// Progress streams the progress messages workers publish for execID, as they
// arrive: LangGraph's "custom" stream mode. Messages are not stored, so only
// those published after the subscription are received; to see an execution
// from its first step, choose its id (WithExecutionID) and subscribe before
// Start. The channel closes when ctx ends or the execution finishes (after the
// messages already received).
func (c *Client) Progress(ctx context.Context, execID string) (<-chan Progress, error) {
	if err := checkExecID(execID); err != nil {
		return nil, err
	}

	if err := c.attach(ctx); err != nil {
		return nil, err
	}

	var (
		out    = make(chan Progress, watchBuffer)
		done   = make(chan struct{})
		mu     sync.RWMutex // a send never races the close of out
		closed bool
	)

	sub, err := c.nc.Subscribe(c.in.Names.ProgressFilter(execID), func(m *nats.Msg) {
		var p Progress
		if json.Unmarshal(m.Data, &p) != nil {
			return
		}

		mu.RLock()
		defer mu.RUnlock()

		if closed {
			return
		}

		select {
		case out <- p:
		case <-done:
		}
	})
	if err != nil {
		return nil, err
	}

	go func() {
		wctx, cancel := context.WithCancel(ctx)
		defer cancel()

		_, _ = c.Wait(wctx, execID)

		// Deliver what already arrived, then close.
		if sub.Drain() == nil {
			for sub.IsValid() && ctx.Err() == nil {
				time.Sleep(drainPoll)
			}
		}

		_ = sub.Unsubscribe()

		close(done) // releases a callback blocked on a full channel

		mu.Lock()
		closed = true

		close(out)
		mu.Unlock()
	}()

	return out, nil
}

const drainPoll = 10 * time.Millisecond

// Wait blocks until the execution finishes and returns its final state. It
// watches event headers only (no payloads are fetched while waiting).
//
// Only ctx ends a wait early, besides an invalid id: a read that fails (a
// timeout under load, a leader election) is retried with backoff instead of
// being returned to a caller that asked to wait.
func (c *Client) Wait(ctx context.Context, execID string) (*State, error) {
	return c.wait(ctx, execID, nil)
}

// WaitUntil blocks until cond holds on the execution's state and returns that
// state: wait until it parks at an await, opens a task, reaches a status.
// cond is checked on the current state, then each time the execution's log
// is caught up after a decision; a state the execution only passes through
// between two checks may be missed, so test what lasts (an open await, a
// result) rather than what flashes by. If the execution finishes without cond
// holding, WaitUntil returns its final state and ErrTerminal. Like Wait, only
// ctx ends it early.
func (c *Client) WaitUntil(ctx context.Context, execID string, cond func(*State) bool) (*State, error) {
	if cond == nil {
		return nil, fmt.Errorf("%w: nil condition", ErrInvalidArgument)
	}

	st, err := c.wait(ctx, execID, cond)
	if err != nil {
		return nil, err
	}

	if !cond(st) {
		return st, fmt.Errorf("%w: %s ended %s", ErrTerminal, execID, st.Status)
	}

	return st, nil
}

// wait returns the state of execID once it is final or cond (when not nil)
// holds on it, retrying failed reads with backoff.
func (c *Client) wait(ctx context.Context, execID string, cond func(*State) bool) (*State, error) {
	if err := checkExecID(execID); err != nil {
		return nil, err
	}

	delay := waitRetry

	for {
		st, err := c.waitStep(ctx, execID, cond)
		if st != nil {
			return st, nil
		}

		if err == nil {
			delay = waitRetry

			continue
		}

		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		if c.in == nil {
			return nil, err // the client never attached: retrying cannot help
		}

		c.in.Logger.Debug("packtrail: wait, will retry", "exec", execID, "in", delay, "err", err)

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}

		delay = min(delay*2, maxWaitRetry) //nolint:mnd // exponential backoff.
	}
}

// waitStep reads the execution and, unless it is already done, follows its
// log. It returns the state once done, nothing when the caller should look
// again, or an error.
func (c *Client) waitStep(ctx context.Context, execID string, cond func(*State) bool) (*State, error) {
	done := func(st *State) bool { return st.Status.Terminal() || (cond != nil && cond(st)) }

	st, err := c.Get(ctx, execID)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return nil, err
	}

	if st != nil && done(st) {
		return st, nil
	}

	from := uint64(0)
	if st != nil {
		from = st.LastSeq + 1
	}

	return c.follow(ctx, execID, from, cond != nil, done)
}

// follow watches the event headers of execID from sequence from and reads
// the state again after a terminal event or, when everyStep is set, whenever
// the watch has caught up with the log. It returns the first state done
// accepts, nothing when the caller should look again, or an error.
func (c *Client) follow(ctx context.Context, execID string, from uint64, everyStep bool,
	done func(*State) bool,
) (*State, error) {
	cfg := jetstream.OrderedConsumerConfig{
		FilterSubjects: []string{c.in.EventSubject(execID)}, HeadersOnly: true,
	}
	if from > 0 {
		cfg.DeliverPolicy = jetstream.DeliverByStartSequencePolicy
		cfg.OptStartSeq = from
	}

	cons, err := c.orderedConsumer(ctx, cfg)
	if err != nil {
		return nil, err
	}

	it, err := cons.Messages()
	if err != nil {
		return nil, err
	}

	defer it.Stop()

	stop := context.AfterFunc(ctx, it.Stop)
	defer stop()

	for {
		msg, lerr := it.Next()
		if lerr != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}

			time.Sleep(waitRetry)

			return nil, nil
		}

		seq, check := checkpoint(msg, everyStep)
		if !check {
			continue
		}

		st, gerr := c.Get(ctx, execID)
		if gerr != nil {
			return nil, gerr
		}

		if done(st) {
			return st, nil
		}

		if st.LastSeq < seq {
			// The read lags behind the watch: look again from the top.
			time.Sleep(waitRetry)

			return nil, nil
		}
	}
}

// checkpoint reports whether the state is worth reading after msg: it ends
// the execution or, with everyStep, the watch has caught up with the log. seq
// is msg's stream sequence (0 when unknown).
func checkpoint(msg jetstream.Msg, everyStep bool) (seq uint64, check bool) {
	md, err := msg.Metadata()
	if err == nil {
		seq = md.Sequence.Stream
	}

	if slices.ContainsFunc(event.Types(msg.Headers()), event.Type.Terminal) {
		return seq, true
	}

	return seq, everyStep && (err != nil || md.NumPending == 0)
}

const (
	waitRetry    = 50 * time.Millisecond
	maxWaitRetry = 2 * time.Second
)

// ---------------------------------------------------------------------------
// Cron schedules

// ScheduleOption configures Schedule.
type ScheduleOption func(*scheduleOpts)

type scheduleOpts struct{ tz, version string }

// ScheduleTimeZone evaluates the cron expression in an IANA time zone.
func ScheduleTimeZone(tz string) ScheduleOption { return func(o *scheduleOpts) { o.tz = tz } }

// ScheduleVersion pins the flow version started by the schedule.
func ScheduleVersion(hash string) ScheduleOption { return func(o *scheduleOpts) { o.version = hash } }

// Schedule installs (or replaces) a cron schedule that starts flowName with
// input on every firing. It is a JetStream message schedule: no process keeps
// time, any engine turns the firing into an execution "<name>-<seq>".
func (c *Client) Schedule(ctx context.Context, name, flowName, cron string, input any,
	opts ...ScheduleOption,
) error {
	if err := checkSchedule(name, flowName, cron); err != nil {
		return err
	}

	if err := c.attach(ctx); err != nil {
		return err
	}

	var o scheduleOpts
	for _, opt := range opts {
		opt(&o)
	}

	b, err := marshalObject(input)
	if err != nil {
		return err
	}

	body, err := json.Marshal(engine.ScheduleSpec{Flow: flowName, Version: o.version, Input: b})
	if err != nil {
		return err
	}

	m := nats.NewMsg(c.in.Names.ScheduleSubject(name))
	m.Header.Set(sched.HeaderSchedule, cron)
	m.Header.Set(sched.HeaderScheduleTarget, c.in.Names.CronSubject(name))

	if o.tz != "" {
		m.Header.Set(sched.HeaderScheduleTZ, o.tz)
	}

	m.Data = body

	if _, err = c.in.JS.PublishMsg(ctx, m); err != nil {
		return fmt.Errorf("packtrail: schedule %s: %w", name, err)
	}

	return nil
}

// Unschedule removes a cron schedule.
func (c *Client) Unschedule(ctx context.Context, name string) error {
	if err := names.CheckToken("schedule name", name); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidArgument, err)
	}

	if err := c.attach(ctx); err != nil {
		return err
	}

	s, err := c.in.JS.Stream(ctx, c.in.Names.StreamCmd)
	if err != nil {
		return err
	}

	return s.Purge(ctx, jetstream.WithPurgeSubject(c.in.Names.ScheduleSubject(name)))
}

// ScheduleInfo describes an installed schedule.
type ScheduleInfo struct {
	Name     string          `json:"name"`
	Cron     string          `json:"cron"`
	TimeZone string          `json:"time_zone,omitempty"`
	Flow     string          `json:"flow"`
	Version  string          `json:"version,omitempty"`
	Input    json.RawMessage `json:"input,omitempty"`
}

// Schedules lists the installed cron schedules.
func (c *Client) Schedules(ctx context.Context) ([]ScheduleInfo, error) {
	if err := c.attach(ctx); err != nil {
		return nil, err
	}

	s, err := c.in.JS.Stream(ctx, c.in.Names.StreamCmd)
	if err != nil {
		return nil, err
	}

	info, err := s.Info(ctx, jetstream.WithSubjectFilter(c.in.Names.ScheduleSubject("*")))
	if err != nil {
		return nil, err
	}

	var out []ScheduleInfo

	for subj := range info.State.Subjects {
		m, lerr := s.GetLastMsgForSubject(ctx, subj)
		if lerr != nil {
			continue
		}

		var spec engine.ScheduleSpec
		if lerr = json.Unmarshal(m.Data, &spec); lerr != nil {
			continue
		}

		out = append(out, ScheduleInfo{
			Name: subj[strings.LastIndexByte(subj, '.')+1:], Cron: m.Header.Get(sched.HeaderSchedule),
			TimeZone: m.Header.Get(sched.HeaderScheduleTZ), Flow: spec.Flow, Version: spec.Version, Input: spec.Input,
		})
	}

	slices.SortFunc(out, func(a, b ScheduleInfo) int { return strings.Compare(a.Name, b.Name) })

	return out, nil
}

// ---------------------------------------------------------------------------
// Dead letters

// DeadLetter is a message packtrail gave up on: an invalid command, a job
// whose deliveries were exhausted, an undeliverable trigger.
type DeadLetter = wire.DeadLetter

// DeadLetters returns the most recent dead letters, newest first.
func (c *Client) DeadLetters(ctx context.Context, limit int) ([]DeadLetter, error) {
	if err := c.attach(ctx); err != nil {
		return nil, err
	}

	s, err := c.in.JS.Stream(ctx, c.in.Names.StreamDLQ)
	if err != nil {
		return nil, err
	}

	info, err := s.Info(ctx)
	if err != nil {
		return nil, err
	}

	var out []DeadLetter

	for seq := info.State.LastSeq; seq >= info.State.FirstSeq && seq > 0; seq-- {
		if limit > 0 && len(out) >= limit {
			break
		}

		m, lerr := s.GetMsg(ctx, seq)
		if lerr != nil {
			continue
		}

		var d DeadLetter
		if json.Unmarshal(m.Data, &d) == nil {
			d.Seq = seq
			out = append(out, d)
		}
	}

	return out, nil
}

// Redrive re-publishes the dead letter at seq to its original subject (with
// a fresh deduplication id) and removes it from the dead-letter stream.
func (c *Client) Redrive(ctx context.Context, seq uint64) error {
	if err := c.attach(ctx); err != nil {
		return err
	}

	s, err := c.in.JS.Stream(ctx, c.in.Names.StreamDLQ)
	if err != nil {
		return err
	}

	m, err := s.GetMsg(ctx, seq)
	if err != nil {
		return fmt.Errorf("%w: dead letter %d: %w", ErrNotFound, seq, err)
	}

	var d DeadLetter
	if err = json.Unmarshal(m.Data, &d); err != nil {
		return err
	}

	msg := nats.NewMsg(d.Subject)
	msg.Data = d.Body

	for k, v := range d.Header {
		if k != wire.HeaderMsgID && !strings.HasPrefix(k, "Nats-") {
			msg.Header[k] = v
		}
	}

	// A redriven command keeps its command id (the fold stays idempotent).
	if _, err = c.in.JS.PublishMsg(ctx, msg); err != nil {
		return err
	}

	return s.DeleteMsg(ctx, seq)
}

// ---------------------------------------------------------------------------
// Long-term store

// Store is a namespaced key-value store for application data that outlives
// executions (LangGraph Store). Namespaces and keys are tokens. Worker jobs
// reach the same store through worker.Job.Store.
type Store struct{ c *Client }

// Store returns the long-term store.
func (c *Client) Store() *Store { return &Store{c: c} }

// Put stores value (any JSON-encodable value) under ns/key.
func (s *Store) Put(ctx context.Context, ns, key string, value any) error {
	if err := s.c.attach(ctx); err != nil {
		return err
	}

	return store.Put(ctx, s.c.in, ns, key, value)
}

// Get returns the value under ns/key, ErrNotFound when there is none.
func (s *Store) Get(ctx context.Context, ns, key string) (json.RawMessage, error) {
	if err := s.c.attach(ctx); err != nil {
		return nil, err
	}

	return store.Get(ctx, s.c.in, ns, key)
}

// Delete removes ns/key.
func (s *Store) Delete(ctx context.Context, ns, key string) error {
	if err := s.c.attach(ctx); err != nil {
		return err
	}

	return store.Delete(ctx, s.c.in, ns, key)
}

// Keys lists the keys of a namespace, sorted.
func (s *Store) Keys(ctx context.Context, ns string) ([]string, error) {
	if err := s.c.attach(ctx); err != nil {
		return nil, err
	}

	return store.Keys(ctx, s.c.in, ns)
}
