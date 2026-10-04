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
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"

	"github.com/henomis/packtrail/event"
	"github.com/henomis/packtrail/flow"
	"github.com/henomis/packtrail/internal/cmd"
	"github.com/henomis/packtrail/internal/eventlog"
	"github.com/henomis/packtrail/internal/fold"
	"github.com/henomis/packtrail/internal/infra"
	"github.com/henomis/packtrail/internal/loader"
	"github.com/henomis/packtrail/internal/names"
	"github.com/henomis/packtrail/internal/projection"
	"github.com/henomis/packtrail/internal/registry"
	"github.com/henomis/packtrail/internal/snapshot"
	"github.com/henomis/packtrail/internal/wire"
)

// State is the folded state of an execution.
type State = fold.State

// Status is an execution status.
type Status = fold.Status

// Execution statuses.
const (
	StatusRunning   = fold.StatusRunning
	StatusWaiting   = fold.StatusWaiting
	StatusCompleted = fold.StatusCompleted
	StatusFailed    = fold.StatusFailed
	StatusCancelled = fold.StatusCancelled
)

// Summary is the indexed view of an execution (List).
type Summary = projection.Summary

// FlowVersion describes a registered flow version.
type FlowVersion = registry.Version

// Client starts, drives and inspects executions. It is safe for concurrent
// use and needs only a NATS connection: it can live in any process.
type Client struct {
	nc       *nats.Conn
	ns       string
	timeouts timeouts

	// attachMu guards attaching: in and ld are set by the first attach that
	// succeeds, and never change after.
	attachMu sync.Mutex
	in       *infra.Infra
	ld       *loader.Loader
}

// ClientOption configures a Client.
type ClientOption func(*Client) error

// WithClientNamespace sets the namespace (default "packtrail").
func WithClientNamespace(ns string) ClientOption {
	return func(c *Client) error {
		if err := ValidateNamespace(ns); err != nil {
			return err
		}

		c.ns = ns

		return nil
	}
}

// WithClientReadTimeout bounds one batched read of an event log (default 5s).
func WithClientReadTimeout(d time.Duration) ClientOption {
	return func(c *Client) error {
		if d <= 0 {
			return fmt.Errorf("%w: read timeout must be positive", ErrInvalidArgument)
		}

		c.timeouts.read = d

		return nil
	}
}

// WithClientBlobTimeout bounds one claim-check transfer when the caller's
// context has no deadline (default 2m).
func WithClientBlobTimeout(d time.Duration) ClientOption {
	return func(c *Client) error {
		if d <= 0 {
			return fmt.Errorf("%w: blob timeout must be positive", ErrInvalidArgument)
		}

		c.timeouts.blob = d

		return nil
	}
}

// NewClient returns a client. It performs no I/O; the namespace is attached
// on first use.
func NewClient(nc *nats.Conn, opts ...ClientOption) (*Client, error) {
	if nc == nil {
		return nil, fmt.Errorf("%w: nil connection", ErrInvalidArgument)
	}

	c := &Client{nc: nc, ns: names.Default}
	for _, o := range opts {
		if err := o(c); err != nil {
			return nil, err
		}
	}

	return c, nil
}

func newClientFromInfra(in *infra.Infra, ld *loader.Loader) *Client {
	return &Client{nc: in.NC, ns: in.Names.Prefix, in: in, ld: ld}
}

// attach binds the client to its namespace on first use. A failed attempt
// (namespace not provisioned yet, ctx ended, NATS unreachable) is not
// remembered: the next call tries again.
func (c *Client) attach(ctx context.Context) error {
	c.attachMu.Lock()
	defer c.attachMu.Unlock()

	if c.in != nil {
		return nil
	}

	in, err := infra.New(c.nc, names.New(c.ns), slog.Default())
	if err != nil {
		return err
	}

	if err = in.Attach(ctx); err != nil {
		return err
	}

	c.timeouts.apply(in)
	c.ld = loader.New(in, eventlog.New(in), snapshot.New(in, -1), registry.New(in))
	c.in = in

	return nil
}

// attached reports whether attach has succeeded. It takes attachMu, since a
// concurrent call may be attaching.
func (c *Client) attached() bool {
	c.attachMu.Lock()
	defer c.attachMu.Unlock()

	return c.in != nil
}

func checkExecID(id string) error {
	if err := names.CheckToken("execution id", id); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidArgument, err)
	}

	return nil
}

// ---------------------------------------------------------------------------
// Start

// StartOption configures Start.
type StartOption func(*startOpts)

type startOpts struct {
	id, version, trace string
}

// WithExecutionID sets the execution id: Start becomes idempotent on it (a
// second Start with the same id returns it without starting anything, I-03).
func WithExecutionID(id string) StartOption { return func(o *startOpts) { o.id = id } }

// TriggerExecID returns the id of the execution a message trigger of flow
// starts for a message with Nats-Msg-Id msgID: "<flow>-t<digest>", unique per
// flow and message. Use it to follow (Wait, Get) what a published message
// started. A message without a Msg-Id gets an id derived from its stream and
// sequence (stream trigger) or a fresh one (core trigger).
func TriggerExecID(flow, msgID string) string { return names.TriggerMsgExecID(flow, msgID) }

// WithVersion pins a flow version hash instead of the latest.
func WithVersion(hash string) StartOption { return func(o *startOpts) { o.version = hash } }

// WithTraceparent propagates a W3C traceparent to every event, job and
// command derived from this one.
func WithTraceparent(tp string) StartOption { return func(o *startOpts) { o.trace = tp } }

// Start starts an execution of flowName with input (a JSON object, a struct
// or nil) and returns its id once the execution exists: Signal, Cancel,
// Resume, Update and Get can address it right away. It needs a running
// engine, and fails with ErrInvalidArgument when the engine refuses the
// input. The execution itself runs asynchronously; use Wait, WaitUntil or
// Watch to follow it.
//
// An error with an empty id means nothing was requested. An error with an id
// (ctx ended before the engine answered) means the outcome is unknown: the
// execution may still start, and retrying with WithExecutionID(id) is safe. A
// reply lost to a reconnect does not hang Start: it returns as soon as the
// execution exists.
func (c *Client) Start(ctx context.Context, flowName string, input any, opts ...StartOption) (string, error) {
	if err := c.attach(ctx); err != nil {
		return "", err
	}

	var o startOpts
	for _, opt := range opts {
		opt(&o)
	}

	if o.id == "" {
		o.id = nuid.Next()
	}

	if err := checkExecID(o.id); err != nil {
		return "", err
	}

	if err := names.CheckToken("flow name", flowName); err != nil {
		return "", fmt.Errorf("%w: %w", ErrInvalidArgument, err)
	}

	b, err := marshalObject(input)
	if err != nil {
		return "", err
	}

	if _, err = c.ld.Flows.Get(ctx, flowName, o.version); err != nil {
		if errors.Is(err, registry.ErrUnknownFlow) {
			return "", fmt.Errorf("%w: %s", ErrUnknownFlow, flowName)
		}

		return "", err
	}

	start, err := cmd.New("start."+o.id, cmd.Start, o.id, cmd.StartData{Flow: flowName, Version: o.version, Input: b})
	if err != nil {
		return "", err
	}

	if _, published, rerr := c.request(ctx, start, o.trace, true); rerr != nil {
		if published {
			return o.id, rerr
		}

		return "", rerr
	}

	return o.id, nil
}

// exists checks that execID can still receive commands.
func (c *Client) exists(ctx context.Context, execID string) error {
	if err := checkExecID(execID); err != nil {
		return err
	}

	if err := c.attach(ctx); err != nil {
		return err
	}

	seq, err := c.ld.Log.LastSeq(ctx, execID)
	if err != nil {
		return err
	}

	if seq > 0 {
		return nil
	}

	archived, err := c.ld.IsArchived(ctx, execID)
	if err != nil {
		return err
	}

	if archived {
		return fmt.Errorf("%w: %s", ErrArchived, execID)
	}

	return fmt.Errorf("%w: %s", ErrNotFound, execID)
}

func (c *Client) send(ctx context.Context, id string, t cmd.Type, execID string, data any) error {
	if err := c.exists(ctx, execID); err != nil {
		return err
	}

	command, err := cmd.New(id, t, execID, data)
	if err != nil {
		return err
	}

	return wire.PublishCmd(ctx, c.in, command, "")
}

// SignalOption configures Signal.
type SignalOption func(*string)

// WithSignalID sets the signal id: delivering the same id twice is
// idempotent (client retries after an ambiguous publish).
func WithSignalID(id string) SignalOption { return func(s *string) { *s = id } }

// Signal delivers a named signal to an execution. A signal sent before the
// execution reaches the matching await is buffered, never lost (I-09); a
// signal to a missing execution is an error.
func (c *Client) Signal(ctx context.Context, execID, name string, payload any, opts ...SignalOption) error {
	if err := names.CheckToken("signal name", name); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidArgument, err)
	}

	b, err := marshalAny(payload)
	if err != nil {
		return err
	}

	id := "signal." + nuid.Next()
	for _, o := range opts {
		o(&id)
	}

	return c.send(ctx, id, cmd.Signal, execID, cmd.SignalData{Name: name, Payload: b})
}

// Resume re-runs an interrupted node, passing value to its worker as
// `resume` (human-in-the-loop, LangGraph interrupt()).
func (c *Client) Resume(ctx context.Context, execID, node string, value any) error {
	b, err := marshalAny(value)
	if err != nil {
		return err
	}

	return c.send(ctx, "resume."+nuid.Next(), cmd.Resume, execID, cmd.ResumeData{Node: node, Value: b})
}

// Cancel cancels an execution. Cancelling a finished execution is a no-op.
func (c *Client) Cancel(ctx context.Context, execID, reason string) error {
	return c.send(ctx, "cancel."+nuid.Next(), cmd.Cancel, execID, cmd.CancelData{Reason: reason})
}

// Register stores a flow definition and returns its version hash.
func (c *Client) Register(ctx context.Context, def *flow.Flow) (string, error) {
	if err := c.attach(ctx); err != nil {
		return "", err
	}

	if def == nil {
		return "", fmt.Errorf("%w: nil flow", ErrInvalidArgument)
	}

	cp, err := def.Clone()
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrInvalidArgument, err)
	}

	return c.ld.Flows.Register(ctx, cp)
}

// Flows lists the registered flow versions.
func (c *Client) Flows(ctx context.Context) ([]FlowVersion, error) {
	if err := c.attach(ctx); err != nil {
		return nil, err
	}

	return c.ld.Flows.List(ctx)
}

// Flow returns a registered definition ("" = latest version).
func (c *Client) Flow(ctx context.Context, name, version string) (*flow.Flow, error) {
	if err := c.attach(ctx); err != nil {
		return nil, err
	}

	def, err := c.ld.Flows.Get(ctx, name, version)
	if errors.Is(err, registry.ErrUnknownFlow) {
		return nil, fmt.Errorf("%w: %s", ErrUnknownFlow, name)
	}

	return def, err
}

// ---------------------------------------------------------------------------
// Reads

// Get returns the current state of an execution (archived executions are
// read from the archive, with Archived set).
func (c *Client) Get(ctx context.Context, execID string) (*State, error) {
	return c.StateAt(ctx, execID, 0)
}

// StateAt returns the state of an execution as it was right after the event
// at stream sequence seq (0 = now): time travel.
func (c *Client) StateAt(ctx context.Context, execID string, seq uint64) (*State, error) {
	if err := checkExecID(execID); err != nil {
		return nil, err
	}

	if err := c.attach(ctx); err != nil {
		return nil, err
	}

	st, _, err := c.ld.Load(ctx, execID, seq)
	if err != nil {
		return nil, err
	}

	if st.Exists() {
		return st, nil
	}

	st, _, err = c.ld.LoadArchived(ctx, execID, seq)
	if errors.Is(err, loader.ErrNotArchived) {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, execID)
	}

	if err != nil {
		return nil, err
	}

	st.Archived = true

	return st, nil
}

// History returns every event of an execution, in order, including the
// stretches archived when it was continued as new.
func (c *Client) History(ctx context.Context, execID string) ([]event.Event, error) {
	if err := checkExecID(execID); err != nil {
		return nil, err
	}

	if err := c.attach(ctx); err != nil {
		return nil, err
	}

	evs, err := c.ld.History(ctx, execID)
	if errors.Is(err, loader.ErrNotArchived) {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, execID)
	}

	return evs, err
}

// OutputHistory returns every output a node produced, oldest first: one per
// completed visit (results only keep the latest, I-14).
func (c *Client) OutputHistory(ctx context.Context, execID, node string) ([]json.RawMessage, error) {
	evs, err := c.History(ctx, execID)
	if err != nil {
		return nil, err
	}

	var out []json.RawMessage

	for _, ev := range evs {
		switch d := ev.Data.(type) {
		case *event.NodeDone:
			if d.Node == node {
				out = append(out, d.Output)
			}
		case *event.ChildDone:
			if d.Node == node {
				out = append(out, d.Output)
			}
		case *event.Join:
			if d.Node == node {
				out = append(out, d.Output)
			}
		}
	}

	return out, nil
}

// List returns indexed executions matching the filter, newest first.
func (c *Client) List(ctx context.Context, f ListFilter) ([]Summary, error) {
	if err := c.attach(ctx); err != nil {
		return nil, err
	}

	out, err := projection.List(ctx, c.in, projection.Filter{
		Status: f.Status, Flow: f.Flow, Attr: f.Attr, Value: f.Value, Limit: f.Limit,
	})
	if errors.Is(err, projection.ErrInvalidFilter) {
		return nil, fmt.Errorf("%w: %w", ErrInvalidArgument, err)
	}

	return out, err
}

// Quarantined lists the executions the dispatcher gave up on (an event it
// could not process; the dead letters say why). Their state is readable up to
// the bad event; continue one with Fork from an earlier sequence.
func (c *Client) Quarantined(ctx context.Context) ([]string, error) {
	if err := c.attach(ctx); err != nil {
		return nil, err
	}

	return projection.Quarantined(ctx, c.in)
}

// Unquarantine resumes a quarantined execution once the cause is fixed (for
// example after upgrading to a version that understands its events): the
// effects of every event from the failing one are replayed and the
// dispatchers stop skipping it. It is asynchronous; if replaying still fails
// the execution stays quarantined and the command is dead-lettered.
func (c *Client) Unquarantine(ctx context.Context, execID string) error {
	if err := checkExecID(execID); err != nil {
		return err
	}

	if err := c.attach(ctx); err != nil {
		return err
	}

	q, err := projection.IsQuarantined(ctx, c.in, execID)
	if err != nil {
		return err
	}

	if !q {
		return fmt.Errorf("%w: %s is not quarantined", ErrNotFound, execID)
	}

	command, err := cmd.New("redispatch."+nuid.Next(), cmd.Redispatch, execID, nil)
	if err != nil {
		return err
	}

	return wire.PublishCmd(ctx, c.in, command, "")
}

// ListFilter selects executions in List. Empty fields match everything.
type ListFilter struct {
	Status Status
	Flow   string
	// Attr / Value select by a declared search attribute.
	Attr  string
	Value string
	Limit int
}

// ---------------------------------------------------------------------------
// Fork and rerun

// ForkOption configures Fork and Rerun.
type ForkOption func(*forkOpts)

type forkOpts struct {
	id     string
	writes map[string]any
}

// WithForkID sets the id of the new execution.
func WithForkID(id string) ForkOption { return func(o *forkOpts) { o.id = id } }

// WithForkWrites edits the fork's channels (through their reducers) before it
// resumes: change the state at the fork point, then continue — LangGraph's
// update_state. The writes are checked against the flow before the fork is
// requested.
func WithForkWrites(writes map[string]any) ForkOption {
	return func(o *forkOpts) { o.writes = writes }
}

// Fork creates a new execution from the state of execID right after the
// event at sequence seq (rounded up to the end of that decision), and
// continues it from there: what was in flight is dispatched again. The source
// is untouched. Like Start, it returns once the new execution exists, and
// returns the new id with an error whose outcome is unknown (retry with
// WithForkID(id)).
func (c *Client) Fork(ctx context.Context, execID string, seq uint64, opts ...ForkOption) (string, error) {
	evs, err := c.History(ctx, execID)
	if err != nil {
		return "", err
	}

	cut := uint64(0)

	for _, ev := range evs {
		if ev.Seq >= seq && ev.DecisionEnd {
			cut = ev.Seq

			break
		}
	}

	if cut == 0 {
		return "", fmt.Errorf("%w: sequence %d is not in the history of %s", ErrInvalidArgument, seq, execID)
	}

	return c.fork(ctx, execID, cut, opts)
}

func (c *Client) fork(ctx context.Context, execID string, seq uint64, opts []ForkOption) (string, error) {
	o := forkOpts{id: nuid.Next()}
	for _, opt := range opts {
		opt(&o)
	}

	if err := checkExecID(o.id); err != nil {
		return "", err
	}

	writes, err := encodeWrites(o.writes)
	if err != nil {
		return "", err
	}

	if len(writes) > 0 {
		if err = c.checkForkWrites(ctx, execID, seq, writes); err != nil {
			return "", err
		}
	}

	f, err := cmd.New("fork."+o.id, cmd.Fork, o.id, cmd.ForkData{From: execID, Seq: seq, Writes: writes})
	if err != nil {
		return "", err
	}

	if _, published, rerr := c.request(ctx, f, "", true); rerr != nil {
		if published {
			return o.id, rerr
		}

		return "", rerr
	}

	return o.id, nil
}

// checkForkWrites validates fork edits against the source's flow and state at
// seq, so a bad edit fails here instead of dead-lettering the fork.
func (c *Client) checkForkWrites(ctx context.Context, execID string, seq uint64,
	writes map[string]json.RawMessage,
) error {
	st, def, err := c.ld.Load(ctx, execID, seq)
	if err == nil && !st.Exists() {
		st, def, err = c.ld.LoadArchived(ctx, execID, seq)
	}

	if err != nil {
		return err
	}

	if st.Status.Terminal() {
		return fmt.Errorf("%w: cannot edit a fork of a finished execution (fork before its end)", ErrInvalidArgument)
	}

	if err = fold.CheckWrites(def, st, writes); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidArgument, err)
	}

	return nil
}

// encodeWrites marshals channel writes; nil means none.
func encodeWrites(writes map[string]any) (map[string]json.RawMessage, error) {
	if len(writes) == 0 {
		return nil, nil
	}

	out := make(map[string]json.RawMessage, len(writes))

	for k, v := range writes {
		b, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("%w: write %q: %w", ErrInvalidArgument, k, err)
		}

		out[k] = b
	}

	return out, nil
}

// UpdateOption configures Update.
type UpdateOption func(*string)

// WithUpdateID sets the update id: the same id is applied once, however often
// it is sent (retry an ambiguous update with the same id).
func WithUpdateID(id string) UpdateOption { return func(s *string) { *s = id } }

// Update writes channels of a running execution (through their reducers) and
// returns its state right after the write: a synchronous, validated change
// from outside the graph. Tasks scheduled from then on see the new values. It fails with ErrInvalidArgument
// when a channel is undeclared or a value does not fit its reducer, and with
// ErrTerminal when the execution already finished. If ctx ends first the
// update may still be applied: retry with the same WithUpdateID.
func (c *Client) Update(ctx context.Context, execID string, writes map[string]any,
	opts ...UpdateOption,
) (*State, error) {
	if err := c.exists(ctx, execID); err != nil {
		return nil, err
	}

	data, err := encodeWrites(writes)
	if err != nil {
		return nil, err
	}

	if len(data) == 0 {
		return nil, fmt.Errorf("%w: update without writes", ErrInvalidArgument)
	}

	id := "update." + nuid.Next()
	for _, o := range opts {
		o(&id)
	}

	command, err := cmd.New(id, cmd.Update, execID, cmd.UpdateData{Writes: data})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidArgument, err)
	}

	seq, _, err := c.request(ctx, command, "", false)
	if err != nil {
		return nil, err
	}

	return c.StateAt(ctx, execID, seq)
}

// Waiting for a reply: how often the wait wakes up, and how often it checks
// the log without a reconnect (a reply dropped on a live connection).
const (
	replyPoll  = 250 * time.Millisecond
	replyCheck = 5 * time.Second
)

// request publishes command and waits for the engine's answer: the sequence
// of the decision it produced (or of the last one, for a no-op). The reply
// subject travels in the command, so an engine that takes over a redelivered
// command answers too. published reports whether the command was stored, so
// an error may leave it to be decided later.
//
// A reply is core NATS: one sent while the client reconnects is lost, and the
// command, already acked, is never answered again. For a command that creates
// its execution (creates), the log answers too: once the execution has an
// event after a reconnect (or every replyCheck), the command was decided.
func (c *Client) request(ctx context.Context, command cmd.Command, traceparent string, creates bool) (
	seq uint64, published bool, err error,
) {
	inbox := c.nc.NewRespInbox()

	sub, err := c.nc.SubscribeSync(inbox)
	if err != nil {
		return 0, false, err
	}

	defer func() { _ = sub.Unsubscribe() }()

	command.Reply = inbox
	reconnects := c.nc.Stats().Reconnects
	checked := time.Now()

	if err = wire.PublishCmd(ctx, c.in, command, traceparent); err != nil {
		return 0, false, err
	}

	for {
		wctx, cancel := context.WithTimeout(ctx, replyPoll)
		m, nerr := sub.NextMsgWithContext(wctx)

		cancel()

		if nerr == nil {
			seq, err = decodeReply(command, m.Data)

			return seq, true, err
		}

		if ctx.Err() != nil {
			return 0, true, ctx.Err()
		}

		if !errors.Is(nerr, context.DeadlineExceeded) {
			return 0, true, nerr
		}

		if n := c.nc.Stats().Reconnects; creates && (n != reconnects || time.Since(checked) >= replyCheck) {
			reconnects, checked = n, time.Now()

			if seq = c.lastSeq(ctx, command.ExecID); seq > 0 {
				return seq, true, nil
			}
		}
	}
}

// lastSeq is the sequence of execID's last event, 0 if it has none or the
// log cannot be read now.
func (c *Client) lastSeq(ctx context.Context, execID string) uint64 {
	rctx, cancel := context.WithTimeout(ctx, c.in.ReadTimeout)
	defer cancel()

	seq, err := c.ld.Log.LastSeq(rctx, execID)
	if err != nil {
		return 0
	}

	return seq
}

func decodeReply(command cmd.Command, data []byte) (uint64, error) {
	var r wire.Reply
	if err := json.Unmarshal(data, &r); err != nil {
		return 0, fmt.Errorf("packtrail: %s reply: %w", command.Type, err)
	}

	if !r.OK {
		return 0, replyError(command.ExecID, r)
	}

	return r.Seq, nil
}

func replyError(execID string, r wire.Reply) error {
	switch r.Code {
	case wire.ReplyInvalid:
		return fmt.Errorf("%w: %s", ErrInvalidArgument, r.Error)
	case wire.ReplyRejected:
		return fmt.Errorf("%w: %s", ErrTerminal, execID)
	case wire.ReplyNotFound:
		return fmt.Errorf("%w: %s", ErrNotFound, execID)
	default:
		return fmt.Errorf("packtrail: %s: %s", execID, r.Error)
	}
}

// Rerun forks execID at the point where node was last entered, so the node
// (and everything after it) runs again in a new execution: a task or a map
// runs its work again, a subflow starts a new child, an await waits again.
//
// A fork starts at the end of a decision, so only a node still in flight at
// the end of the decision that entered it can be rerun. A node that settled
// in that same decision (a choice, a join, an await whose signal was already
// buffered, a node that failed on entry) fails with ErrInvalidArgument: rerun
// the node before it, or Fork at an earlier sequence.
func (c *Client) Rerun(ctx context.Context, execID, node string, opts ...ForkOption) (string, error) {
	evs, err := c.History(ctx, execID)
	if err != nil {
		return "", err
	}

	var at, cut uint64

	for _, ev := range evs {
		if d, ok := ev.Data.(*event.Entered); ok && d.Node == node {
			at = ev.Seq
		}
	}

	if at == 0 {
		return "", fmt.Errorf("%w: node %q never ran in %s", ErrInvalidArgument, node, execID)
	}

	for _, ev := range evs {
		if ev.Seq >= at && ev.DecisionEnd {
			cut = ev.Seq

			break
		}
	}

	st, err := c.StateAt(ctx, execID, cut)
	if err != nil {
		return "", err
	}

	if !st.Open(node) {
		return "", fmt.Errorf("%w: node %q of %s settled in the decision that entered it; "+
			"rerun the node before it", ErrInvalidArgument, node, execID)
	}

	return c.fork(ctx, execID, cut, opts)
}
