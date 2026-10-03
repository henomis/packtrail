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

// Package engine processes commands: for each partition of the command stream
// it loads the execution state (cache, or snapshot + tail), decides the events
// with the pure fold, appends them with optimistic concurrency and acks the
// command. A conflicting append means another engine wrote first: the state
// is reloaded and the command decided again. Invalid commands are
// dead-lettered instead of redelivered forever.
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail/event"
	"github.com/henomis/packtrail/flow"
	"github.com/henomis/packtrail/internal/blob"
	"github.com/henomis/packtrail/internal/cmd"
	"github.com/henomis/packtrail/internal/consume"
	"github.com/henomis/packtrail/internal/eventlog"
	"github.com/henomis/packtrail/internal/fold"
	"github.com/henomis/packtrail/internal/infra"
	"github.com/henomis/packtrail/internal/loader"
	"github.com/henomis/packtrail/internal/metrics"
	"github.com/henomis/packtrail/internal/names"
	"github.com/henomis/packtrail/internal/projection"
	"github.com/henomis/packtrail/internal/registry"
	"github.com/henomis/packtrail/internal/statecache"
	"github.com/henomis/packtrail/internal/wire"
)

const (
	maxConflicts = 8
	nakDelay     = time.Second
	maxNakDelay  = 30 * time.Second
	// maxDeliver bounds redelivery of a command that keeps failing on
	// transient errors: with the backoff it is about 30 minutes, after which
	// the command is dead-lettered (I-11) and can be redriven.
	maxDeliver = 64
)

// Engine processes commands.
type Engine struct {
	In      *infra.Infra
	Loader  *loader.Loader
	Cache   *statecache.Cache
	Metrics *metrics.M
	Drain   time.Duration
	// Now is the clock (tests inject one); time enters the fold as data.
	Now func() time.Time
	// MaxEvents is the largest decision that can be appended (0 =
	// eventlog.MaxEvents).
	MaxEvents int
	// HistoryLimit continues an execution as new once its live log holds
	// that many events (0: never).
	HistoryLimit int
	// Redispatch lifts the quarantine of an execution (wired to the
	// dispatcher's replay).
	Redispatch func(ctx context.Context, execID string) error
}

func (e *Engine) maxEvents() int {
	if e.MaxEvents > 0 {
		return e.MaxEvents
	}

	return eventlog.MaxEvents
}

func (e *Engine) now() time.Time {
	if e.Now != nil {
		return e.Now()
	}

	return time.Now()
}

// RunPartition processes the commands of partition p until ctx is done.
func (e *Engine) RunPartition(ctx context.Context, p int, ready func()) error {
	return consume.Run(ctx, e.In.JS, consume.Config{
		Stream: e.In.Names.StreamCmd,
		Consumer: jetstream.ConsumerConfig{
			Durable: e.In.Names.DurEngine(p), FilterSubject: e.In.Names.CmdPartitionFilter(p),
			AckPolicy: jetstream.AckExplicitPolicy, MaxDeliver: maxDeliver + 1, AckWait: e.In.AckWait,
		},
		Group:      "engine",
		PullExpiry: e.In.PullExpiry,
		Drain:      e.Drain,
		Logger:     e.In.Logger,
		Pulling:    ready,
		Handler:    e.handle,
	})
}

func (e *Engine) handle(ctx context.Context, msg jetstream.Msg) {
	start := time.Now()
	defer func() { e.Metrics.LatencyNanos.Add(int64(time.Since(start))) }()

	e.Metrics.Commands.Add(1)

	if v := msg.Headers().Get(wire.HeaderProtocol); v != "" && v != infra.ProtocolVersion {
		e.deadLetter(ctx, msg, "", "unsupported protocol version "+v)

		return
	}

	// A body that cannot be read yet (object-store blip, lagging replica) is
	// retried; only a body that cannot be decoded is poison (F4-01).
	body, err := blob.Resolve(ctx, e.In, msg.Data(), msg.Headers())
	if err != nil {
		e.ackOrRetry(ctx, msg, cmd.Command{}, fmt.Errorf("command body: %w", err))

		return
	}

	c, err := cmd.Decode(body)
	if err != nil {
		e.deadLetter(ctx, msg, "", fmt.Sprintf("undecodable command: %v", err))

		return
	}

	switch c.Type { //nolint:exhaustive // operational commands that bypass the fold.
	case cmd.Archive:
		e.ackOrRetry(ctx, msg, c, e.Archive(ctx, c.ExecID))

		return
	case cmd.Redispatch:
		var rerr error
		if e.Redispatch != nil {
			rerr = e.Redispatch(ctx, c.ExecID)
		}

		e.ackOrRetry(ctx, msg, c, rerr)

		return
	}

	seq, err := e.apply(ctx, c, msg.Headers().Get(wire.HeaderTraceparent))
	if e.answer(c, seq, err) {
		_ = msg.Ack() // a rejection answered to the caller is not poison

		return
	}

	e.ackOrRetry(ctx, msg, c, err)
}

// answer replies to a command that asked for it (c.Reply): on success, and on
// a rejection, which then ends the command (it reports true). Other errors are
// retried as usual and answered when they end.
func (e *Engine) answer(c cmd.Command, seq uint64, err error) bool {
	if c.Reply == "" {
		return false
	}

	switch {
	case err == nil:
		e.reply(c, wire.Reply{OK: true, Seq: seq})

		return false
	case errors.Is(err, fold.ErrInvalid):
		e.reply(c, wire.Reply{Code: wire.ReplyInvalid, Error: err.Error()})

		return true
	case errors.Is(err, fold.ErrRejected):
		e.reply(c, wire.Reply{Code: wire.ReplyRejected, Error: err.Error()})

		return true
	default:
		return false
	}
}

// reply publishes r to c.Reply (core NATS, best effort: the caller may be
// gone).
func (e *Engine) reply(c cmd.Command, r wire.Reply) {
	if c.Reply == "" {
		return
	}

	b, err := json.Marshal(r)
	if err == nil {
		err = e.In.NC.Publish(c.Reply, b)
	}

	if err != nil {
		e.In.Logger.Warn("packtrail: reply not sent", "exec", c.ExecID, "cmd", c.ID, "err", err)
	}
}

// isNotFound reports a "not found" that a lagging replica can produce for
// something written moments ago: a flow version, a blob, an execution's log.
func isNotFound(err error) bool {
	return errors.Is(err, registry.ErrUnknownFlow) || errors.Is(err, jetstream.ErrObjectNotFound) ||
		errors.Is(err, fold.ErrNotFound)
}

// retryDelay backs off 1s, 2s, 4s … 30s with the delivery count.
func retryDelay(delivered uint64) time.Duration {
	shift := min(max(delivered, 1)-1, 5) //nolint:mnd // up to 32s, capped below.

	return min(nakDelay<<shift, maxNakDelay)
}

func (e *Engine) ackOrRetry(ctx context.Context, msg jetstream.Msg, c cmd.Command, err error) {
	n := delivered(msg)

	switch {
	case err == nil:
		_ = msg.Ack()
	case errors.Is(err, fold.ErrRejected):
		// Refused in the execution's current state: final, not poison.
		e.In.Logger.Info("packtrail: command rejected", "exec", c.ExecID, "type", c.Type, "err", err)

		_ = msg.Ack()
	case isNotFound(err) && n < notFoundGrace:
		// A replica may serve a read before it caught up with what was
		// written moments ago: give it a few deliveries.
		_ = msg.NakWithDelay(retryDelay(n))
	case errors.Is(err, fold.ErrInvalid) || errors.Is(err, registry.ErrUnknownFlow) ||
		errors.Is(err, jetstream.ErrObjectNotFound):
		e.reply(c, wire.Reply{Code: wire.ReplyFailed, Error: err.Error()})
		e.deadLetter(ctx, msg, c.ExecID, err.Error())
	case errors.Is(err, fold.ErrNotFound):
		// A command for an archived execution is stale, not poison (I-18).
		e.reply(c, wire.Reply{Code: wire.ReplyNotFound, Error: err.Error()})

		if archived, aerr := e.Loader.IsArchived(ctx, c.ExecID); aerr == nil && archived {
			e.In.Logger.Info("packtrail: command for archived execution ignored", "exec", c.ExecID, "type", c.Type)

			_ = msg.Ack()

			return
		}

		e.deadLetter(ctx, msg, c.ExecID, err.Error())
	default:
		// Transient: back off; give up only after a long outage (~30 min).
		if n >= maxDeliver {
			e.reply(c, wire.Reply{Code: wire.ReplyFailed, Error: "delivery attempts exhausted: " + err.Error()})
			e.deadLetter(ctx, msg, c.ExecID, "delivery attempts exhausted: "+err.Error())

			return
		}

		e.In.Logger.Warn("packtrail: command failed, will retry", "exec", c.ExecID, "type", c.Type,
			"deliveries", n, "err", err)

		_ = msg.NakWithDelay(retryDelay(n))
	}
}

// notFoundGrace is how many deliveries an unknown flow gets before the
// command is dead-lettered.
const notFoundGrace = 3

func delivered(msg jetstream.Msg) uint64 {
	md, err := msg.Metadata()
	if err != nil {
		return 0
	}

	return md.NumDelivered
}

func (e *Engine) deadLetter(ctx context.Context, msg jetstream.Msg, execID, reason string) {
	md, _ := msg.Metadata()

	var deliveries, seq uint64
	if md != nil {
		deliveries, seq = md.NumDelivered, md.Sequence.Stream
	}

	d := wire.DeadLetter{
		Kind: wire.DLQCommand, Key: execID, Reason: reason, Deliveries: deliveries, Subject: msg.Subject(),
		Header: msg.Headers(), Body: msg.Data(),
	}

	if err := wire.PublishDLQ(ctx, e.In, d, "cmd."+strconv.FormatUint(seq, 10)); err != nil {
		e.In.Logger.Error("packtrail: dead letter failed", "err", err)

		_ = msg.NakWithDelay(nakDelay)

		return
	}

	e.Metrics.DeadLetters.Add(1)
	e.In.Logger.Warn("packtrail: command dead-lettered", "exec", execID, "reason", reason)

	if err := msg.Term(); err != nil {
		e.In.Logger.Warn("packtrail: term failed", "err", err)
	}
}

// apply decides and appends, retrying on concurrent appends.
// apply decides and appends, retrying on concurrent appends. It returns the
// sequence of the stored decision (for a no-op, of the last one).
func (e *Engine) apply(ctx context.Context, c cmd.Command, trace string) (uint64, error) {
	for range maxConflicts {
		ent, expected, evs, err := e.decide(ctx, c)
		if err != nil {
			e.Cache.Drop(c.ExecID)

			return 0, err
		}

		if len(evs) == 0 {
			return expected, nil
		}

		if ent, evs, err = e.fitDecision(ctx, c, ent, evs, trace); err != nil {
			return 0, err
		}

		before := ent.State.Events - len(evs)

		last, err := e.Loader.Log.Append(ctx, c.ExecID, expected, evs)
		if errors.Is(err, eventlog.ErrConflict) {
			e.Metrics.Conflicts.Add(1)
			e.Cache.Drop(c.ExecID)

			continue
		}

		if err != nil {
			e.Cache.Drop(c.ExecID)

			return 0, err
		}

		e.Metrics.EventsAppended.Add(int64(len(evs)))

		ent.State.LastSeq = last
		e.Cache.Put(c.ExecID, ent)

		e.afterAppend(ctx, c.ExecID, ent, before)

		return last, nil
	}

	return 0, fmt.Errorf("engine: %s: too many concurrent appends", c.ExecID)
}

// fitDecision stamps the trace on the decided events and replaces a decision
// too large to append by the failure of the execution.
func (e *Engine) fitDecision(ctx context.Context, c cmd.Command, ent statecache.Entry, evs []event.Event,
	trace string,
) (statecache.Entry, []event.Event, error) {
	if len(evs) > e.maxEvents() {
		var err error
		if ent, evs, err = e.abortTooLarge(ctx, c, evs); err != nil {
			return ent, nil, err
		}
	}

	for i := range evs {
		evs[i].Trace = trace
	}

	return ent, evs, nil
}

// abortTooLarge replaces a decision over the events limit by the
// failure of the execution: a clear terminal state instead of a command that is
// retried, dead-lettered and leaves the execution running forever (F-04).
func (e *Engine) abortTooLarge(ctx context.Context, c cmd.Command,
	evs []event.Event,
) (statecache.Entry, []event.Event, error) {
	n := len(evs)

	e.Cache.Drop(c.ExecID)

	ent, err := e.load(ctx, c.ExecID)
	if err != nil {
		return ent, nil, err
	}

	var first *event.Event

	if !ent.State.Exists() {
		first = &evs[0]

		if ent.Def == nil {
			if ent.Def, err = e.Loader.DefOf(ctx, evs[0]); err != nil {
				return ent, nil, err
			}
		}
	}

	msg := fmt.Sprintf("command %s (%s) produced %d events, more than the %d a decision may append",
		c.ID, c.Type, n, e.maxEvents())

	out, err := fold.Abort(ent.Def, ent.State, first, c.ID, event.ReasonDecisionTooLarge, msg, e.now())
	if err != nil {
		return ent, nil, err
	}

	e.In.Logger.Error("packtrail: decision too large, execution failed", "exec", c.ExecID, "events", n)

	return ent, out, nil
}

// decide returns the state entry (mutated by the decision), the expected
// sequence for the append, and the events.
func (e *Engine) decide(ctx context.Context, c cmd.Command) (statecache.Entry, uint64, []event.Event, error) {
	ent, err := e.load(ctx, c.ExecID)
	if err != nil {
		return ent, 0, nil, err
	}

	expected := ent.State.LastSeq
	now := e.now()

	switch c.Type { //nolint:exhaustive // start and fork need a definition lookup; the rest go to the fold.
	case cmd.Start:
		evs, def, lerr := e.decideStart(ctx, ent.State, c, now)
		ent.Def = def

		return ent, expected, evs, lerr
	case cmd.Fork:
		evs, def, lerr := e.decideFork(ctx, ent.State, c, now)
		ent.Def = def

		return ent, expected, evs, lerr
	default:
		evs, lerr := fold.Decide(ent.Def, ent.State, c, now)

		return ent, expected, evs, lerr
	}
}

func (e *Engine) load(ctx context.Context, execID string) (statecache.Entry, error) {
	if ent, ok := e.Cache.Get(execID); ok {
		return ent, nil
	}

	st, def, err := e.Loader.Load(ctx, execID, 0)
	if err != nil {
		return statecache.Entry{}, err
	}

	return statecache.Entry{State: st, Def: def}, nil
}

func (e *Engine) decideStart(ctx context.Context, st *fold.State, c cmd.Command,
	now time.Time,
) ([]event.Event, *flow.Flow, error) {
	if st.Exists() {
		return nil, nil, nil
	}

	// A start for an archived id is a duplicate, not a new execution.
	if archived, err := e.Loader.IsArchived(ctx, c.ExecID); err != nil || archived {
		return nil, nil, err
	}

	var p cmd.StartData
	if err := c.Payload(&p); err != nil {
		return nil, nil, fmt.Errorf("%w: %w", fold.ErrInvalid, err)
	}

	def, err := e.Loader.Flows.Get(ctx, p.Flow, p.Version)
	if err != nil {
		return nil, nil, err
	}

	evs, err := fold.Decide(def, st, c, now)

	return evs, def, err
}

func (e *Engine) decideFork(ctx context.Context, st *fold.State, c cmd.Command,
	now time.Time,
) ([]event.Event, *flow.Flow, error) {
	if st.Exists() {
		return nil, nil, nil
	}

	var p cmd.ForkData
	if err := c.Payload(&p); err != nil {
		return nil, nil, fmt.Errorf("%w: %w", fold.ErrInvalid, err)
	}

	src, def, err := e.Loader.Load(ctx, p.From, p.Seq)
	if err == nil && !src.Exists() {
		src, def, err = e.Loader.LoadArchived(ctx, p.From, p.Seq)
	}

	if err != nil {
		if errors.Is(err, loader.ErrNotArchived) {
			return nil, nil, fmt.Errorf("%w: fork source %s does not exist", fold.ErrInvalid, p.From)
		}

		return nil, nil, err
	}

	evs, err := fold.DecideFork(def, st, src, p.From, p.Seq, c.ID, p.Writes, now)

	return evs, def, err
}

// afterAppend is the housekeeping after a stored decision: a snapshot when
// due, a continuation when the live log reached the history limit. Failures
// are logged and retried after the next decision; the decision is stored.
func (e *Engine) afterAppend(ctx context.Context, execID string, ent statecache.Entry, before int) {
	if e.Loader.Snaps.Due(before, ent.State.Events) {
		if err := e.Loader.Snaps.Save(ctx, ent.State); err != nil {
			e.In.Logger.Warn("packtrail: snapshot failed", "exec", execID, "err", err)
		}
	}

	if e.HistoryLimit > 0 && ent.State.Events-ent.State.Base >= e.HistoryLimit {
		if err := e.continueAsNew(ctx, execID, ent); err != nil {
			e.In.Logger.Warn("packtrail: continue-as-new failed", "exec", execID, "err", err)
		}
	}
}

// continueAsNew bounds the live log of execID (G5-07): it archives the
// current segment (from the last continuation, or the start), appends one
// ExecutionContinued carrying the folded state, then purges what came before.
// Each step is idempotent: a crash between them leaves extra messages that
// readers skip (they are already in the segment) and the next continuation
// purges.
func (e *Engine) continueAsNew(ctx context.Context, execID string, ent statecache.Entry) error {
	st := ent.State
	if st.Status.Terminal() {
		return nil
	}

	evs, err := e.Loader.Log.Read(ctx, execID, 1, st.LastSeq)
	if err != nil {
		return err
	}

	seg := evs[:0:0]

	for _, ev := range evs {
		if ev.Index >= st.Base { // leftovers of an interrupted purge are older
			seg = append(seg, ev)
		}
	}

	if len(seg) == 0 {
		return nil
	}

	first, last := seg[0], seg[len(seg)-1]
	segment := event.Segment{
		Object: loader.SegmentObject(execID, first.Index), FirstIndex: first.Index, LastIndex: last.Index,
		FirstSeq: first.Seq, LastSeq: last.Seq,
	}

	if err = e.putArchive(ctx, segment.Object, seg); err != nil {
		return err
	}

	// Decide on a copy: the cached state must not move unless the append lands.
	b, err := json.Marshal(st)
	if err != nil {
		return err
	}

	next, err := fold.Unmarshal(b)
	if err != nil {
		return err
	}

	cmdID := "continue." + execID + "." + strconv.Itoa(st.Events+1)

	out, err := fold.Continue(ent.Def, next, segment, cmdID, e.now())
	if err != nil || len(out) == 0 {
		return err
	}

	seq, err := e.Loader.Log.Append(ctx, execID, st.LastSeq, out)
	if errors.Is(err, eventlog.ErrConflict) {
		e.Cache.Drop(execID)

		return nil // another engine wrote first; the next decision retries
	}

	if err != nil {
		return err
	}

	next.LastSeq = seq
	e.Cache.Put(execID, statecache.Entry{State: next, Def: ent.Def})
	e.Metrics.EventsAppended.Add(int64(len(out)))

	if err = e.Loader.Snaps.Save(ctx, next); err != nil {
		e.In.Logger.Warn("packtrail: snapshot failed", "exec", execID, "err", err)
	}

	// Purge only what the dispatcher already processed: a message it has not
	// seen yet still has effects to run (a job to publish). The rest goes at
	// the next continuation or at archival.
	floor, err := e.dispatchedUpTo(ctx, execID)
	if err != nil || floor == 0 {
		return err
	}

	return e.Loader.Log.PurgeBefore(ctx, execID, min(seq, floor+1))
}

// dispatchedUpTo returns the stream sequence up to which the dispatcher of
// execID's partition has processed every event (its consumer's ack floor);
// 0 if unknown.
func (e *Engine) dispatchedUpTo(ctx context.Context, execID string) (uint64, error) {
	dur := e.In.Names.DurDispatch(names.Partition(execID, e.In.Partitions))

	cons, err := e.In.JS.Consumer(ctx, e.In.Names.StreamEvents, dur)
	if errors.Is(err, jetstream.ErrConsumerNotFound) {
		return 0, nil
	}

	if err != nil {
		return 0, err
	}

	info, err := cons.Info(ctx)
	if err != nil {
		return 0, err
	}

	return info.AckFloor.Stream, nil
}

// putArchive writes evs as one object of the archive store.
func (e *Engine) putArchive(ctx context.Context, name string, evs []event.Event) error {
	raw := make([]loader.ArchivedEvent, 0, len(evs))

	for _, ev := range evs {
		body, err := event.Encode(ev)
		if err != nil {
			return err
		}

		raw = append(raw, loader.ArchivedEvent{Seq: ev.Seq, DecisionEnd: ev.DecisionEnd, Event: body})
	}

	b, err := json.Marshal(raw)
	if err != nil {
		return err
	}

	obs, err := e.In.Object(ctx, e.In.Names.ObjectArchive)
	if err != nil {
		return err
	}

	if _, err = obs.PutBytes(ctx, name, b); err != nil {
		return fmt.Errorf("engine: archive %s: %w", name, err)
	}

	return nil
}

// Archive moves a terminal execution's events to the archive object store and
// purges it from the hot resources: event subject, snapshot, blobs, timers.
// Reads of an archived execution come from the archive (I-17).
func (e *Engine) Archive(ctx context.Context, execID string) error {
	st, _, err := e.Loader.Load(ctx, execID, 0)
	if err != nil {
		return err
	}

	if !st.Exists() || !st.Status.Terminal() {
		return nil
	}

	evs, err := e.Loader.Log.Read(ctx, execID, 1, 0)
	if err != nil {
		return err
	}

	if err = e.putArchive(ctx, execID, evs); err != nil {
		return err
	}

	if err = e.Loader.Log.Purge(ctx, execID); err != nil {
		return err
	}

	e.Cache.Drop(execID)

	errs := []error{
		e.Loader.Snaps.Delete(ctx, execID), blob.DeleteExec(ctx, e.In, execID),
		projection.MarkArchived(ctx, e.In, execID),
	}

	e.Metrics.Archived.Add(1)

	return errors.Join(errs...)
}
