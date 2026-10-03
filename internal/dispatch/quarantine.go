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

package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail/event"
	"github.com/henomis/packtrail/flow"
	"github.com/henomis/packtrail/internal/fold"
	"github.com/henomis/packtrail/internal/infra"
	"github.com/henomis/packtrail/internal/projection"
	"github.com/henomis/packtrail/internal/registry"
	"github.com/henomis/packtrail/internal/statecache"
	"github.com/henomis/packtrail/internal/wire"
)

// Quarantine lifecycle (F-03, F2-03, F2-04):
//
//   - An event that fails deterministically (it can never succeed by being
//     retried) quarantines its execution at once; transient failures are
//     retried with backoff forever, stalling the partition while the
//     infrastructure is down rather than giving up on healthy executions.
//   - While quarantined, an execution's events are skipped — except terminal
//     ones, whose effects (parent notification, children cancel, index,
//     archive) still run, so a quarantined child never strands its parent.
//   - Unquarantine (a redispatch command) replays the effects of every event
//     from the failing one, then lifts the mark on every dispatcher.

const (
	ctlQuarantine = "quarantine"
	ctlRelease    = "release"
	// notFoundGrace is how many deliveries a "not found" read gets before it
	// counts as deterministic: on a replicated deployment a direct get can be
	// served by a replica that has not caught up with a just-written blob or
	// flow version yet.
	notFoundGrace = 3
)

// quarantinable reports whether err, on its delivered-th delivery, should
// quarantine the execution.
func quarantinable(err error, delivered uint64) bool {
	if errors.Is(err, jetstream.ErrObjectNotFound) || errors.Is(err, registry.ErrUnknownFlow) {
		return delivered >= notFoundGrace
	}

	return Deterministic(err)
}

// skip reports whether the events of execID are to be skipped. The in-memory
// set (fed by broadcasts) is only a cache: core NATS does not guarantee
// delivery, so a positive is confirmed against the q.<exec> index key and a
// stale entry — a release this process missed — is dropped (F3-01).
func (d *Dispatcher) skip(ctx context.Context, execID string) (bool, error) {
	if !d.isQuarantined(execID) {
		return false, nil
	}

	q, err := projection.IsQuarantined(ctx, d.In, execID)
	if err != nil {
		return false, err
	}

	if !q {
		d.unmarkQuarantined(execID)
		d.Cache.Drop(execID)
	}

	return q, nil
}

// Deterministic reports whether err can never be fixed by retrying the same
// event: an undecodable or unfoldable event, an unknown flow, malformed data,
// a request the server rejects as invalid, a missing blob.
func Deterministic(err error) bool {
	var (
		syn *json.SyntaxError
		typ *json.UnmarshalTypeError
		api *jetstream.APIError
	)

	switch {
	case errors.Is(err, event.ErrUnknown), errors.Is(err, fold.ErrApply), errors.Is(err, fold.ErrInvalid),
		errors.Is(err, registry.ErrUnknownFlow), errors.Is(err, jetstream.ErrObjectNotFound):
		return true
	case errors.As(err, &syn), errors.As(err, &typ):
		return true
	case errors.As(err, &api):
		return api.Code == 400 //nolint:mnd // HTTP-style "bad request".
	default:
		return false
	}
}

// Quarantine marks execID as quarantined from event seq and tells every
// dispatcher to skip its events.
func Quarantine(ctx context.Context, in *infra.Infra, execID string, seq uint64, reason string) error {
	if err := projection.Quarantine(ctx, in, execID, seq, reason); err != nil {
		return err
	}

	return in.NC.Publish(in.Names.CtlSubject(ctlQuarantine), []byte(execID))
}

func (d *Dispatcher) quarantine(ctx context.Context, msg jetstream.Msg, execID string, cause error) error {
	md, _ := msg.Metadata()

	var seq, deliveries uint64
	if md != nil {
		seq, deliveries = md.Sequence.Stream, md.NumDelivered
	}

	reason := fmt.Sprintf("event %d: %v", seq, cause)

	dl := wire.DeadLetter{
		Kind: wire.DLQDispatch, Key: execID, Reason: reason, Deliveries: deliveries, Subject: msg.Subject(),
		Header: msg.Headers(), Body: msg.Data(),
	}

	if err := wire.PublishDLQ(ctx, d.In, dl, "dispatch."+strconv.FormatUint(seq, 10)); err != nil {
		return err
	}

	if err := Quarantine(ctx, d.In, execID, seq, reason); err != nil {
		return err
	}

	d.markQuarantined(execID)
	d.Cache.Drop(execID)
	d.Metrics.DeadLetters.Add(1)
	d.In.Logger.Error("packtrail: execution quarantined", "exec", execID, "reason", reason)

	return nil
}

// handleQuarantined skips the decision of a quarantined execution, except a
// terminal event whose closing effects still run. Events are decoded one by
// one, so an unreadable event cannot hide the terminal one.
func (d *Dispatcher) handleQuarantined(ctx context.Context, msg jetstream.Msg, execID string) {
	evs, err := d.Loader.Log.DecodeEach(ctx, msg)
	if err != nil {
		var delivered uint64
		if md, merr := msg.Metadata(); merr == nil {
			delivered = md.NumDelivered
		}

		// Only an unreadable body is given up on: a claim-checked decision
		// whose blob cannot be read yet may be the terminal one (I-51).
		if quarantinable(err, delivered) {
			_ = msg.Ack()

			return
		}

		d.In.Logger.Warn("packtrail: quarantined decision unreadable, will retry", "exec", execID,
			"deliveries", delivered, "err", err)

		_ = msg.NakWithDelay(backoff(delivered))

		return
	}

	i := slices.IndexFunc(evs, isTerminal)
	if i < 0 {
		_ = msg.Ack()

		return
	}

	ev := evs[i]

	if err = d.closeQuarantined(ctx, execID, ev); err != nil {
		var delivered uint64
		if md, merr := msg.Metadata(); merr == nil {
			delivered = md.NumDelivered
		}

		d.In.Logger.Warn("packtrail: closing a quarantined execution failed, will retry", "exec", execID,
			"err", err)

		_ = msg.NakWithDelay(backoff(delivered))

		return
	}

	_ = msg.Ack()
}

func isTerminal(ev event.Event) bool {
	switch ev.Data.(type) {
	case *event.Completed, *event.Failed, *event.Cancelled:
		return true
	default:
		return false
	}
}

// closeQuarantined runs the terminal effects of an execution whose state cannot
// be folded: what they need comes from the terminal event itself and from
// the first event (parent, flow definition).
func (d *Dispatcher) closeQuarantined(ctx context.Context, execID string, ev event.Event) error {
	st, def, err := d.stateBeforeMark(ctx, execID)
	if err != nil {
		return err
	}

	if st != nil {
		return d.closeWith(ctx, execID, st, def, ev)
	}

	st = fold.New(execID)

	err = d.Loader.Log.Scan(ctx, execID, 1, func(first event.Event) bool {
		switch f := first.Data.(type) {
		case *event.Started:
			st.Parent, st.Flow = f.Parent, f.Flow
		case *event.Forked:
			if src, uerr := fold.Unmarshal(f.State); uerr == nil {
				st.Parent, st.Flow = src.Parent, src.Flow
			}
		}

		def, _ = d.Loader.DefOf(ctx, first)

		return false
	})
	if err != nil {
		return err
	}

	return d.closeWith(ctx, execID, st, def, ev)
}

// stateBeforeMark folds the log up to the event before the one that
// quarantined the execution, so the closing effects carry what is known —
// counters for the parent's budgets above all (F3-04). It returns a nil state
// when that is not possible (e.g. the bad event is the first one).
func (d *Dispatcher) stateBeforeMark(ctx context.Context, execID string) (*fold.State, *flow.Flow, error) {
	mark, ok, err := projection.Mark(ctx, d.In, execID)
	if err != nil {
		return nil, nil, err
	}

	if !ok || mark.Seq <= 1 {
		return nil, nil, nil
	}

	st, def, err := d.Loader.Load(ctx, execID, mark.Seq-1)
	if err != nil || !st.Exists() {
		return nil, nil, nil //nolint:nilerr // fall back to the minimal state.
	}

	return st, def, nil
}

func (d *Dispatcher) closeWith(ctx context.Context, execID string, st *fold.State, def *flow.Flow,
	ev event.Event,
) error {
	switch t := ev.Data.(type) {
	case *event.Completed:
		st.Status, st.Output = fold.StatusCompleted, t.Output
	case *event.Failed:
		st.Status, st.Error, st.Reason = fold.StatusFailed, t.Error, t.Reason
	case *event.Cancelled:
		st.Status, st.Reason = fold.StatusCancelled, t.Reason
	}

	if err := d.terminal(ctx, statecache.Entry{State: st, Def: def}, ev); err != nil {
		return err
	}

	return projection.SetTerminal(ctx, d.In, execID, st.Status, st.Reason, st.Error, ev.Time)
}

// Replay lifts the quarantine of execID: it runs the effects of every event
// from the one that failed (which must now succeed), then releases the mark
// on every dispatcher and catches up on events skipped meanwhile. On error the
// execution stays quarantined.
func (d *Dispatcher) Replay(ctx context.Context, execID string) error {
	mark, ok, err := projection.Mark(ctx, d.In, execID)
	if err != nil || !ok {
		return err
	}

	d.Cache.Drop(execID)

	// The replay runs outside the partition runner, possibly while this
	// process's dispatcher applies the same events: it folds into a private
	// cache so the two never mutate one state.
	cache := statecache.New(1)

	last, err := d.replayFrom(ctx, cache, execID, max(mark.Seq, 1))
	if err != nil {
		return fmt.Errorf("dispatch: replay %s: %w", execID, err)
	}

	if err = projection.Unquarantine(ctx, d.In, execID); err != nil {
		return err
	}

	if err = d.In.NC.Publish(d.In.Names.CtlSubject(ctlRelease), []byte(execID)); err != nil {
		return err
	}

	d.unmarkQuarantined(execID)

	// Events appended while the replay ran may have been skipped by
	// dispatchers that still saw the mark. The mark is gone now, so every
	// later skip decision (always confirmed against the index) processes
	// them; this pass covers those already skipped. Effects are idempotent,
	// so a dispatcher also running one does no harm.
	_, err = d.replayFrom(ctx, cache, execID, last+1)

	return err
}

func (d *Dispatcher) replayFrom(ctx context.Context, cache *statecache.Cache, execID string,
	from uint64,
) (uint64, error) {
	last := from - 1

	var aerr error

	err := d.Loader.Log.ScanDecisions(ctx, execID, from, func(evs []event.Event) bool {
		if aerr = d.applyDecision(ctx, cache, execID, evs); aerr != nil {
			return false
		}

		last = evs[0].Seq

		return true
	})

	return last, errors.Join(err, aerr)
}

// WatchControl follows quarantine/release broadcasts, so every dispatcher
// process skips (or resumes) an execution at once, until stop is called. Call
// it before Run: the subscriptions then precede each partition's read of the
// quarantine list, so no broadcast falls between the two.
func (d *Dispatcher) WatchControl() (stop func(), err error) {
	subs := make([]*nats.Subscription, 0, 2) //nolint:mnd // quarantine and release.

	stop = func() {
		for _, s := range subs {
			_ = s.Unsubscribe()
		}
	}

	for kind, fn := range map[string]func(string){ctlQuarantine: d.markQuarantined, ctlRelease: d.unmarkQuarantined} {
		sub, serr := d.In.NC.Subscribe(d.In.Names.CtlSubject(kind), func(m *nats.Msg) {
			execID := string(m.Data)
			fn(execID)
			d.Cache.Drop(execID)
		})
		if serr != nil {
			stop()

			return nil, fmt.Errorf("dispatch: control subscription %s: %w", kind, serr)
		}

		subs = append(subs, sub)
	}

	return stop, nil
}

func (d *Dispatcher) loadQuarantined(ctx context.Context) {
	ids, err := projection.Quarantined(ctx, d.In)
	if err != nil {
		return
	}

	for _, id := range ids {
		d.markQuarantined(id)
	}
}

func (d *Dispatcher) markQuarantined(execID string) {
	d.qmu.Lock()
	defer d.qmu.Unlock()

	if d.quarantined == nil {
		d.quarantined = map[string]bool{}
	}

	d.quarantined[execID] = true
}

func (d *Dispatcher) unmarkQuarantined(execID string) {
	d.qmu.Lock()
	defer d.qmu.Unlock()

	delete(d.quarantined, execID)
}

func (d *Dispatcher) isQuarantined(execID string) bool {
	d.qmu.Lock()
	defer d.qmu.Unlock()

	return d.quarantined[execID]
}
