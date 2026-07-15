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

// Package scheduler wraps the NATS JetStream Message Scheduler. The engine never
// runs its own timers: every programmed pause (retry backoff, signal/wait
// timeout, recurring flow start) is published as a scheduled message and the
// NATS server delivers it, at the right time, to a "fire" subject that the
// engine consumes.
//
// The server requires a schedule's target subject to be captured by the same
// schedule-enabled stream, so fired messages land on packtrail.sched.fire.<key>
// and a durable consumer (ConsumeFired) forwards them onward.
package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"

	"github.com/henomis/packtrail/internal/names"
)

// Scheduler publishes scheduled messages to a schedule-enabled stream.
type Scheduler struct {
	js     jetstream.JetStream
	stream string
	subj   string // schedule subject prefix
	fire   string // fire subject prefix (targets must live under the stream)
}

// New returns a Scheduler bound to the given JetStream context and namespace.
// It performs no I/O; call EnsureStream once before publishing or consuming.
func New(js jetstream.JetStream, n names.Names) *Scheduler {
	return &Scheduler{js: js, stream: n.StreamSchedule, subj: n.SubjSchedPrefix, fire: n.SubjSchedFirePrefix}
}

// EnsureStream creates (idempotently) the schedule-enabled stream that carries
// both schedule definitions (sched.once.*, sched.cron.*) and fired deliveries
// (sched.fire.*).
//
// Retention is deliberately LimitsPolicy with no MaxAge/MaxMsgs, despite fired
// messages accumulating (one per fired timer — they are NOT rolled up per key,
// and Acking does not remove them under LimitsPolicy). A stream-wide age/size
// limit is NOT safe here: this same stream must retain a cron definition
// indefinitely and a long-delay one-shot (e.g. a multi-day signal timeout) until
// it fires, so pruning by age would silently delete a pending timer. The server
// requires a schedule's target (fire) subject to live in this same
// schedule-enabled stream, so the fired messages cannot be split onto a separate
// WorkQueue/Interest stream that would drop them on ack.
//
// The scheduler self-purges a one-shot definition once it fires, so the unbounded
// growth is limited to already-consumed sched.fire.* messages. Those are
// reclaimed by ReclaimFired on the server's full-reconcile maintenance cadence:
// it purges the fire.> subject only below the fired consumer's ack floor (never a
// blanket age limit). AllowRollup stays enabled so a cron definition can be
// replaced by name.
func (s *Scheduler) EnsureStream(ctx context.Context) error {
	_, err := s.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:              s.stream,
		Subjects:          []string{s.subj + ">"},
		Storage:           jetstream.FileStorage,
		Retention:         jetstream.LimitsPolicy,
		AllowMsgSchedules: true,
		AllowRollup:       true,
		// Explicit (rather than the implicit ~2m default) so AtID's Nats-Msg-Id
		// dedup — which drops a re-published identical timer within the window —
		// is self-documenting and stable across server-default changes.
		Duplicates: scheduleDedupWindow,
	})
	if err != nil {
		return fmt.Errorf("schedule stream: %w", err)
	}

	return nil
}

// FireSubject returns the fire subject for a logical key (e.g. an execution id).
func (s *Scheduler) FireSubject(key string) string { return s.fire + key }

// After schedules a one-shot delivery of payload to FireSubject(key) after d.
// Each call creates an independent schedule, so concurrent timers for the same
// key never overwrite one another.
func (s *Scheduler) After(ctx context.Context, key string, d time.Duration, payload []byte) error {
	return s.At(ctx, key, time.Now().Add(d), payload)
}

// At schedules a one-shot delivery of payload to FireSubject(key) at when.
func (s *Scheduler) At(ctx context.Context, key string, when time.Time, payload []byte) error {
	subj := s.subj + "once." + nuid.Next()
	_, err := s.js.Publish(ctx, subj, payload,
		jetstream.WithScheduleAt(when),
		jetstream.WithScheduleTarget(s.FireSubject(key)))

	return err
}

// AtID is At with a caller-supplied idempotency id: the publish carries
// Nats-Msg-Id, so re-publishing the same scheduled item inside the stream's
// dedup window is dropped instead of installing a duplicate timer. (Beyond the
// window a duplicate timer is benign for guarded consumers — the earliest
// fires at the right time, later ones no-op.)
func (s *Scheduler) AtID(ctx context.Context, key, msgID string, when time.Time, payload []byte) error {
	subj := s.subj + "once." + nuid.Next()
	_, err := s.js.Publish(ctx, subj, payload,
		jetstream.WithScheduleAt(when),
		jetstream.WithScheduleTarget(s.FireSubject(key)),
		jetstream.WithMsgID(msgID))

	return err
}

// Cron installs (or replaces) a recurring schedule named name that delivers
// payload to FireSubject(key) on the given 6-field cron expression
// ("sec min hour dom mon dow"). Reusing name replaces the schedule.
// The server evaluates the expression in UTC, and ticks missed while the
// server was down are skipped, not replayed (the schedule resumes at its
// next occurrence).
func (s *Scheduler) Cron(ctx context.Context, name, key, expr string, payload []byte) error {
	subj := s.subj + "cron." + name
	_, err := s.js.Publish(ctx, subj, payload,
		jetstream.WithScheduleCron(expr),
		jetstream.WithScheduleTarget(s.FireSubject(key)))

	return err
}

const (
	firedAckWait  = 30 * time.Second
	firedNakDelay = 2 * time.Second
	// scheduleDedupWindow is the explicit JetStream dedup window for the schedule
	// stream, matching NATS's implicit ~2m default (see AtID).
	scheduleDedupWindow = 2 * time.Minute
)

// ConsumeFired sets up a durable consumer that invokes handler for every fired
// schedule. handler receives the fire subject's key, the original payload, and a
// stable per-firing id (the fired message's stream sequence, identical across
// redeliveries of the same firing) that the handler can use as an idempotency
// key — e.g. so a redelivered cron tick starts at most one execution. The id is
// empty only when the message metadata is unavailable. The returned
// ConsumeContext must be stopped by the caller.
//
// A handler error normally Naks for redelivery, but a fired message is
// dead-lettered (Term, never redelivered) when the handler returns a terminal
// error — one implementing interface{ Terminal() bool } returning true, e.g. a
// cron start for a flow that was removed — or after maxDeliver deliveries. Either
// case would otherwise Nak-loop forever on every cron tick. onDeadLetter (when
// non-nil) is called with the key, reason and delivery count just before a Term,
// so the caller can record a durable trace.
func (s *Scheduler) ConsumeFired(
	ctx context.Context, durable string, maxDeliver int,
	onDeadLetter func(key, reason string, deliveries uint64),
	handler func(key string, payload []byte, firedID string) error,
) (jetstream.ConsumeContext, error) {
	cons, err := s.js.CreateOrUpdateConsumer(ctx, s.stream, jetstream.ConsumerConfig{
		Durable:       durable,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       firedAckWait,
		FilterSubject: s.fire + ">",
	})
	if err != nil {
		return nil, fmt.Errorf("fired consumer: %w", err)
	}

	return cons.Consume(func(msg jetstream.Msg) {
		key := msg.Subject()[len(s.fire):]

		handlerErr := handler(key, msg.Data(), firedID(msg))
		if handlerErr == nil {
			_ = msg.Ack()
			return
		}

		if isTerminal(handlerErr) || deliveriesExhausted(msg, maxDeliver) {
			slog.Warn("dead-lettering fired schedule", "key", key, "err", handlerErr)

			if onDeadLetter != nil {
				onDeadLetter(key, handlerErr.Error(), numDelivered(msg))
			}

			_ = msg.Term()

			return
		}

		_ = msg.NakWithDelay(firedNakDelay)
	})
}

// ReclaimFired purges already-processed fired-schedule messages (sched.fire.*)
// from the schedule stream: those strictly below the fired consumer's ack floor
// have been delivered and acked, so they can never be redelivered and are safe to
// remove. It bounds the otherwise-unbounded growth of consumed fire.* messages
// (they are not rolled up and Acking does not remove them under LimitsPolicy).
//
// It never applies a blanket age/size limit: the same stream retains cron
// definitions (sched.cron.*) and pending one-shot timers (sched.once.*, e.g. a
// multi-day signal timeout) indefinitely, so a MaxAge would silently delete a
// pending timer. The purge is scoped to the fire.> subject and bounded by the ack
// floor, so definitions and undelivered timers are untouched. Returns the number
// of messages purged.
func (s *Scheduler) ReclaimFired(ctx context.Context, durable string) (uint64, error) {
	stream, err := s.js.Stream(ctx, s.stream)
	if err != nil {
		return 0, fmt.Errorf("schedule stream: %w", err)
	}

	cons, err := stream.Consumer(ctx, durable)
	if err != nil {
		return 0, fmt.Errorf("fired consumer: %w", err)
	}

	info, err := cons.Info(ctx)
	if err != nil {
		return 0, fmt.Errorf("fired consumer info: %w", err)
	}

	// AckFloor.Stream is the highest stream sequence up to which every fire.*
	// delivery is acked. Purge fire.* messages strictly below it (WithPurgeSequence
	// purges seq < floor), so the boundary message and anything pending/undelivered
	// is kept.
	floor := info.AckFloor.Stream
	if floor <= 1 {
		return 0, nil // nothing acked yet
	}

	before, err := stream.Info(ctx)
	if err != nil {
		return 0, fmt.Errorf("stream info: %w", err)
	}

	if err = stream.Purge(ctx,
		jetstream.WithPurgeSubject(s.fire+">"), jetstream.WithPurgeSequence(floor)); err != nil {
		return 0, fmt.Errorf("purge fired: %w", err)
	}

	after, err := stream.Info(ctx)
	if err != nil {
		return 0, fmt.Errorf("stream info: %w", err)
	}

	if before.State.Msgs < after.State.Msgs {
		return 0, nil // concurrent growth; report nothing purged rather than underflow
	}

	return before.State.Msgs - after.State.Msgs, nil
}

// numDelivered returns a message's delivery count, or 0 if unavailable.
func numDelivered(msg jetstream.Msg) uint64 {
	if meta, err := msg.Metadata(); err == nil {
		return meta.NumDelivered
	}

	return 0
}

// firedID returns a stable idempotency id for a fired message: its stream
// sequence, which is fixed for a stored message and identical across
// redeliveries. It is empty when metadata is unavailable, in which case the
// handler falls back to non-idempotent handling.
func firedID(msg jetstream.Msg) string {
	meta, err := msg.Metadata()
	if err != nil {
		return ""
	}

	return strconv.FormatUint(meta.Sequence.Stream, 10)
}

// isTerminal reports whether err (or one it wraps) declares itself non-retryable
// via interface{ Terminal() bool }. The check is structural so this package need
// not import the runtime package that defines the terminal error.
func isTerminal(err error) bool {
	var t interface{ Terminal() bool }

	return errors.As(err, &t) && t.Terminal()
}

// deliveriesExhausted reports whether a message has been delivered at least
// maxDeliver times. A metadata read failure is treated as not-exhausted so a
// transient fault keeps retrying rather than prematurely dead-lettering.
func deliveriesExhausted(msg jetstream.Msg, maxDeliver int) bool {
	meta, err := msg.Metadata()
	if err != nil {
		return false
	}

	//nolint:gosec // maxDeliver is a small positive config value
	return maxDeliver > 0 && meta.NumDelivered >= uint64(maxDeliver)
}
