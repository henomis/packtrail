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
// scheduled and fired messages.
func (s *Scheduler) EnsureStream(ctx context.Context) error {
	_, err := s.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:              s.stream,
		Subjects:          []string{s.subj + ">"},
		Storage:           jetstream.FileStorage,
		Retention:         jetstream.LimitsPolicy,
		AllowMsgSchedules: true,
		AllowRollup:       true, // fired schedules roll up on their target subject
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
)

// ConsumeFired sets up a durable consumer that invokes handler for every fired
// schedule. handler receives the fire subject's key and the original payload.
// The returned ConsumeContext must be stopped by the caller.
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
	handler func(key string, payload []byte) error,
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

		handlerErr := handler(key, msg.Data())
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

// numDelivered returns a message's delivery count, or 0 if unavailable.
func numDelivered(msg jetstream.Msg) uint64 {
	if meta, err := msg.Metadata(); err == nil {
		return meta.NumDelivered
	}

	return 0
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
