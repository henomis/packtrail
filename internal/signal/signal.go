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

// Package signal carries external signals to waiting executions. Signals are
// published to a durable JetStream stream so they survive restarts and are
// redelivered until acknowledged; the engine applies them idempotently using
// each message's stream sequence number (spec §7).
package signal

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail/internal/names"
)

// tokenPattern bounds the execution id and signal name, which become NATS
// subject tokens on the signals stream. Validating at publish time turns a
// malformed (or injection-shaped) input into a clear error instead of an opaque
// NATS rejection or a misrouted subject.
var tokenPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

// defaultRetention is the signals-stream MaxAge when none is configured: an
// undelivered signal survives an engine outage of up to this long before it is
// dropped by the stream's age limit.
const defaultRetention = 7 * 24 * time.Hour

// Signals publishes and consumes external signals within one namespace.
type Signals struct {
	js        jetstream.JetStream
	stream    string
	prefix    string // subject prefix, followed by "<execID>.<signalName>"
	retention time.Duration
}

// New returns a Signals bound to the given JetStream context and namespace, with
// the default signal retention. Override with SetRetention before EnsureStream.
func New(js jetstream.JetStream, n names.Names) *Signals {
	return &Signals{js: js, stream: n.StreamSignals, prefix: n.SubjSignalPrefix, retention: defaultRetention}
}

// SetRetention overrides the signals-stream MaxAge applied by EnsureStream. A
// positive duration bounds how long an undelivered signal survives; a negative
// value disables the age limit; zero keeps the current (default) value. Call
// before EnsureStream.
func (s *Signals) SetRetention(d time.Duration) {
	switch {
	case d > 0:
		s.retention = d
	case d < 0:
		s.retention = 0 // no MaxAge
	}
}

// Subject returns the signal subject for an execution and signal name.
func (s *Signals) Subject(execID, name string) string { return s.prefix + execID + "." + name }

// EnsureStream creates the signals stream if it does not exist.
func (s *Signals) EnsureStream(ctx context.Context) error {
	_, err := s.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      s.stream,
		Subjects:  []string{s.prefix + ">"},
		Storage:   jetstream.FileStorage,
		Retention: jetstream.LimitsPolicy,
		MaxAge:    s.retention,
	})
	if err != nil {
		return fmt.Errorf("signals stream: %w", err)
	}

	return nil
}

// Publish sends a signal for execID/name with the given payload. Both execID
// and name must match [A-Za-z0-9_-]{1,128} (they become subject tokens).
func (s *Signals) Publish(ctx context.Context, execID, name string, payload []byte) error {
	if !tokenPattern.MatchString(execID) {
		return fmt.Errorf("signal: invalid execution id %q: must match [A-Za-z0-9_-]{1,128}", execID)
	}

	if !tokenPattern.MatchString(name) {
		return fmt.Errorf("signal: invalid signal name %q: must match [A-Za-z0-9_-]{1,128}", name)
	}

	_, err := s.js.Publish(ctx, s.Subject(execID, name), payload)

	return err
}

// Delivery is a received signal with its stream sequence (for idempotency).
type Delivery struct {
	ExecID  string
	Name    string
	Seq     uint64
	Payload []byte
}

const (
	signalAckWait  = 30 * time.Second
	signalNakDelay = 2 * time.Second
)

// Consume sets up a durable consumer and invokes handler for every signal. The
// handler must persist state before returning nil; only then is the message
// acked (CAS-before-ack). A returned error triggers redelivery. The returned
// ConsumeContext must be stopped by the caller.
//
// A handler error normally Naks for redelivery, but a signal is dead-lettered
// (Term) when the handler returns a terminal error (interface{ Terminal() bool }
// → true) or after maxDeliver deliveries, so a persistently unappliable signal
// cannot Nak-loop forever (the waiting execution falls back to its wait timeout).
// onDeadLetter (when non-nil) is called with the execution id, signal name, reason
// and delivery count just before a Term, so the caller can record a durable trace.
func (s *Signals) Consume(
	ctx context.Context, durable string, maxDeliver int,
	onDeadLetter func(execID, name, reason string, deliveries uint64),
	handler func(context.Context, Delivery) error,
) (jetstream.ConsumeContext, error) {
	cons, err := s.js.CreateOrUpdateConsumer(ctx, s.stream, jetstream.ConsumerConfig{
		Durable:       durable,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       signalAckWait,
		FilterSubject: s.prefix + ">",
	})
	if err != nil {
		return nil, fmt.Errorf("signals consumer: %w", err)
	}

	return cons.Consume(func(msg jetstream.Msg) {
		s.handleDelivery(ctx, msg, maxDeliver, onDeadLetter, handler)
	})
}

// handleDelivery parses, dispatches, and acks/naks/terms a single signal
// delivery on behalf of Consume.
func (s *Signals) handleDelivery(
	ctx context.Context, msg jetstream.Msg, maxDeliver int,
	onDeadLetter func(execID, name, reason string, deliveries uint64),
	handler func(context.Context, Delivery) error,
) {
	execID, name, ok := s.parseSubject(msg.Subject())
	if !ok {
		// Unparseable subjects can never be applied; Term, but leave a
		// durable trace instead of dropping the signal invisibly.
		slog.Warn("dead-lettering signal with unparseable subject", "subject", msg.Subject())

		if onDeadLetter != nil {
			var deliveries uint64
			if meta, metaErr := msg.Metadata(); metaErr == nil {
				deliveries = meta.NumDelivered
			}

			onDeadLetter(msg.Subject(), "", "unparseable signal subject", deliveries)
		}

		_ = msg.Term()

		return
	}

	meta, metaErr := msg.Metadata()
	if metaErr != nil {
		_ = msg.NakWithDelay(time.Second)
		return
	}

	d := Delivery{ExecID: execID, Name: name, Seq: meta.Sequence.Stream, Payload: msg.Data()}

	handlerErr := handler(ctx, d)
	if handlerErr == nil {
		_ = msg.Ack()
		return
	}

	//nolint:gosec // maxDeliver is a small positive config value
	exhausted := maxDeliver > 0 && meta.NumDelivered >= uint64(maxDeliver)
	if isTerminal(handlerErr) || exhausted {
		slog.Warn("dead-lettering signal", "exec", execID, "name", name, "err", handlerErr)

		if onDeadLetter != nil {
			onDeadLetter(execID, name, handlerErr.Error(), meta.NumDelivered)
		}

		_ = msg.Term()

		return
	}

	_ = msg.NakWithDelay(signalNakDelay)
}

// isTerminal reports whether err (or one it wraps) declares itself non-retryable
// via interface{ Terminal() bool }. Structural so this package need not import
// the runtime package that defines the terminal error.
func isTerminal(err error) bool {
	var t interface{ Terminal() bool }

	return errors.As(err, &t) && t.Terminal()
}

// parseSubject extracts execID and signal name from "<prefix><exec>.<name>".
func (s *Signals) parseSubject(subject string) (execID, name string, ok bool) {
	rest, found := strings.CutPrefix(subject, s.prefix)
	if !found {
		return "", "", false
	}

	i := strings.IndexByte(rest, '.')
	if i < 0 {
		return "", "", false
	}

	return rest[:i], rest[i+1:], true
}
