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
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail/internal/names"
)

// Signals publishes and consumes external signals within one namespace.
type Signals struct {
	js     jetstream.JetStream
	stream string
	prefix string // subject prefix, followed by "<execID>.<signalName>"
}

// New returns a Signals bound to the given JetStream context and namespace.
func New(js jetstream.JetStream, n names.Names) *Signals {
	return &Signals{js: js, stream: n.StreamSignals, prefix: n.SubjSignalPrefix}
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
		MaxAge:    7 * 24 * time.Hour,
	})
	if err != nil {
		return fmt.Errorf("signals stream: %w", err)
	}

	return nil
}

// Publish sends a signal for execID/name with the given payload.
func (s *Signals) Publish(ctx context.Context, execID, name string, payload []byte) error {
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
func (s *Signals) Consume(
	ctx context.Context, durable string, handler func(context.Context, Delivery) error,
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
		execID, name, ok := s.parseSubject(msg.Subject())
		if !ok {
			_ = msg.Term()
			return
		}

		meta, metaErr := msg.Metadata()
		if metaErr != nil {
			_ = msg.NakWithDelay(time.Second)
			return
		}

		d := Delivery{ExecID: execID, Name: name, Seq: meta.Sequence.Stream, Payload: msg.Data()}
		if handlerErr := handler(ctx, d); handlerErr != nil {
			_ = msg.NakWithDelay(signalNakDelay)
			return
		}

		_ = msg.Ack()
	})
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
