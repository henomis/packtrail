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

package asyncqueue

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// panicOnInProgressMsg is a minimal jetstream.Msg fake whose InProgress call
// panics, so a test can drive heartbeat's tick handling without a real NATS
// connection.
type panicOnInProgressMsg struct{}

func (panicOnInProgressMsg) Metadata() (*jetstream.MsgMetadata, error) { return nil, nil }
func (panicOnInProgressMsg) Data() []byte                              { return nil }
func (panicOnInProgressMsg) Headers() nats.Header                      { return nil }
func (panicOnInProgressMsg) Subject() string                           { return "test" }
func (panicOnInProgressMsg) Reply() string                             { return "" }
func (panicOnInProgressMsg) Ack() error                                { return nil }
func (panicOnInProgressMsg) DoubleAck(context.Context) error           { return nil }
func (panicOnInProgressMsg) Nak() error                                { return nil }
func (panicOnInProgressMsg) NakWithDelay(time.Duration) error          { return nil }
func (panicOnInProgressMsg) Term() error                               { return nil }
func (panicOnInProgressMsg) TermWithReason(string) error               { return nil }

func (panicOnInProgressMsg) InProgress() error {
	panic("boom")
}

// TestHeartbeatRecoversPanic is a regression test: heartbeat runs on its own
// goroutine, separate from the one handle's panic-recovering defer covers, so
// an unrecovered panic here used to crash the whole worker process instead of
// just this job's heartbeat.
func TestHeartbeatRecoversPanic(t *testing.T) {
	w := &Worker{
		cfg: config{ackWait: 30 * time.Millisecond}, // small: fire a tick quickly
		log: slog.New(slog.DiscardHandler),
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	done := make(chan struct{})

	go func() {
		w.heartbeat(ctx, panicOnInProgressMsg{})
		close(done)
	}()

	select {
	case <-done:
		// heartbeat recovered its own panic and returned once ctx expired,
		// rather than propagating the panic and crashing the test binary.
	case <-time.After(2 * time.Second):
		t.Fatal("heartbeat never returned; panic likely escaped recover")
	}
}

// TestHeartbeatFloorsTinyAckWait is a regression test: an ackWait too small to
// divide into a positive duration (e.g. below heartbeatDivisor) used to make
// time.NewTicker itself panic, crashing the worker before the first tick.
func TestHeartbeatFloorsTinyAckWait(t *testing.T) {
	w := &Worker{
		cfg: config{ackWait: 1}, // 1ns / heartbeatDivisor == 0 without the floor
		log: slog.New(slog.DiscardHandler),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	done := make(chan struct{})

	go func() {
		defer close(done)

		w.heartbeat(ctx, panicOnInProgressMsg{}) // must not panic constructing the ticker
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("heartbeat never returned")
	}
}
