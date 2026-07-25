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

package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// fakeMsg is a minimal jetstream.Msg whose Metadata() is fully controllable,
// so numDelivered/firedID can be tested without a real NATS connection.
type fakeMsg struct {
	meta    *jetstream.MsgMetadata
	metaErr error
}

func (f *fakeMsg) Metadata() (*jetstream.MsgMetadata, error) { return f.meta, f.metaErr }
func (f *fakeMsg) Data() []byte                              { return nil }
func (f *fakeMsg) Headers() nats.Header                      { return nil }
func (f *fakeMsg) Subject() string                           { return "test" }
func (f *fakeMsg) Reply() string                             { return "" }
func (f *fakeMsg) Ack() error                                { return nil }
func (f *fakeMsg) DoubleAck(context.Context) error           { return nil }
func (f *fakeMsg) Nak() error                                { return nil }
func (f *fakeMsg) NakWithDelay(time.Duration) error          { return nil }
func (f *fakeMsg) InProgress() error                         { return nil }
func (f *fakeMsg) Term() error                               { return nil }
func (f *fakeMsg) TermWithReason(string) error               { return nil }

// TestNumDelivered verifies numDelivered reads NumDelivered from a message's
// metadata, and falls back to 0 (rather than propagating the error) when
// metadata is unavailable.
func TestNumDelivered(t *testing.T) {
	msg := &fakeMsg{meta: &jetstream.MsgMetadata{NumDelivered: 7}}
	if got := numDelivered(msg); got != 7 {
		t.Fatalf("numDelivered = %d, want 7", got)
	}

	failing := &fakeMsg{metaErr: errors.New("no metadata")}
	if got := numDelivered(failing); got != 0 {
		t.Fatalf("numDelivered (metadata error) = %d, want 0", got)
	}
}

// TestFiredID verifies firedID returns the message's stream sequence as a
// stable idempotency id, and "" when metadata is unavailable.
func TestFiredID(t *testing.T) {
	msg := &fakeMsg{meta: &jetstream.MsgMetadata{Sequence: jetstream.SequencePair{Stream: 42}}}
	if got := firedID(msg); got != "42" {
		t.Fatalf("firedID = %q, want %q", got, "42")
	}

	failing := &fakeMsg{metaErr: errors.New("no metadata")}
	if got := firedID(failing); got != "" {
		t.Fatalf("firedID (metadata error) = %q, want \"\"", got)
	}
}
