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

package event

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

// Headers carried by every stored decision (one message per decision).
const (
	// HeaderTypes lists the types of the decision's events, in order,
	// comma-separated: enough to react to an event without decoding the body.
	HeaderTypes = "Pt-Event-Types"
	// HeaderCmdID is the id of the command whose decision the message holds.
	HeaderCmdID = "Pt-Cmd-Id"
)

// ErrUnknown is returned when decoding an event of an unknown type or version:
// folding it wrong would be worse than stopping.
var ErrUnknown = errors.New("event: unknown type or version")

// newPayload returns a pointer to a fresh payload struct for t.
func newPayload(t Type) (any, bool) {
	switch t { //nolint:exhaustive // split in two for size; newPayload2 has the rest.
	case ExecutionStarted:
		return &Started{}, true
	case ExecutionForked:
		return &Forked{}, true
	case ExecutionContinued:
		return &Continued{}, true
	case ExecutionCompleted:
		return &Completed{}, true
	case ExecutionFailed:
		return &Failed{}, true
	case ExecutionCancelled:
		return &Cancelled{}, true
	case NodeEntered:
		return &Entered{}, true
	case NodeScheduled:
		return &Scheduled{}, true
	case NodeCompleted:
		return &NodeDone{}, true
	case NodeFailed:
		return &NodeFail{}, true
	case NodeInterrupted:
		return &Interrupted{}, true
	case NodeCancelled:
		return &NodeCancel{}, true
	default:
		return newPayload2(t)
	}
}

func newPayload2(t Type) (any, bool) {
	switch t { //nolint:exhaustive // newPayload has the rest.
	case ChoiceEvaluated:
		return &Choice{}, true
	case FanoutStarted:
		return &Fanout{}, true
	case JoinCompleted:
		return &Join{}, true
	case AwaitStarted:
		return &Await{}, true
	case AwaitTimedOut:
		return &AwaitTimeout{}, true
	case SignalReceived:
		return &Signal{}, true
	case SignalConsumed:
		return &SignalUse{}, true
	case MapStarted:
		return &Map{}, true
	case MapCompleted:
		return &MapDone{}, true
	case MapAborted:
		return &MapAbort{}, true
	case ChannelsUpdated:
		return &Update{}, true
	case ChildStarted:
		return &Child{}, true
	case ChildCompleted:
		return &ChildDone{}, true
	case TimerScheduled:
		return &Timer{}, true
	case TimerFired:
		return &Fired{}, true
	default:
		return nil, false
	}
}

// New builds an event with the current version.
func New(t Type, data any) Event { return Event{Type: t, Version: Version, Data: data} }

// Encode returns the JSON envelope of ev.
func Encode(ev Event) ([]byte, error) {
	if ev.Version == 0 {
		ev.Version = Version
	}

	body, err := json.Marshal(ev)
	if err != nil {
		return nil, fmt.Errorf("event: encode %s: %w", ev.Type, err)
	}

	return body, nil
}

// EncodeDecision returns the message body and headers of one decision: a JSON
// array of the events' envelopes. A decision is stored as one message, so it
// is appended atomically without a JetStream batch.
func EncodeDecision(evs []Event) ([]byte, nats.Header, error) {
	if len(evs) == 0 {
		return nil, nil, errors.New("event: empty decision")
	}

	raws := make([]json.RawMessage, len(evs))
	types := make([]string, len(evs))

	for i, ev := range evs {
		b, err := Encode(ev)
		if err != nil {
			return nil, nil, err
		}

		raws[i], types[i] = b, string(ev.Type)
	}

	body, err := json.Marshal(raws)
	if err != nil {
		return nil, nil, fmt.Errorf("event: encode decision: %w", err)
	}

	h := nats.Header{}
	h.Set(HeaderTypes, strings.Join(types, ","))

	if evs[0].CmdID != "" {
		h.Set(HeaderCmdID, evs[0].CmdID)
	}

	return body, h, nil
}

// SplitDecision returns the envelopes of a decision body, undecoded.
func SplitDecision(body []byte) ([]json.RawMessage, error) {
	var raws []json.RawMessage
	if err := json.Unmarshal(body, &raws); err != nil {
		return nil, fmt.Errorf("event: decode decision: %w", err)
	}

	if len(raws) == 0 {
		return nil, errors.New("event: decode decision: empty")
	}

	return raws, nil
}

// DecodeDecision parses a decision body.
func DecodeDecision(body []byte) ([]Event, error) {
	raws, err := SplitDecision(body)
	if err != nil {
		return nil, err
	}

	out := make([]Event, len(raws))

	for i, r := range raws {
		if out[i], err = Decode(r); err != nil {
			return nil, err
		}
	}

	return out, nil
}

// Types returns the event types listed in a decision's headers.
func Types(h nats.Header) []Type {
	v := h.Get(HeaderTypes)
	if v == "" {
		return nil
	}

	parts := strings.Split(v, ",")
	out := make([]Type, len(parts))

	for i, p := range parts {
		out[i] = Type(p)
	}

	return out
}

// Decode parses an event body.
func Decode(body []byte) (Event, error) {
	var env struct {
		Type    Type            `json:"type"`
		Version int             `json:"v"`
		Time    time.Time       `json:"time"`
		Index   int             `json:"i"`
		CmdID   string          `json:"cmd_id"`
		Trace   string          `json:"trace"`
		Data    json.RawMessage `json:"data"`
	}

	if err := json.Unmarshal(body, &env); err != nil {
		return Event{}, fmt.Errorf("event: decode: %w", err)
	}

	payload, ok := newPayload(env.Type)
	if !ok || env.Version != Version {
		return Event{}, fmt.Errorf("%w: %q v%d", ErrUnknown, env.Type, env.Version)
	}

	if len(env.Data) > 0 && string(env.Data) != "null" {
		if err := json.Unmarshal(env.Data, payload); err != nil {
			return Event{}, fmt.Errorf("event: decode %s: %w", env.Type, err)
		}
	}

	return Event{
		Type: env.Type, Version: env.Version, Time: env.Time, Index: env.Index, CmdID: env.CmdID, Trace: env.Trace,
		Data: payload,
	}, nil
}
