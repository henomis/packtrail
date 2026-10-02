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

// Package eventlog appends to and reads an execution's event log: the subject
// <ns>.ev.<partition>.<exec> of the events stream.
//
// Append is the single-writer guarantee: it carries
// Nats-Expected-Last-Subject-Sequence, so two writers deciding on the same
// state can never both commit — the loser gets ErrConflict and re-decides on
// the fresh state. The events of one decision are written as one message (a
// JSON array), so a decision is stored all or nothing without a JetStream
// atomic batch, and the stream sequence identifies a decision.
package eventlog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail/event"
	"github.com/henomis/packtrail/internal/blob"
	"github.com/henomis/packtrail/internal/infra"
)

// NATS headers used by append and read.
const (
	hdrExpectedSubjSeq = "Nats-Expected-Last-Subject-Sequence"
	hdrSequence        = "Nats-Sequence"
	hdrNumPending      = "Nats-Num-Pending"
	hdrStatus          = "Status"

	// errCodeWrongLastSeq is JSStreamWrongLastSequenceErr.
	errCodeWrongLastSeq = 10071
	// MaxEvents bounds the events of one decision. NATS imposes no such
	// limit (a large decision is claim-checked); it is a guard against a
	// runaway decision.
	MaxEvents = 1000

	readBatch = 256
)

// ErrConflict means another writer appended first: reload and decide again.
var ErrConflict = errors.New("eventlog: concurrent append (expected sequence mismatch)")

// Log reads and writes event logs.
type Log struct {
	in *infra.Infra

	mu     sync.Mutex
	stream jetstream.Stream
}

// events returns the cached events-stream handle (fetching it costs a
// STREAM.INFO round trip, too much for the per-job path, F2-05).
func (l *Log) events(ctx context.Context) (jetstream.Stream, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.stream != nil {
		return l.stream, nil
	}

	s, err := l.in.JS.Stream(ctx, l.in.Names.StreamEvents)
	if err != nil {
		return nil, err
	}

	l.stream = s

	return s, nil
}

// New returns a Log.
func New(in *infra.Infra) *Log { return &Log{in: in} }

func (l *Log) timeout() time.Duration {
	if l.in.ReadTimeout > 0 {
		return l.in.ReadTimeout
	}

	return infra.DefaultReadTimeout
}

// Append writes the decision evs after expected (the stream sequence of the
// execution's last decision, 0 for a new execution) as one message and returns
// its sequence.
func (l *Log) Append(ctx context.Context, execID string, expected uint64, evs []event.Event) (uint64, error) {
	if len(evs) == 0 {
		return expected, nil
	}

	if len(evs) > MaxEvents {
		return 0, fmt.Errorf("eventlog: decision produced %d events, more than the limit %d", len(evs), MaxEvents)
	}

	body, h, err := event.EncodeDecision(evs)
	if err != nil {
		return 0, err
	}

	if body, err = blob.Offload(ctx, l.in, execID, body, h); err != nil {
		return 0, err
	}

	h.Set(hdrExpectedSubjSeq, strconv.FormatUint(expected, 10))

	ack, err := l.in.JS.PublishMsg(ctx, &nats.Msg{Subject: l.in.EventSubject(execID), Header: h, Data: body})
	if err != nil {
		var apiErr *jetstream.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode == errCodeWrongLastSeq {
			return 0, ErrConflict
		}

		return 0, fmt.Errorf("eventlog: append: %w", err)
	}

	return ack.Sequence, nil
}

// Read returns the events of execID with stream sequence in [from, to]
// (to = 0: up to the end), with Seq and DecisionEnd set.
func (l *Log) Read(ctx context.Context, execID string, from, to uint64) ([]event.Event, error) {
	var out []event.Event

	err := l.Scan(ctx, execID, from, func(ev event.Event) bool {
		if to > 0 && ev.Seq > to {
			return false
		}

		out = append(out, ev)

		return true
	})

	return out, err
}

// Scan calls fn for every event of execID from sequence from, in order, until
// fn returns false. It uses batched direct gets: no consumer is created.
func (l *Log) Scan(ctx context.Context, execID string, from uint64, fn func(event.Event) bool) error {
	return l.ScanDecisions(ctx, execID, from, func(evs []event.Event) bool {
		for _, ev := range evs {
			if !fn(ev) {
				return false
			}
		}

		return true
	})
}

// ScanDecisions calls fn for every decision of execID from sequence from, in
// order, until fn returns false.
func (l *Log) ScanDecisions(ctx context.Context, execID string, from uint64, fn func([]event.Event) bool) error {
	return l.scanSubject(ctx, l.in.EventSubject(execID), from, fn)
}

// scanSubject is ScanDecisions over an explicit subject.
func (l *Log) scanSubject(ctx context.Context, subject string, from uint64, fn func([]event.Event) bool) error {
	if from == 0 {
		from = 1
	}

	for {
		next, more, stop, err := l.readBatch(ctx, subject, from, fn)
		if err != nil || stop || !more {
			return err
		}

		from = next
	}
}

type directGet struct {
	Seq        uint64 `json:"seq"`
	NextBySubj string `json:"next_by_subj"`
	Batch      int    `json:"batch"`
}

// readBatch issues one batched direct get. It returns the sequence to resume
// from, whether more messages remain, and whether fn asked to stop.
func (l *Log) readBatch(ctx context.Context, subject string, from uint64,
	fn func([]event.Event) bool,
) (next uint64, more, stop bool, err error) {
	req, err := json.Marshal(directGet{Seq: from, NextBySubj: subject, Batch: readBatch})
	if err != nil {
		return 0, false, false, err
	}

	inbox := l.in.NC.NewRespInbox()

	sub, err := l.in.NC.SubscribeSync(inbox)
	if err != nil {
		return 0, false, false, err
	}

	defer func() { _ = sub.Unsubscribe() }()

	if err = l.in.NC.PublishRequest("$JS.API.DIRECT.GET."+l.in.Names.StreamEvents, inbox, req); err != nil {
		return 0, false, false, err
	}

	rctx, cancel := context.WithTimeout(ctx, l.timeout())
	defer cancel()

	// Collect the batch first and decode afterwards: resolving claim-checked
	// bodies can be slow and must not eat the read deadline.
	var msgs []*nats.Msg

	next = from

	for {
		m, lerr := sub.NextMsgWithContext(rctx)
		if lerr != nil {
			return 0, false, false, fmt.Errorf("eventlog: read %s: %w", subject, lerr)
		}

		if st := m.Header.Get(hdrStatus); st != "" {
			// 204: end of batch; 404: nothing (more) to read.
			more = st == "204" && m.Header.Get(hdrNumPending) != "0"

			break
		}

		msgs = append(msgs, m)
	}

	for _, m := range msgs {
		seq, lerr := strconv.ParseUint(m.Header.Get(hdrSequence), 10, 64)
		if lerr != nil {
			return 0, false, false, fmt.Errorf("eventlog: message without sequence: %w", lerr)
		}

		evs, lerr := l.decode(ctx, m.Data, m.Header, seq)
		if lerr != nil {
			return 0, false, false, lerr
		}

		next = seq + 1

		if !fn(evs) {
			return next, false, true, nil
		}
	}

	return next, more, false, nil
}

// decode resolves a claim-checked body and decodes the decision stored at seq.
func (l *Log) decode(ctx context.Context, data []byte, h nats.Header, seq uint64) ([]event.Event, error) {
	body, err := blob.Resolve(ctx, l.in, data, h)
	if err != nil {
		return nil, err
	}

	evs, err := event.DecodeDecision(body)
	if err != nil {
		return nil, fmt.Errorf("eventlog: seq %d: %w", seq, err)
	}

	for i := range evs {
		evs[i].Seq = seq
	}

	evs[len(evs)-1].DecisionEnd = true

	return evs, nil
}

// Decode decodes the decision delivered by a consumer (seq from its metadata).
func (l *Log) Decode(ctx context.Context, m jetstream.Msg) ([]event.Event, error) {
	md, err := m.Metadata()
	if err != nil {
		return nil, err
	}

	return l.decode(ctx, m.Data(), m.Headers(), md.Sequence.Stream)
}

// DecodeEach decodes the events of a delivered decision one by one, keeping
// those that decode: a quarantined execution still needs its terminal event
// when another event of the same decision is unreadable.
func (l *Log) DecodeEach(ctx context.Context, m jetstream.Msg) ([]event.Event, error) {
	md, err := m.Metadata()
	if err != nil {
		return nil, err
	}

	body, err := blob.Resolve(ctx, l.in, m.Data(), m.Headers())
	if err != nil {
		return nil, err
	}

	raws, err := event.SplitDecision(body)
	if err != nil {
		return nil, err
	}

	var out []event.Event

	for _, r := range raws {
		if ev, derr := event.Decode(r); derr == nil {
			ev.Seq = md.Sequence.Stream
			out = append(out, ev)
		}
	}

	return out, nil
}

// LastSeq returns the sequence of the last event of execID, 0 if none.
func (l *Log) LastSeq(ctx context.Context, execID string) (uint64, error) {
	s, err := l.events(ctx)
	if err != nil {
		return 0, err
	}

	m, err := s.GetLastMsgForSubject(ctx, l.in.EventSubject(execID))
	if err != nil {
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			return 0, nil
		}

		return 0, err
	}

	return m.Sequence, nil
}

// PurgeBefore removes the events of execID stored before sequence seq (they
// were archived by a continuation).
func (l *Log) PurgeBefore(ctx context.Context, execID string, seq uint64) error {
	s, err := l.events(ctx)
	if err != nil {
		return err
	}

	return s.Purge(ctx, jetstream.WithPurgeSubject(l.in.EventSubject(execID)), jetstream.WithPurgeSequence(seq))
}

// Purge removes every event of execID (archival).
func (l *Log) Purge(ctx context.Context, execID string) error {
	s, err := l.events(ctx)
	if err != nil {
		return err
	}

	return s.Purge(ctx, jetstream.WithPurgeSubject(l.in.EventSubject(execID)))
}
