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

// Package loader rebuilds execution states: snapshot + tail of the log, the
// flow definition the execution is bound to, and archived executions.
package loader

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail/event"
	"github.com/henomis/packtrail/flow"
	"github.com/henomis/packtrail/internal/eventlog"
	"github.com/henomis/packtrail/internal/fold"
	"github.com/henomis/packtrail/internal/infra"
	"github.com/henomis/packtrail/internal/registry"
	"github.com/henomis/packtrail/internal/snapshot"
)

// Loader rebuilds states.
type Loader struct {
	In    *infra.Infra
	Log   *eventlog.Log
	Snaps *snapshot.Store
	Flows *registry.Registry
}

// New returns a Loader.
func New(in *infra.Infra, log *eventlog.Log, snaps *snapshot.Store, flows *registry.Registry) *Loader {
	return &Loader{In: in, Log: log, Snaps: snaps, Flows: flows}
}

// Load returns the state of execID folded up to stream sequence upTo
// (0 = everything) and its flow definition. A missing execution yields an
// empty state (Exists() == false) and a nil definition.
func (l *Loader) Load(ctx context.Context, execID string, upTo uint64) (*fold.State, *flow.Flow, error) {
	st := l.snapshotFor(ctx, execID, upTo)

	from := uint64(1)
	if st != nil {
		from = st.LastSeq + 1
	}

	evs, err := l.Log.Read(ctx, execID, from, upTo)
	if err != nil {
		return nil, nil, err
	}

	if st == nil && len(evs) == 0 {
		if upTo > 0 {
			// Before the live log (continue-as-new): fold the segments.
			return l.loadFromHistory(ctx, execID, upTo)
		}

		return fold.New(execID), nil, nil
	}

	var def *flow.Flow

	if st != nil {
		def, err = l.Flows.Get(ctx, st.Flow, st.FlowHash)
	} else {
		def, err = l.DefOf(ctx, evs[0])
	}

	if err != nil {
		return nil, nil, err
	}

	if st == nil {
		st = fold.New(execID)
	}

	for _, ev := range evs {
		if err = st.Apply(def, ev); err != nil {
			return nil, nil, err
		}

		st.LastSeq = ev.Seq
	}

	return st, def, nil
}

// loadFromHistory folds execID up to upTo from its whole history, when upTo is
// older than its live log. A missing execution yields an empty state.
func (l *Loader) loadFromHistory(ctx context.Context, execID string, upTo uint64) (*fold.State, *flow.Flow, error) {
	live, err := l.Log.Read(ctx, execID, 1, 0)
	if err != nil || len(live) == 0 {
		return fold.New(execID), nil, err
	}

	evs, err := l.withSegments(ctx, live)
	if err != nil {
		return nil, nil, err
	}

	return l.foldUpTo(ctx, execID, evs, upTo)
}

// snapshotFor returns a usable snapshot or nil. An unreadable snapshot only
// costs a longer replay: snapshots are a cache.
func (l *Loader) snapshotFor(ctx context.Context, execID string, upTo uint64) *fold.State {
	if l.Snaps == nil {
		return nil
	}

	st, err := l.Snaps.Load(ctx, execID)
	if err != nil || st == nil || (upTo > 0 && st.LastSeq > upTo) {
		return nil
	}

	return st
}

// DefOf returns the flow definition named by the first event of a log.
func (l *Loader) DefOf(ctx context.Context, first event.Event) (*flow.Flow, error) {
	switch d := first.Data.(type) {
	case *event.Started:
		return l.Flows.Get(ctx, d.Flow, d.FlowHash)
	case *event.Forked:
		return l.defOfState(ctx, d.State)
	case *event.Continued:
		// A live log that was continued as new starts here.
		return l.defOfState(ctx, d.State)
	default:
		return nil, fmt.Errorf("loader: log starts with %s", first.Type)
	}
}

func (l *Loader) defOfState(ctx context.Context, b []byte) (*flow.Flow, error) {
	st, err := fold.Unmarshal(b)
	if err != nil {
		return nil, err
	}

	return l.Flows.Get(ctx, st.Flow, st.FlowHash)
}

// ArchivedEvent is one event in an archive object.
type ArchivedEvent struct {
	Seq         uint64          `json:"seq"`
	DecisionEnd bool            `json:"decision_end"`
	Event       json.RawMessage `json:"event"`
}

// ErrNotArchived is returned when an execution has no archive.
var ErrNotArchived = errors.New("loader: execution is not archived")

// Archived returns the archived events of execID (the live log it had when it
// was archived; segments are separate, see History).
func (l *Loader) Archived(ctx context.Context, execID string) ([]event.Event, error) {
	return l.ArchiveObject(ctx, execID)
}

// SegmentObject names the archive object of the history segment of execID
// starting at event index first. Execution ids contain no dots, so it cannot
// collide with an execution's own archive object.
func SegmentObject(execID string, first int) string {
	return execID + ".seg." + strconv.Itoa(first)
}

// ArchiveObject reads one object of the archive store as events.
func (l *Loader) ArchiveObject(ctx context.Context, name string) ([]event.Event, error) {
	obs, err := l.In.Object(ctx, l.In.Names.ObjectArchive)
	if err != nil {
		return nil, err
	}

	b, err := obs.GetBytes(ctx, name)
	if err != nil {
		if errors.Is(err, jetstream.ErrObjectNotFound) {
			return nil, ErrNotArchived
		}

		return nil, err
	}

	var raw []ArchivedEvent
	if err = json.Unmarshal(b, &raw); err != nil {
		return nil, fmt.Errorf("loader: archive %s: %w", name, err)
	}

	evs := make([]event.Event, 0, len(raw))

	for _, r := range raw {
		ev, lerr := event.Decode(r.Event)
		if lerr != nil {
			return nil, lerr
		}

		ev.Seq, ev.DecisionEnd = r.Seq, r.DecisionEnd
		evs = append(evs, ev)
	}

	return evs, nil
}

// IsArchived reports whether an archive object exists for execID.
func (l *Loader) IsArchived(ctx context.Context, execID string) (bool, error) {
	obs, err := l.In.Object(ctx, l.In.Names.ObjectArchive)
	if err != nil {
		return false, err
	}

	if _, err = obs.GetInfo(ctx, execID); err != nil {
		if errors.Is(err, jetstream.ErrObjectNotFound) {
			return false, nil
		}

		return false, err
	}

	return true, nil
}

// LoadArchived folds an archived execution.
func (l *Loader) LoadArchived(ctx context.Context, execID string, upTo uint64) (*fold.State, *flow.Flow, error) {
	evs, err := l.Archived(ctx, execID)
	if err != nil {
		return nil, nil, err
	}

	if len(evs) == 0 {
		return nil, nil, ErrNotArchived
	}

	if upTo > 0 && evs[0].Seq > upTo {
		// Before the live log it had when archived: in its segments.
		if evs, err = l.withSegments(ctx, evs); err != nil {
			return nil, nil, err
		}
	}

	return l.foldUpTo(ctx, execID, evs, upTo)
}

// History returns every event of execID, oldest first: the archived history
// segments (continue-as-new) followed by the live log, or by the archive
// object of an archived execution. ErrNotArchived means it does not exist.
func (l *Loader) History(ctx context.Context, execID string) ([]event.Event, error) {
	evs, err := l.Log.Read(ctx, execID, 1, 0)
	if err != nil {
		return nil, err
	}

	if len(evs) == 0 {
		if evs, err = l.Archived(ctx, execID); err != nil {
			return nil, err
		}
	}

	return l.withSegments(ctx, evs)
}

// withSegments prepends the archived segments to a log that starts with (or
// contains, if a purge after a continuation did not happen) an
// ExecutionContinued; events before the last one are already in a segment.
func (l *Loader) withSegments(ctx context.Context, evs []event.Event) ([]event.Event, error) {
	last := -1

	for i, ev := range evs {
		if ev.Type == event.ExecutionContinued {
			last = i
		}
	}

	if last < 0 {
		return evs, nil
	}

	d, ok := evs[last].Data.(*event.Continued)
	if !ok {
		return nil, fmt.Errorf("loader: %s without payload", event.ExecutionContinued)
	}

	st, err := fold.Unmarshal(d.State)
	if err != nil {
		return nil, err
	}

	var out []event.Event

	for _, seg := range st.Segments {
		part, serr := l.ArchiveObject(ctx, seg.Object)
		if serr != nil {
			return nil, fmt.Errorf("loader: history segment %s: %w", seg.Object, serr)
		}

		out = append(out, part...)
	}

	return append(out, evs[last:]...), nil
}

// foldUpTo folds evs up to stream sequence upTo (0: all) from scratch.
func (l *Loader) foldUpTo(ctx context.Context, execID string, evs []event.Event,
	upTo uint64,
) (*fold.State, *flow.Flow, error) {
	if len(evs) == 0 {
		return fold.New(execID), nil, nil
	}

	def, err := l.DefOf(ctx, evs[0])
	if err != nil {
		return nil, nil, err
	}

	st := fold.New(execID)

	for _, ev := range evs {
		if upTo > 0 && ev.Seq > upTo {
			break
		}

		if err = st.Apply(def, ev); err != nil {
			return nil, nil, err
		}

		st.LastSeq = ev.Seq
	}

	return st, def, nil
}
