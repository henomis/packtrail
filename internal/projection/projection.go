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

// Package projection maintains the visibility index: a KV projection of every
// execution's summary plus membership keys by status, flow and declared search
// attribute, so List/Query never scan the event stream. It is rebuildable from
// scratch by replaying the events (reset the dispatcher consumers).
//
// Keys:
//
//	x.<exec>                 summary JSON
//	s.<status>.<exec>        status membership
//	f.<flow>.<exec>          flow membership
//	a.<attr>.<hash>.<exec>   search attribute membership (hash of the value)
package projection

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail/event"
	"github.com/henomis/packtrail/internal/fold"
	"github.com/henomis/packtrail/internal/infra"
	"github.com/henomis/packtrail/internal/names"
)

// Summary is the indexed view of an execution.
type Summary struct {
	ExecID     string            `json:"exec_id"`
	Flow       string            `json:"flow"`
	FlowHash   string            `json:"flow_hash"`
	Status     fold.Status       `json:"status"`
	Created    time.Time         `json:"created"`
	Updated    time.Time         `json:"updated"`
	Attrs      map[string]string `json:"attrs,omitempty"`
	Parent     *event.ParentRef  `json:"parent,omitempty"`
	ForkedFrom string            `json:"forked_from,omitempty"`
	Error      string            `json:"error,omitempty"`
	Reason     string            `json:"reason,omitempty"`
	LastSeq    uint64            `json:"last_seq"`
	Archived   bool              `json:"archived,omitempty"`
	// Quarantined is set when the dispatcher gave up on this execution's
	// events (see Quarantine); QuarantineReason says why.
	Quarantined      bool   `json:"quarantined,omitempty"`
	QuarantineReason string `json:"quarantine_reason,omitempty"`
}

var statuses = []fold.Status{
	fold.StatusRunning, fold.StatusWaiting, fold.StatusCompleted, fold.StatusFailed, fold.StatusCancelled,
}

// SummaryOf builds the summary of a state.
func SummaryOf(st *fold.State) Summary {
	return Summary{
		ExecID: st.ExecID, Flow: st.Flow, FlowHash: st.FlowHash, Status: st.Status, Created: st.Created,
		Updated: st.Updated, Attrs: st.Attrs, Parent: st.Parent, ForkedFrom: st.ForkedFrom, Error: st.Error,
		Reason: st.Reason, LastSeq: st.LastSeq,
	}
}

// AttrHash maps an attribute value to a key-safe token.
func AttrHash(v string) string {
	sum := sha256.Sum256([]byte(v))

	return hex.EncodeToString(sum[:8])
}

// Index writes the summary of st. created says the execution is new (flow
// and attribute memberships are written once); statusChanged says the status
// membership must move.
func Index(ctx context.Context, in *infra.Infra, st *fold.State, created, statusChanged bool) error {
	return index(ctx, in, st, created, func(kv jetstream.KeyValue) error {
		if created || statusChanged {
			return moveStatus(ctx, kv, st.ExecID, st.Status)
		}

		return nil
	})
}

// IndexFrom is Index for a caller that knows the status before the change
// (the dispatcher folds it): a new execution gets its status key, a change
// moves the key from prev, and no other status key is touched. On a cluster
// every key write is a replicated round trip, so this saves four per change.
func IndexFrom(ctx context.Context, in *infra.Infra, st *fold.State, created bool, prev fold.Status) error {
	return index(ctx, in, st, created, func(kv jetstream.KeyValue) error {
		if !created && prev == st.Status {
			return nil
		}

		if _, err := kv.Put(ctx, statusKey(st.Status, st.ExecID), nil); err != nil {
			return err
		}

		if created {
			return nil
		}

		if err := kv.Delete(ctx, statusKey(prev, st.ExecID)); err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
			return err
		}

		return nil
	})
}

func statusKey(s fold.Status, execID string) string { return "s." + string(s) + "." + execID }

func index(ctx context.Context, in *infra.Infra, st *fold.State, created bool,
	status func(jetstream.KeyValue) error,
) error {
	kv, err := in.KV(ctx, in.Names.BucketIndex)
	if err != nil {
		return err
	}

	b, err := json.Marshal(SummaryOf(st))
	if err != nil {
		return err
	}

	if _, err = kv.Put(ctx, "x."+st.ExecID, b); err != nil {
		return err
	}

	if created {
		if _, err = kv.Put(ctx, "f."+st.Flow+"."+st.ExecID, nil); err != nil {
			return err
		}

		for _, k := range slices.Sorted(maps.Keys(st.Attrs)) {
			if !names.ValidToken(k) {
				continue
			}

			if _, err = kv.Put(ctx, "a."+k+"."+AttrHash(st.Attrs[k])+"."+st.ExecID, nil); err != nil {
				return err
			}
		}
	}

	return status(kv)
}

func moveStatus(ctx context.Context, kv jetstream.KeyValue, execID string, status fold.Status) error {
	for _, s := range statuses {
		key := statusKey(s, execID)
		if s == status {
			if _, err := kv.Put(ctx, key, nil); err != nil {
				return err
			}

			continue
		}

		if err := kv.Delete(ctx, key); err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
			return err
		}
	}

	return nil
}

// QuarantineMark is the value of q.<exec>: the first event the dispatcher
// could not process.
type QuarantineMark struct {
	Seq    uint64 `json:"seq"`
	Reason string `json:"reason"`
}

// Quarantine records that the dispatcher gave up on execID's events from
// sequence seq: membership key q.<exec> (checked by every dispatcher) and the
// summary flag.
func Quarantine(ctx context.Context, in *infra.Infra, execID string, seq uint64, reason string) error {
	kv, err := in.KV(ctx, in.Names.BucketIndex)
	if err != nil {
		return err
	}

	mark, err := json.Marshal(QuarantineMark{Seq: seq, Reason: reason})
	if err != nil {
		return err
	}

	if _, err = kv.Put(ctx, "q."+execID, mark); err != nil {
		return err
	}

	return updateSummary(ctx, kv, execID, func(s *Summary) {
		s.Quarantined, s.QuarantineReason = true, reason
	})
}

// updateSummary applies fn to the summary of execID, if it exists.
func updateSummary(ctx context.Context, kv jetstream.KeyValue, execID string, fn func(*Summary)) error {
	e, err := kv.Get(ctx, "x."+execID)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil
	}

	if err != nil {
		return err
	}

	var sum Summary
	if err = json.Unmarshal(e.Value(), &sum); err != nil {
		return err
	}

	fn(&sum)

	b, err := json.Marshal(sum)
	if err != nil {
		return err
	}

	_, err = kv.Put(ctx, "x."+execID, b)

	return err
}

// Mark returns the quarantine mark of execID (ok=false when not quarantined).
func Mark(ctx context.Context, in *infra.Infra, execID string) (QuarantineMark, bool, error) {
	kv, err := in.KV(ctx, in.Names.BucketIndex)
	if err != nil {
		return QuarantineMark{}, false, err
	}

	e, err := kv.Get(ctx, "q."+execID)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return QuarantineMark{}, false, nil
	}

	if err != nil {
		return QuarantineMark{}, false, err
	}

	var m QuarantineMark
	if err = json.Unmarshal(e.Value(), &m); err != nil {
		m = QuarantineMark{Reason: string(e.Value())}
	}

	return m, true, nil
}

// IsQuarantined reports whether execID is quarantined.
func IsQuarantined(ctx context.Context, in *infra.Infra, execID string) (bool, error) {
	_, ok, err := Mark(ctx, in, execID)

	return ok, err
}

// Quarantined lists the quarantined executions.
func Quarantined(ctx context.Context, in *infra.Infra) ([]string, error) {
	kv, err := in.KV(ctx, in.Names.BucketIndex)
	if err != nil {
		return nil, err
	}

	return listIDs(ctx, kv, "q.*")
}

// Unquarantine removes the quarantine mark of execID.
func Unquarantine(ctx context.Context, in *infra.Infra, execID string) error {
	kv, err := in.KV(ctx, in.Names.BucketIndex)
	if err != nil {
		return err
	}

	if err = kv.Purge(ctx, "q."+execID); err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		return err
	}

	return nil
}

// SetTerminal records the final status of an execution whose state cannot be
// folded (quarantined), keeping the rest of its summary.
func SetTerminal(ctx context.Context, in *infra.Infra, execID string, status fold.Status, reason, errMsg string,
	at time.Time,
) error {
	kv, err := in.KV(ctx, in.Names.BucketIndex)
	if err != nil {
		return err
	}

	err = updateSummary(ctx, kv, execID, func(s *Summary) {
		s.Status, s.Reason, s.Error, s.Updated = status, reason, errMsg, at
	})
	if err != nil {
		return err
	}

	return moveStatus(ctx, kv, execID, status)
}

// Delete removes an execution from the index: its summary and its flow,
// attribute, status and quarantine memberships. The memberships are read
// from the summary, so it goes last: a delete that failed half-way finds
// them again when it is retried. Deleting an unknown execution is a no-op.
func Delete(ctx context.Context, in *infra.Infra, execID string) error {
	kv, err := in.KV(ctx, in.Names.BucketIndex)
	if err != nil {
		return err
	}

	keys := []string{"q." + execID}
	for _, s := range statuses {
		keys = append(keys, statusKey(s, execID))
	}

	e, err := kv.Get(ctx, "x."+execID)

	switch {
	case errors.Is(err, jetstream.ErrKeyNotFound):
	case err != nil:
		return err
	default:
		var s Summary
		if err = json.Unmarshal(e.Value(), &s); err != nil {
			return err
		}

		keys = append(keys, "f."+s.Flow+"."+execID)

		for k, v := range s.Attrs {
			if names.ValidToken(k) {
				keys = append(keys, "a."+k+"."+AttrHash(v)+"."+execID)
			}
		}
	}

	// Erased, not deleted: a deleted execution leaves no markers behind.
	for _, k := range append(keys, "x."+execID) {
		if err = in.EraseKey(ctx, in.Names.BucketIndex, k); err != nil {
			return err
		}
	}

	return nil
}

// MarkArchived flags the summary of an execution as archived.
func MarkArchived(ctx context.Context, in *infra.Infra, execID string) error {
	kv, err := in.KV(ctx, in.Names.BucketIndex)
	if err != nil {
		return err
	}

	e, err := kv.Get(ctx, "x."+execID)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil
		}

		return err
	}

	var s Summary
	if err = json.Unmarshal(e.Value(), &s); err != nil {
		return err
	}

	s.Archived = true

	b, err := json.Marshal(s)
	if err != nil {
		return err
	}

	_, err = kv.Update(ctx, "x."+execID, b, e.Revision())

	return err
}

// Filter selects executions. Empty fields match everything.
type Filter struct {
	Status fold.Status
	Flow   string
	// Attr / Value select by search attribute.
	Attr  string
	Value string
	Limit int
}

// ErrInvalidFilter is returned for a filter value that is not a valid token:
// it would otherwise become a wildcard in the KV key filter.
var ErrInvalidFilter = errors.New("projection: invalid filter")

// List returns the summaries matching f, newest first.
func List(ctx context.Context, in *infra.Infra, f Filter) ([]Summary, error) {
	pattern, err := f.pattern()
	if err != nil {
		return nil, err
	}

	kv, err := in.KV(ctx, in.Names.BucketIndex)
	if err != nil {
		return nil, err
	}

	ids, err := listIDs(ctx, kv, pattern)
	if err != nil {
		return nil, err
	}

	out := make([]Summary, 0, len(ids))

	for _, id := range ids {
		s, lerr := get(ctx, kv, id)
		if lerr != nil {
			continue
		}

		if s.matches(f) {
			out = append(out, s)
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Created.After(out[j].Created) })

	if f.Limit > 0 && len(out) > f.Limit {
		out = out[:f.Limit]
	}

	return out, nil
}

func (f Filter) pattern() (string, error) {
	check := func(what, v string) error {
		if v != "" && !names.ValidToken(v) {
			return fmt.Errorf("%w: %s %q", ErrInvalidFilter, what, v)
		}

		return nil
	}

	err := errors.Join(check("status", string(f.Status)), check("flow", f.Flow), check("attribute", f.Attr))
	if err != nil {
		return "", err
	}

	switch {
	case f.Attr != "":
		return "a." + f.Attr + "." + AttrHash(f.Value) + ".*", nil
	case f.Status != "":
		if !slices.Contains(statuses, f.Status) {
			return "", fmt.Errorf("%w: unknown status %q", ErrInvalidFilter, f.Status)
		}

		return "s." + string(f.Status) + ".*", nil
	case f.Flow != "":
		return "f." + f.Flow + ".*", nil
	default:
		return "x.*", nil
	}
}

func (s Summary) matches(f Filter) bool {
	return (f.Status == "" || s.Status == f.Status) && (f.Flow == "" || s.Flow == f.Flow) &&
		(f.Attr == "" || s.Attrs[f.Attr] == f.Value)
}

func listIDs(ctx context.Context, kv jetstream.KeyValue, pattern string) ([]string, error) {
	lister, err := kv.ListKeysFiltered(ctx, pattern)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return nil, nil
		}

		return nil, err
	}

	var ids []string

	for k := range lister.Keys() {
		ids = append(ids, lastToken(k))
	}

	return ids, nil
}

func lastToken(k string) string {
	for i := len(k) - 1; i >= 0; i-- {
		if k[i] == '.' {
			return k[i+1:]
		}
	}

	return k
}

func get(ctx context.Context, kv jetstream.KeyValue, execID string) (Summary, error) {
	var s Summary

	e, err := kv.Get(ctx, "x."+execID)
	if err != nil {
		return s, err
	}

	err = json.Unmarshal(e.Value(), &s)

	return s, err
}

// Get returns the summary of one execution.
func Get(ctx context.Context, in *infra.Infra, execID string) (Summary, error) {
	kv, err := in.KV(ctx, in.Names.BucketIndex)
	if err != nil {
		return Summary{}, err
	}

	return get(ctx, kv, execID)
}
