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

package packtrail

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail/internal/dsl"
	"github.com/henomis/packtrail/internal/store"
)

// FlowGraph is the static structure of a flow, for visualisation. It is
// published to a KV registry at startup so observability tools can render a flow
// without its source YAML.
type FlowGraph struct {
	Version string      `json:"version,omitempty"`
	Name    string      `json:"name"`
	Nodes   []GraphNode `json:"nodes"`
	Edges   []GraphEdge `json:"edges"`
}

// GraphNode is one node of a FlowGraph. Fields are type-specific; empty ones are
// omitted.
type GraphNode struct {
	ID         string      `json:"id"`
	Type       string      `json:"type"` // task | fanout | fanin | choice | signal
	Invoker    string      `json:"invoker,omitempty"`
	Target     string      `json:"target,omitempty"`
	Branches   []string    `json:"branches,omitempty"`
	WaitFor    []string    `json:"wait_for,omitempty"`
	JoinPolicy string      `json:"join_policy,omitempty"`
	Rules      []GraphRule `json:"rules,omitempty"`
	OnError    string      `json:"on_error,omitempty"`
	SignalName string      `json:"signal_name,omitempty"`
	OnTimeout  string      `json:"on_timeout,omitempty"`
}

// GraphRule is one routing rule of a choice node.
type GraphRule struct {
	When    string `json:"when,omitempty"`
	Default bool   `json:"default,omitempty"`
	To      string `json:"to"`
}

// GraphEdge is a static edge between two nodes.
type GraphEdge struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// Event is a flow execution transition, suitable for a live activity feed.
type Event struct {
	ExecID   string    `json:"exec_id"`
	Flow     string    `json:"flow"`
	Status   string    `json:"status"`
	Node     string    `json:"node"`
	Error    string    `json:"error,omitempty"`
	Revision uint64    `json:"revision"`
	Time     time.Time `json:"time"`
}

// DeadLetter is a durable record of a work item a durable consumer gave up on
// (Term'd) — a terminal error or an exhausted delivery cap. Kind is the source
// consumer ("work", "schedule", "signal" or "async").
type DeadLetter = store.DeadLetter

// DeadLetterCount returns the durable number of dead-letter records retained in
// the dead-letter stream (bounded by its ~30-day retention). It is the
// operational signal that poisoned work is being dropped; a non-zero, growing
// count warrants investigation.
func (s *Server) DeadLetterCount(ctx context.Context) (uint64, error) {
	if err := s.Init(ctx); err != nil {
		return 0, err
	}

	return s.store.DeadLetterCount(ctx)
}

// RecentDeadLetters returns up to limit of the most recent dead-letter records,
// oldest-first (limit <= 0 uses a sane default). Use it to inspect what poisoned
// work was dropped and why, without scanning logs.
func (s *Server) RecentDeadLetters(ctx context.Context, limit int) ([]DeadLetter, error) {
	if err := s.Init(ctx); err != nil {
		return nil, err
	}

	return s.store.RecentDeadLetters(ctx, limit)
}

// buildFlowGraph projects a parsed flow into its public, serialisable graph.
func buildFlowGraph(f *dsl.Flow) FlowGraph {
	g := FlowGraph{Version: f.Version, Name: f.Name}
	for i := range f.Nodes {
		n := &f.Nodes[i]

		gn := GraphNode{
			ID:         n.ID,
			Type:       n.Type,
			Invoker:    n.Invoker,
			Target:     n.InvokeTarget(),
			Branches:   n.Branches,
			WaitFor:    n.WaitFor,
			JoinPolicy: n.JoinPolicy,
			OnError:    n.OnError,
			SignalName: n.SignalName,
			OnTimeout:  n.OnTimeout,
		}
		for _, r := range n.Rules {
			gn.Rules = append(gn.Rules, GraphRule{When: r.When, Default: r.Default, To: r.To})
		}

		g.Nodes = append(g.Nodes, gn)
	}

	for _, e := range f.Edges {
		g.Edges = append(g.Edges, GraphEdge{From: e.From, To: e.To})
	}

	return g
}

// ListFlows returns the names of every flow in the registry. Unlike Flows() (the
// flows this Server instance loaded), this reads the shared KV registry, so an
// observer process that loaded no flows still sees them.
func (s *Server) ListFlows(ctx context.Context) ([]string, error) {
	if err := s.Init(ctx); err != nil {
		return nil, err
	}

	keys, err := s.flowsKV.Keys(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return nil, nil
		}

		return nil, err
	}

	sort.Strings(keys)

	return keys, nil
}

// FlowGraph returns a flow's graph from the registry, or ErrNotFound.
func (s *Server) FlowGraph(ctx context.Context, name string) (*FlowGraph, error) {
	if err := s.Init(ctx); err != nil {
		return nil, err
	}

	entry, err := s.flowsKV.Get(ctx, name)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, ErrNotFound
		}

		return nil, err
	}

	var g FlowGraph

	err = json.Unmarshal(entry.Value(), &g)
	if err != nil {
		return nil, err
	}

	return &g, nil
}

// WatchEvents streams execution transitions as they happen. It delivers events
// published after the call (an ephemeral consumer with DeliverNew); load current
// state via Get/ByStatus first, then apply events live. The channel is closed
// when ctx is cancelled.
func (s *Server) WatchEvents(ctx context.Context) (<-chan Event, error) {
	if err := s.Init(ctx); err != nil {
		return nil, err
	}

	js := s.store.JS()
	n := s.store.Names()

	cons, err := js.CreateOrUpdateConsumer(ctx, n.StreamEvents, jetstream.ConsumerConfig{
		FilterSubject: n.SubjEventsPrefix + ">",
		DeliverPolicy: jetstream.DeliverNewPolicy,
		AckPolicy:     jetstream.AckNonePolicy,
	})
	if err != nil {
		return nil, fmt.Errorf("events consumer: %w", err)
	}

	const eventChanBuf = 64

	out := make(chan Event, eventChanBuf)

	// cc.Stop does not wait for a callback already executing, so closing out
	// right after it races a late sender into a send-on-closed-channel panic.
	// The closed flag (under mu) fences that: a sender inside the critical
	// section blocks the close until its select returns — and it always returns
	// promptly, because the close only happens after ctx is cancelled, which is
	// the select's other case.
	var (
		mu     sync.Mutex
		closed bool
	)

	cc, err := cons.Consume(func(msg jetstream.Msg) {
		var ev store.Event
		if unmarshalErr := json.Unmarshal(msg.Data(), &ev); unmarshalErr != nil {
			return
		}

		mu.Lock()
		defer mu.Unlock()

		if closed {
			return
		}

		select {
		case out <- storeEventToPublic(ev):
		case <-ctx.Done():
		}
	})
	if err != nil {
		return nil, fmt.Errorf("consume events: %w", err)
	}

	go func() {
		<-ctx.Done()
		cc.Stop()

		mu.Lock()
		closed = true
		mu.Unlock()

		close(out)
	}()

	return out, nil
}

// ByStatusEvents returns a summary event for every execution currently indexed
// under status, read directly from the visibility index without a per-execution
// round-trip. The index is eventually consistent; use Get for authoritative state.
func (s *Server) ByStatusEvents(ctx context.Context, status string) ([]Event, error) {
	return s.ByStatusEventsLimit(ctx, status, 0)
}

// ByStatusEventsLimit is ByStatusEvents capped at limit entries (0 = no cap).
// Since KV keys have no inherent order the cap yields an arbitrary subset, not
// an ordered page; it is a guardrail against an unbounded transfer.
func (s *Server) ByStatusEventsLimit(ctx context.Context, status string, limit int) ([]Event, error) {
	if err := s.Init(ctx); err != nil {
		return nil, err
	}

	evs, err := s.indexer.ByStatusEventsLimit(ctx, status, limit)
	if err != nil {
		return nil, err
	}

	return convertStoreEvents(evs), nil
}

// ByFlowEvents returns a summary event for every execution belonging to flow,
// read directly from the visibility index without a per-execution round-trip.
func (s *Server) ByFlowEvents(ctx context.Context, flow string) ([]Event, error) {
	return s.ByFlowEventsLimit(ctx, flow, 0)
}

// ByFlowEventsLimit is ByFlowEvents capped at limit entries (0 = no cap). The
// same arbitrary-subset caveat as ByStatusEventsLimit applies.
func (s *Server) ByFlowEventsLimit(ctx context.Context, flow string, limit int) ([]Event, error) {
	if err := s.Init(ctx); err != nil {
		return nil, err
	}

	evs, err := s.indexer.ByFlowEventsLimit(ctx, flow, limit)
	if err != nil {
		return nil, err
	}

	return convertStoreEvents(evs), nil
}

// History returns an execution's ordered transition trace (oldest first, up to
// limit; non-positive = a generous default). It requires WithHistory —
// otherwise it returns nothing — and records expire after the configured
// retention.
func (s *Server) History(ctx context.Context, execID string, limit int) ([]Event, error) {
	if !validExecID(execID) {
		return nil, fmt.Errorf("invalid execution id %q: must match [A-Za-z0-9_-]{1,128}", execID)
	}

	if err := s.Init(ctx); err != nil {
		return nil, err
	}

	evs, err := s.store.History(ctx, execID, limit)
	if err != nil {
		return nil, err
	}

	return convertStoreEvents(evs), nil
}

func storeEventToPublic(ev store.Event) Event {
	return Event{
		ExecID: ev.ExecID, Flow: ev.FlowName, Status: ev.Status,
		Node: ev.Node, Error: ev.Error, Revision: ev.Revision, Time: ev.Time,
	}
}

func convertStoreEvents(evs []store.Event) []Event {
	out := make([]Event, len(evs))
	for i, ev := range evs {
		out[i] = storeEventToPublic(ev)
	}

	return out
}
