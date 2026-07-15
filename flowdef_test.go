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

package packtrail_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/internal/natstest"
)

// ValidateFlowDef accepts a structurally valid flow offline (no NATS).
func TestValidateFlowDefValid(t *testing.T) {
	def := packtrail.FlowDef{
		Name: "ok",
		Nodes: []packtrail.NodeDef{
			{ID: "a", Type: "task", Subject: "tasks.a", Retry: &packtrail.RetryPolicy{MaxAttempts: 3}},
			{ID: "b", Type: "task", Subject: "tasks.b"},
		},
		Edges: []packtrail.EdgeDef{{From: "a", To: "b"}},
	}
	if err := packtrail.ValidateFlowDef(def); err != nil {
		t.Fatalf("valid flow rejected: %v", err)
	}
}

// ValidateFlowDef rejects an over-cap retry — the same bound New enforces — without
// a NATS connection, so a builder can catch it offline.
func TestValidateFlowDefRejectsOverCapRetry(t *testing.T) {
	def := packtrail.FlowDef{
		Name: "bad",
		Nodes: []packtrail.NodeDef{
			{ID: "a", Type: "task", Subject: "tasks.a", Retry: &packtrail.RetryPolicy{MaxAttempts: 65}},
		},
	}

	err := packtrail.ValidateFlowDef(def)
	if err == nil {
		t.Fatal("over-cap retry accepted; want a validation error")
	}

	if !strings.Contains(err.Error(), "max_attempts") {
		t.Fatalf("error = %q, want it to mention max_attempts", err)
	}
}

func TestValidateFlowDefRejectsUnsupportedVersion(t *testing.T) {
	def := packtrail.FlowDef{
		Version: "2.0",
		Name:    "bad-version",
		Nodes:   []packtrail.NodeDef{{ID: "a", Type: "task", Subject: "tasks.a"}},
	}

	err := packtrail.ValidateFlowDef(def)
	if err == nil {
		t.Fatal("unsupported version accepted; want validation error")
	}

	if !strings.Contains(err.Error(), "unsupported version") {
		t.Fatalf("error = %q, want unsupported-version rejection", err)
	}
}

// ValidateFlowDef rejects a structurally-invalid graph (an edge to an unknown
// node), validating across multiple defs and reporting the offending flow.
func TestValidateFlowDefRejectsStructural(t *testing.T) {
	good := packtrail.FlowDef{
		Name:  "good",
		Nodes: []packtrail.NodeDef{{ID: "a", Type: "task", Subject: "s"}},
	}
	bad := packtrail.FlowDef{
		Name: "broken",
		Nodes: []packtrail.NodeDef{
			{ID: "a", Type: "task", Subject: "s"},
		},
		Edges: []packtrail.EdgeDef{{From: "a", To: "nope"}},
	}

	err := packtrail.ValidateFlowDef(good, bad)
	if err == nil {
		t.Fatal("dangling edge accepted; want a validation error")
	}

	if !strings.Contains(err.Error(), "broken") {
		t.Fatalf("error = %q, want it to name the offending flow %q", err, "broken")
	}
}

func TestValidateFlowDefRejectsInvalidChoiceExpression(t *testing.T) {
	def := packtrail.FlowDef{
		Name: "bad-choice",
		Nodes: []packtrail.NodeDef{
			{
				ID: "pick", Type: "choice",
				Rules: []packtrail.RuleDef{
					{When: "input.x >", To: "done"},
					{Default: true, To: "done"},
				},
			},
			{ID: "done", Type: "task", Subject: "tasks.done"},
		},
	}

	err := packtrail.ValidateFlowDef(def)
	if err == nil {
		t.Fatal("invalid choice expression accepted; want compile error")
	}

	if !strings.Contains(err.Error(), "rules: compile") {
		t.Fatalf("error = %q, want choice compile failure", err)
	}
}

func TestValidateFlowDefRejectsInvalidChoiceOnError(t *testing.T) {
	def := packtrail.FlowDef{
		Name: "bad-on-error",
		Nodes: []packtrail.NodeDef{
			{
				ID: "pick", Type: "choice", OnError: "retry",
				Rules: []packtrail.RuleDef{
					{When: "input.x == 1", To: "done"},
					{Default: true, To: "done"},
				},
			},
			{ID: "done", Type: "task", Subject: "tasks.done"},
		},
	}

	err := packtrail.ValidateFlowDef(def)
	if err == nil {
		t.Fatal("invalid choice on_error accepted; want validation error")
	}

	if !strings.Contains(err.Error(), "unknown on_error") {
		t.Fatalf("error = %q, want unknown-on_error rejection", err)
	}
}

func TestValidateFlowDefRejectsDuplicateNames(t *testing.T) {
	def := packtrail.FlowDef{
		Name:  "dupe",
		Nodes: []packtrail.NodeDef{{ID: "a", Type: "task", Subject: "tasks.a"}},
	}

	err := packtrail.ValidateFlowDef(def, def)
	if err == nil {
		t.Fatal("duplicate flow names accepted; want validation error")
	}

	if !strings.Contains(err.Error(), `duplicate flow "dupe"`) {
		t.Fatalf("error = %q, want duplicate-flow rejection", err)
	}
}

func TestFlowDefConversionCopiesSlices(t *testing.T) {
	srv := natstest.Start(t)

	def := packtrail.FlowDef{
		Name: "immutable",
		Nodes: []packtrail.NodeDef{
			{ID: "fo", Type: "fanout", Branches: []string{"a", "b"}},
			{ID: "a", Type: "task", Subject: "tasks.a"},
			{ID: "b", Type: "task", Subject: "tasks.b"},
			{ID: "join", Type: "fanin", WaitFor: []string{"a", "b"}},
		},
		Edges: []packtrail.EdgeDef{{From: "fo", To: "join"}},
	}

	s, err := packtrail.New(srv.NC, packtrail.WithNamespace("flowdef-copy"), packtrail.WithFlowDef(def))
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	def.Nodes[0].Branches[0] = "mutated"
	def.Nodes[3].WaitFor[0] = "mutated"

	graph, err := s.FlowGraph(context.Background(), "immutable")
	if err != nil {
		t.Fatalf("flow graph: %v", err)
	}

	var fanout, fanin *packtrail.GraphNode

	for i := range graph.Nodes {
		switch graph.Nodes[i].ID {
		case "fo":
			fanout = &graph.Nodes[i]
		case "join":
			fanin = &graph.Nodes[i]
		}
	}

	if fanout == nil || fanin == nil {
		t.Fatalf("graph missing fan nodes: %+v", graph.Nodes)
	}

	if fanout.Branches[0] != "a" || fanin.WaitFor[0] != "a" {
		t.Fatalf("flow graph was mutated through caller slices: fanout=%v fanin=%v", fanout.Branches, fanin.WaitFor)
	}
}

func TestFlowDefVersionAndChoiceOnErrorFail(t *testing.T) {
	srv := natstest.Start(t)

	s, err := packtrail.New(srv.NC,
		packtrail.WithNamespace("flowdef-parity"),
		packtrail.WithFlowDef(packtrail.FlowDef{
			Version: "1.0",
			Name:    "choice-onerror-def",
			Nodes: []packtrail.NodeDef{
				{
					ID: "route", Type: "choice", OnError: "fail",
					Rules: []packtrail.RuleDef{
						{When: "input.n > input.s", To: "hi"},
						{Default: true, To: "lo"},
					},
				},
				{ID: "hi", Type: "task", Invoker: "custom", Target: "hi"},
				{ID: "lo", Type: "task", Invoker: "custom", Target: "lo"},
			},
		}),
		packtrail.WithInvoker("custom", packtrail.InvokerFunc(
			func(_ context.Context, req packtrail.Request) (packtrail.Result, error) {
				return packtrail.Result{Status: packtrail.StatusOK, Payload: req.Payload}, nil
			})),
	)
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	graph, err := s.FlowGraph(ctx, "choice-onerror-def")
	if err != nil {
		t.Fatalf("flow graph: %v", err)
	}

	if graph.Version != "1.0" {
		t.Fatalf("graph version = %q, want 1.0", graph.Version)
	}

	var route *packtrail.GraphNode

	for i := range graph.Nodes {
		if graph.Nodes[i].ID == "route" {
			route = &graph.Nodes[i]
			break
		}
	}

	if route == nil || route.OnError != "fail" {
		t.Fatalf("route graph node = %+v, want on_error fail", route)
	}

	go func() { _ = s.Run(ctx) }()

	id, err := s.Start(ctx, "choice-onerror-def", []byte(`{"n":1,"s":"a"}`))
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ex, getErr := s.Get(ctx, id)
		if getErr == nil && ex.Status == packtrail.ExecFailed {
			if !strings.Contains(ex.Error, "evaluation error") {
				t.Fatalf("error = %q, want choice evaluation error", ex.Error)
			}

			return
		}

		time.Sleep(15 * time.Millisecond)
	}

	t.Fatal("execution did not fail on choice evaluation error")
}
