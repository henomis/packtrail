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
	"strings"
	"testing"

	"github.com/henomis/packtrail"
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
