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

package dsl

import "testing"

const researchPipeline = `
version: "1.0"
name: research-pipeline
nodes:
  - id: triage
    type: task
    subject: tasks.triage.{execution_id}
    timeout: 30s
    retry:
      max_attempts: 3
      backoff: exponential
  - id: fanout-research
    type: fanout
    branches: [tech-research, market-research, legal-research]
  - id: tech-research
    type: task
    subject: tasks.tech-research.{execution_id}
  - id: market-research
    type: task
    subject: tasks.market-research.{execution_id}
  - id: legal-research
    type: task
    subject: tasks.legal-research.{execution_id}
  - id: join-research
    type: fanin
    wait_for: [tech-research, market-research, legal-research]
    join_policy: all
  - id: route
    type: choice
    rules:
      - when: "results.triage.risk_score > 80"
        to: escalation
      - default: true
        to: synthesis
  - id: synthesis
    type: task
    subject: tasks.synthesis.{execution_id}
  - id: wait-approval
    type: signal
    signal_name: approval
    timeout: 24h
    on_timeout: escalation
  - id: escalation
    type: task
    subject: tasks.escalation.{execution_id}
edges:
  - from: triage
    to: fanout-research
  - from: fanout-research
    to: join-research
  - from: join-research
    to: route
  - from: synthesis
    to: wait-approval
`

func TestParseResearchPipeline(t *testing.T) {
	f, err := Parse([]byte(researchPipeline))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if f.StartNode() != "triage" {
		t.Errorf("start node = %q, want triage", f.StartNode())
	}

	if got := f.Successor("triage"); got != "fanout-research" {
		t.Errorf("successor(triage) = %q, want fanout-research", got)
	}

	if got := f.Successor("escalation"); got != "" {
		t.Errorf("successor(escalation) = %q, want terminal", got)
	}

	if got := f.Node("triage").Timeout.D().String(); got != "30s" {
		t.Errorf("triage timeout = %q, want 30s", got)
	}

	if k, _ := f.Node("join-research").JoinKind(); k != JoinAll {
		t.Errorf("join kind = %q, want all", k)
	}

	if got := ResolvePlaceholders(f.Node("triage").InvokeTarget(), "exec-1"); got != "tasks.triage.exec-1" {
		t.Errorf("resolved target = %q", got)
	}
}

func TestJoinKind(t *testing.T) {
	cases := map[string]struct {
		kind   string
		quorum int
	}{
		"":         {JoinAll, 0},
		"all":      {JoinAll, 0},
		"any":      {JoinAny, 0},
		"quorum:2": {JoinQuorum, 2},
	}
	for jp, want := range cases {
		n := Node{JoinPolicy: jp}

		k, q := n.JoinKind()
		if k != want.kind || q != want.quorum {
			t.Errorf("JoinKind(%q) = (%q,%d), want (%q,%d)", jp, k, q, want.kind, want.quorum)
		}
	}
}

func TestValidateErrors(t *testing.T) {
	cases := map[string]string{
		"dup node": `
name: f
nodes:
  - {id: a, type: task, subject: s}
  - {id: a, type: task, subject: s}`,
		"task missing subject": `
name: f
nodes:
  - {id: a, type: task}`,
		"fanin bad quorum": `
name: f
nodes:
  - {id: a, type: task, subject: s}
  - {id: j, type: fanin, wait_for: [a], join_policy: "quorum:5"}
edges:
  - {from: a, to: j}`,
		"choice no default": `
name: f
nodes:
  - {id: a, type: task, subject: s}
  - {id: c, type: choice, rules: [{when: "x > 1", to: a}]}
edges:
  - {from: a, to: c}`,
		"multiple starts": `
name: f
nodes:
  - {id: a, type: task, subject: s}
  - {id: b, type: task, subject: s}`,
		"edge unknown node": `
name: f
nodes:
  - {id: a, type: task, subject: s}
edges:
  - {from: a, to: nope}`,
	}
	for name, yml := range cases {
		if _, err := Parse([]byte(yml)); err == nil {
			t.Errorf("%s: expected error, got nil", name)
		}
	}
}
