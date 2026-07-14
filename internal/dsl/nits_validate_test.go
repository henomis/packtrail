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

import (
	"strings"
	"testing"
)

// TestValidateRejectsUnreachableNode: a node no transition can ever reach is
// dead configuration — almost always a typo'd edge or rule target. A node with
// no inbound transition at all is already caught by start-node detection, so
// the interesting case is an island: nodes referencing each other but detached
// from the start node.
// TestValidateRejectsMultipleDefaults: two default rules silently last-win at
// runtime, so validation must reject the ambiguity at load time.
func TestValidateRejectsMultipleDefaults(t *testing.T) {
	_, err := Parse([]byte(`
name: two-defaults
nodes:
  - {id: a, type: task, subject: "x"}
  - {id: b, type: task, subject: "y"}
  - {id: c, type: task, subject: "z"}
  - {id: d, type: choice, rules: [{default: true, to: b}, {default: true, to: c}]}
edges:
  - {from: a, to: d}
`))
	if err == nil || !strings.Contains(err.Error(), "only one default") {
		t.Fatalf("err = %v, want multiple-default rejection", err)
	}
}

// TestValidateRejectsDefaultWithWhen: a default rule that also carries a when
// silently ignores the when at runtime, so it is rejected as ambiguous.
func TestValidateRejectsDefaultWithWhen(t *testing.T) {
	_, err := Parse([]byte(`
name: default-with-when
nodes:
  - {id: a, type: task, subject: "x"}
  - {id: b, type: task, subject: "y"}
  - {id: d, type: choice, rules: [{when: 'input.x == 1', default: true, to: b}]}
edges:
  - {from: a, to: d}
`))
	if err == nil || !strings.Contains(err.Error(), "must not also carry a when") {
		t.Fatalf("err = %v, want default-with-when rejection", err)
	}
}

func TestValidateRejectsUnreachableNode(t *testing.T) {
	_, err := Parse([]byte(`
name: island
nodes:
  - {id: a, type: task, subject: "x"}
  - {id: b, type: task, subject: "y"}
  - {id: c, type: task, subject: "z"}
  - {id: d, type: choice, rules: [{default: true, to: c}]}
edges:
  - {from: a, to: b}
  - {from: c, to: d}
`))
	if err == nil || !strings.Contains(err.Error(), "unreachable node(s) [c d]") {
		t.Fatalf("err = %v, want unreachable-node rejection naming [c d]", err)
	}
}

// TestValidateReachabilityFollowsAllTransitions: choice rules, on_timeout and
// fanout branches all count as reachability edges — a flow wired only through
// them must stay valid.
func TestValidateReachabilityFollowsAllTransitions(t *testing.T) {
	if _, err := Parse([]byte(`
name: all-routes
nodes:
  - {id: pick, type: choice, rules: [{when: 'input.x == 1', to: gate}, {default: true, to: fo}]}
  - {id: gate, type: signal, signal_name: go, timeout: 1m, on_timeout: fallback}
  - {id: fallback, type: task, subject: "f"}
  - {id: fo, type: fanout, branches: [b1]}
  - {id: b1, type: task, subject: "x"}
  - {id: j, type: fanin, wait_for: [b1]}
edges:
  - {from: fo, to: j}
`)); err != nil {
		t.Fatalf("fully-wired flow rejected: %v", err)
	}
}

// TestValidateRejectsOnTimeoutWithoutTimeout: the wait schedule is only
// installed for a positive timeout, so on_timeout without one can never fire.
func TestValidateRejectsOnTimeoutWithoutTimeout(t *testing.T) {
	_, err := Parse([]byte(`
name: dead-route
nodes:
  - {id: gate, type: signal, signal_name: go, on_timeout: fallback}
  - {id: fallback, type: task, subject: "f"}
`))
	if err == nil || !strings.Contains(err.Error(), "requires a positive timeout") {
		t.Fatalf("err = %v, want dead on_timeout rejection", err)
	}
}

// TestValidateRejectsWildcardSubject: a nats-task subject with wildcard or
// whitespace characters can never be published to.
func TestValidateRejectsWildcardSubject(t *testing.T) {
	for _, subject := range []string{"tasks.>", "tasks.*.go", "tasks. spaced"} {
		_, err := Parse([]byte(`
name: bad-subject
nodes:
  - {id: a, type: task, subject: "` + subject + `"}
`))
		if err == nil || !strings.Contains(err.Error(), "NATS request subject") {
			t.Errorf("subject %q: err = %v, want bad-subject rejection", subject, err)
		}
	}
}

func TestValidateRejectsMalformedSubjectTokens(t *testing.T) {
	for _, subject := range []string{"tasks..notify", ".tasks.notify", "tasks.notify.", "tasks.\nnotify"} {
		_, err := Parse([]byte(`
name: bad-subject-shape
nodes:
  - {id: a, type: task, subject: "` + subject + `"}
`))
		if err == nil || !strings.Contains(err.Error(), "NATS request subject") {
			t.Errorf("subject %q: err = %v, want malformed-subject rejection", subject, err)
		}
	}
}

// TestValidateAllowsPlaceholderSubject: the {execution_id} placeholder is part
// of the nats-task contract and must stay legal.
func TestValidateAllowsPlaceholderSubject(t *testing.T) {
	if _, err := Parse([]byte(`
name: placeholder
nodes:
  - {id: a, type: task, subject: "tasks.notify.{execution_id}"}
`)); err != nil {
		t.Fatalf("placeholder subject rejected: %v", err)
	}
}

// TestValidateAllowsFreeFormCustomTarget: custom invoker kinds interpret Target
// freely (it may be a URL), so the subject check applies to nats-task only.
func TestValidateAllowsFreeFormCustomTarget(t *testing.T) {
	if _, err := Parse([]byte(`
name: custom-target
nodes:
  - {id: a, type: task, invoker: http, target: "https://example.com/hook?x=1 y"}
`)); err != nil {
		t.Fatalf("custom target rejected: %v", err)
	}
}

func TestValidateRejectsUnknownJoinPolicy(t *testing.T) {
	for _, jp := range []string{"majority", "quorm:2", "Any", "banana"} {
		_, err := Parse([]byte(`
name: bad-join
nodes:
  - {id: fo, type: fanout, branches: [b1, b2]}
  - {id: b1, type: task, subject: "x"}
  - {id: b2, type: task, subject: "y"}
  - {id: j, type: fanin, wait_for: [b1, b2], join_policy: "` + jp + `"}
edges:
  - {from: fo, to: j}
`))
		if err == nil || !strings.Contains(err.Error(), "unknown join_policy") {
			t.Fatalf("join_policy %q: err = %v, want unknown-join_policy rejection", jp, err)
		}
	}
}

func TestValidateRejectsSelfEdge(t *testing.T) {
	_, err := Parse([]byte(`
name: self-loop
nodes:
  - {id: a, type: task, subject: "x"}
  - {id: b, type: task, subject: "y"}
edges:
  - {from: a, to: b}
  - {from: b, to: b}
`))
	if err == nil || !strings.Contains(err.Error(), "self-edge") {
		t.Fatalf("err = %v, want self-edge rejection", err)
	}
}

func TestValidateRejectsUnknownBackoff(t *testing.T) {
	_, err := Parse([]byte(`
name: bad-backoff
nodes:
  - {id: a, type: task, subject: "x", retry: {max_attempts: 3, backoff: expontential}}
`))
	if err == nil || !strings.Contains(err.Error(), "unknown retry.backoff") {
		t.Fatalf("err = %v, want unknown-backoff rejection", err)
	}
}

func TestValidateAcceptsKnownBackoffs(t *testing.T) {
	for _, b := range []string{"fixed", "linear", "exponential"} {
		if _, err := Parse([]byte(`
name: ok-backoff
nodes:
  - {id: a, type: task, subject: "x", retry: {max_attempts: 3, backoff: ` + b + `}}
`)); err != nil {
			t.Fatalf("backoff %q rejected: %v", b, err)
		}
	}
}

func TestValidateRejectsNodeReachableOnlyViaBranchEdge(t *testing.T) {
	_, err := Parse([]byte(`
name: dead-via-branch-edge
nodes:
  - id: fo
    type: fanout
    branches: [b1]
  - id: b1
    type: task
    subject: "x"
  - id: j
    type: fanin
    wait_for: [b1]
  - id: ghost
    type: task
    subject: "g"
edges:
  - from: fo
    to: j
  - from: b1
    to: ghost
`))
	if err == nil || !strings.Contains(err.Error(), "unreachable node(s) [ghost]") {
		t.Fatalf("err = %v, want unreachable ghost rejection", err)
	}
}

// TestValidateRejectsUnknownVersion: a version other than SupportedVersion is a
// future/typo'd schema and must fail fast at parse (F-032).
func TestValidateRejectsUnknownVersion(t *testing.T) {
	_, err := Parse([]byte(`
version: "2.0"
name: bad-version
nodes:
  - {id: a, type: task, subject: "x"}
`))
	if err == nil || !strings.Contains(err.Error(), "unsupported version") {
		t.Fatalf("err = %v, want unsupported-version rejection", err)
	}
}

// TestValidateAcceptsVersions: the supported version and an omitted version both
// parse (omitted is accepted with a warning until v1.0) (F-032).
func TestValidateAcceptsVersions(t *testing.T) {
	for _, doc := range []string{
		"version: \"1.0\"\nname: v\nnodes:\n  - {id: a, type: task, subject: \"x\"}\n",
		"name: no-version\nnodes:\n  - {id: a, type: task, subject: \"x\"}\n",
	} {
		if _, err := Parse([]byte(doc)); err != nil {
			t.Fatalf("Parse(%q) = %v, want accepted", doc, err)
		}
	}
}

// TestValidateRejectsUnknownOnError: a choice on_error other than "fail" (or
// omitted) is rejected at parse (F-033).
func TestValidateRejectsUnknownOnError(t *testing.T) {
	_, err := Parse([]byte(`
name: bad-onerror
nodes:
  - {id: a, type: task, subject: "x"}
  - {id: b, type: task, subject: "y"}
  - {id: d, type: choice, on_error: retry, rules: [{default: true, to: b}]}
edges:
  - {from: a, to: d}
`))
	if err == nil || !strings.Contains(err.Error(), "unknown on_error") {
		t.Fatalf("err = %v, want unknown-on_error rejection", err)
	}
}

// TestValidateAcceptsOnErrorFail: on_error: fail is a valid choice option (F-033).
func TestValidateAcceptsOnErrorFail(t *testing.T) {
	_, err := Parse([]byte(`
name: good-onerror
nodes:
  - {id: s, type: task, subject: "s"}
  - {id: a, type: task, subject: "x"}
  - {id: b, type: task, subject: "y"}
  - {id: d, type: choice, on_error: fail, rules: [{when: 'input.x == 1', to: a}, {default: true, to: b}]}
edges:
  - {from: s, to: d}
`))
	if err != nil {
		t.Fatalf("Parse = %v, want accepted", err)
	}
}
