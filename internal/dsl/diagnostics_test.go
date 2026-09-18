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
	"fmt"
	"strings"
	"testing"
)

// TestNoStartNodeNamesTheReferences: "every node has an inbound transition" is a
// true statement about the graph and a useless one to act on — the author knows
// which node was meant to start the flow and needs to be told what routes into
// it. The shape below is the one that actually trips this in practice: an
// ordinary retry loop whose "go back and revise" case targets the first node.
func TestNoStartNodeNamesTheReferences(t *testing.T) {
	_, err := Parse([]byte(`
version: "1.0"
name: loop
nodes:
  - {id: draft, type: task, subject: "d"}
  - {id: review, type: task, subject: "r"}
  - id: gate
    type: choice
    rules:
      - {when: 'true', to: draft}
      - {default: true, to: review}
edges:
  - {from: draft, to: review}
  - {from: review, to: gate}
`))
	if err == nil {
		t.Fatal("a flow with no start node must be rejected")
	}

	msg := err.Error()

	// The culprit: draft is the intended start, and the gate's rule is what
	// disqualified it. Naming both is the whole point of the change.
	if !strings.Contains(msg, `"draft" by the rule of "gate"`) {
		t.Errorf("error must name the reference that consumed the start node, got %v", err)
	}

	// The other nodes' references are listed too, in declaration order, so a
	// longer cycle can be read off the message rather than re-derived by hand.
	if !strings.Contains(msg, `"review" by the edge of "draft"`) {
		t.Errorf("error must list every node's inbound reference, got %v", err)
	}

	if !strings.Contains(msg, `"gate" by the edge of "review"`) {
		t.Errorf("error must list every node's inbound reference, got %v", err)
	}
}

// TestNoStartNodeListingIsBounded: the listing must stay an error message rather
// than a graph dump, so a large flow is truncated.
func TestNoStartNodeListingIsBounded(t *testing.T) {
	const n = maxReportedRefs + 5

	var b strings.Builder

	fmt.Fprint(&b, "version: \"1.0\"\nname: big\nnodes:\n")

	for i := range n {
		fmt.Fprintf(&b, "  - {id: n%d, type: task, subject: \"s\"}\n", i)
	}

	// A full cycle: every node routed into by the one before it, and the first
	// by the last — so no node is left without an inbound transition.
	fmt.Fprint(&b, "edges:\n")

	for i := range n {
		fmt.Fprintf(&b, "  - {from: n%d, to: n%d}\n", i, (i+1)%n)
	}

	_, err := Parse([]byte(b.String()))
	if err == nil {
		t.Fatal("a fully cyclic flow must be rejected for having no start node")
	}

	if !strings.Contains(err.Error(), "and 5 more") {
		t.Errorf("long listing must be truncated, got %v", err)
	}
}

// TestValidateRejectsBackoffWithoutAttempts: attemptBudget reads max_attempts <= 0
// as a single attempt, so `retry: {backoff: exponential}` disables the retries it
// appears to configure — silently, at run time, on the first transient fault.
func TestValidateRejectsBackoffWithoutAttempts(t *testing.T) {
	_, err := Parse([]byte(`
version: "1.0"
name: contradictory-retry
nodes:
  - {id: a, type: task, subject: "x", retry: {backoff: exponential}}
`))
	if err == nil || !strings.Contains(err.Error(), "declares backoff") {
		t.Fatalf("err = %v, want backoff-without-attempts rejection", err)
	}

	// It must say what the flow would do, not merely that the combination is
	// disallowed: "would run once and never retry" is the surprise.
	if !strings.Contains(err.Error(), "never retry") {
		t.Errorf("error should state the effective behaviour, got %v", err)
	}
}

// TestValidateAcceptsRetryWithoutBackoff: the inverse is not a contradiction —
// max_attempts with no backoff is a complete statement (backoff defaults to
// fixed), and must keep parsing.
func TestValidateAcceptsRetryWithoutBackoff(t *testing.T) {
	if _, err := Parse([]byte(`
version: "1.0"
name: plain-retry
nodes:
  - {id: a, type: task, subject: "x", retry: {max_attempts: 3}}
`)); err != nil {
		t.Fatalf("max_attempts without backoff must stay valid, got %v", err)
	}
}

// TestValidateAcceptsNoRetryBlock: a node declaring nothing keeps the engine's
// "one attempt" default. Only the self-contradictory combination is rejected;
// this fix must not become a back door to requiring retries.
func TestValidateAcceptsNoRetryBlock(t *testing.T) {
	if _, err := Parse([]byte(`
version: "1.0"
name: no-retry
nodes:
  - {id: a, type: task, subject: "x"}
`)); err != nil {
		t.Fatalf("a node with no retry block must stay valid, got %v", err)
	}
}
