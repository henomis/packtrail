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

package flow

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

const fullFlow = `
version: "1"
name: review
channels:
  notes: {reducer: append}
  total: {reducer: sum, default: 0}
  meta: {reducer: merge}
  last: {}
budget: {tokens: 1000}
search_attributes: {customer: input.customer}
nodes:
  - id: draft
    type: task
    kind: writer
    timeout: 30s
    retry: {max_attempts: 3, backoff: exponential, delay: 1s, max_delay: 1m}
    output_schema: {type: object, required: [text]}
    cache: {ttl: 1h}
    concurrency: {key: input.customer, max: 2}
    dynamic: [route]
    next: route
  - id: route
    type: choice
    rules:
      - {when: "results.draft.score > 5", to: par}
      - {default: true, to: approve}
  - id: par
    type: fanout
    branches: [a, b, c]
    next: j
  - {id: a, type: task, kind: k}
  - {id: b, type: task, kind: k}
  - {id: c, type: task, kind: k}
  - id: j
    type: join
    policy: quorum:2
    next: approve
  - id: approve
    type: await
    signal: approval
    timeout: 1h
    on_timeout: items
    next: items
  - id: items
    type: map
    kind: item-worker
    over: input.items
    max_parallel: 4
    next: child
  - id: child
    type: subflow
    flow: other
    input: input.child
    on_parent_close: abandon
`

func TestParseFull(t *testing.T) {
	f, err := Parse([]byte(fullFlow))
	if err != nil {
		t.Fatal(err)
	}

	if f.StartNode() != "draft" {
		t.Fatalf("start = %q", f.StartNode())
	}

	if f.FanoutOf("b") != "par" || f.JoinOf("par") != "j" {
		t.Fatal("fan indexes not built")
	}

	if got := f.WaitFor("j"); len(got) != 3 {
		t.Fatalf("WaitFor default = %v", got)
	}

	if kind, q := f.Node("j").JoinKind(); kind != JoinQuorum || q != 2 {
		t.Fatalf("join kind = %s %d", kind, q)
	}

	if f.Node("draft").Timeout.D() != 30*time.Second || f.Node("draft").MaxAttempts() != 3 {
		t.Fatal("task policy not decoded")
	}

	if f.Node("route").RulePrograms()[0] == nil || f.Node("items").OverProgram() == nil {
		t.Fatal("expressions not compiled")
	}

	if f.Node("child").ParentClose() != ParentCloseAbandon || f.AttrProgram("customer") == nil {
		t.Fatal("subflow/attrs not decoded")
	}
}

func TestHashStableAcrossYAMLAndJSON(t *testing.T) {
	f, err := Parse([]byte(fullFlow))
	if err != nil {
		t.Fatal(err)
	}

	h1, err := f.Hash()
	if err != nil {
		t.Fatal(err)
	}

	b, _ := f.JSON()

	g, err := ParseJSON(b)
	if err != nil {
		t.Fatal(err)
	}

	h2, _ := g.Hash()
	if h1 != h2 || len(h1) != 16 {
		t.Fatalf("hash %q != %q", h1, h2)
	}

	g.Nodes[0].Kind = "other"
	if h3, _ := g.Hash(); h3 == h1 {
		t.Fatal("hash ignores content")
	}

	c, err := f.Clone()
	if err != nil || c.Node("par") == f.Node("par") {
		t.Fatal("clone must be deep")
	}
}

func TestOutputSchema(t *testing.T) {
	f, err := Parse([]byte(fullFlow))
	if err != nil {
		t.Fatal(err)
	}

	n := f.Node("draft")
	if err = n.ValidateOutput([]byte(`{"text":"x"}`)); err != nil {
		t.Fatal(err)
	}

	if err = n.ValidateOutput([]byte(`{"nope":1}`)); err == nil {
		t.Fatal("schema not enforced")
	}

	if err = f.Node("a").ValidateOutput([]byte(`42`)); err != nil {
		t.Fatal("node without schema must accept anything")
	}
}

func TestBackoffBounded(t *testing.T) {
	n := &Node{Retry: &Retry{
		MaxAttempts: 64, Backoff: BackoffExponential, Delay: Duration(time.Second),
		MaxDelay: Duration(time.Hour),
	}}

	prev := time.Duration(0)

	for a := 2; a <= 200; a++ {
		d := n.Backoff(a)
		if d < time.Second || d > time.Hour || d < prev {
			t.Fatalf("attempt %d: backoff %v out of bounds", a, d)
		}

		prev = d
	}

	if n.Backoff(2) != time.Second || n.Backoff(3) != 2*time.Second {
		t.Fatal("exponential backoff does not double")
	}

	lin := &Node{Retry: &Retry{MaxAttempts: 5, Backoff: BackoffLinear, Delay: Duration(time.Second)}}
	if lin.Backoff(4) != 3*time.Second {
		t.Fatalf("linear = %v", lin.Backoff(4))
	}

	if (&Node{}).Backoff(3) != 0 {
		t.Fatal("no retry policy means no backoff")
	}
}

func TestParseStrict(t *testing.T) {
	if _, err := Parse([]byte("name: x\nnodes: [{id: a, type: task, kind: k, retires: 3}]")); err == nil {
		t.Fatal("unknown field accepted")
	}

	if _, err := Parse([]byte("name: x\nnodes: [{id: a, type: task, kind: k}]\n---\nname: y\n")); err == nil {
		t.Fatal("multi-document accepted")
	}

	if _, err := Parse([]byte("")); err == nil {
		t.Fatal("empty accepted")
	}

	if _, err := Parse([]byte("name: x\nnodes: [{id: a, type: task, kind: k}]\n---\n")); err != nil {
		t.Fatalf("trailing separator rejected: %v", err)
	}
}

func TestValidateRejects(t *testing.T) {
	cases := []struct{ name, yaml, want string }{
		{"bad name", "name: a.b\nnodes: [{id: a, type: task, kind: k}]", "invalid flow name"},
		{"version", "version: '2'\nname: x\nnodes: [{id: a, type: task, kind: k}]", "unsupported version"},
		{"no nodes", "name: x\nnodes: []", "no nodes"},
		{"dup id", "name: x\nnodes: [{id: a, type: task, kind: k}, {id: a, type: task, kind: k}]", "duplicate node id"},
		{"bad id", "name: x\nnodes: [{id: 'a*', type: task, kind: k}]", "invalid node id"},
		{"unknown type", "name: x\nnodes: [{id: a, type: nope}]", "unknown type"},
		{"no kind", "name: x\nnodes: [{id: a, type: task}]", "invalid worker kind"},
		{"foreign field", "name: x\nnodes: [{id: a, type: task, kind: k, branches: [a]}]", "do not apply"},
		{"unknown next", "name: x\nnodes: [{id: a, type: task, kind: k, next: zz}]", "unknown node"},
		{"self next", "name: x\nstart: a\nnodes: [{id: a, type: task, kind: k, next: a}]", "itself"},
		{"retry cap", "name: x\nnodes: [{id: a, type: task, kind: k, retry: {max_attempts: 65}}]", "between 0 and 64"},
		{"retry contradiction", "name: x\nnodes: [{id: a, type: task, kind: k, retry: {backoff: fixed}}]", "never retry"},
		{"bad backoff", "name: x\nnodes: [{id: a, type: task, kind: k, retry: {max_attempts: 2, backoff: x}}]", "unknown retry.backoff"},
		{"bad schema", "name: x\nnodes: [{id: a, type: task, kind: k, output_schema: {type: 12}}]", "output_schema"},
		{"remote ref", "name: x\nnodes: [{id: a, type: task, kind: k, output_schema: {$ref: 'file:///etc/passwd'}}]", "output_schema"},
		{"cache ttl", "name: x\nnodes: [{id: a, type: task, kind: k, cache: {ttl: 0s}}]", "cache.ttl"},
		{"conc max", "name: x\nnodes: [{id: a, type: task, kind: k, concurrency: {key: input.x, max: 0}}]", "concurrency.max"},
		{"choice no default", "name: x\nnodes: [{id: c, type: choice, rules: [{when: 'input.x', to: a}]}, {id: a, type: task, kind: k}]", "exactly one default"},
		{"choice two defaults", "name: x\nnodes: [{id: c, type: choice, rules: [{default: true, to: a}, {default: true, to: a}]}, {id: a, type: task, kind: k}]", "exactly one default"},
		{"choice default with when", "name: x\nnodes: [{id: c, type: choice, rules: [{default: true, when: 'true', to: a}]}, {id: a, type: task, kind: k}]", "must not also carry"},
		{"choice bad expr", "name: x\nnodes: [{id: c, type: choice, rules: [{when: 'input.a + input.b > 1', to: a}, {default: true, to: a}]}, {id: a, type: task, kind: k}]", "concatenation"},
		{"choice next", "name: x\nnodes: [{id: c, type: choice, next: a, rules: [{default: true, to: a}]}, {id: a, type: task, kind: k}]", "do not apply"},
		{"choice on_error", "name: x\nnodes: [{id: c, type: choice, on_error: x, rules: [{default: true, to: a}]}, {id: a, type: task, kind: k}]", "on_error"},
		{"fanout to task", "name: x\nnodes: [{id: f, type: fanout, branches: [a], next: t}, {id: a, type: task, kind: k}, {id: t, type: task, kind: k}]", "must be a join"},
		{"fanout no next", "name: x\nnodes: [{id: f, type: fanout, branches: [a]}, {id: a, type: task, kind: k}]", "next is required"},
		{"branch twice", "name: x\nnodes: [{id: f, type: fanout, branches: [a, a], next: j}, {id: a, type: task, kind: k}, {id: j, type: join}]", "twice"},
		{"shared branch", fanYAML("[{id: f, type: fanout, branches: [a], next: j}, {id: g, type: fanout, branches: [a], next: j2}, {id: a, type: task, kind: k}, {id: j, type: join, next: g}, {id: j2, type: join}]"), "at most one fanout"},
		{"branch non-task", "name: x\nnodes: [{id: f, type: fanout, branches: [c], next: j}, {id: c, type: choice, rules: [{default: true, to: j}]}, {id: j, type: join}]", "must be task nodes"},
		{"on_failure unknown", "name: x\nnodes: [{id: a, type: task, kind: k, on_failure: zz}]", "on_failure references unknown node"},
		{"on_failure self", "name: x\nstart: a\nnodes: [{id: a, type: task, kind: k, on_failure: a}]", "on_failure points to itself"},
		{"on_failure on await", "name: x\nnodes: [{id: w, type: await, signal: s, timeout: 1m, on_failure: a}, {id: a, type: task, kind: k}]", "do not apply"},
		{"on_failure on branch", "name: x\nnodes: [{id: f, type: fanout, branches: [a], next: j}, {id: a, type: task, kind: k, on_failure: h}, {id: j, type: join}, {id: h, type: task, kind: k}]", "settled by its join's policy"},
		{"on_failure into branch", "name: x\nnodes: [{id: t, type: task, kind: k, on_failure: a, next: f}, {id: f, type: fanout, branches: [a], next: j}, {id: a, type: task, kind: k}, {id: j, type: join}]", "a branch of fanout"},
		{"branch exit", "name: x\nnodes: [{id: f, type: fanout, branches: [a], next: j}, {id: a, type: task, kind: k, next: t}, {id: j, type: join, next: t}, {id: t, type: task, kind: k}]", "routes onward"},
		{"wait foreign", "name: x\nnodes: [{id: f, type: fanout, branches: [a], next: j}, {id: a, type: task, kind: k}, {id: j, type: join, wait_for: [t], next: t}, {id: t, type: task, kind: k}]", "not a branch of its fanout"},
		{"wait dup", "name: x\nnodes: [{id: f, type: fanout, branches: [a], next: j}, {id: a, type: task, kind: k}, {id: j, type: join, wait_for: [a, a]}]", "twice"},
		{"quorum too big", "name: x\nnodes: [{id: f, type: fanout, branches: [a], next: j}, {id: a, type: task, kind: k}, {id: j, type: join, policy: 'quorum:2'}]", "exceeds"},
		{"bad policy", "name: x\nnodes: [{id: f, type: fanout, branches: [a], next: j}, {id: a, type: task, kind: k}, {id: j, type: join, policy: some}]", "unknown policy"},
		{"orphan join", "name: x\nnodes: [{id: t, type: task, kind: k, next: j}, {id: j, type: join}]", "join"},
		{"route into branch", "name: x\nnodes: [{id: t, type: task, kind: k, next: f}, {id: f, type: fanout, branches: [a], next: j}, {id: a, type: task, kind: k}, {id: j, type: join}, {id: c, type: choice, rules: [{default: true, to: a}]}]", "a branch of fanout"},
		{"await no timeout", "name: x\nnodes: [{id: w, type: await, signal: s}]", "timeout is required"},
		{"await bad signal", "name: x\nnodes: [{id: w, type: await, signal: 'a.b', timeout: 1s}]", "invalid signal name"},
		{"map bad over", "name: x\nnodes: [{id: m, type: map, kind: k, over: '1..10'}]", "range"},
		{"map no kind", "name: x\nnodes: [{id: m, type: map, over: input.x}]", "invalid worker kind"},
		{"subflow policy", "name: x\nnodes: [{id: s, type: subflow, flow: y, on_parent_close: x}]", "on_parent_close"},
		{"no start", "name: x\nnodes: [{id: a, type: task, kind: k, next: b}, {id: b, type: task, kind: k, next: a}]", "no start node"},
		{"two starts", "name: x\nnodes: [{id: a, type: task, kind: k}, {id: b, type: task, kind: k}]", "multiple start nodes"},
		{"unreachable", "name: x\nstart: a\nnodes: [{id: a, type: task, kind: k}, {id: b, type: task, kind: k, next: a}]", "unreachable"},
		{"channel reducer", "name: x\nchannels: {c: {reducer: max}}\nnodes: [{id: a, type: task, kind: k}]", "unknown reducer"},
		{"channel default", "name: x\nchannels: {c: {reducer: append, default: 3}}\nnodes: [{id: a, type: task, kind: k}]", "does not fit"},
		{"budget", "name: x\nbudget: {tokens: 0}\nnodes: [{id: a, type: task, kind: k}]", "must be positive"},
		{"map parallel cap", "name: x\nnodes: [{id: m, type: map, kind: k, over: input.x, max_parallel: 257}]", "max_parallel"},
		{"fanout width cap", fanWide(257), "at most 256 branches"},
		{"dynamic unknown", "name: x\nnodes: [{id: a, type: task, kind: k, dynamic: [zz]}]", "unknown node"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := Parse([]byte(c.yaml))
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("err = %v, want containing %q", err, c.want)
			}
		})
	}
}

func fanYAML(nodes string) string { return "name: x\nstart: f\nnodes: " + nodes }

func TestLoopsAreAllowed(t *testing.T) {
	// Per-visit state (generations) makes revisiting a fan legal.
	y := `
name: loop
start: f
nodes:
  - {id: f, type: fanout, branches: [a, b], next: j}
  - {id: a, type: task, kind: k}
  - {id: b, type: task, kind: k}
  - {id: j, type: join, next: c}
  - id: c
    type: choice
    rules: [{when: "visits.f < 3", to: f}, {default: true, to: done}]
  - {id: done, type: task, kind: k}
`
	if _, err := Parse([]byte(y)); err != nil {
		t.Fatal(err)
	}
}

func fanWide(n int) string {
	var b strings.Builder

	b.WriteString("name: x\nnodes:\n  - {id: f, type: fanout, next: j, branches: [")

	for i := range n {
		if i > 0 {
			b.WriteString(", ")
		}

		fmt.Fprintf(&b, "b%d", i)
	}

	b.WriteString("]}\n  - {id: j, type: join}\n")

	for i := range n {
		fmt.Fprintf(&b, "  - {id: b%d, type: task, kind: k}\n", i)
	}

	return b.String()
}
