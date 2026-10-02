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

package expr

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func env(t *testing.T, doc string) map[string]any {
	t.Helper()

	var m map[string]any
	if err := json.Unmarshal([]byte(doc), &m); err != nil {
		t.Fatal(err)
	}

	return m
}

func TestMatch(t *testing.T) {
	p, err := CompilePredicate("results.triage.risk_score > 80")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cases := []struct {
		doc  string
		want bool
	}{
		{`{"results":{"triage":{"risk_score": 90}}}`, true},
		{`{"results":{"triage":{"risk_score": 50}}}`, false},
		{`{"results":{"triage":{"risk_score": 80}}}`, false},
	}
	for _, c := range cases {
		got, lerr := p.Match(context.Background(), env(t, c.doc))
		if lerr != nil || got != c.want {
			t.Errorf("match %s = %v, %v; want %v", c.doc, got, lerr, c.want)
		}
	}
}

func TestMatchInputSignalsChannels(t *testing.T) {
	p, err := CompilePredicate(`input.tier == "pro" && signals.approval.granted && channels.count >= 2`)
	if err != nil {
		t.Fatal(err)
	}

	got, err := p.Match(context.Background(),
		env(t, `{"input":{"tier":"pro"},"signals":{"approval":{"granted":true}},"channels":{"count":2}}`))
	if err != nil || !got {
		t.Fatalf("match = %v, %v", got, err)
	}
}

func TestMatchMissingFieldErrors(t *testing.T) {
	p, _ := CompilePredicate("results.triage.risk_score > 80")
	if _, err := p.Match(context.Background(), env(t, `{"results":{}}`)); err == nil {
		t.Error("expected error for missing field")
	}
}

func TestCompileInvalid(t *testing.T) {
	if _, err := CompilePredicate("results.x >"); err == nil {
		t.Error("expected compile error")
	}
}

func TestCompileRejectsUnboundedExpressions(t *testing.T) {
	cases := []struct{ code, wantErr string }{
		{"len(1..1000) > 0", "range expressions are not allowed"},
		{"all(input.items, #.ok)", "iteration expressions are not allowed"},
		{`upper(input.name) == "X"`, `builtin "upper" is not allowed`},
		{"sort(input.items) != nil", "function calls other than len() are not allowed"},
		{`input.name + input.name != ""`, "concatenation and slicing are not allowed"},
		{"input.x + input.y > 0", "concatenation and slicing are not allowed"},
		{"input.items[1:2] != nil", "concatenation and slicing are not allowed"},
	}

	for _, tt := range cases {
		if _, err := CompilePredicate(tt.code); err == nil || !strings.Contains(err.Error(), tt.wantErr) {
			t.Errorf("Compile(%q) err = %v, want %q", tt.code, err, tt.wantErr)
		}
	}
}

func TestCompileAllowsLenMembershipRegex(t *testing.T) {
	p, err := CompilePredicate(`len(input.items) > 0 && input.tier in ["pro"] && input.name matches "^acme-"`)
	if err != nil {
		t.Fatal(err)
	}

	got, err := p.Match(context.Background(), env(t, `{"input":{"items":[1],"tier":"pro","name":"acme-x"}}`))
	if err != nil || !got {
		t.Fatalf("match = %v, %v", got, err)
	}
}

func TestMatchLastNodeBranchesVisits(t *testing.T) {
	p, err := CompilePredicate(`results[last_node] == "yes" && branches.b1 == "completed" && visits.review == 2`)
	if err != nil {
		t.Fatal(err)
	}

	doc := `{"results":{"review":"yes"},"branches":{"b1":"completed"},"last_node":"review","visits":{"review":2}}`
	if got, ierr := p.Match(context.Background(), env(t, doc)); ierr != nil || !got {
		t.Fatalf("match = %v, %v", got, ierr)
	}
}

func TestEvalValue(t *testing.T) {
	p, err := CompileValue("input.items")
	if err != nil {
		t.Fatal(err)
	}

	out, err := p.Eval(context.Background(), env(t, `{"input":{"items":[1,2,3]}}`))
	if err != nil {
		t.Fatal(err)
	}

	if l, ok := out.([]any); !ok || len(l) != 3 {
		t.Fatalf("out = %#v", out)
	}
}

func TestPredicateNonBool(t *testing.T) {
	if _, err := CompilePredicate("input.x"); err != nil {
		t.Fatal(err)
	}
}

func TestEvalContextCancelled(t *testing.T) {
	p, _ := CompilePredicate("input.x == 1")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := p.Match(ctx, env(t, `{"input":{"x":1}}`)); err == nil || !strings.Contains(err.Error(), "canceled") {
		t.Fatalf("err = %v", err)
	}
}

func TestMatchMemoryBudget(t *testing.T) {
	var b strings.Builder

	b.WriteByte('[')

	for i := range 600 {
		if i > 0 {
			b.WriteByte(',')
		}

		b.WriteString("input.x")
	}

	b.WriteString("] != nil")

	p, err := CompilePredicate(b.String())
	if err != nil {
		t.Fatal(err)
	}

	if _, err = p.Match(context.Background(), env(t, `{"input":{"x":1}}`)); err == nil ||
		!strings.Contains(err.Error(), "memory budget exceeded") {
		t.Fatalf("err = %v, want memory budget exceeded", err)
	}
}
