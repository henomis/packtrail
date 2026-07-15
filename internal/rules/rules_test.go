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

package rules

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
)

func TestMatch(t *testing.T) {
	p, err := Compile("results.triage.risk_score > 80")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cases := []struct {
		contextDoc string
		want       bool
	}{
		{`{"input":{},"results":{"triage":{"risk_score": 90}},"signals":{}}`, true},
		{`{"input":{},"results":{"triage":{"risk_score": 50}},"signals":{}}`, false},
		{`{"input":{},"results":{"triage":{"risk_score": 80}},"signals":{}}`, false},
	}
	for _, c := range cases {
		got, matchErr := p.Match(json.RawMessage(c.contextDoc))
		if matchErr != nil {
			t.Errorf("match %s: %v", c.contextDoc, matchErr)
			continue
		}

		if got != c.want {
			t.Errorf("match %s = %v, want %v", c.contextDoc, got, c.want)
		}
	}
}

func TestMatchInputAndSignals(t *testing.T) {
	p, err := Compile(`input.tier == "pro" && signals.approval.granted`)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	doc := `{"input":{"tier":"pro"},"results":{},"signals":{"approval":{"granted":true}}}`

	got, err := p.Match(json.RawMessage(doc))
	if err != nil || !got {
		t.Fatalf("match = %v, %v; want true, nil", got, err)
	}
}

func TestMatchMissingFieldErrors(t *testing.T) {
	p, _ := Compile("results.triage.risk_score > 80")
	// Missing node output: expr errors fetching from nil; callers treat that as
	// no-match and fall through to the default rule.
	if _, err := p.Match(json.RawMessage(`{"input":{},"results":{},"signals":{}}`)); err == nil {
		t.Error("expected error for missing field")
	}
}

func TestCompileInvalid(t *testing.T) {
	if _, err := Compile("results.x >"); err == nil {
		t.Error("expected compile error")
	}
}

func TestCompileRejectsUnboundedExpressions(t *testing.T) {
	cases := []struct {
		name    string
		code    string
		wantErr string
	}{
		{
			name:    "range allocation",
			code:    "len(1..1000) > 0",
			wantErr: "range expressions are not allowed",
		},
		{
			name:    "iteration",
			code:    "all(input.items, #.ok)",
			wantErr: "iteration expressions are not allowed",
		},
		{
			name:    "builtin other than len",
			code:    `upper(input.name) == "X"`,
			wantErr: `builtin "upper" is not allowed`,
		},
		{
			name:    "function call",
			code:    "sort(input.items) != nil",
			wantErr: "function calls other than len() are not allowed",
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Compile(tt.code)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Compile(%q) err = %v, want containing %q", tt.code, err, tt.wantErr)
			}
		})
	}
}

func TestCompileAllowsLenAndMembership(t *testing.T) {
	if _, err := Compile(`len(input.items) > 0 && input.tier in ["pro", "team"]`); err != nil {
		t.Fatalf("compile bounded expression: %v", err)
	}
}

func TestMatchLastNodeAndBranches(t *testing.T) {
	p, err := Compile(`results[last_node] == "yes" && branches.b1 == "ok"`)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	doc := `{"input":{},"results":{"b1":"ok","review":"yes"},"signals":{},"branches":{"b1":"ok"},"last_node":"review"}`

	got, err := p.Match(json.RawMessage(doc))
	if err != nil || !got {
		t.Fatalf("match = %v, %v; want true, nil", got, err)
	}
}

func TestMatchContextCancelled(t *testing.T) {
	p, err := Compile("input.x == 1")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err = p.MatchContext(ctx, json.RawMessage(`{"input":{"x":1}}`)); err == nil ||
		!strings.Contains(err.Error(), context.Canceled.Error()) {
		t.Fatalf("MatchContext cancelled err = %v, want context canceled", err)
	}
}

func TestMatchMemoryBudget(t *testing.T) {
	p, err := Compile(largeArrayPredicate(600))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	if _, err = p.Match(json.RawMessage(`{"input":{"x":1}}`)); err == nil ||
		!strings.Contains(err.Error(), "memory budget exceeded") {
		t.Fatalf("Match large array err = %v, want memory budget exceeded", err)
	}
}

func largeArrayPredicate(entries int) string {
	var b strings.Builder
	b.WriteByte('[')

	for i := range entries {
		if i > 0 {
			b.WriteByte(',')
		}

		b.WriteString("input.x")

		if i%10 == 0 {
			b.WriteString(" + ")
			b.WriteString(strconv.Itoa(i))
		}
	}

	b.WriteString("] != nil")

	return b.String()
}
