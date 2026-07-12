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
	"encoding/json"
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
