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

// TestParseRejectsUnknownField: a typo'd field must be a parse error, not a
// silently dropped setting ("retires:" instead of "retry:" would otherwise
// quietly disable the retry policy).
func TestParseRejectsUnknownField(t *testing.T) {
	_, err := Parse([]byte(`
name: typo
nodes:
  - {id: a, type: task, subject: "x", retires: {max_attempts: 3}}
`))
	if err == nil || !strings.Contains(err.Error(), "retires") {
		t.Fatalf("err = %v, want unknown-field rejection naming 'retires'", err)
	}
}

// TestParseRejectsMultipleDocuments: yaml.Unmarshal-style decoding would take
// the first document and silently drop the rest; every document after the
// first must be an explicit error.
func TestParseRejectsMultipleDocuments(t *testing.T) {
	_, err := Parse([]byte(`
name: first
nodes:
  - {id: a, type: task, subject: "x"}
---
name: second
nodes:
  - {id: b, type: task, subject: "y"}
`))
	if err == nil || !strings.Contains(err.Error(), "multiple flow documents") {
		t.Fatalf("err = %v, want multi-document rejection", err)
	}
}

// TestParseAllowsTrailingSeparator: a trailing "---" (an empty document) is
// harmless and stays accepted.
func TestParseAllowsTrailingSeparator(t *testing.T) {
	if _, err := Parse([]byte(`
name: trailing
nodes:
  - {id: a, type: task, subject: "x"}
---
`)); err != nil {
		t.Fatalf("trailing separator rejected: %v", err)
	}
}

// TestParseRejectsEmptyInput: empty bytes get a clear error instead of an
// opaque EOF or a zero-value flow failing name validation.
func TestParseRejectsEmptyInput(t *testing.T) {
	if _, err := Parse(nil); err == nil || !strings.Contains(err.Error(), "empty flow definition") {
		t.Fatalf("err = %v, want empty-input rejection", err)
	}
}
