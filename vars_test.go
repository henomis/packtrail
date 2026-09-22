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
	"reflect"
	"strings"
	"testing"

	"github.com/henomis/packtrail"
)

// The exported Var* constants exist so a layer compiling its own syntax down to
// `when` expressions can name the context variables instead of restating string
// literals — a mismatch evaluates to nil, and a nil comparison takes the default
// branch silently rather than erroring. This is the check that they still name
// what the document actually carries, and that every field has one: a variable
// added to the context without a constant leaves that layer writing a literal.
func TestExportedVarsMatchTheContextDocument(t *testing.T) {
	consts := map[string]string{
		"Input":      packtrail.VarInput,
		"Results":    packtrail.VarResults,
		"Signals":    packtrail.VarSignals,
		"Branches":   packtrail.VarBranches,
		"LastNode":   packtrail.VarLastNode,
		"ReleasedBy": packtrail.VarReleasedBy,
		"Visits":     packtrail.VarVisits,
	}

	doc := reflect.TypeOf(packtrail.InvocationContext{})

	for i := range doc.NumField() {
		field := doc.Field(i)
		tag, _, _ := strings.Cut(field.Tag.Get("json"), ",")

		want, ok := consts[field.Name]
		if !ok {
			t.Errorf("context field %s has no exported Var constant", field.Name)
			continue
		}

		if want != tag {
			t.Errorf("Var for %s = %q, but the document calls it %q", field.Name, want, tag)
		}

		delete(consts, field.Name)
	}

	for name := range consts {
		t.Errorf("Var constant for %s names a field the context no longer has", name)
	}
}
