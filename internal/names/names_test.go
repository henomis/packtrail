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

package names

import (
	"reflect"
	"strings"
	"testing"
)

func TestNewEmptyPrefixFallsBackToDefault(t *testing.T) {
	if got := New(""); got != New(Default) {
		t.Fatalf("New(\"\") = %+v, want same as New(%q)", got, Default)
	}

	if New("").Prefix != Default {
		t.Errorf("Prefix = %q, want %q", New("").Prefix, Default)
	}
}

func TestNewDefaultValues(t *testing.T) {
	n := New("")

	want := map[string]string{
		"BucketExecutions":    "packtrail-executions",
		"BucketLeases":        "packtrail-leases",
		"BucketIdxStatus":     "packtrail-idx-status",
		"BucketIdxFlow":       "packtrail-idx-flow",
		"BucketResultCache":   "packtrail-result-cache",
		"BucketFlows":         "packtrail-flows",
		"StreamEvents":        "packtrail-events",
		"StreamWork":          "packtrail-work",
		"StreamSignals":       "packtrail-signals",
		"StreamSchedule":      "packtrail-schedule",
		"SubjEventsPrefix":    "packtrail.events.",
		"SubjWorkPrefix":      "packtrail.work.",
		"SubjSignalPrefix":    "packtrail.signal.",
		"SubjSchedPrefix":     "packtrail.sched.",
		"SubjSchedFirePrefix": "packtrail.sched.fire.",
		"DurEngine":           "packtrail-engine",
		"DurFired":            "packtrail-engine-fired",
		"DurSignals":          "packtrail-engine-signals",
		"DurIndexer":          "packtrail-indexer",
	}

	v := reflect.ValueOf(n)
	for field, wantVal := range want {
		got := v.FieldByName(field).String()
		if got != wantVal {
			t.Errorf("%s = %q, want %q", field, got, wantVal)
		}
	}
}

func TestNewCustomPrefixAppliedEverywhere(t *testing.T) {
	const prefix = "myapp"

	n := New(prefix)

	if n.Prefix != prefix {
		t.Errorf("Prefix = %q, want %q", n.Prefix, prefix)
	}

	v := reflect.ValueOf(n)

	tp := v.Type()
	for i := 0; i < v.NumField(); i++ {
		val := v.Field(i).String()
		if !strings.HasPrefix(val, prefix) {
			t.Errorf("%s = %q, missing prefix %q", tp.Field(i).Name, val, prefix)
		}
	}
}

func TestNewNamesAreUnique(t *testing.T) {
	n := New(Default)

	v := reflect.ValueOf(n)
	tp := v.Type()
	seen := make(map[string]string)

	for i := 0; i < v.NumField(); i++ {
		name := tp.Field(i).Name
		if name == "Prefix" {
			continue
		}

		val := v.Field(i).String()
		if prev, dup := seen[val]; dup {
			t.Errorf("duplicate resource name %q used by both %s and %s", val, prev, name)
		}

		seen[val] = name
	}
}
