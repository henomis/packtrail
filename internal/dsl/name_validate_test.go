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

// TestValidateRejectsUnsafeNames: flow names, node ids and signal names end up
// as NATS subject tokens and KV-key segments, so unsafe characters are rejected
// at parse time instead of failing opaquely (or routing ambiguously) at runtime.
func TestValidateRejectsUnsafeNames(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		want string
	}{
		{
			"flow name with space",
			`{name: "my flow", nodes: [{id: a, type: task, subject: "x"}]}`,
			"name must match",
		},
		{
			"flow name with dot",
			`{name: "my.flow", nodes: [{id: a, type: task, subject: "x"}]}`,
			"name must match",
		},
		{
			"node id with dot",
			`{name: ok, nodes: [{id: "a.b", type: task, subject: "x"}]}`,
			"node id",
		},
		{
			"node id with wildcard",
			`{name: ok, nodes: [{id: "a*", type: task, subject: "x"}]}`,
			"node id",
		},
		{
			"signal name with space",
			`{name: ok, nodes: [{id: s, type: signal, signal_name: "go now"}]}`,
			"signal_name",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(tc.yaml))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want mention of %q", err, tc.want)
			}
		})
	}
}
