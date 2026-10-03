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
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

type at struct{ node, field string }

func locations(err error) []at {
	errs := ValidationErrors(err)

	out := make([]at, 0, len(errs))
	for _, e := range errs {
		out = append(out, at{e.Node, e.Field})
	}

	return out
}

func sameLocations(t *testing.T, err error, want []at) {
	t.Helper()

	got := locations(err)
	if len(got) != len(want) {
		t.Fatalf("got %d problems %v, want %v\n%v", len(got), got, want, err)
	}

	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("problem %d at %v, want %v\n%v", i, got[i], want[i], err)
		}
	}
}

func TestValidateCollectsAll(t *testing.T) {
	// Independent problems on the flow and on several nodes are all reported,
	// in order; the graph checks (start, reachability) are skipped while a
	// reference is broken, instead of panicking on it.
	y := `
name: x
channels: {z: {reducer: max}, c: {reducer: append, default: 3}}
budget: {tokens: 0}
nodes:
  - {id: a, type: task, kind: k, retry: {max_attempts: 65}, next: zz}
  - {id: b, type: task, kind: k, retry: {max_attempts: 2, backoff: x}, cache: {ttl: 0s}}
  - {id: c, type: choice, rules: [{when: 'input.x', to: a}]}
`

	for range 5 { // map-backed sections must come out in a stable order.
		_, err := Parse([]byte(y))
		sameLocations(t, err, []at{
			{"", "channels.c.default"},
			{"", "channels.z.reducer"},
			{"", "budget.tokens"},
			{"a", "retry.max_attempts"},
			{"a", "next"},
			{"b", "retry.backoff"},
			{"b", "cache.ttl"},
			{"c", "rules"},
		})
	}
}

func TestValidateGraphCollectsAll(t *testing.T) {
	y := `
name: x
start: f
nodes:
  - {id: f, type: fanout, branches: [a], next: t}
  - {id: a, type: task, kind: k}
  - {id: t, type: task, kind: k}
  - {id: u, type: task, kind: k}
  - {id: v, type: task, kind: k}
`

	_, err := Parse([]byte(y))
	sameLocations(t, err, []at{{"f", "next"}, {"u", ""}, {"v", ""}})

	if !strings.Contains(err.Error(), `flow "x": node "f": next: leads to "t"`) {
		t.Fatalf("err = %v", err)
	}
}

func TestValidateRouteFieldNamed(t *testing.T) {
	y := `
name: x
nodes:
  - {id: t, type: task, kind: k, next: f}
  - {id: f, type: fanout, branches: [a], next: j}
  - {id: a, type: task, kind: k}
  - {id: j, type: join, next: c}
  - {id: c, type: choice, rules: [{when: 'input.x', to: e}, {default: true, to: a}]}
  - {id: e, type: task, kind: k}
`

	_, err := Parse([]byte(y))
	sameLocations(t, err, []at{{"c", "rules[1].to"}})
}

func TestValidationErrorFormat(t *testing.T) {
	cases := []struct {
		e    ValidationError
		want string
	}{
		{ValidationError{Flow: "x", Msg: "no nodes"}, `flow "x": no nodes`},
		{ValidationError{Flow: "x", Field: "start", Msg: "m"}, `flow "x": start: m`},
		{ValidationError{Flow: "x", Node: "a", Msg: "m"}, `flow "x": node "a": m`},
		{ValidationError{Flow: "x", Node: "a", Field: "retry.delay", Msg: "m"}, `flow "x": node "a": retry.delay: m`},
	}

	for _, c := range cases {
		if got := c.e.Error(); got != c.want {
			t.Errorf("Error() = %q, want %q", got, c.want)
		}
	}

	if ValidationErrors(nil) != nil || ValidationErrors(errors.New("plain")) != nil {
		t.Fatal("no ValidationError expected")
	}
}

func TestValidationErrorUnwrap(t *testing.T) {
	_, err := Parse([]byte("name: x\nnodes: [{id: a, type: task, kind: k, output_schema: {$ref: 'file:///etc/passwd'}}]"))

	var load *jsonschema.LoadURLError
	if !errors.As(err, &load) || !errors.Is(load.Err, errRemoteRef) {
		t.Fatalf("errors.As does not reach the cause: %v", err)
	}

	var ve *ValidationError
	if !errors.As(err, &ve) || ve.Node != "a" || ve.Field != "output_schema" || ve.Err == nil {
		t.Fatalf("errors.As = %+v", ve)
	}

	path := filepath.Join(t.TempDir(), "bad.yaml")
	if werr := os.WriteFile(path, []byte("name: x\nnodes: [{id: a, type: task}, {id: b, type: task}]"), 0o600); werr != nil {
		t.Fatal(werr)
	}

	_, err = ParseFile(path)
	if !strings.HasPrefix(err.Error(), path+": ") {
		t.Fatalf("err = %v, want the path prefix", err)
	}

	sameLocations(t, err, []at{{"a", "kind"}, {"b", "kind"}})
}
