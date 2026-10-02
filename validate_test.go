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
	"errors"
	"strings"
	"testing"

	"github.com/nats-io/nats.go"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/flow"
)

func TestValidateNamespace(t *testing.T) {
	for _, ns := range []string{"packtrail", "a", "A-b_9", strings.Repeat("n", 64)} {
		if err := packtrail.ValidateNamespace(ns); err != nil {
			t.Errorf("ValidateNamespace(%q) = %v", ns, err)
		}
	}

	for _, ns := range []string{"", "a.b", "a*", "a>", "a b", strings.Repeat("n", 65)} {
		if err := packtrail.ValidateNamespace(ns); !errors.Is(err, packtrail.ErrInvalidArgument) {
			t.Errorf("ValidateNamespace(%q) = %v, want ErrInvalidArgument", ns, err)
		}
	}
}

func TestValidateName(t *testing.T) {
	for _, s := range []string{"a", "A-b_9", strings.Repeat("n", 128)} {
		if err := packtrail.ValidateName("flow name", s); err != nil {
			t.Errorf("ValidateName(%q) = %v", s, err)
		}
	}

	for _, s := range []string{"", "a.b", "*", ">", "a b", "ä", strings.Repeat("n", 129)} {
		err := packtrail.ValidateName("flow name", s)
		if !errors.Is(err, packtrail.ErrInvalidArgument) {
			t.Errorf("ValidateName(%q) = %v, want ErrInvalidArgument", s, err)
		}

		if err != nil && !strings.Contains(err.Error(), "flow name") {
			t.Errorf("ValidateName(%q) error %q does not name the identifier", s, err)
		}
	}
}

func TestValidateCron(t *testing.T) {
	for _, c := range []string{"0 0 * * * *", "*/5 * * * * *", "@daily"} {
		if err := packtrail.ValidateCron(c); err != nil {
			t.Errorf("ValidateCron(%q) = %v", c, err)
		}
	}

	for _, c := range []string{"", "* * *", "0 0 * * *", "@sometimes", "61 * * * * *"} {
		if err := packtrail.ValidateCron(c); !errors.Is(err, packtrail.ErrInvalidArgument) {
			t.Errorf("ValidateCron(%q) = %v, want ErrInvalidArgument", c, err)
		}
	}
}

// ValidateOptions must accept and reject exactly what New does: it exists so a
// configuration can be checked before a connection is available.
func TestValidateOptionsAgreesWithNew(t *testing.T) {
	def, err := flow.Parse([]byte(validateFlowYAML))
	if err != nil {
		t.Fatal(err)
	}

	cases := map[string]struct {
		opts []packtrail.Option
		ok   bool
	}{
		"empty":            {ok: true},
		"valid set":        {opts: []packtrail.Option{packtrail.WithNamespace("ns"), packtrail.WithFlow(def), packtrail.WithSchedule("nightly", "greet", "0 0 3 * * *", map[string]any{"x": 1})}, ok: true},
		"bad namespace":    {opts: []packtrail.Option{packtrail.WithNamespace("a.b")}},
		"bad cron":         {opts: []packtrail.Option{packtrail.WithSchedule("nightly", "greet", "0 3 * * *", nil)}},
		"bad sched name":   {opts: []packtrail.Option{packtrail.WithSchedule("night.ly", "greet", "@daily", nil)}},
		"non-object input": {opts: []packtrail.Option{packtrail.WithSchedule("nightly", "greet", "@daily", []int{1})}},
		"duplicate flow":   {opts: []packtrail.Option{packtrail.WithFlow(def), packtrail.WithFlow(def)}},
		"bad partitions":   {opts: []packtrail.Option{packtrail.WithPartitions(0)}},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			verr := packtrail.ValidateOptions(c.opts...)
			_, nerr := packtrail.New(&nats.Conn{}, c.opts...)

			if (verr == nil) != c.ok {
				t.Fatalf("ValidateOptions = %v, want ok=%v", verr, c.ok)
			}

			if (verr == nil) != (nerr == nil) {
				t.Fatalf("ValidateOptions = %v but New = %v", verr, nerr)
			}

			if verr != nil && !errors.Is(verr, packtrail.ErrInvalidArgument) {
				t.Fatalf("ValidateOptions = %v, want ErrInvalidArgument", verr)
			}
		})
	}
}

const validateFlowYAML = `
name: greet
start: hello
nodes:
  - id: hello
    type: task
    kind: greeter
`
