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

package worker

import (
	"errors"
	"fmt"
	"maps"
	"testing"

	"github.com/henomis/packtrail/internal/cmd"
	"github.com/henomis/packtrail/internal/wire"
)

// WithUsage composes with Interrupt, Permanent and %w in any order: the
// outcome is classified as without it, and the outermost usage is reported.
func TestWithUsageClassification(t *testing.T) {
	u := map[string]float64{"tokens": 7}
	inner := map[string]float64{"tokens": 1}
	boom := errors.New("boom")

	cases := []struct {
		name      string
		res       *Result
		err       error
		typ       cmd.Type
		retryable bool
		usage     map[string]float64
	}{
		{"interrupt", nil, WithUsage(Interrupt("q"), u), cmd.Interrupt, false, u},
		{"wrapped interrupt", nil, fmt.Errorf("ask: %w", WithUsage(Interrupt("q"), u)), cmd.Interrupt, false, u},
		{"usage around permanent", nil, WithUsage(Permanent(boom), u), cmd.Fail, false, u},
		{"permanent around usage", nil, Permanent(WithUsage(boom, u)), cmd.Fail, false, u},
		{"wrapped retryable", nil, fmt.Errorf("call: %w", WithUsage(boom, u)), cmd.Fail, true, u},
		{"retryable", nil, WithUsage(boom, u), cmd.Fail, true, u},
		{"outermost wins", nil, WithUsage(fmt.Errorf("x: %w", WithUsage(boom, inner)), u), cmd.Fail, true, u},
		{"plain error", nil, boom, cmd.Fail, true, nil},
		{"plain interrupt", nil, Interrupt("q"), cmd.Interrupt, false, nil},
		{"nil error", &Result{Usage: u}, WithUsage(nil, inner), cmd.Complete, false, u},
	}

	wj := wire.Job{ExecID: "e1", Key: "n", Generation: 1, Attempt: 1}
	ref := cmd.TaskRef{Key: "n", Generation: 1, Attempt: 1}

	for _, c := range cases {
		got, err := resultCmd("id", wj, ref, c.res, c.err)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}

		if got.Type != c.typ {
			t.Errorf("%s: type %s, want %s", c.name, got.Type, c.typ)

			continue
		}

		var usage map[string]float64

		switch got.Type { //nolint:exhaustive // a handler only answers with these.
		case cmd.Interrupt:
			var p cmd.InterruptData

			_ = got.Payload(&p)
			usage = p.Usage
		case cmd.Fail:
			var p cmd.FailData

			_ = got.Payload(&p)
			usage = p.Usage

			if p.Retryable != c.retryable || p.Error == "" {
				t.Errorf("%s: retryable %v error %q", c.name, p.Retryable, p.Error)
			}
		case cmd.Complete:
			var p cmd.CompleteData

			_ = got.Payload(&p)
			usage = p.Usage
		}

		if !maps.Equal(usage, c.usage) {
			t.Errorf("%s: usage %v, want %v", c.name, usage, c.usage)
		}
	}
}

func TestWithUsageKeepsMessage(t *testing.T) {
	err := WithUsage(errors.New("schema mismatch"), map[string]float64{"tokens": 1})
	if err.Error() != "schema mismatch" {
		t.Fatal(err)
	}

	var ue *UsageError
	if !errors.As(fmt.Errorf("x: %w", err), &ue) || ue.Usage["tokens"] != 1 {
		t.Fatal("usage not found with errors.As")
	}
}
