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

package store

import "testing"

// TestPayloadKeyBuildersValidateTokens is a regression test: InputKey,
// OutputKey, OutputVersionKey and SignalKey used to trust the execution-id/
// node-id/name charset invariant enforced only upstream in internal/runtime,
// with no independent re-validation — unlike internal/signal, which
// re-validates its own subject tokens. A future caller skipping that upstream
// validation must fail fast here instead of building an ambiguous key.
func TestPayloadKeyBuildersValidateTokens(t *testing.T) {
	const bad = "has.dot"

	cases := []struct {
		name string
		fn   func()
	}{
		{"InputKey", func() { InputKey(bad) }},
		{"OutputKey execID", func() { OutputKey(bad, "n") }},
		{"OutputKey node", func() { OutputKey("e", bad) }},
		{"OutputVersionKey execID", func() { OutputVersionKey(bad, "n", "v") }},
		{"OutputVersionKey node", func() { OutputVersionKey("e", bad, "v") }},
		{"OutputVersionKey version", func() { OutputVersionKey("e", "n", bad) }},
		{"SignalKey execID", func() { SignalKey(bad, "sig", 1) }},
		{"SignalKey name", func() { SignalKey("e", bad, 1) }},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Errorf("%s did not panic on invalid token %q", c.name, bad)
				}
			}()

			c.fn()
		})
	}
}

// TestPayloadKeyBuildersAcceptValidTokens confirms the validation added above
// doesn't reject legitimate, already-validated tokens.
func TestPayloadKeyBuildersAcceptValidTokens(t *testing.T) {
	if got, want := InputKey("exec-1"), "exec-1.in"; got != want {
		t.Errorf("InputKey = %q, want %q", got, want)
	}

	if got, want := OutputKey("exec-1", "node-1"), "exec-1.out.node-1"; got != want {
		t.Errorf("OutputKey = %q, want %q", got, want)
	}

	if got, want := OutputVersionKey("exec-1", "node-1", "v1"), "exec-1.outv.node-1.v1"; got != want {
		t.Errorf("OutputVersionKey = %q, want %q", got, want)
	}

	if got, want := SignalKey("exec-1", "go", 3), "exec-1.sig.go.3"; got != want {
		t.Errorf("SignalKey = %q, want %q", got, want)
	}
}
