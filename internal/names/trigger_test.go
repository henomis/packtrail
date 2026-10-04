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
	"strings"
	"testing"
)

// Trigger ids never collide across flows and messages, whatever '-' they
// hold, and stay valid tokens for the longest flow names.
func TestTriggerExecIDs(t *testing.T) {
	long := strings.Repeat("f", 128)

	ids := map[string]string{
		"a / b-c":       TriggerMsgExecID("a", "b-c"),
		"a-b / c":       TriggerMsgExecID("a-b", "c"),
		"a / S-1 (msg)": TriggerMsgExecID("a", "S-1"),
		"a / S 1 (seq)": TriggerSeqExecID("a", "S", 1),
		"a-S / 1 (seq)": TriggerSeqExecID("a-S", "", 1),
		"long / x":      TriggerMsgExecID(long, "x"),
		"long+g / x":    TriggerMsgExecID(long+"g", "x"),
		"long / seq 1":  TriggerSeqExecID(long, "S", 1),
		"x / a.b c*>":   TriggerMsgExecID("x", "a.b c*>"),
		"x / 200 chars": TriggerMsgExecID("x", strings.Repeat("m", 200)),
	}

	seen := map[string]string{}

	for name, id := range ids {
		if !ValidToken(id) {
			t.Errorf("%s: %q is not a valid token", name, id)
		}

		if prev, dup := seen[id]; dup {
			t.Errorf("%s and %s share id %q", name, prev, id)
		}

		seen[id] = name
	}

	if TriggerMsgExecID("a", "b-c") != ids["a / b-c"] {
		t.Error("the same flow and message must give the same id")
	}
}
