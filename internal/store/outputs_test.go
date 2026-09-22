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

import (
	"slices"
	"testing"
)

// Outputs is in settle order and last_node is its final element, so a node
// settling again in a cycle must move to the end — once, not twice.
func TestSetOutputMovesRevisitToEnd(t *testing.T) {
	var e Execution

	for _, n := range []string{"implement", "verify", "fix", "verify"} {
		e.SetOutput(n, "v-"+n)
	}

	want := []string{"implement", "fix", "verify"}
	if !slices.Equal(e.Outputs, want) {
		t.Fatalf("Outputs = %v, want %v (the revisited node last, listed once)", e.Outputs, want)
	}

	// Settling the node that is already last changes nothing.
	e.AddOutput("verify")

	if !slices.Equal(e.Outputs, want) {
		t.Errorf("Outputs = %v after re-settling the last node, want %v", e.Outputs, want)
	}
}
