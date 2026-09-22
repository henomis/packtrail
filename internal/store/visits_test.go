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

// A cycle enters the same node repeatedly: the count is per node and rises on
// every entry, while the generation keeps rising across all of them.
func TestEnterNodeCountsVisits(t *testing.T) {
	e := &Execution{CurrentNode: "implement", NodeGeneration: 1, Visits: map[string]uint64{"implement": 1}}

	for _, node := range []string{"verify", "fix", "verify", "fix", "verify"} {
		e.EnterNode(node)
	}

	if got := e.Visit("verify"); got != 3 {
		t.Errorf("visits[verify] = %d, want 3", got)
	}

	if got := e.Visit("fix"); got != 2 {
		t.Errorf("visits[fix] = %d, want 2", got)
	}

	if got := e.Visit("implement"); got != 1 {
		t.Errorf("visits[implement] = %d, want 1", got)
	}

	if got := e.Visit("never"); got != 0 {
		t.Errorf("visits[never] = %d, want 0", got)
	}

	if e.CurrentNode != "verify" {
		t.Errorf("current node = %q, want verify", e.CurrentNode)
	}

	// Six entries on top of the starting generation of 1.
	if e.NodeGeneration != 6 {
		t.Errorf("generation = %d, want 6", e.NodeGeneration)
	}
}

// A branch runs as a node but never becomes CurrentNode, so it is counted
// without moving the execution.
func TestCountVisitLeavesCurrentNodeAlone(t *testing.T) {
	e := &Execution{CurrentNode: "fan", NodeGeneration: 3}

	e.CountVisit("worker-a")
	e.CountVisit("worker-a")

	if got := e.Visit("worker-a"); got != 2 {
		t.Errorf("visits[worker-a] = %d, want 2", got)
	}

	if e.CurrentNode != "fan" || e.NodeGeneration != 3 {
		t.Errorf("current = %q gen = %d; CountVisit must not move the execution", e.CurrentNode, e.NodeGeneration)
	}

	// The zero value must not panic, and an empty node id is not a visit.
	var zero Execution

	zero.CountVisit("")

	if len(zero.Visits) != 0 {
		t.Errorf("visits = %v, want none", zero.Visits)
	}
}
