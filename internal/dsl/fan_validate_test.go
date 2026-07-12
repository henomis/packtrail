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

// TestValidateRejectsSharedFanoutBranch: branch state is keyed by node id per
// execution, so a node dispatched by two fanouts would reuse settled state.
func TestValidateRejectsSharedFanoutBranch(t *testing.T) {
	_, err := Parse([]byte(`
name: shared-branch
nodes:
  - {id: fo1, type: fanout, branches: [b]}
  - {id: b, type: task, subject: "x"}
  - {id: j1, type: fanin, wait_for: [b]}
  - {id: fo2, type: fanout, branches: [b]}
  - {id: j2, type: fanin, wait_for: [b]}
edges:
  - {from: fo1, to: j1}
  - {from: j1, to: fo2}
  - {from: fo2, to: j2}
`))
	if err == nil || !strings.Contains(err.Error(), "at most one fanout") {
		t.Fatalf("err = %v, want shared-branch rejection", err)
	}
}

// TestValidateRejectsDuplicateBranch: the same branch listed twice in one fanout.
func TestValidateRejectsDuplicateBranch(t *testing.T) {
	_, err := Parse([]byte(`
name: dup-branch
nodes:
  - {id: fo, type: fanout, branches: [b, b]}
  - {id: b, type: task, subject: "x"}
  - {id: j, type: fanin, wait_for: [b]}
edges:
  - {from: fo, to: j}
`))
	if err == nil || !strings.Contains(err.Error(), "twice") {
		t.Fatalf("err = %v, want duplicate-branch rejection", err)
	}
}

// TestValidateRejectsOrphanWaitFor: a fanin waiting on a node no fanout
// dispatches would never settle.
func TestValidateRejectsOrphanWaitFor(t *testing.T) {
	_, err := Parse([]byte(`
name: orphan-wait
nodes:
  - {id: fo, type: fanout, branches: [b]}
  - {id: b, type: task, subject: "x"}
  - {id: ghost, type: task, subject: "y"}
  - {id: j, type: fanin, wait_for: [b, ghost]}
edges:
  - {from: fo, to: j}
  - {from: j, to: ghost}
`))
	if err == nil || !strings.Contains(err.Error(), "not a branch of any fanout") {
		t.Fatalf("err = %v, want orphan wait_for rejection", err)
	}
}

// TestValidateRejectsNonTaskBranch: the branch runner invokes task nodes only;
// a branch of any other type would stay pending forever and strand the join.
func TestValidateRejectsNonTaskBranch(t *testing.T) {
	_, err := Parse([]byte(`
name: sig-branch
nodes:
  - {id: fo, type: fanout, branches: [gate]}
  - {id: gate, type: signal, signal_name: go}
  - {id: j, type: fanin, wait_for: [gate]}
edges:
  - {from: fo, to: j}
`))
	if err == nil || !strings.Contains(err.Error(), "branches must be task nodes") {
		t.Fatalf("err = %v, want non-task branch rejection", err)
	}
}

// TestValidateRejectsFanoutWithoutEdge: stepFanout parks the execution at its
// successor fanin; a fanout with no outgoing edge is a guaranteed runtime error.
func TestValidateRejectsFanoutWithoutEdge(t *testing.T) {
	_, err := Parse([]byte(`
name: no-fanin-edge
nodes:
  - {id: fo, type: fanout, branches: [b]}
  - {id: b, type: task, subject: "x"}
  - {id: j, type: fanin, wait_for: [b]}
edges:
  - {from: b, to: j}
`))
	if err == nil || !strings.Contains(err.Error(), "no outgoing edge") {
		t.Fatalf("err = %v, want fanout-without-edge rejection", err)
	}
}

// TestValidateRejectsFanoutToNonFanin: the fanout's successor is where the
// execution parks and the join is evaluated; anything but a fanin can't join.
func TestValidateRejectsFanoutToNonFanin(t *testing.T) {
	_, err := Parse([]byte(`
name: fanout-to-task
nodes:
  - {id: fo, type: fanout, branches: [b]}
  - {id: b, type: task, subject: "x"}
  - {id: after, type: task, subject: "y"}
  - {id: j, type: fanin, wait_for: [b]}
edges:
  - {from: fo, to: after}
  - {from: after, to: j}
`))
	if err == nil || !strings.Contains(err.Error(), "successor must be a fanin") {
		t.Fatalf("err = %v, want fanout-to-non-fanin rejection", err)
	}
}

// TestValidateRejectsCrossFanWaitFor: a fanin paired with fanout A must not
// wait on a branch dispatched by fanout B — that branch is never pending in
// A's fan, so A's join would never settle it.
func TestValidateRejectsCrossFanWaitFor(t *testing.T) {
	_, err := Parse([]byte(`
name: cross-fan
nodes:
  - {id: fo1, type: fanout, branches: [a]}
  - {id: a, type: task, subject: "x"}
  - {id: j1, type: fanin, wait_for: [b]}
  - {id: fo2, type: fanout, branches: [b]}
  - {id: b, type: task, subject: "y"}
  - {id: j2, type: fanin, wait_for: [b]}
edges:
  - {from: fo1, to: j1}
  - {from: j1, to: fo2}
  - {from: fo2, to: j2}
`))
	if err == nil || !strings.Contains(err.Error(), "not a branch of its fanout") {
		t.Fatalf("err = %v, want cross-fan wait_for rejection", err)
	}
}

// TestValidateAllowsWaitForSubset: a fanin may wait on a subset of its fanout's
// branches (e.g. join on the critical ones, let the rest settle in background).
func TestValidateAllowsWaitForSubset(t *testing.T) {
	if _, err := Parse([]byte(`
name: subset-wait
nodes:
  - {id: fo, type: fanout, branches: [a, b, c]}
  - {id: a, type: task, subject: "x"}
  - {id: b, type: task, subject: "y"}
  - {id: c, type: task, subject: "z"}
  - {id: j, type: fanin, wait_for: [a, b]}
edges:
  - {from: fo, to: j}
`)); err != nil {
		t.Fatalf("subset wait_for rejected: %v", err)
	}
}

// TestValidateRejectsFanCycle: a choice routing back upstream of a fanout puts
// the fanout on a cycle; its per-execution branch state would be reused on the
// second visit instead of re-running the branches.
func TestValidateRejectsFanCycle(t *testing.T) {
	_, err := Parse([]byte(`
name: fan-cycle
nodes:
  - {id: entry, type: task, subject: "e"}
  - {id: fo, type: fanout, branches: [b]}
  - {id: b, type: task, subject: "x"}
  - {id: j, type: fanin, wait_for: [b]}
  - {id: again, type: choice, rules: [{when: 'input.retry == true', to: fo}, {default: true, to: out}]}
  - {id: out, type: task, subject: "o"}
edges:
  - {from: entry, to: fo}
  - {from: fo, to: j}
  - {from: j, to: again}
`))
	if err == nil || !strings.Contains(err.Error(), "lies on a cycle") {
		t.Fatalf("err = %v, want fan-cycle rejection", err)
	}
}

// TestValidateAllowsTaskCycle: cycles that avoid fanout/fanin nodes stay legal —
// e.g. a retry loop between plain tasks via a choice.
func TestValidateAllowsTaskCycle(t *testing.T) {
	if _, err := Parse([]byte(`
name: task-cycle
nodes:
  - {id: entry, type: task, subject: "e"}
  - {id: work, type: task, subject: "w"}
  - {id: again, type: choice, rules: [{when: 'input.retry == true', to: work}, {default: true, to: out}]}
  - {id: out, type: task, subject: "o"}
edges:
  - {from: entry, to: work}
  - {from: work, to: again}
`)); err != nil {
		t.Fatalf("task-only cycle rejected: %v", err)
	}
}
