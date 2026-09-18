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

// A fan-out branch is reached one way only: its fanout invokes it inline and its
// fanin joins it. Routing into one from anywhere else lands the execution on a
// node with no successor, which the engine settles as **completed** — the fan,
// the join and everything after them skipped, with a terminal status claiming
// success. These cases pin the rejection for each way a flow can express that
// transition.
//
// Every fixture routes into the branch from *after* the join. That is not
// incidental: the offending reference has to be an addition to an otherwise
// complete flow, or start-node detection fires first and the test proves nothing
// about this rule. TestValidateAcceptsBranchEntryBaseline holds the fixture
// honest by parsing the same flow with the reference removed.

const branchEntryNodes = `
version: "1.0"
name: branch-entry
nodes:
  - {id: seed, type: task, subject: "s"}
  - {id: fo, type: fanout, branches: [b1, b2]}
  - {id: b1, type: task, subject: "x"}
  - {id: b2, type: task, subject: "y"}
  - {id: j, type: fanin, wait_for: [b1, b2]}
  - {id: after, type: task, subject: "a"}
`

// TestValidateAcceptsBranchEntryBaseline: the fixture without any branch entry
// must parse, so each rejection below is attributable to the reference it adds
// and to nothing else.
func TestValidateAcceptsBranchEntryBaseline(t *testing.T) {
	if _, err := Parse([]byte(branchEntryNodes + `edges:
  - {from: seed, to: fo}
  - {from: fo, to: j}
  - {from: j, to: after}
`)); err != nil {
		t.Fatalf("baseline fixture must be valid, got %v", err)
	}
}

func TestValidateRejectsEdgeIntoBranch(t *testing.T) {
	_, err := Parse([]byte(branchEntryNodes + `edges:
  - {from: seed, to: fo}
  - {from: fo, to: j}
  - {from: j, to: after}
  - {from: after, to: b1}
`))
	if err == nil || !strings.Contains(err.Error(), `edge targets "b1", a branch of fanout "fo"`) {
		t.Fatalf("err = %v, want edge-into-branch rejection", err)
	}

	// The consequence, not just the shape: the message must say what the flow
	// would otherwise do, because "completed" is the part nobody expects.
	if !strings.Contains(err.Error(), "report completed") {
		t.Errorf("error should explain the silent-completion consequence, got %v", err)
	}
}

func TestValidateRejectsChoiceRuleIntoBranch(t *testing.T) {
	_, err := Parse([]byte(branchEntryNodes + `  - id: gate
    type: choice
    rules:
      - {when: 'true', to: b2}
      - {default: true, to: after}
edges:
  - {from: seed, to: fo}
  - {from: fo, to: j}
  - {from: j, to: gate}
`))
	if err == nil || !strings.Contains(err.Error(), `rule target targets "b2", a branch of fanout "fo"`) {
		t.Fatalf("err = %v, want rule-into-branch rejection", err)
	}
}

func TestValidateRejectsSignalTimeoutIntoBranch(t *testing.T) {
	_, err := Parse([]byte(branchEntryNodes + `  - id: hold
    type: signal
    signal_name: approve
    timeout: 1m
    on_timeout: b1
edges:
  - {from: seed, to: fo}
  - {from: fo, to: j}
  - {from: j, to: hold}
  - {from: hold, to: after}
`))
	if err == nil || !strings.Contains(err.Error(), `on_timeout targets "b1", a branch of fanout "fo"`) {
		t.Fatalf("err = %v, want on_timeout-into-branch rejection", err)
	}
}

// TestValidateAcceptsFaninWaitForBranches: a fanin's wait_for names branches by
// design and must not be caught by the entry rule. Without this exemption the
// fix would reject every fan-out flow there is — the baseline above would fail.
// Asserted explicitly so the exemption cannot be dropped silently.
func TestValidateAcceptsFaninWaitForBranches(t *testing.T) {
	f, err := Parse([]byte(branchEntryNodes + `edges:
  - {from: seed, to: fo}
  - {from: fo, to: j}
  - {from: j, to: after}
`))
	if err != nil {
		t.Fatalf("wait_for referencing branches must stay valid, got %v", err)
	}

	if got := f.byID["j"].WaitFor; len(got) != 2 {
		t.Fatalf("fanin wait_for = %v, want both branches", got)
	}
}
