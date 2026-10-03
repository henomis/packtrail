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
	"fmt"
	"sort"
	"strings"
)

// WaitFor returns the branches a join waits for: its wait_for list, or every
// branch of its fanout when wait_for is omitted. Valid after Validate.
func (f *Flow) WaitFor(join string) []string {
	n := f.byID[join]
	if n == nil {
		return nil
	}

	if len(n.WaitFor) > 0 {
		return n.WaitFor
	}

	for fan, j := range f.joinOf {
		if j == join {
			return f.byID[fan].Branches
		}
	}

	return nil
}

// validateFans ties every fanout to the join that closes it and rejects shapes
// the engine could never drive to completion:
//   - a node is a branch of at most one fanout, listed once;
//   - every branch is a task or subflow node with no successor of its own (the join owns
//     what happens after the fan);
//   - a fanout's next is a join, each join closes exactly one fanout, and its
//     wait_for is a subset of that fanout's branches;
//   - a quorum fits the number of awaited branches.
func (f *Flow) validateFans() error {
	f.branchOf = map[string]string{}
	f.joinOf = map[string]string{}

	for i := range f.Nodes {
		n := &f.Nodes[i]
		if n.Type != NodeFanout {
			continue
		}

		if err := f.validateFanBranches(n); err != nil {
			return err
		}

		j := f.byID[n.Next]
		if j.Type != NodeJoin {
			return f.errorf("fanout %q leads to %q, a %s node; a fanout's next must be a join", n.ID, j.ID, j.Type)
		}

		for fan, other := range f.joinOf {
			if other == j.ID {
				return f.errorf("join %q closes both fanouts %q and %q; use one join per fanout", j.ID, fan, n.ID)
			}
		}

		f.joinOf[n.ID] = j.ID
	}

	for i := range f.Nodes {
		n := &f.Nodes[i]
		if n.Type != NodeJoin {
			continue
		}

		if err := f.validateJoinFan(n); err != nil {
			return err
		}
	}

	return nil
}

func (f *Flow) validateFanBranches(n *Node) error {
	for _, b := range n.Branches {
		if owner, seen := f.branchOf[b]; seen {
			if owner == n.ID {
				return f.errorf("fanout %q lists branch %q twice", n.ID, b)
			}

			return f.errorf("node %q is a branch of fanouts %q and %q; a node may belong to at most one fanout",
				b, owner, n.ID)
		}

		f.branchOf[b] = n.ID

		bn := f.byID[b]
		if bn.Type != NodeTask && bn.Type != NodeSubflow {
			return f.errorf("fanout %q: branch %q is a %s node; branches must be task or subflow nodes "+
				"(a subflow can hold any graph)", n.ID, b, bn.Type)
		}

		if bn.OnFailure != "" {
			return f.errorf("node %q is a branch of fanout %q and has on_failure; a failed branch is settled "+
				"by its join's policy — put on_failure on join %q instead", b, n.ID, n.Next)
		}

		if bn.Next != "" || len(bn.Dynamic) > 0 {
			return f.errorf("node %q is a branch of fanout %q and routes onward; a branch does not advance the "+
				"execution (its join does) — put the successor after the join instead", b, n.ID)
		}
	}

	return nil
}

func (f *Flow) validateJoinFan(n *Node) error {
	var fan string

	for fo, j := range f.joinOf {
		if j == n.ID {
			fan = fo
		}
	}

	if fan == "" {
		return f.errorf("join %q is not the next of any fanout (it would never be reached)", n.ID)
	}

	branches := map[string]bool{}
	for _, b := range f.byID[fan].Branches {
		branches[b] = true
	}

	for _, w := range n.WaitFor {
		if !branches[w] {
			return f.errorf("join %q waits for %q, which is not a branch of its fanout %q (it would never settle)",
				n.ID, w, fan)
		}
	}

	if kind, q := n.JoinKind(); kind == JoinQuorum && q > len(f.WaitFor(n.ID)) {
		return f.errorf("join %q: quorum:%d exceeds the %d awaited branches", n.ID, q, len(f.WaitFor(n.ID)))
	}

	return nil
}

// routes returns every ordinary routing transition out of n (not fanout
// branches): next, choice rule targets, await on_timeout, on_failure, dynamic
// targets.
func routes(n *Node) []string {
	var out []string

	if n.Next != "" {
		out = append(out, n.Next)
	}

	for _, r := range n.Rules {
		out = append(out, r.To)
	}

	if n.OnTimeout != "" {
		out = append(out, n.OnTimeout)
	}

	if n.OnFailure != "" {
		out = append(out, n.OnFailure)
	}

	return append(out, n.Dynamic...)
}

// rejectBranchEntry refuses any ordinary routing into a fan-out branch or into
// a join from anything but its own fanout. A branch reached by ordinary routing
// would run alone with nowhere to advance, and the execution would report
// completed with the fan and everything after it skipped — the worst outcome a
// validator can permit.
func (f *Flow) rejectBranchEntry() error {
	for i := range f.Nodes {
		n := &f.Nodes[i]

		for _, to := range routes(n) {
			if owner, isBranch := f.branchOf[to]; isBranch {
				return f.errorf("node %q routes to %q, a branch of fanout %q; route to %q instead",
					n.ID, to, owner, owner)
			}

			if f.byID[to].Type == NodeJoin && f.joinOf[n.ID] != to {
				return f.errorf("node %q routes to join %q; a join is reached only through its fanout", n.ID, to)
			}
		}
	}

	return nil
}

// resolveStart sets the entry node: the explicit start, or the unique node
// with no inbound transition. The no-start error names what routes into each
// node, because the usual cause is a retry loop back to the first node and the
// author needs the reference to change (or an explicit start).
func (f *Flow) resolveStart() error {
	if f.Start != "" {
		n := f.byID[f.Start]
		if n == nil {
			return f.errorf("start references unknown node %q", f.Start)
		}

		if _, isBranch := f.branchOf[n.ID]; isBranch || n.Type == NodeJoin {
			return f.errorf("start node %q must not be a fanout branch or a join", n.ID)
		}

		f.startID = n.ID

		return nil
	}

	inbound := map[string]string{}

	for i := range f.Nodes {
		n := &f.Nodes[i]

		for _, to := range append(routes(n), n.Branches...) {
			if _, seen := inbound[to]; !seen {
				inbound[to] = n.ID
			}
		}
	}

	var starts []string

	for i := range f.Nodes {
		if _, ok := inbound[f.Nodes[i].ID]; !ok {
			starts = append(starts, f.Nodes[i].ID)
		}
	}

	switch len(starts) {
	case 1:
		f.startID = starts[0]

		return nil
	case 0:
		parts := make([]string, 0, len(f.Nodes))
		for i := range f.Nodes {
			parts = append(parts, fmt.Sprintf("%q by %q", f.Nodes[i].ID, inbound[f.Nodes[i].ID]))
		}

		return f.errorf("no start node (every node is routed into: %s); set start: explicitly",
			strings.Join(parts, ", "))
	default:
		return f.errorf("multiple start nodes %v; set start: explicitly or connect them", starts)
	}
}

// rejectUnreachable refuses nodes no execution can ever visit: dead
// configuration is almost always a typo'd route.
func (f *Flow) rejectUnreachable() error {
	seen := map[string]bool{f.startID: true}
	stack := []string{f.startID}

	for len(stack) > 0 {
		id := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		n := f.byID[id]

		for _, next := range append(routes(n), n.Branches...) {
			if !seen[next] {
				seen[next] = true

				stack = append(stack, next)
			}
		}
	}

	var unreachable []string

	for i := range f.Nodes {
		if !seen[f.Nodes[i].ID] {
			unreachable = append(unreachable, f.Nodes[i].ID)
		}
	}

	if len(unreachable) > 0 {
		sort.Strings(unreachable)

		return f.errorf("unreachable node(s) %v: not connected to the start node %q", unreachable, f.startID)
	}

	return nil
}
