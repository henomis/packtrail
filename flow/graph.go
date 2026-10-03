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
//
// It runs after every reference has resolved.
func (f *Flow) validateFans(p *problems) {
	f.branchOf = map[string]string{}
	f.joinOf = map[string]string{}

	for i := range f.Nodes {
		n := &f.Nodes[i]
		if n.Type != NodeFanout {
			continue
		}

		f.validateFanBranches(p, n)

		j := f.byID[n.Next]
		if j.Type != NodeJoin {
			p.add(n.ID, fNext, nil, "leads to %q, a %s node; a fanout's next must be a join", j.ID, j.Type)

			continue
		}

		if fan, closed := f.fanOf(j.ID); closed {
			p.add(j.ID, "", nil, "closes both fanouts %q and %q; use one join per fanout", fan, n.ID)

			continue
		}

		f.joinOf[n.ID] = j.ID
	}

	for i := range f.Nodes {
		n := &f.Nodes[i]
		if n.Type == NodeJoin {
			f.validateJoinFan(p, n)
		}
	}
}

// fanOf returns the fanout that join closes, if any.
func (f *Flow) fanOf(join string) (string, bool) {
	for fan, j := range f.joinOf {
		if j == join {
			return fan, true
		}
	}

	return "", false
}

func (f *Flow) validateFanBranches(p *problems, n *Node) {
	for i, b := range n.Branches {
		field := fmt.Sprintf("branches[%d]", i)

		if owner, seen := f.branchOf[b]; seen {
			if owner == n.ID {
				p.add(n.ID, field, nil, "lists branch %q twice", b)
			} else {
				p.add(b, "", nil, "is a branch of fanouts %q and %q; a node may belong to at most one fanout",
					owner, n.ID)
			}

			continue
		}

		f.branchOf[b] = n.ID

		bn := f.byID[b]
		if bn.Type != NodeTask && bn.Type != NodeSubflow {
			p.add(n.ID, field, nil, "branch %q is a %s node; branches must be task or subflow nodes "+
				"(a subflow can hold any graph)", b, bn.Type)
		}

		if bn.OnFailure != "" {
			p.add(b, fOnFailure, nil, "node is a branch of fanout %q; a failed branch is settled "+
				"by its join's policy — put on_failure on join %q instead", n.ID, n.Next)
		}

		if bn.Next != "" || len(bn.Dynamic) > 0 {
			p.add(b, "", nil, "node is a branch of fanout %q and routes onward; a branch does not advance the "+
				"execution (its join does) — put the successor after the join instead", n.ID)
		}
	}
}

func (f *Flow) validateJoinFan(p *problems, n *Node) {
	fan, ok := f.fanOf(n.ID)
	if !ok {
		p.add(n.ID, "", nil, "join is not the next of any fanout (it would never be reached)")

		return
	}

	branches := map[string]bool{}
	for _, b := range f.byID[fan].Branches {
		branches[b] = true
	}

	for i, w := range n.WaitFor {
		if !branches[w] {
			p.add(n.ID, fmt.Sprintf("wait_for[%d]", i), nil,
				"waits for %q, which is not a branch of its fanout %q (it would never settle)", w, fan)
		}
	}

	if kind, q := n.JoinKind(); kind == JoinQuorum && q > len(f.WaitFor(n.ID)) {
		p.add(n.ID, fPolicy, nil, "quorum:%d exceeds the %d awaited branches", q, len(f.WaitFor(n.ID)))
	}
}

// route is one ordinary routing transition: the field it is declared in and
// its target node.
type route struct{ field, to string }

// routes returns every ordinary routing transition out of n (not fanout
// branches): next, choice rule targets, await on_timeout, on_failure, dynamic
// targets.
func routes(n *Node) []route {
	var out []route

	if n.Next != "" {
		out = append(out, route{fNext, n.Next})
	}

	for i, r := range n.Rules {
		out = append(out, route{fmt.Sprintf("rules[%d].to", i), r.To})
	}

	if n.OnTimeout != "" {
		out = append(out, route{fOnTimeout, n.OnTimeout})
	}

	if n.OnFailure != "" {
		out = append(out, route{fOnFailure, n.OnFailure})
	}

	for i, d := range n.Dynamic {
		out = append(out, route{fmt.Sprintf("dynamic[%d]", i), d})
	}

	return out
}

// successors returns every node n can transfer control to: its routes and
// its fanout branches.
func successors(n *Node) []string {
	rs := routes(n)
	out := make([]string, 0, len(rs)+len(n.Branches))

	for _, r := range rs {
		out = append(out, r.to)
	}

	return append(out, n.Branches...)
}

// rejectBranchEntry refuses any ordinary routing into a fan-out branch or into
// a join from anything but its own fanout. A branch reached by ordinary routing
// would run alone with nowhere to advance, and the execution would report
// completed with the fan and everything after it skipped — the worst outcome a
// validator can permit.
func (f *Flow) rejectBranchEntry(p *problems) {
	for i := range f.Nodes {
		n := &f.Nodes[i]

		for _, r := range routes(n) {
			if owner, isBranch := f.branchOf[r.to]; isBranch {
				p.add(n.ID, r.field, nil, "routes to %q, a branch of fanout %q; route to %q instead",
					r.to, owner, owner)

				continue
			}

			if f.byID[r.to].Type == NodeJoin && f.joinOf[n.ID] != r.to {
				p.add(n.ID, r.field, nil, "routes to join %q; a join is reached only through its fanout", r.to)
			}
		}
	}
}

// resolveStart sets the entry node: the explicit start, or the unique node
// with no inbound transition. The no-start error names what routes into each
// node, because the usual cause is a retry loop back to the first node and the
// author needs the reference to change (or an explicit start). It reports
// whether a start was resolved.
func (f *Flow) resolveStart(p *problems) bool {
	if f.Start != "" {
		n := f.byID[f.Start]
		if n == nil {
			p.add("", "start", nil, "references unknown node %q", f.Start)

			return false
		}

		if _, isBranch := f.branchOf[n.ID]; isBranch || n.Type == NodeJoin {
			p.add("", "start", nil, "start node %q must not be a fanout branch or a join", n.ID)

			return false
		}

		f.startID = n.ID

		return true
	}

	inbound := map[string]string{}

	for i := range f.Nodes {
		n := &f.Nodes[i]

		for _, to := range successors(n) {
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

		return true
	case 0:
		parts := make([]string, 0, len(f.Nodes))
		for i := range f.Nodes {
			parts = append(parts, fmt.Sprintf("%q by %q", f.Nodes[i].ID, inbound[f.Nodes[i].ID]))
		}

		p.add("", "start", nil, "no start node (every node is routed into: %s); set start: explicitly",
			strings.Join(parts, ", "))
	default:
		p.add("", "start", nil, "multiple start nodes %v; set start: explicitly or connect them", starts)
	}

	return false
}

// rejectUnreachable refuses nodes no execution can ever visit: dead
// configuration is almost always a typo'd route. Each unreachable node is
// reported on its own.
func (f *Flow) rejectUnreachable(p *problems) {
	seen := map[string]bool{f.startID: true}
	stack := []string{f.startID}

	for len(stack) > 0 {
		id := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		for _, next := range successors(f.byID[id]) {
			if !seen[next] {
				seen[next] = true

				stack = append(stack, next)
			}
		}
	}

	for i := range f.Nodes {
		if !seen[f.Nodes[i].ID] {
			p.add(f.Nodes[i].ID, "", nil, "unreachable node: not connected to the start node %q", f.startID)
		}
	}
}
