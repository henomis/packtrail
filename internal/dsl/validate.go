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
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// namePattern bounds identifiers that end up as NATS subject tokens or KV-key
// segments: flow names feed cron fire-subjects and visibility index keys, node
// ids feed result-cache keys and async job dedup ids, signal names feed signal
// subjects. Validating here fails fast with a clear error instead of an opaque
// NATS rejection (or a silently ambiguous key) at runtime.
var namePattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

// MaxRetryAttempts bounds a task node's retry.max_attempts. The ceiling keeps
// the exponential backoff shift (base << (attempt-1)) well clear of int64
// overflow and stops a pathological config from scheduling an unreasonable
// number of retries; 64 is far more than any real workload needs.
const MaxRetryAttempts = 64

// Validate checks structural and semantic correctness of the flow and builds
// the internal indexes used by the graph-walk helpers. It is called by Parse.
//
//nolint:gocognit,funlen
func (f *Flow) Validate() error {
	if strings.TrimSpace(f.Name) == "" {
		return fmt.Errorf("flow: missing name")
	}

	if !namePattern.MatchString(f.Name) {
		return fmt.Errorf(
			"flow %q: name must match [A-Za-z0-9_-]{1,128} (it becomes a NATS subject token and KV key segment)", f.Name)
	}

	if len(f.Nodes) == 0 {
		return fmt.Errorf("flow %q: no nodes", f.Name)
	}

	// Index nodes, checking for duplicates.
	f.byID = make(map[string]*Node, len(f.Nodes))
	for i := range f.Nodes {
		n := &f.Nodes[i]
		if n.ID == "" {
			return fmt.Errorf("flow %q: node with empty id", f.Name)
		}

		if !namePattern.MatchString(n.ID) {
			return fmt.Errorf(
				"flow %q: node id %q must match [A-Za-z0-9_-]{1,128} (it becomes a NATS subject token and KV key segment)",
				f.Name, n.ID)
		}

		if _, dup := f.byID[n.ID]; dup {
			return fmt.Errorf("flow %q: duplicate node id %q", f.Name, n.ID)
		}

		f.byID[n.ID] = n
	}

	// Per-type field validation.
	for i := range f.Nodes {
		if err := f.validateNode(&f.Nodes[i]); err != nil {
			return err
		}
	}

	// Build the edge map; an explicit edge defines the single successor of a node.
	f.next = make(map[string]string, len(f.Edges))
	inbound := make(map[string]bool)

	for _, e := range f.Edges {
		if f.byID[e.From] == nil {
			return fmt.Errorf("flow %q: edge from unknown node %q", f.Name, e.From)
		}

		if f.byID[e.To] == nil {
			return fmt.Errorf("flow %q: edge to unknown node %q", f.Name, e.To)
		}

		// A self-edge is an unconditional advance loop with no exit.
		if e.From == e.To {
			return fmt.Errorf("flow %q: node %q has a self-edge (would advance-loop forever)", f.Name, e.From)
		}

		if _, dup := f.next[e.From]; dup {
			return fmt.Errorf("flow %q: node %q has more than one outgoing edge", f.Name, e.From)
		}

		f.next[e.From] = e.To
		inbound[e.To] = true
	}

	// Mark targets reachable only via node-internal transitions as inbound, so
	// they are not mistaken for start nodes (fanout branches, choice/signal targets).
	for i := range f.Nodes {
		n := &f.Nodes[i]
		switch n.Type {
		case NodeFanout:
			for _, b := range n.Branches {
				inbound[b] = true
			}
		case NodeFanin:
			for _, w := range n.WaitFor {
				inbound[w] = true
			}
		case NodeChoice:
			for _, r := range n.Rules {
				inbound[r.To] = true
			}
		case NodeSignal:
			if n.OnTimeout != "" {
				inbound[n.OnTimeout] = true
			}
		}
	}

	// Determine the unique start node.
	var starts []string

	for i := range f.Nodes {
		if !inbound[f.Nodes[i].ID] {
			starts = append(starts, f.Nodes[i].ID)
		}
	}

	switch len(starts) {
	case 0:
		return fmt.Errorf("flow %q: no start node (every node has an inbound transition)", f.Name)
	case 1:
		f.startID = starts[0]
	default:
		return fmt.Errorf("flow %q: multiple start nodes %v (exactly one required)", f.Name, starts)
	}

	if err := f.validateFanMembership(); err != nil {
		return err
	}

	if err := f.validateFanAdjacency(); err != nil {
		return err
	}

	if err := f.rejectUnreachable(); err != nil {
		return err
	}

	return f.rejectFanCycles()
}

// rejectUnreachable refuses flows containing nodes no execution can ever
// visit: dead configuration is almost always a typo'd edge or rule target, and
// silently carrying it hides the mistake. The walk follows every runtime
// transition from the start node — the single outgoing edge, choice rule
// targets, a signal node's on_timeout route, and fanout branches (invoked
// inline by their fanout).
func (f *Flow) rejectUnreachable() error {
	seen := map[string]bool{f.startID: true}
	stack := []string{f.startID}

	for len(stack) > 0 {
		id := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		for _, next := range f.reachableFrom(id) {
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

		return fmt.Errorf(
			"flow %q: unreachable node(s) %v: not connected to the start node by any edge, "+
				"choice rule, fanout branch or on_timeout",
			f.Name, unreachable)
	}

	return nil
}

// reachableFrom returns every node id reachable from id via a single runtime
// transition: the linear edge (except out of fanout branch tasks, which are
// invoked inline and never advance), choice rule targets, a signal's on_timeout
// route, or a fanout's branches.
func (f *Flow) reachableFrom(id string) []string {
	var succ []string

	if to := f.next[id]; to != "" && !f.isFanoutBranch(id) {
		succ = append(succ, to)
	}

	n := f.byID[id]
	if n == nil {
		return succ
	}

	switch n.Type {
	case NodeFanout:
		succ = append(succ, n.Branches...)
	case NodeChoice:
		for _, r := range n.Rules {
			succ = append(succ, r.To)
		}
	case NodeSignal:
		if n.OnTimeout != "" {
			succ = append(succ, n.OnTimeout)
		}
	}

	return succ
}

func (f *Flow) isFanoutBranch(id string) bool {
	for i := range f.Nodes {
		n := &f.Nodes[i]
		if n.Type != NodeFanout {
			continue
		}

		for _, b := range n.Branches {
			if b == id {
				return true
			}
		}
	}

	return false
}

// validateFanAdjacency ties each fanout to the fanin that joins it, rejecting
// shapes the runtime can never drive to completion:
//   - every branch must be a task node — the branch runner invokes tasks only,
//     so a branch of any other type stays pending forever and strands the join;
//   - a fanout's single outgoing edge must lead to a fanin — stepFanout parks
//     the execution at that successor and evalFanin evaluates the join there;
//     any other successor is a terminal runtime error today;
//   - that fanin may wait only for branches of *this* fanout — a wait_for node
//     dispatched by a different fanout is never pending in this fan, so the
//     join would evaluate against state this fanout never produces.
func (f *Flow) validateFanAdjacency() error {
	for i := range f.Nodes {
		n := &f.Nodes[i]
		if n.Type != NodeFanout {
			continue
		}

		branches := make(map[string]bool, len(n.Branches))

		for _, b := range n.Branches {
			if bn := f.byID[b]; bn.Type != NodeTask {
				return fmt.Errorf(
					"flow %q: fanout %q: branch %q is a %s node; branches must be task nodes (any other type never settles)",
					f.Name, n.ID, b, bn.Type)
			}

			branches[b] = true
		}

		fanin := f.next[n.ID]
		if fanin == "" {
			return fmt.Errorf("flow %q: fanout %q has no outgoing edge; a fanout must lead to a fanin", f.Name, n.ID)
		}

		if jn := f.byID[fanin]; jn.Type != NodeFanin {
			return fmt.Errorf(
				"flow %q: fanout %q leads to %q, a %s node; a fanout's successor must be a fanin",
				f.Name, n.ID, fanin, jn.Type)
		}

		for _, w := range f.byID[fanin].WaitFor {
			if !branches[w] {
				return fmt.Errorf(
					"flow %q: fanin %q waits for %q, which is not a branch of its fanout %q (the join could never settle it)",
					f.Name, fanin, w, n.ID)
			}
		}
	}

	return nil
}

// validateFanMembership enforces the structural assumptions behind per-execution
// branch state (Execution.Branches is keyed by node id):
//   - a node is a branch of at most one fanout — two fanouts sharing a branch
//     would see (and silently skip on) each other's settled state;
//   - every fanin wait_for node is some fanout's branch — a wait_for node that
//     is never dispatched stays pending forever and the join can never settle.
func (f *Flow) validateFanMembership() error {
	branchOwner := map[string]string{} // branch node id -> owning fanout id

	for i := range f.Nodes {
		n := &f.Nodes[i]
		if n.Type != NodeFanout {
			continue
		}

		for _, b := range n.Branches {
			owner, seen := branchOwner[b]

			switch {
			case seen && owner == n.ID:
				return fmt.Errorf("flow %q: fanout %q lists branch %q twice", f.Name, n.ID, b)
			case seen:
				return fmt.Errorf("flow %q: node %q is a branch of fanouts %q and %q; a node may belong to at most one fanout",
					f.Name, b, owner, n.ID)
			}

			branchOwner[b] = n.ID
		}
	}

	for i := range f.Nodes {
		n := &f.Nodes[i]
		if n.Type != NodeFanin {
			continue
		}

		for _, w := range n.WaitFor {
			if _, ok := branchOwner[w]; !ok {
				return fmt.Errorf(
					"flow %q: fanin %q waits for node %q, which is not a branch of any fanout (it would never settle)",
					f.Name, n.ID, w)
			}
		}
	}

	return nil
}

// rejectFanCycles refuses flows where a fanout or fanin node lies on a cycle.
// Branch and join state is stored per execution, not per visit, so revisiting
// a fanout would silently reuse the previous visit's settled branches instead
// of re-running them (and a revisited fanin would re-join on stale state). The
// walk follows every runtime transition: the single outgoing edge, choice rule
// targets, and a signal node's on_timeout route. Branch nodes do not advance
// the execution (they are invoked inline by the fanout) and are not followed.
func (f *Flow) rejectFanCycles() error {
	for i := range f.Nodes {
		start := &f.Nodes[i]
		if start.Type != NodeFanout && start.Type != NodeFanin {
			continue
		}

		if f.advanceCycleFrom(start.ID) {
			return fmt.Errorf(
				"flow %q: %s node %q lies on a cycle; fanout/fanin state is per-execution and a revisit would reuse it",
				f.Name, start.Type, start.ID)
		}
	}

	return nil
}

// advanceCycleFrom reports whether a DFS along runtime-advance edges, starting
// from startID's successors, ever revisits startID.
func (f *Flow) advanceCycleFrom(startID string) bool {
	stack := f.advanceSucc(startID)
	seen := map[string]bool{}

	for len(stack) > 0 {
		id := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		if id == startID {
			return true
		}

		if seen[id] {
			continue
		}

		seen[id] = true
		stack = append(stack, f.advanceSucc(id)...)
	}

	return false
}

// advanceSucc returns id's runtime-advance successors: the linear edge, choice
// rule targets, and a signal node's on_timeout route. Branch nodes do not
// advance the execution (they are invoked inline by the fanout) and are not
// followed.
func (f *Flow) advanceSucc(id string) []string {
	var out []string

	if to := f.next[id]; to != "" {
		out = append(out, to)
	}

	n := f.byID[id]
	if n == nil {
		return out
	}

	switch n.Type {
	case NodeChoice:
		for _, r := range n.Rules {
			out = append(out, r.To)
		}
	case NodeSignal:
		if n.OnTimeout != "" {
			out = append(out, n.OnTimeout)
		}
	}

	return out
}

// validateTaskNode checks a task node's required transport and retry bounds.
func (f *Flow) validateTaskNode(n *Node) error {
	if strings.TrimSpace(n.Subject) == "" && strings.TrimSpace(n.Target) == "" {
		return fmt.Errorf("flow %q: task node %q: subject or target is required", f.Name, n.ID)
	}

	if n.Retry != nil && (n.Retry.MaxAttempts < 0 || n.Retry.MaxAttempts > MaxRetryAttempts) {
		return fmt.Errorf("flow %q: task node %q: retry.max_attempts must be between 0 and %d",
			f.Name, n.ID, MaxRetryAttempts)
	}

	if n.Retry != nil {
		switch strings.TrimSpace(n.Retry.Backoff) {
		case "", "fixed", "linear", "exponential":
		default:
			return fmt.Errorf("flow %q: task node %q: unknown retry.backoff %q (want exponential, linear, or fixed)",
				f.Name, n.ID, n.Retry.Backoff)
		}
	}

	// For the built-in nats-task kind the target becomes a NATS request
	// subject; whitespace or wildcard characters can never publish. Custom
	// invoker kinds interpret Target freely (it may be a URL), so only the
	// built-in kind is checked. The {execution_id} placeholder is legal.
	if n.InvokerKind() == DefaultInvoker {
		target := n.Target
		if target == "" {
			target = n.Subject
		}

		resolved := ResolvePlaceholders(target, "x")
		if strings.ContainsAny(resolved, " \t\n\r*>") || strings.Contains(resolved, "..") ||
			strings.HasPrefix(resolved, ".") || strings.HasSuffix(resolved, ".") {
			return fmt.Errorf(
				"flow %q: task node %q: subject %q contains whitespace or wildcard characters (it becomes a NATS request subject)",
				f.Name, n.ID, target)
		}
	}

	return nil
}

//nolint:gocognit,funlen
func (f *Flow) validateNode(n *Node) error {
	ref := func(id, field string) error {
		if id == "" {
			return fmt.Errorf("flow %q: node %q: %s is required", f.Name, n.ID, field)
		}

		if f.byID[id] == nil {
			return fmt.Errorf("flow %q: node %q: %s references unknown node %q", f.Name, n.ID, field, id)
		}

		return nil
	}

	switch n.Type {
	case NodeTask:
		return f.validateTaskNode(n)
	case NodeFanout:
		if len(n.Branches) == 0 {
			return fmt.Errorf("flow %q: fanout node %q: branches is required", f.Name, n.ID)
		}

		for _, b := range n.Branches {
			if err := ref(b, "branch"); err != nil {
				return err
			}
		}
	case NodeFanin:
		if len(n.WaitFor) == 0 {
			return fmt.Errorf("flow %q: fanin node %q: wait_for is required", f.Name, n.ID)
		}

		for _, w := range n.WaitFor {
			if err := ref(w, "wait_for"); err != nil {
				return err
			}
		}

		if jp := strings.TrimSpace(n.JoinPolicy); jp != "" && jp != JoinAll && jp != JoinAny && !strings.HasPrefix(jp, JoinQuorum+":") {
			return fmt.Errorf("flow %q: fanin node %q: unknown join_policy %q (want all, any, or quorum:N)",
				f.Name, n.ID, n.JoinPolicy)
		}

		kind, quorum := n.JoinKind()
		if kind == JoinQuorum && (quorum <= 0 || quorum > len(n.WaitFor)) {
			return fmt.Errorf("flow %q: fanin node %q: quorum:N must satisfy 0 < N <= len(wait_for)", f.Name, n.ID)
		}
	case NodeChoice:
		if err := f.validateChoiceRules(n, ref); err != nil {
			return err
		}
	case NodeSignal:
		if strings.TrimSpace(n.SignalName) == "" {
			return fmt.Errorf("flow %q: signal node %q: signal_name is required", f.Name, n.ID)
		}

		if !namePattern.MatchString(n.SignalName) {
			return fmt.Errorf(
				"flow %q: signal node %q: signal_name %q must match [A-Za-z0-9_-]{1,128} (it becomes a NATS subject token)",
				f.Name, n.ID, n.SignalName)
		}

		if n.OnTimeout != "" {
			if err := ref(n.OnTimeout, "on_timeout"); err != nil {
				return err
			}

			// Without a timeout the on_timeout route can never fire: the wait
			// schedule is only installed for a positive timeout, so the route
			// is dead configuration — almost certainly a forgotten timeout.
			if n.Timeout.D() <= 0 {
				return fmt.Errorf(
					"flow %q: signal node %q: on_timeout %q requires a positive timeout (without one the route never fires)",
					f.Name, n.ID, n.OnTimeout)
			}
		}
	default:
		return fmt.Errorf("flow %q: node %q: unknown type %q", f.Name, n.ID, n.Type)
	}

	return nil
}

// validateChoiceRules checks a choice node's rule set: at least one rule, every
// non-default rule has a when, exactly one default, no default that also carries
// a when (its when would be silently ignored), and every target references a
// known node. ref is validateNode's target-checking closure.
func (f *Flow) validateChoiceRules(n *Node, ref func(id, field string) error) error {
	if len(n.Rules) == 0 {
		return fmt.Errorf("flow %q: choice node %q: at least one rule is required", f.Name, n.ID)
	}

	defaults := 0

	for _, r := range n.Rules {
		switch {
		case r.Default && strings.TrimSpace(r.When) != "":
			// A default rule with a when is ambiguous — the when is silently
			// ignored at runtime. Reject it rather than surprise the author.
			return fmt.Errorf("flow %q: choice node %q: a default rule must not also carry a when expression", f.Name, n.ID)
		case r.Default:
			defaults++
		case strings.TrimSpace(r.When) == "":
			return fmt.Errorf("flow %q: choice node %q: non-default rule needs a when expression", f.Name, n.ID)
		}

		if err := ref(r.To, "rule.to"); err != nil {
			return err
		}
	}

	if defaults == 0 {
		return fmt.Errorf("flow %q: choice node %q: a default rule is required", f.Name, n.ID)
	}

	if defaults > 1 {
		// More than one default silently last-wins at runtime; reject the
		// ambiguity at load time.
		return fmt.Errorf("flow %q: choice node %q: only one default rule is allowed", f.Name, n.ID)
	}

	return nil
}
