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

// Package invocation defines the document the engine assembles for every
// invocation — the single description of what an invoker receives, what
// Server.Results returns, and what a choice expression is evaluated against.
//
// It exists because those three consumers previously each restated the same
// five field names: the engine in an anonymous marshal struct, the rules
// package in its compile environment, and every embedder in a hand-written
// mirror decoded with encoding/json. A rename in one place did not fail to
// compile in the others — it deserialised to a zero value, so an invoker
// silently saw no previous result and a choice rule silently took its default
// branch. Declaring the shape once makes that a build error.
//
// The root package re-exports Context as packtrail.InvocationContext.
package invocation

import "encoding/json"

// Variable names, as they appear in the JSON document and as the identifiers a
// choice expression may reference. They are constants so a rename reaches the
// rules environment and the decoder together.
const (
	VarInput      = "input"
	VarResults    = "results"
	VarSignals    = "signals"
	VarBranches   = "branches"
	VarLastNode   = "last_node"
	VarReleasedBy = "released_by"
	VarVisits     = "visits"
)

// Context is the assembled invocation document.
//
// Every field is derived state: the engine rebuilds it from the store for each
// invocation rather than threading a mutable payload between nodes, so a node
// can never corrupt what a later node reads.
type Context struct {
	// Input is the payload the execution was started with. Always a JSON
	// object; an execution started with no payload gets `{}`.
	Input json.RawMessage `json:"input"`
	// Results holds every settled node's output, keyed by node id. An agent-style
	// invoker that returns a bare JSON string finds it here unwrapped.
	Results map[string]json.RawMessage `json:"results"`
	// Signals holds the payload of every signal received so far, keyed by signal
	// name. A signal that has been consumed by a waiting node stays here.
	Signals map[string]json.RawMessage `json:"signals"`
	// Branches holds the outputs of the fan currently being joined, keyed by
	// branch node id — not every branch the execution has ever run. A flow with
	// two sequential fan-outs sees only the second fan's replies at the second
	// join. Whether the join just happened is `last_node ∈ branches`.
	Branches map[string]json.RawMessage `json:"branches"`
	// LastNode is the id of the most recently settled output, so "the previous
	// step's result" is Results[LastNode]. Signal nodes produce no output and
	// therefore never appear here — see ReleasedBy.
	LastNode string `json:"last_node"`
	// ReleasedBy is the name of the signal that released the wait immediately
	// preceding this invocation, and is empty for every other node. It is what
	// makes a human-in-the-loop payload reachable: the signal's own payload is
	// Signals[ReleasedBy].
	//
	// It is scoped to the node the release advanced to. A later node does not
	// inherit it, so it cannot be mistaken for "some signal arrived at some
	// point" — that question is answered by Signals.
	ReleasedBy string `json:"released_by,omitempty"`
	// Visits counts how many times each node has been entered so far, keyed by
	// node id, including the node being invoked (so its own count is at least
	// 1). It counts visits, not attempts: a node retried three times on one
	// visit counts once.
	//
	// It exists so a cycle can bound itself. A flow that loops until something
	// passes has no other way to ask "how many times have I been here?" — the
	// engine knows, the document did not say, and every caller re-invented the
	// count in its own node outputs, which breaks as soon as a flow has two
	// loops.
	Visits map[string]uint64 `json:"visits,omitempty"`
}

// Decode parses an assembled document, tolerating an empty one (which yields a
// zero Context rather than an error, since an execution with no input and no
// settled node is a legitimate state).
func Decode(doc json.RawMessage) (Context, error) {
	var c Context

	if len(doc) == 0 {
		return c, nil
	}

	if err := json.Unmarshal(doc, &c); err != nil {
		return Context{}, err
	}

	return c, nil
}

// Env returns the variable skeleton a choice expression is compiled against.
// Derived from the same constants as the document, so a field added to Context
// is referenceable from a rule only once it is named here too.
func Env() map[string]any {
	return map[string]any{
		VarInput:      map[string]any{},
		VarResults:    map[string]any{},
		VarSignals:    map[string]any{},
		VarBranches:   map[string]any{},
		VarLastNode:   "",
		VarReleasedBy: "",
		VarVisits:     map[string]any{},
	}
}
