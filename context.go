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

package packtrail

import (
	"encoding/json"

	"github.com/henomis/packtrail/internal/invocation"
)

// InvocationContext is the document the engine assembles for every invocation:
// what an [Invoker] receives as Request.Payload, what [Server.Results] returns,
// and what a choice node's `when` expression is evaluated against. Its fields
// are the variables such an expression may reference.
//
// It is an alias for one internal declaration shared with the engine and the
// rule compiler, so an invoker written against this type cannot drift from what
// the engine actually sends. Decoding the payload by hand into a private mirror
// can: a renamed field unmarshals to a zero value instead of failing to
// compile, which reads downstream as "the previous node produced nothing".
//
//	func (x *myInvoker) Invoke(ctx context.Context, req packtrail.Request) (packtrail.Result, error) {
//		in, err := packtrail.DecodeContext(req.Payload)
//		if err != nil {
//			return packtrail.Result{}, err
//		}
//		previous := in.Results[in.LastNode]
//		…
//	}
type InvocationContext = invocation.Context

// DecodeContext parses an assembled invocation document — Request.Payload, or
// the result of [Server.Results].
//
// An empty document decodes to a zero [InvocationContext] without error: an
// execution started with no input, at a node with nothing settled before it, is
// a legitimate state and not a decode failure.
func DecodeContext(doc json.RawMessage) (InvocationContext, error) {
	return invocation.Decode(doc)
}

// The identifiers a choice expression may reference, and the corresponding keys
// in the assembled document. Exported so a layer that compiles its own surface
// syntax down to `when` expressions can name them instead of restating the
// string literals — a mismatch there evaluates to nil, and a nil comparison
// takes the default branch silently rather than erroring.
const (
	// VarInput is the payload the execution was started with.
	VarInput = invocation.VarInput
	// VarResults maps node id to that node's output.
	VarResults = invocation.VarResults
	// VarSignals maps signal name to the payload it carried.
	VarSignals = invocation.VarSignals
	// VarBranches maps branch node id to its output, for the fan being joined.
	VarBranches = invocation.VarBranches
	// VarLastNode is the id of the most recently settled output, so the previous
	// step's result is results[last_node].
	VarLastNode = invocation.VarLastNode
	// VarReleasedBy is the signal that released the wait immediately before this
	// node, and empty at every other node.
	VarReleasedBy = invocation.VarReleasedBy
)
