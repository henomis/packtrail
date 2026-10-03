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
	"errors"
	"fmt"
	"strings"
)

// ValidationError is one problem found by Validate. Validate reports every
// problem it finds, joined with errors.Join; ValidationErrors lists them.
type ValidationError struct {
	// Flow is the name of the flow being validated.
	Flow string
	// Node is the id of the node at fault, as written; empty for a
	// flow-level problem.
	Node string
	// Field is the YAML path of the offending field inside Node, or inside
	// the flow when Node is empty: "retry.max_attempts", "rules[1].when",
	// "channels.c.reducer", "start". Empty when no single field applies.
	Field string
	// Msg describes the problem, without the flow, node and field.
	Msg string
	// Err is the underlying cause (an invalid name, an expression or schema
	// compile error), or nil.
	Err error
}

// Error renders the problem as `flow "x": node "a": retry.max_attempts: msg`,
// omitting the empty parts.
func (e *ValidationError) Error() string {
	var b strings.Builder

	fmt.Fprintf(&b, "flow %q: ", e.Flow)

	if e.Node != "" {
		fmt.Fprintf(&b, "node %q: ", e.Node)
	}

	if e.Field != "" {
		b.WriteString(e.Field)
		b.WriteString(": ")
	}

	b.WriteString(e.Msg)

	return b.String()
}

// Unwrap returns the underlying cause.
func (e *ValidationError) Unwrap() error { return e.Err }

// ValidationErrors returns every *ValidationError in err's tree, in order,
// looking through errors.Join and fmt.Errorf wrapping (such as the file path
// ParseFile adds). It returns nil if there are none.
func ValidationErrors(err error) []*ValidationError {
	var out []*ValidationError

	var walk func(error)

	walk = func(e error) {
		switch x := e.(type) { //nolint:errorlint // walking the tree by hand.
		case nil:
		case *ValidationError:
			out = append(out, x)
		case interface{ Unwrap() []error }:
			for _, inner := range x.Unwrap() {
				walk(inner)
			}
		default:
			walk(errors.Unwrap(e))
		}
	}

	walk(err)

	return out
}

// problems collects the ValidationErrors of one Validate pass.
type problems struct {
	flow string
	list []error
}

// add records a problem on node (empty for the flow) at field. cause, if not
// nil, is kept as Err and its text is appended to the message.
func (p *problems) add(node, field string, cause error, format string, args ...any) {
	msg := fmt.Sprintf(format, args...)

	if cause != nil {
		if msg == "" {
			msg = cause.Error()
		} else {
			msg += ": " + cause.Error()
		}
	}

	p.list = append(p.list, &ValidationError{Flow: p.flow, Node: node, Field: field, Msg: msg, Err: cause})
}

func (p *problems) err() error { return errors.Join(p.list...) }
