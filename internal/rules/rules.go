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

// Package rules compiles and evaluates the boolean expressions used by choice
// nodes. Expressions are written in expr-lang and evaluated against the
// invocation context, exposed as three variables: `input` (the start payload),
// `results` (each visited node's output, keyed by node id) and `signals`
// (received signal payloads, keyed by signal name).
//
//	when: "results.triage.risk_score > 80"
//	when: "input.tier == 'pro' && signals.approval.granted"
package rules

import (
	"encoding/json"
	"fmt"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
)

// compileEnv declares the variables an expression may reference; they mirror
// the top-level fields of the assembled invocation context document. last_node
// is the id of the most recently settled output, so "the previous step's
// result" is results[last_node]; branches holds the current fan's outputs.
func compileEnv() map[string]any {
	return map[string]any{
		"input":     map[string]any{},
		"results":   map[string]any{},
		"signals":   map[string]any{},
		"branches":  map[string]any{},
		"last_node": "",
	}
}

// Program is a compiled choice expression.
type Program struct {
	src  string
	prog *vm.Program
}

// Compile compiles a boolean expression that may reference `input`, `results`,
// `signals`, `branches` and `last_node`.
func Compile(code string) (*Program, error) {
	prog, err := expr.Compile(code,
		expr.Env(compileEnv()),
		expr.AsBool(),
		expr.AllowUndefinedVariables(),
	)
	if err != nil {
		return nil, fmt.Errorf("rules: compile %q: %w", code, err)
	}

	return &Program{src: code, prog: prog}, nil
}

// Match evaluates the program against the assembled context document
// ({"input": …, "results": {…}, "signals": {…}}). A false result and a
// non-nil error are returned when evaluation fails (e.g. a referenced field is
// missing); callers typically treat that as "no match" and fall through to the
// default rule.
func (p *Program) Match(contextDoc json.RawMessage) (bool, error) {
	var m map[string]any
	if len(contextDoc) > 0 {
		if err := json.Unmarshal(contextDoc, &m); err != nil {
			return false, fmt.Errorf("rules: context: %w", err)
		}
	}

	if m == nil {
		m = map[string]any{}
	}

	out, err := expr.Run(p.prog, m)
	if err != nil {
		return false, fmt.Errorf("rules: eval %q: %w", p.src, err)
	}

	b, ok := out.(bool)
	if !ok {
		return false, fmt.Errorf("rules: %q did not evaluate to bool", p.src)
	}

	return b, nil
}
