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
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/builtin"
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

const (
	maxExpressionNodes  uint = 2048
	maxEvaluationMemory uint = 512
)

// Compile compiles a boolean expression that may reference `input`, `results`,
// `signals`, `branches` and `last_node`.
func Compile(code string) (*Program, error) {
	prog, err := expr.Compile(code,
		expr.Env(compileEnv()),
		expr.AsBool(),
		expr.AllowUndefinedVariables(),
		expr.MaxNodes(maxExpressionNodes),
		expr.Optimize(false),
	)
	if err != nil {
		return nil, fmt.Errorf("rules: compile %q: %w", code, err)
	}

	if err = validateBudgetedProgram(code, prog); err != nil {
		return nil, err
	}

	return &Program{src: code, prog: prog}, nil
}

// validateBudgetedProgram keeps choice predicates in a bounded routing subset.
// Expr has no interruptible VM deadline, so reject constructs that can loop over
// caller-controlled data or allocate arbitrarily; the remaining bytecode is a
// straight-line predicate over a size-limited execution document.
func validateBudgetedProgram(code string, prog *vm.Program) error {
	for ip, op := range prog.Bytecode {
		//nolint:exhaustive // Validation only rejects disallowed opcodes; the rest are allowed.
		switch op {
		case vm.OpRange:
			return fmt.Errorf("rules: compile %q: range expressions are not allowed in choice rules", code)
		case vm.OpJumpBackward:
			return fmt.Errorf("rules: compile %q: iteration expressions are not allowed in choice rules", code)
		case vm.OpCall, vm.OpCall0, vm.OpCall1, vm.OpCall2, vm.OpCall3,
			vm.OpCallN, vm.OpCallFast, vm.OpCallSafe, vm.OpCallTyped:
			return fmt.Errorf("rules: compile %q: function calls other than len() are not allowed in choice rules", code)
		case vm.OpCallBuiltin1:
			if name := builtinName(prog.Arguments[ip]); name != "len" {
				return fmt.Errorf("rules: compile %q: builtin %q is not allowed in choice rules", code, name)
			}
		default:
			continue
		}
	}

	return nil
}

func builtinName(arg int) string {
	if arg < 0 || arg >= len(builtin.Builtins) {
		return "unknown"
	}

	return builtin.Builtins[arg].Name
}

// Match evaluates the program against the assembled context document
// ({"input": …, "results": {…}, "signals": {…}}). A false result and a
// non-nil error are returned when evaluation fails (e.g. a referenced field is
// missing); callers typically treat that as "no match" and fall through to the
// default rule.
func (p *Program) Match(contextDoc json.RawMessage) (bool, error) {
	return p.MatchContext(context.Background(), contextDoc)
}

// MatchContext is Match using ctx for pre/post evaluation cancellation checks.
// Choice expressions are intentionally compiled to straight-line predicates and
// run with an explicit VM memory budget; expr does not provide a preemptive VM
// deadline, so compile-time restrictions are the primary CPU bound.
func (p *Program) MatchContext(ctx context.Context, contextDoc json.RawMessage) (bool, error) {
	if ctx == nil {
		return false, errors.New("rules: nil context")
	}

	if err := ctx.Err(); err != nil {
		return false, fmt.Errorf("rules: context: %w", err)
	}

	var m map[string]any
	if len(contextDoc) > 0 {
		if err := json.Unmarshal(contextDoc, &m); err != nil {
			return false, fmt.Errorf("rules: context: %w", err)
		}
	}

	if m == nil {
		m = map[string]any{}
	}

	out, err := (&vm.VM{MemoryBudget: maxEvaluationMemory}).Run(p.prog, m)
	if err != nil {
		return false, fmt.Errorf("rules: eval %q: %w", p.src, err)
	}

	if err = ctx.Err(); err != nil {
		return false, fmt.Errorf("rules: context: %w", err)
	}

	b, ok := out.(bool)
	if !ok {
		return false, fmt.Errorf("rules: %q did not evaluate to bool", p.src)
	}

	return b, nil
}
