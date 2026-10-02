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

// Package expr compiles and evaluates the expressions used by flow definitions:
// choice predicates (`when:`), map sources (`over:`), subflow inputs, search
// attributes and concurrency keys. Expressions are written in expr-lang and
// evaluated against the execution's context view:
//
//	input      the start payload
//	channels   the reduced state channels
//	results    each settled node's latest output, keyed by node id
//	last_node  the most recently settled node
//	visits     how many times each node was entered
//	signals    consumed signal payloads, keyed by signal name
//	branches   branch status of the current fan-out, keyed by branch id
//	counters   generic usage counters
//	item/index the current element of a map node (map instances only)
//
// Expressions are restricted to a bounded, straight-line subset (see
// validateBudgetedProgram): flow authors are not fully trusted, and expr has no
// preemptive VM deadline.
package expr

import (
	"context"
	"errors"
	"fmt"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/builtin"
	"github.com/expr-lang/expr/vm"
)

// Context view variable names.
const (
	VarInput    = "input"
	VarChannels = "channels"
	VarResults  = "results"
	VarLastNode = "last_node"
	VarVisits   = "visits"
	VarSignals  = "signals"
	VarBranches = "branches"
	VarCounters = "counters"
	VarErrors   = "errors"
	VarItem     = "item"
	VarIndex    = "index"
)

// compileEnv declares the variables an expression may reference, with their
// shapes, so that e.g. `results[last_node]` type-checks.
func compileEnv() map[string]any {
	return map[string]any{
		VarInput:    map[string]any{},
		VarChannels: map[string]any{},
		VarResults:  map[string]any{},
		VarLastNode: "",
		VarVisits:   map[string]any{},
		VarSignals:  map[string]any{},
		VarBranches: map[string]any{},
		VarCounters: map[string]any{},
		VarErrors:   map[string]any{},
		VarItem:     nil,
		VarIndex:    0,
	}
}

// Program is a compiled expression.
type Program struct {
	src  string
	prog *vm.Program
	pred bool
}

const (
	maxExpressionNodes  uint = 2048
	maxEvaluationMemory uint = 512
)

// CompilePredicate compiles a boolean expression (a choice rule).
func CompilePredicate(code string) (*Program, error) { return compile(code, true) }

// CompileValue compiles an expression producing any value (map `over`, subflow
// `input`, search attributes, concurrency keys).
func CompileValue(code string) (*Program, error) { return compile(code, false) }

func compile(code string, pred bool) (*Program, error) {
	opts := []expr.Option{
		expr.Env(compileEnv()),
		expr.AllowUndefinedVariables(),
		expr.MaxNodes(maxExpressionNodes),
		expr.Optimize(false),
	}
	if pred {
		opts = append(opts, expr.AsBool())
	}

	prog, err := expr.Compile(code, opts...)
	if err != nil {
		return nil, fmt.Errorf("expr: compile %q: %w", code, err)
	}

	if err = validateBudgetedProgram(code, prog); err != nil {
		return nil, err
	}

	return &Program{src: code, prog: prog, pred: pred}, nil
}

// String returns the source of the expression.
func (p *Program) String() string { return p.src }

// validateBudgetedProgram keeps expressions in a bounded subset. expr has no
// interruptible VM deadline and vm.VM.MemoryBudget only instruments a subset of
// opcodes — string concatenation (OpAdd) and slicing (OpSlice) allocate
// without ever calling memGrow, so a few chained `+` over a large payload field
// can allocate hundreds of megabytes despite the budget.
// Ranges, iteration and function calls other than len() are rejected for the
// same reason. Regex matching stays allowed: Go's regexp is RE2 (linear time).
func validateBudgetedProgram(code string, prog *vm.Program) error {
	for ip, op := range prog.Bytecode {
		//nolint:exhaustive // Validation only rejects disallowed opcodes; the rest are allowed.
		switch op {
		case vm.OpRange:
			return fmt.Errorf("expr: compile %q: range expressions are not allowed", code)
		case vm.OpJumpBackward:
			return fmt.Errorf("expr: compile %q: iteration expressions are not allowed", code)
		case vm.OpAdd, vm.OpSlice:
			return fmt.Errorf("expr: compile %q: concatenation and slicing are not allowed", code)
		case vm.OpCall, vm.OpCall0, vm.OpCall1, vm.OpCall2, vm.OpCall3,
			vm.OpCallN, vm.OpCallFast, vm.OpCallSafe, vm.OpCallTyped:
			return fmt.Errorf("expr: compile %q: function calls other than len() are not allowed", code)
		case vm.OpCallBuiltin1:
			if name := builtinName(prog.Arguments[ip]); name != "len" {
				return fmt.Errorf("expr: compile %q: builtin %q is not allowed", code, name)
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

// Eval evaluates the program against env (a context view as produced by the
// fold). ctx is checked before and after evaluation; the compile-time
// restrictions are the primary CPU bound.
func (p *Program) Eval(ctx context.Context, env map[string]any) (any, error) {
	if ctx == nil {
		return nil, errors.New("expr: nil context")
	}

	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("expr: context: %w", err)
	}

	if env == nil {
		env = map[string]any{}
	}

	out, err := (&vm.VM{MemoryBudget: maxEvaluationMemory}).Run(p.prog, env)
	if err != nil {
		return nil, fmt.Errorf("expr: eval %q: %w", p.src, err)
	}

	if err = ctx.Err(); err != nil {
		return nil, fmt.Errorf("expr: context: %w", err)
	}

	return out, nil
}

// Match evaluates a predicate. A false result and a non-nil error are returned
// when evaluation fails (e.g. a referenced field is missing); callers decide
// whether that means "no match" or a failure (choice on_error).
func (p *Program) Match(ctx context.Context, env map[string]any) (bool, error) {
	out, err := p.Eval(ctx, env)
	if err != nil {
		return false, err
	}

	b, ok := out.(bool)
	if !ok {
		return false, fmt.Errorf("expr: %q did not evaluate to bool", p.src)
	}

	return b, nil
}
