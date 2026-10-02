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
	"slices"
	"sort"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/henomis/packtrail/internal/expr"
	"github.com/henomis/packtrail/internal/names"
)

// Validate checks structural and semantic correctness of the flow and builds
// the indexes and compiled expressions the engine uses. It is called by Parse
// and ParseJSON; a Flow built in Go must be validated before use.
func (f *Flow) Validate() error {
	if err := names.CheckToken("flow name", f.Name); err != nil {
		return err
	}

	if v := strings.TrimSpace(f.Version); v != "" && v != SupportedVersion {
		return fmt.Errorf("flow %q: unsupported version %q (this build supports %q)", f.Name, f.Version, SupportedVersion)
	}

	if len(f.Nodes) == 0 {
		return fmt.Errorf("flow %q: no nodes", f.Name)
	}

	steps := []func() error{
		f.indexNodes,
		f.validateFlowLevel,
		f.validateNodes,
		f.validateFans,
		f.rejectBranchEntry,
		f.resolveStart,
		f.rejectUnreachable,
	}

	for _, step := range steps {
		if err := step(); err != nil {
			return err
		}
	}

	return nil
}

func (f *Flow) errorf(format string, args ...any) error {
	return fmt.Errorf("flow %q: "+format, append([]any{f.Name}, args...)...)
}

func (f *Flow) indexNodes() error {
	f.byID = make(map[string]*Node, len(f.Nodes))

	for i := range f.Nodes {
		n := &f.Nodes[i]
		if err := names.CheckToken("node id", n.ID); err != nil {
			return f.errorf("%w", err)
		}

		if _, dup := f.byID[n.ID]; dup {
			return f.errorf("duplicate node id %q", n.ID)
		}

		f.byID[n.ID] = n
	}

	return nil
}

func (f *Flow) validateFlowLevel() error {
	if f.MaxSteps < 0 {
		return f.errorf("max_steps must not be negative")
	}

	if f.Retention < 0 {
		return f.errorf("retention must not be negative")
	}

	if err := f.validateChannels(); err != nil {
		return err
	}

	if err := f.validateBudgetAndAttrs(); err != nil {
		return err
	}

	return f.validateTriggers()
}

func (f *Flow) validateBudgetAndAttrs() error {
	for k, v := range f.Budget {
		if err := names.CheckToken("budget counter", k); err != nil {
			return f.errorf("%w", err)
		}

		if v <= 0 {
			return f.errorf("budget %q must be positive", k)
		}
	}

	f.attrs = make(map[string]*expr.Program, len(f.SearchAttributes))

	for k, src := range f.SearchAttributes {
		if err := names.CheckToken("search attribute", k); err != nil {
			return f.errorf("%w", err)
		}

		p, err := expr.CompileValue(src)
		if err != nil {
			return f.errorf("search attribute %q: %w", k, err)
		}

		f.attrs[k] = p
	}

	return nil
}

func (f *Flow) validateTriggers() error {
	for _, tr := range f.Triggers {
		if strings.TrimSpace(tr.Subject) == "" || strings.ContainsAny(tr.Subject, " \t\r\n") {
			return f.errorf("trigger subject %q is invalid", tr.Subject)
		}

		if tr.Stream != "" {
			if err := names.CheckToken("trigger stream", tr.Stream); err != nil {
				return f.errorf("%w", err)
			}
		}
	}

	return nil
}

func (f *Flow) validateChannels() error {
	for name, c := range f.Channels {
		if err := names.CheckToken("channel name", name); err != nil {
			return f.errorf("%w", err)
		}

		var ok bool

		switch c.ReducerOrDefault() {
		case ReducerReplace:
			ok = true
		case ReducerAppend:
			_, ok = c.Default.([]any)
			ok = ok || c.Default == nil
		case ReducerMerge:
			_, ok = c.Default.(map[string]any)
			ok = ok || c.Default == nil
		case ReducerSum:
			_, ok = c.Default.(float64)
			ok = ok || c.Default == nil
		default:
			return f.errorf("channel %q: unknown reducer %q (want replace, append, merge or sum)", name, c.Reducer)
		}

		if !ok {
			return f.errorf("channel %q: default does not fit reducer %q", name, c.ReducerOrDefault())
		}
	}

	return nil
}

func (f *Flow) validateNodes() error {
	for i := range f.Nodes {
		n := &f.Nodes[i]

		if err := f.validateNode(n); err != nil {
			return err
		}

		if n.Next != "" {
			if err := f.ref(n, fNext, n.Next); err != nil {
				return err
			}

			if n.Next == n.ID {
				return f.errorf("node %q: next points to itself (would loop forever with no exit)", n.ID)
			}
		}

		if n.OnFailure != "" {
			if err := f.ref(n, fOnFailure, n.OnFailure); err != nil {
				return err
			}

			if n.OnFailure == n.ID {
				return f.errorf("node %q: on_failure points to itself; to try again, use retry", n.ID)
			}
		}
	}

	return nil
}

func (f *Flow) ref(n *Node, field, id string) error {
	if id == "" {
		return f.errorf("node %q: %s is required", n.ID, field)
	}

	if f.byID[id] == nil {
		return f.errorf("node %q: %s references unknown node %q", n.ID, field, id)
	}

	return nil
}

func (f *Flow) validateNode(n *Node) error {
	if err := f.rejectForeignFields(n); err != nil {
		return err
	}

	switch n.Type {
	case NodeTask:
		return f.validateTask(n)
	case NodeChoice:
		return f.validateChoice(n)
	case NodeFanout:
		return f.validateFanout(n)
	case NodeJoin:
		return f.validateJoin(n)
	case NodeAwait:
		return f.validateAwait(n)
	case NodeMap:
		return f.validateMap(n)
	case NodeSubflow:
		return f.validateSubflow(n)
	default:
		return f.errorf("node %q: unknown type %q", n.ID, n.Type)
	}
}

// Node field names, as written in YAML.
const (
	fKind          = "kind"
	fTimeout       = "timeout"
	fRetry         = "retry"
	fOutputSchema  = "output_schema"
	fCache         = "cache"
	fConcurrency   = "concurrency"
	fDynamic       = "dynamic"
	fRules         = "rules"
	fOnError       = "on_error"
	fBranches      = "branches"
	fWaitFor       = "wait_for"
	fPolicy        = "policy"
	fSignal        = "signal"
	fOnTimeout     = "on_timeout"
	fOver          = "over"
	fMaxParallel   = "max_parallel"
	fFlow          = "flow"
	fInput         = "input"
	fOnParentClose = "on_parent_close"
	fNext          = "next"
	fOnFailure     = "on_failure"
)

// fieldSet names the type-specific fields set on a node.
func fieldSet(n *Node) map[string]bool {
	return map[string]bool{
		fKind:          n.Kind != "",
		fTimeout:       n.Timeout != 0,
		fRetry:         n.Retry != nil,
		fOutputSchema:  n.OutputSchema != nil,
		fCache:         n.Cache != nil,
		fConcurrency:   n.Concurrency != nil,
		fDynamic:       len(n.Dynamic) > 0,
		fRules:         len(n.Rules) > 0,
		fOnError:       n.OnError != "",
		fBranches:      len(n.Branches) > 0,
		fWaitFor:       len(n.WaitFor) > 0,
		fPolicy:        n.Policy != "",
		fSignal:        n.Signal != "",
		fOnTimeout:     n.OnTimeout != "",
		fOver:          n.Over != "",
		fMaxParallel:   n.MaxParallel != 0,
		fFlow:          n.Flow != "",
		fInput:         n.Input != "",
		fOnParentClose: n.OnParentClose != "",
		fNext:          n.Next != "",
		fOnFailure:     n.OnFailure != "",
	}
}

var allowedFields = map[string][]string{
	NodeTask:    {fKind, fTimeout, fRetry, fOutputSchema, fCache, fConcurrency, fDynamic, fNext, fOnFailure},
	NodeChoice:  {fRules, fOnError},
	NodeFanout:  {fBranches, fNext},
	NodeJoin:    {fWaitFor, fPolicy, fNext, fOnFailure},
	NodeAwait:   {fSignal, fTimeout, fOnTimeout, fNext},
	NodeMap:     {fKind, fOver, fMaxParallel, fTimeout, fRetry, fOutputSchema, fNext, fOnFailure},
	NodeSubflow: {fFlow, fInput, fOnParentClose, fNext, fOnFailure},
}

// rejectForeignFields refuses a field that does not belong to the node's type:
// it would be silently ignored, which almost always hides a mistake.
func (f *Flow) rejectForeignFields(n *Node) error {
	allowed, known := allowedFields[n.Type]
	if !known {
		return nil
	}

	var foreign []string

	for field, set := range fieldSet(n) {
		if set && !slices.Contains(allowed, field) {
			foreign = append(foreign, field)
		}
	}

	if len(foreign) > 0 {
		sort.Strings(foreign)

		return f.errorf("node %q: field(s) %v do not apply to a %s node", n.ID, foreign, n.Type)
	}

	return nil
}

func (f *Flow) validateTask(n *Node) error {
	if err := names.CheckToken("worker kind", n.Kind); err != nil {
		return f.errorf("task node %q: %w", n.ID, err)
	}

	if err := f.validateAttemptPolicy(n); err != nil {
		return err
	}

	if n.Cache != nil && n.Cache.TTL <= 0 {
		return f.errorf("task node %q: cache.ttl must be positive", n.ID)
	}

	if c := n.Concurrency; c != nil {
		if c.Max <= 0 {
			return f.errorf("task node %q: concurrency.max must be positive", n.ID)
		}

		p, err := expr.CompileValue(c.Key)
		if err != nil {
			return f.errorf("task node %q: concurrency.key: %w", n.ID, err)
		}

		n.conc = p
	}

	for _, d := range n.Dynamic {
		if err := f.ref(n, "dynamic", d); err != nil {
			return err
		}
	}

	return nil
}

// validateAttemptPolicy checks the per-attempt policies shared by task and map
// nodes: timeout, retry and output schema.
func (f *Flow) validateAttemptPolicy(n *Node) error {
	if n.Timeout < 0 {
		return f.errorf("node %q: timeout must not be negative", n.ID)
	}

	if err := f.validateRetry(n); err != nil {
		return err
	}

	if n.OutputSchema == nil {
		n.schema = nil

		return nil
	}

	sch, err := compileSchema(n.OutputSchema)
	if err != nil {
		return f.errorf("node %q: output_schema: %w", n.ID, err)
	}

	n.schema = sch

	return nil
}

func (f *Flow) validateRetry(n *Node) error {
	r := n.Retry
	if r == nil {
		return nil
	}

	if r.MaxAttempts < 0 || r.MaxAttempts > MaxRetryAttempts {
		return f.errorf("node %q: retry.max_attempts must be between 0 and %d", n.ID, MaxRetryAttempts)
	}

	switch r.Backoff {
	case "", BackoffFixed, BackoffLinear, BackoffExponential:
	default:
		return f.errorf("node %q: unknown retry.backoff %q (want fixed, linear or exponential)", n.ID, r.Backoff)
	}

	if r.Delay < 0 || r.MaxDelay < 0 {
		return f.errorf("node %q: retry delays must not be negative", n.ID)
	}

	// A backoff with no attempts contradicts itself: it would run once and
	// never retry.
	if r.Backoff != "" && r.MaxAttempts < 2 { //nolint:mnd // one retry needs two attempts.
		return f.errorf("node %q: retry declares backoff %q but max_attempts < 2, so it would never retry",
			n.ID, r.Backoff)
	}

	return nil
}

var errRemoteRef = errors.New("output_schema may not load external references")

type denyLoader struct{}

func (denyLoader) Load(string) (any, error) { return nil, errRemoteRef }

func compileSchema(doc any) (*jsonschema.Schema, error) {
	c := jsonschema.NewCompiler()
	c.UseLoader(denyLoader{})

	const url = "mem://output_schema.json"

	if err := c.AddResource(url, doc); err != nil {
		return nil, err
	}

	return c.Compile(url)
}

func (f *Flow) validateChoice(n *Node) error {
	if len(n.Rules) == 0 {
		return f.errorf("choice node %q: at least one rule is required", n.ID)
	}

	defaults := 0
	n.rules = make([]*expr.Program, len(n.Rules))

	for i, r := range n.Rules {
		switch {
		case r.Default && strings.TrimSpace(r.When) != "":
			return f.errorf("choice node %q: a default rule must not also carry a when expression", n.ID)
		case r.Default:
			defaults++
		case strings.TrimSpace(r.When) == "":
			return f.errorf("choice node %q: non-default rule needs a when expression", n.ID)
		default:
			p, err := expr.CompilePredicate(r.When)
			if err != nil {
				return f.errorf("choice node %q: rule %d: %w", n.ID, i, err)
			}

			n.rules[i] = p
		}

		if err := f.ref(n, "rule.to", r.To); err != nil {
			return err
		}
	}

	if defaults != 1 {
		return f.errorf("choice node %q: exactly one default rule is required (got %d)", n.ID, defaults)
	}

	if n.OnError != OnErrorDefault && n.OnError != OnErrorFail {
		return f.errorf("choice node %q: unknown on_error %q (want \"fail\" or omit)", n.ID, n.OnError)
	}

	return nil
}

func (f *Flow) validateFanout(n *Node) error {
	if len(n.Branches) == 0 {
		return f.errorf("fanout node %q: branches is required", n.ID)
	}

	if len(n.Branches) > MaxBranches {
		return f.errorf("fanout node %q: at most %d branches (use a map node for wider fan-outs)", n.ID, MaxBranches)
	}

	for _, b := range n.Branches {
		if err := f.ref(n, "branch", b); err != nil {
			return err
		}
	}

	if n.Next == "" {
		return f.errorf("fanout node %q: next is required and must be a join node", n.ID)
	}

	return nil
}

func (f *Flow) validateJoin(n *Node) error {
	seen := map[string]bool{}

	for _, w := range n.WaitFor {
		if err := f.ref(n, "wait_for", w); err != nil {
			return err
		}

		// A duplicate would double-count that branch in the join tally.
		if seen[w] {
			return f.errorf("join node %q: wait_for lists %q twice", n.ID, w)
		}

		seen[w] = true
	}

	kind, quorum := n.JoinKind()
	switch kind {
	case JoinAll, JoinAny:
	case JoinQuorum:
		if quorum <= 0 {
			return f.errorf("join node %q: quorum:N needs N > 0", n.ID)
		}
	default:
		return f.errorf("join node %q: unknown policy %q (want all, any or quorum:N)", n.ID, n.Policy)
	}

	return nil
}

func (f *Flow) validateAwait(n *Node) error {
	if err := names.CheckToken("signal name", n.Signal); err != nil {
		return f.errorf("await node %q: %w", n.ID, err)
	}

	// An await without a timeout can park an execution forever on a signal
	// nobody sends; the deadline is mandatory and on_timeout chooses between
	// failing and routing.
	if n.Timeout <= 0 {
		return f.errorf("await node %q: timeout is required and must be positive", n.ID)
	}

	if n.OnTimeout != "" {
		return f.ref(n, "on_timeout", n.OnTimeout)
	}

	return nil
}

func (f *Flow) validateMap(n *Node) error {
	if err := names.CheckToken("worker kind", n.Kind); err != nil {
		return f.errorf("map node %q: %w", n.ID, err)
	}

	p, err := expr.CompileValue(n.Over)
	if err != nil {
		return f.errorf("map node %q: over: %w", n.ID, err)
	}

	n.over = p

	if n.MaxParallel < 0 || n.MaxParallel > MaxParallel {
		return f.errorf("map node %q: max_parallel must be between 0 and %d", n.ID, MaxParallel)
	}

	return f.validateAttemptPolicy(n)
}

func (f *Flow) validateSubflow(n *Node) error {
	if err := names.CheckToken("subflow name", n.Flow); err != nil {
		return f.errorf("subflow node %q: %w", n.ID, err)
	}

	switch n.OnParentClose {
	case "", ParentCloseCancel, ParentCloseAbandon:
	default:
		return f.errorf("subflow node %q: unknown on_parent_close %q (want cancel or abandon)", n.ID, n.OnParentClose)
	}

	n.input = nil

	if strings.TrimSpace(n.Input) != "" {
		p, err := expr.CompileValue(n.Input)
		if err != nil {
			return f.errorf("subflow node %q: input: %w", n.ID, err)
		}

		n.input = p
	}

	return nil
}
