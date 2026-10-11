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
	"encoding/json"
	"errors"
	"fmt"
	"maps"
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
//
// Validate reports every problem it finds, each a *ValidationError, joined
// with errors.Join (list them with ValidationErrors), in a stable order:
// flow-level problems, then nodes in declaration order, then the graph. Graph
// checks (fans, start, reachability) run only once every node is valid on its
// own and every reference resolves.
func (f *Flow) Validate() error {
	p := &problems{flow: f.Name}

	if err := names.CheckToken("flow name", f.Name); err != nil {
		p.add("", "name", err, "")
	}

	if v := strings.TrimSpace(f.Version); v != "" && v != SupportedVersion {
		p.add("", "version", nil, "unsupported version %q (this build supports %q)", f.Version, SupportedVersion)
	}

	if len(f.Nodes) == 0 {
		p.add("", "nodes", nil, "no nodes")

		return p.err()
	}

	f.indexNodes(p)
	f.validateFlowLevel(p)
	f.validateNodes(p)

	if len(p.list) > 0 {
		return p.err()
	}

	f.validateFans(p)
	f.rejectBranchEntry(p)

	if f.resolveStart(p) {
		f.rejectUnreachable(p)
	}

	return p.err()
}

// indexNodes builds byID. A duplicate id is reported and the first node with
// that id is kept, so references still resolve while the rest is checked.
func (f *Flow) indexNodes(p *problems) {
	f.byID = make(map[string]*Node, len(f.Nodes))

	for i := range f.Nodes {
		n := &f.Nodes[i]
		if err := names.CheckToken("node id", n.ID); err != nil {
			p.add(n.ID, "id", err, "")
		}

		if _, dup := f.byID[n.ID]; dup {
			p.add(n.ID, "id", nil, "duplicate node id")

			continue
		}

		f.byID[n.ID] = n
	}
}

// validateExpiry checks on_expire and archive_retention against retention.
func (f *Flow) validateExpiry(p *problems) {
	switch f.OnExpire {
	case "", ExpireArchive, ExpireDelete:
	default:
		p.add("", "on_expire", nil, "unknown value %q (%s or %s)", f.OnExpire, ExpireArchive, ExpireDelete)
	}

	if f.OnExpire != "" && f.Retention == 0 {
		p.add("", "on_expire", nil, "needs a retention: without one the execution never expires")
	}

	if f.ArchiveRetention < 0 {
		p.add("", "archive_retention", nil, "must not be negative")
	}

	if f.ArchiveRetention > 0 && f.OnExpire == ExpireDelete {
		p.add("", "archive_retention", nil, "has no effect with on_expire: %s, which leaves no archive", ExpireDelete)
	}
}

func (f *Flow) validateFlowLevel(p *problems) {
	if f.MaxSteps < 0 {
		p.add("", "max_steps", nil, "must not be negative")
	}

	if f.Retention < 0 {
		p.add("", "retention", nil, "must not be negative")
	}

	f.validateExpiry(p)

	f.validateChannels(p)
	f.validateOutput(p)
	f.validateBudgetAndAttrs(p)
	f.validateTriggers(p)
}

func (f *Flow) validateBudgetAndAttrs(p *problems) {
	for _, k := range slices.Sorted(maps.Keys(f.Budget)) {
		field := "budget." + k

		if err := names.CheckToken("budget counter", k); err != nil {
			p.add("", field, err, "")
		}

		if f.Budget[k] <= 0 {
			p.add("", field, nil, "must be positive")
		}
	}

	f.attrs = make(map[string]*expr.Program, len(f.SearchAttributes))

	for _, k := range slices.Sorted(maps.Keys(f.SearchAttributes)) {
		field := "search_attributes." + k

		if err := names.CheckToken("search attribute", k); err != nil {
			p.add("", field, err, "")
		}

		prog, err := expr.CompileValue(f.SearchAttributes[k])
		if err != nil {
			p.add("", field, err, "")

			continue
		}

		f.attrs[k] = prog
	}
}

func (f *Flow) validateTriggers(p *problems) {
	for i, tr := range f.Triggers {
		if strings.TrimSpace(tr.Subject) == "" || strings.ContainsAny(tr.Subject, " \t\r\n") {
			p.add("", fmt.Sprintf("triggers[%d].subject", i), nil, "%q is not a valid subject", tr.Subject)
		}

		if tr.Stream != "" {
			if err := names.CheckToken("trigger stream", tr.Stream); err != nil {
				p.add("", fmt.Sprintf("triggers[%d].stream", i), err, "")
			}
		}
	}
}

func (f *Flow) validateOutput(p *problems) {
	f.output = nil

	if strings.TrimSpace(f.Output) == "" {
		return
	}

	prog, err := expr.CompileValue(f.Output)
	if err != nil {
		p.add("", "output", err, "")

		return
	}

	f.output = prog
}

func (f *Flow) validateChannels(p *problems) {
	for _, name := range slices.Sorted(maps.Keys(f.Channels)) {
		c := f.Channels[name]
		field := "channels." + name

		if err := names.CheckToken("channel name", name); err != nil {
			p.add("", field, err, "")
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
			p.add("", field+".reducer", nil, "unknown reducer %q (want replace, append, merge or sum)", c.Reducer)

			continue
		}

		if !ok {
			p.add("", field+".default", nil, "does not fit reducer %q", c.ReducerOrDefault())
		}
	}
}

func (f *Flow) validateNodes(p *problems) {
	for i := range f.Nodes {
		n := &f.Nodes[i]

		f.validateNode(p, n)

		if n.Next != "" {
			f.ref(p, n, fNext, n.Next)

			if n.Next == n.ID {
				p.add(n.ID, fNext, nil, "points to itself (would loop forever with no exit)")
			}
		}

		if n.OnFailure != "" {
			f.ref(p, n, fOnFailure, n.OnFailure)

			if n.OnFailure == n.ID {
				p.add(n.ID, fOnFailure, nil, "points to itself; to try again, use retry")
			}
		}
	}
}

// ref checks that field of n names an existing node.
func (f *Flow) ref(p *problems, n *Node, field, id string) {
	if id == "" {
		p.add(n.ID, field, nil, "is required")

		return
	}

	if f.byID[id] == nil {
		p.add(n.ID, field, nil, "references unknown node %q", id)
	}
}

func (f *Flow) validateNode(p *problems, n *Node) {
	f.rejectForeignFields(p, n)
	f.encodeMeta(p, n)

	switch n.Type {
	case NodeTask:
		f.validateTask(p, n)
	case NodeChoice:
		f.validateChoice(p, n)
	case NodeFanout:
		f.validateFanout(p, n)
	case NodeJoin:
		f.validateJoin(p, n)
	case NodeAwait:
		f.validateAwait(p, n)
	case NodeMap:
		f.validateMap(p, n)
	case NodeSubflow:
		f.validateSubflow(p, n)
	default:
		p.add(n.ID, "type", nil, "unknown type %q", n.Type)
	}
}

// encodeMeta checks that the node's meta encodes as JSON and keeps the
// encoding for jobs.
func (f *Flow) encodeMeta(p *problems, n *Node) {
	n.meta = nil

	if len(n.Meta) == 0 {
		return
	}

	b, err := json.Marshal(n.Meta)
	if err != nil {
		p.add(n.ID, "meta", err, "must encode as JSON (use string keys in nested maps)")

		return
	}

	n.meta = b
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
	NodeTask:   {fKind, fTimeout, fRetry, fOutputSchema, fCache, fConcurrency, fDynamic, fNext, fOnFailure},
	NodeChoice: {fRules, fOnError},
	NodeFanout: {fBranches, fNext},
	NodeJoin:   {fWaitFor, fPolicy, fNext, fOnFailure},
	NodeAwait:  {fSignal, fTimeout, fOnTimeout, fNext},
	NodeMap: {
		fKind, fOver, fMaxParallel, fTimeout, fRetry, fOutputSchema, fNext, fOnFailure,
		fFlow, fInput, fOnParentClose,
	},
	NodeSubflow: {fFlow, fInput, fOnParentClose, fNext, fOnFailure},
}

// rejectForeignFields refuses a field that does not belong to the node's type:
// it would be silently ignored, which almost always hides a mistake.
func (f *Flow) rejectForeignFields(p *problems, n *Node) {
	allowed, known := allowedFields[n.Type]
	if !known {
		return
	}

	var foreign []string

	for field, set := range fieldSet(n) {
		if set && !slices.Contains(allowed, field) {
			foreign = append(foreign, field)
		}
	}

	if len(foreign) > 0 {
		sort.Strings(foreign)

		p.add(n.ID, "", nil, "field(s) %v do not apply to a %s node", foreign, n.Type)
	}
}

func (f *Flow) validateTask(p *problems, n *Node) {
	if err := names.CheckToken("worker kind", n.Kind); err != nil {
		p.add(n.ID, fKind, err, "")
	}

	f.validateAttemptPolicy(p, n)

	if n.Cache != nil && n.Cache.TTL <= 0 {
		p.add(n.ID, "cache.ttl", nil, "must be positive")
	}

	n.conc = nil

	if c := n.Concurrency; c != nil {
		if c.Max <= 0 {
			p.add(n.ID, "concurrency.max", nil, "must be positive")
		}

		prog, err := expr.CompileValue(c.Key)
		if err != nil {
			p.add(n.ID, "concurrency.key", err, "")
		} else {
			n.conc = prog
		}
	}

	for i, d := range n.Dynamic {
		f.ref(p, n, fmt.Sprintf("dynamic[%d]", i), d)
	}
}

// validateAttemptPolicy checks the per-attempt policies shared by task and map
// nodes: timeout, retry and output schema.
func (f *Flow) validateAttemptPolicy(p *problems, n *Node) {
	if n.Timeout < 0 {
		p.add(n.ID, fTimeout, nil, "must not be negative")
	}

	validateRetry(p, n)

	n.schema = nil

	if n.OutputSchema == nil {
		return
	}

	sch, err := compileSchema(n.OutputSchema)
	if err != nil {
		p.add(n.ID, fOutputSchema, err, "")

		return
	}

	n.schema = sch
}

func validateRetry(p *problems, n *Node) {
	r := n.Retry
	if r == nil {
		return
	}

	if r.MaxAttempts < 0 || r.MaxAttempts > MaxRetryAttempts {
		p.add(n.ID, "retry.max_attempts", nil, "must be between 0 and %d", MaxRetryAttempts)
	}

	switch r.Backoff {
	case "", BackoffFixed, BackoffLinear, BackoffExponential:
		// A backoff with no attempts contradicts itself: it would run once
		// and never retry.
		if r.Backoff != "" && r.MaxAttempts < 2 { //nolint:mnd // one retry needs two attempts.
			p.add(n.ID, "retry.max_attempts", nil, "retry declares backoff %q but max_attempts < 2, "+
				"so it would never retry", r.Backoff)
		}
	default:
		p.add(n.ID, "retry.backoff", nil, "unknown backoff %q (want fixed, linear or exponential)", r.Backoff)
	}

	if r.Delay < 0 {
		p.add(n.ID, "retry.delay", nil, "must not be negative")
	}

	if r.MaxDelay < 0 {
		p.add(n.ID, "retry.max_delay", nil, "must not be negative")
	}
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

func (f *Flow) validateChoice(p *problems, n *Node) {
	if len(n.Rules) == 0 {
		p.add(n.ID, fRules, nil, "at least one rule is required")
	}

	defaults := 0
	n.rules = make([]*expr.Program, len(n.Rules))

	for i, r := range n.Rules {
		field := fmt.Sprintf("rules[%d]", i)

		switch {
		case r.Default && strings.TrimSpace(r.When) != "":
			p.add(n.ID, field+".when", nil, "a default rule must not also carry a when expression")

			defaults++
		case r.Default:
			defaults++
		case strings.TrimSpace(r.When) == "":
			p.add(n.ID, field+".when", nil, "non-default rule needs a when expression")
		default:
			prog, err := expr.CompilePredicate(r.When)
			if err != nil {
				p.add(n.ID, field+".when", err, "")
			} else {
				n.rules[i] = prog
			}
		}

		if r.To != End {
			f.ref(p, n, field+".to", r.To)
		}
	}

	if len(n.Rules) > 0 && defaults != 1 {
		p.add(n.ID, fRules, nil, "exactly one default rule is required (got %d)", defaults)
	}

	if n.OnError != OnErrorDefault && n.OnError != OnErrorFail {
		p.add(n.ID, fOnError, nil, "unknown value %q (want \"fail\" or omit)", n.OnError)
	}
}

func (f *Flow) validateFanout(p *problems, n *Node) {
	switch {
	case len(n.Branches) == 0:
		p.add(n.ID, fBranches, nil, "at least one branch is required")
	case len(n.Branches) > MaxBranches:
		p.add(n.ID, fBranches, nil, "at most %d branches (use a map node for wider fan-outs)", MaxBranches)
	}

	for i, b := range n.Branches {
		f.ref(p, n, fmt.Sprintf("branches[%d]", i), b)
	}

	if n.Next == "" {
		p.add(n.ID, fNext, nil, "is required and must be a join node")
	}
}

func (f *Flow) validateJoin(p *problems, n *Node) {
	seen := map[string]bool{}

	for i, w := range n.WaitFor {
		field := fmt.Sprintf("wait_for[%d]", i)

		f.ref(p, n, field, w)

		// A duplicate would double-count that branch in the join tally.
		if seen[w] {
			p.add(n.ID, field, nil, "lists %q twice", w)
		}

		seen[w] = true
	}

	kind, quorum := n.JoinKind()
	switch kind {
	case JoinAll, JoinAny:
	case JoinQuorum:
		if quorum <= 0 {
			p.add(n.ID, fPolicy, nil, "quorum:N needs N > 0")
		}
	default:
		p.add(n.ID, fPolicy, nil, "unknown policy %q (want all, any or quorum:N)", n.Policy)
	}
}

func (f *Flow) validateAwait(p *problems, n *Node) {
	if err := names.CheckToken("signal name", n.Signal); err != nil {
		p.add(n.ID, fSignal, err, "")
	}

	// An await without a timeout can park an execution forever on a signal
	// nobody sends; the deadline is mandatory and on_timeout chooses between
	// failing and routing.
	if n.Timeout <= 0 {
		p.add(n.ID, fTimeout, nil, "is required and must be positive")
	}

	if n.OnTimeout != "" {
		f.ref(p, n, fOnTimeout, n.OnTimeout)
	}
}

func (f *Flow) validateMap(p *problems, n *Node) {
	f.validateMapItems(p, n)

	n.over = nil

	prog, err := expr.CompileValue(n.Over)
	if err != nil {
		p.add(n.ID, fOver, err, "")
	} else {
		n.over = prog
	}

	if n.MaxParallel < 0 || n.MaxParallel > MaxParallel {
		p.add(n.ID, fMaxParallel, nil, "must be between 0 and %d", MaxParallel)
	}

	if n.Flow == "" {
		f.validateAttemptPolicy(p, n)
	}
}

// validateMapItems checks what runs each item of map n: a task of a worker
// kind, or a child execution of a flow (with an optional input expression
// that sees item and index).
func (f *Flow) validateMapItems(p *problems, n *Node) {
	switch {
	case n.Kind != "" && n.Flow != "":
		p.add(n.ID, "", nil, "set kind (a task per item) or flow (a subflow per item), not both")

		return
	case n.Flow == "":
		if n.Input != "" || n.OnParentClose != "" {
			p.add(n.ID, "", nil, "input and on_parent_close need flow (a subflow per item)")
		}

		if err := names.CheckToken("worker kind", n.Kind); err != nil {
			p.add(n.ID, fKind, err, "")
		}

		return
	}

	if n.Timeout > 0 || n.Retry != nil || n.OutputSchema != nil {
		p.add(n.ID, "", nil, "timeout, retry and output_schema apply to tasks; "+
			"with flow, set them on the child flow's nodes")
	}

	f.validateSubflow(p, n)
}

// validateSubflow checks the child-execution fields of a subflow node, or of
// a map node with flow.
func (f *Flow) validateSubflow(p *problems, n *Node) {
	if err := names.CheckToken("subflow name", n.Flow); err != nil {
		p.add(n.ID, fFlow, err, "")
	}

	switch n.OnParentClose {
	case "", ParentCloseCancel, ParentCloseAbandon:
	default:
		p.add(n.ID, fOnParentClose, nil, "unknown value %q (want cancel or abandon)", n.OnParentClose)
	}

	n.input = nil

	if strings.TrimSpace(n.Input) != "" {
		prog, err := expr.CompileValue(n.Input)
		if err != nil {
			p.add(n.ID, fInput, err, "")
		} else {
			n.input = prog
		}
	}
}
