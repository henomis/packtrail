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

// Package flow defines packtrail flow definitions: a declarative graph of
// nodes (task, choice, fanout, join, await, map, subflow) plus the typed state
// channels the nodes write to. A Flow is pure data: it is parsed from YAML (or
// built in Go), validated, hashed and stored immutably; every execution is bound
// to the hash of the definition it started with.
//
// Validation reports every problem at once; each is a *ValidationError naming
// the node and YAML field at fault (see ValidationErrors).
//
// packtrail is agnostic: a task is a unit of work executed by a worker of the
// given kind, whatever that worker does.
package flow

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/henomis/packtrail/internal/expr"
)

// SupportedVersion is the flow-definition schema version this build accepts.
// An omitted version means SupportedVersion.
const SupportedVersion = "1"

// Node types.
const (
	NodeTask    = "task"
	NodeChoice  = "choice"
	NodeFanout  = "fanout"
	NodeJoin    = "join"
	NodeAwait   = "await"
	NodeMap     = "map"
	NodeSubflow = "subflow"
)

// Join policies.
const (
	JoinAll    = "all"
	JoinAny    = "any"
	JoinQuorum = "quorum"
)

// Retry backoff kinds.
const (
	BackoffFixed       = "fixed"
	BackoffLinear      = "linear"
	BackoffExponential = "exponential"
)

// Channel reducers.
const (
	ReducerReplace = "replace"
	ReducerAppend  = "append"
	ReducerMerge   = "merge"
	ReducerSum     = "sum"
)

// Choice on_error modes.
const (
	OnErrorDefault = ""
	OnErrorFail    = "fail"
)

// Subflow on_parent_close policies.
const (
	ParentCloseCancel  = "cancel"
	ParentCloseAbandon = "abandon"
)

// Defaults and bounds.
const (
	// MaxRetryAttempts bounds retry.max_attempts, keeping the exponential
	// backoff shift far from overflow.
	MaxRetryAttempts = 64
	// DefaultMaxSteps is the default recursion limit: the total number of node
	// entries an execution may perform before it fails with "max_steps".
	DefaultMaxSteps = 1000
	// DefaultRetryDelay is the base delay of a retry policy without delay.
	DefaultRetryDelay = time.Second
	// DefaultMapParallel is the max_parallel of a map node that sets none: it
	// also keeps a single decision well under the decision events limit.
	DefaultMapParallel = 100
	// MaxParallel bounds max_parallel and MaxBranches bounds a fan-out: with
	// the events each scheduled task needs (entered, scheduled, timeout timer),
	// one decision stays under the 1 000-event decision limit (F-04).
	MaxParallel = 256
	MaxBranches = 256
	// DefaultRetryMaxDelay caps a retry backoff without max_delay.
	DefaultRetryMaxDelay = 5 * time.Minute
)

// Duration is a time.Duration that (un)marshals as a string like "30s".
//
//nolint:recvcheck // encoders on the value, decoders on the pointer, as encoding/json expects.
type Duration time.Duration

// D returns the value as a time.Duration.
func (d Duration) D() time.Duration { return time.Duration(d) }

// MarshalJSON encodes the duration as a string.
func (d Duration) MarshalJSON() ([]byte, error) {
	if d == 0 {
		return []byte(`""`), nil
	}

	return json.Marshal(time.Duration(d).String())
}

// UnmarshalJSON decodes a duration string ("30s") or a number of nanoseconds.
func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		var n int64
		if err2 := json.Unmarshal(b, &n); err2 != nil {
			return fmt.Errorf("invalid duration %s", b)
		}

		*d = Duration(n)

		return nil
	}

	return d.parse(s)
}

// UnmarshalText decodes a duration string; it is what yaml.v3 uses.
func (d *Duration) UnmarshalText(b []byte) error { return d.parse(string(b)) }

// MarshalText encodes the duration as a string.
func (d Duration) MarshalText() ([]byte, error) {
	if d == 0 {
		return []byte{}, nil
	}

	return []byte(time.Duration(d).String()), nil
}

func (d *Duration) parse(s string) error {
	s = strings.TrimSpace(s)
	if s == "" {
		*d = 0
		return nil
	}

	v, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}

	*d = Duration(v)

	return nil
}

// Flow is a flow definition.
type Flow struct {
	Version     string `yaml:"version,omitempty" json:"version,omitempty"`
	Name        string `yaml:"name" json:"name"`
	Description string `yaml:"description,omitempty" json:"description,omitempty"`
	// Start names the entry node. When empty, the entry node is the unique node
	// with no inbound transition.
	Start string `yaml:"start,omitempty" json:"start,omitempty"`
	// Channels declares the typed state of the flow. Nodes write deltas to
	// channels and each channel folds them with its reducer.
	Channels map[string]Channel `yaml:"channels,omitempty" json:"channels,omitempty"`
	// Output is an expression, evaluated on the context when the execution
	// completes, that yields its output: an object or null. It picks what the
	// execution returns, e.g. "{answer: channels.answer}" to keep working
	// memory out, or "results[last_node]" for the last node's result. When
	// empty the output is every channel, or the last node's result if the flow
	// declares no channels.
	Output string `yaml:"output,omitempty" json:"output,omitempty"`
	Nodes  []Node `yaml:"nodes" json:"nodes"`
	// Budget caps generic usage counters reported by workers (and the
	// built-in "steps" counter): exceeding one fails the execution with reason
	// budget_exceeded.
	Budget map[string]float64 `yaml:"budget,omitempty" json:"budget,omitempty"`
	// MaxSteps is the recursion limit (total node entries); 0 = DefaultMaxSteps.
	MaxSteps int `yaml:"max_steps,omitempty" json:"max_steps,omitempty"`
	// SearchAttributes are expressions evaluated on the start input and
	// indexed for List/Query.
	SearchAttributes map[string]string `yaml:"search_attributes,omitempty" json:"search_attributes,omitempty"`
	// Retention archives a terminal execution after this long (0 = never).
	Retention Duration `yaml:"retention,omitempty" json:"retention,omitempty"`
	// Triggers start this flow when a message arrives on a subject.
	Triggers []Trigger `yaml:"triggers,omitempty" json:"triggers,omitempty"`

	byID     map[string]*Node
	startID  string
	branchOf map[string]string // branch id -> owning fanout
	joinOf   map[string]string // fanout id -> join id
	attrs    map[string]*expr.Program
	output   *expr.Program
}

// Channel declares one state channel.
type Channel struct {
	// Reducer folds writes: replace (default), append, merge or sum.
	Reducer string `yaml:"reducer,omitempty" json:"reducer,omitempty"`
	// Default is the initial value.
	Default any `yaml:"default,omitempty" json:"default,omitempty"`
}

// ReducerOrDefault returns the reducer, defaulting to replace.
func (c Channel) ReducerOrDefault() string {
	if c.Reducer == "" {
		return ReducerReplace
	}

	return c.Reducer
}

// Trigger starts the flow when a message arrives on Subject. When Stream is
// set the trigger is a durable JetStream consumer of that stream (at-least
// once; a message that cannot be started is eventually dead-lettered);
// otherwise a core NATS queue subscription (at-most-once). The execution id
// is "<flow>-<Nats-Msg-Id>", so a duplicate message starts nothing twice and
// every flow triggered by the same message runs; without a Msg-Id it is
// "<flow>-<stream>-<sequence>" (stream) or a fresh id (core).
type Trigger struct {
	Subject string `yaml:"subject" json:"subject"`
	Stream  string `yaml:"stream,omitempty" json:"stream,omitempty"`
}

// Node is a single node of the graph. Fields are type-specific; Validate
// enforces which are required for each type.
type Node struct {
	ID   string `yaml:"id" json:"id"`
	Type string `yaml:"type" json:"type"`
	// Next is the static successor. Empty on a terminal node.
	Next string `yaml:"next,omitempty" json:"next,omitempty"`
	// OnFailure routes a permanent failure of the node (task, map, subflow,
	// join) to another node instead of failing the execution: retries
	// exhausted, a permanent error, an invalid output, a failed child, a join
	// whose policy was not met. The handler sees errors.<node>.
	OnFailure string `yaml:"on_failure,omitempty" json:"on_failure,omitempty"`
	// Meta is free-form configuration for whoever runs or shows the node: the
	// engine ignores it, but it is part of the definition, so it is versioned
	// with the flow hash and handed to the worker of each job (Job.Meta). It
	// must encode as JSON.
	Meta map[string]any `yaml:"meta,omitempty" json:"meta,omitempty"`

	// task, map: the worker kind and per-attempt policies.
	Kind         string       `yaml:"kind,omitempty" json:"kind,omitempty"`
	Timeout      Duration     `yaml:"timeout,omitempty" json:"timeout,omitempty"`
	Retry        *Retry       `yaml:"retry,omitempty" json:"retry,omitempty"`
	OutputSchema any          `yaml:"output_schema,omitempty" json:"output_schema,omitempty"`
	Cache        *Cache       `yaml:"cache,omitempty" json:"cache,omitempty"`
	Concurrency  *Concurrency `yaml:"concurrency,omitempty" json:"concurrency,omitempty"`
	// Dynamic lists the nodes a task worker may route to by returning `next`
	// (dynamic edges, LangGraph Command(goto)).
	Dynamic []string `yaml:"dynamic,omitempty" json:"dynamic,omitempty"`

	// choice
	Rules   []Rule `yaml:"rules,omitempty" json:"rules,omitempty"`
	OnError string `yaml:"on_error,omitempty" json:"on_error,omitempty"`

	// fanout
	Branches []string `yaml:"branches,omitempty" json:"branches,omitempty"`

	// join
	WaitFor []string `yaml:"wait_for,omitempty" json:"wait_for,omitempty"`
	Policy  string   `yaml:"policy,omitempty" json:"policy,omitempty"`

	// await (Timeout is required)
	Signal    string `yaml:"signal,omitempty" json:"signal,omitempty"`
	OnTimeout string `yaml:"on_timeout,omitempty" json:"on_timeout,omitempty"`

	// map
	Over        string `yaml:"over,omitempty" json:"over,omitempty"`
	MaxParallel int    `yaml:"max_parallel,omitempty" json:"max_parallel,omitempty"`

	// subflow
	Flow          string `yaml:"flow,omitempty" json:"flow,omitempty"`
	Input         string `yaml:"input,omitempty" json:"input,omitempty"`
	OnParentClose string `yaml:"on_parent_close,omitempty" json:"on_parent_close,omitempty"`

	rules  []*expr.Program
	over   *expr.Program
	input  *expr.Program
	conc   *expr.Program
	schema *jsonschema.Schema
	meta   json.RawMessage
}

// Retry is a task retry policy.
type Retry struct {
	// MaxAttempts is the total number of attempts (1 = no retry).
	MaxAttempts int      `yaml:"max_attempts" json:"max_attempts"`
	Backoff     string   `yaml:"backoff,omitempty" json:"backoff,omitempty"`
	Delay       Duration `yaml:"delay,omitempty" json:"delay,omitempty"`
	MaxDelay    Duration `yaml:"max_delay,omitempty" json:"max_delay,omitempty"`
}

// Cache enables the result cache of a task node. A result is stored once the
// node completes, so concurrent runs with the same key all miss and all run
// the job (no single-flight).
type Cache struct {
	TTL Duration `yaml:"ttl" json:"ttl"`
}

// Concurrency bounds how many jobs of a node run at once per key, across all
// executions.
type Concurrency struct {
	Key string `yaml:"key" json:"key"`
	Max int    `yaml:"max" json:"max"`
}

// Rule is one branch of a choice node. Exactly one of When / Default is set.
type Rule struct {
	When    string `yaml:"when,omitempty" json:"when,omitempty"`
	Default bool   `yaml:"default,omitempty" json:"default,omitempty"`
	To      string `yaml:"to" json:"to"`
}

// Node returns the node with the given id, or nil. Valid after Validate.
func (f *Flow) Node(id string) *Node { return f.byID[id] }

// StartNode returns the entry node id. Valid after Validate.
func (f *Flow) StartNode() string { return f.startID }

// FanoutOf returns the fanout owning branch id, or "".
func (f *Flow) FanoutOf(branch string) string { return f.branchOf[branch] }

// JoinOf returns the join node of fanout id.
func (f *Flow) JoinOf(fanout string) string { return f.joinOf[fanout] }

// MaxStepsOrDefault returns the recursion limit.
func (f *Flow) MaxStepsOrDefault() int {
	if f.MaxSteps <= 0 {
		return DefaultMaxSteps
	}

	return f.MaxSteps
}

// AttrProgram returns the compiled search attribute expression.
func (f *Flow) AttrProgram(name string) *expr.Program { return f.attrs[name] }

// JSON returns the canonical JSON encoding of the definition (map keys are
// sorted by encoding/json, so equal definitions encode equally).
func (f *Flow) JSON() ([]byte, error) { return json.Marshal(f) }

// Hash returns the content hash identifying this version of the definition.
func (f *Flow) Hash() (string, error) {
	b, err := f.JSON()
	if err != nil {
		return "", err
	}

	sum := sha256.Sum256(b)

	return hex.EncodeToString(sum[:8]), nil
}

// Clone returns a deep copy of the definition, validated.
func (f *Flow) Clone() (*Flow, error) {
	b, err := f.JSON()
	if err != nil {
		return nil, err
	}

	return ParseJSON(b)
}

// JoinKind returns the join policy and, for quorum, the required count.
func (n *Node) JoinKind() (kind string, quorum int) {
	p := strings.TrimSpace(n.Policy)

	switch {
	case p == "" || p == JoinAll:
		return JoinAll, 0
	case p == JoinAny:
		return JoinAny, 0
	case strings.HasPrefix(p, JoinQuorum+":"):
		q, _ := strconv.Atoi(strings.TrimPrefix(p, JoinQuorum+":"))
		return JoinQuorum, q
	default:
		return p, 0
	}
}

// MaxAttempts returns the attempt budget of a task (at least 1).
func (n *Node) MaxAttempts() int {
	if n.Retry == nil || n.Retry.MaxAttempts < 1 {
		return 1
	}

	return n.Retry.MaxAttempts
}

// maxBackoffShift saturates the exponential shift before int64 overflow.
const maxBackoffShift = 62

// Backoff returns the delay before attempt (2, 3, …). It never returns less
// than the base delay nor more than the cap: an overflowed shift must not
// wrap into a too-short delay.
func (n *Node) Backoff(attempt int) time.Duration {
	if n.Retry == nil {
		return 0
	}

	base := n.Retry.Delay.D()
	if base <= 0 {
		base = DefaultRetryDelay
	}

	maxDelay := n.Retry.MaxDelay.D()
	if maxDelay <= 0 {
		maxDelay = DefaultRetryMaxDelay
	}

	if maxDelay < base {
		maxDelay = base
	}

	retry := max(attempt-1, 1)

	var d time.Duration

	switch n.Retry.Backoff {
	case BackoffLinear:
		d = base * time.Duration(retry)
		if d/time.Duration(retry) != base {
			d = maxDelay
		}
	case BackoffExponential:
		if retry-1 >= maxBackoffShift {
			return maxDelay
		}

		d = base << (retry - 1)
	default:
		d = base
	}

	if d <= 0 || d > maxDelay {
		d = maxDelay
	}

	return d
}

// RulePrograms returns the compiled choice predicates (nil for the default).
func (n *Node) RulePrograms() []*expr.Program { return n.rules }

// OverProgram returns the compiled map source expression.
func (n *Node) OverProgram() *expr.Program { return n.over }

// OutputProgram returns the compiled output expression (nil = the default
// output).
func (f *Flow) OutputProgram() *expr.Program { return f.output }

// InputProgram returns the compiled subflow input expression (nil = pass the
// parent input through).
func (n *Node) InputProgram() *expr.Program { return n.input }

// ConcurrencyProgram returns the compiled concurrency key expression.
func (n *Node) ConcurrencyProgram() *expr.Program { return n.conc }

// ParentClose returns the subflow on_parent_close policy (default cancel).
func (n *Node) ParentClose() string {
	if n.OnParentClose == "" {
		return ParentCloseCancel
	}

	return n.OnParentClose
}

// ValidateOutput checks an output against the node's output_schema. A node
// without a schema accepts any output.
func (n *Node) ValidateOutput(raw []byte) error {
	if n.schema == nil {
		return nil
	}

	v, err := jsonschema.UnmarshalJSON(strings.NewReader(string(raw)))
	if err != nil {
		return fmt.Errorf("output is not valid JSON: %w", err)
	}

	if err = n.schema.Validate(v); err != nil {
		return fmt.Errorf("output does not match output_schema: %w", err)
	}

	return nil
}

// MetaJSON returns the JSON encoding of Meta (nil when there is none).
func (n *Node) MetaJSON() json.RawMessage { return n.meta }

// AllowsDynamic reports whether target is a declared dynamic successor.
func (n *Node) AllowsDynamic(target string) bool {
	for _, d := range n.Dynamic {
		if d == target {
			return true
		}
	}

	return false
}
