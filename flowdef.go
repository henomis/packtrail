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
	"fmt"
	"time"

	"github.com/henomis/packtrail/internal/dsl"
)

// FlowDef is a programmatic flow definition. It mirrors the YAML schema and
// can be passed to WithFlowDef instead of writing YAML.
type FlowDef struct {
	Version string // "1.0"; empty is accepted for legacy parity with YAML
	Name    string
	Nodes   []NodeDef
	Edges   []EdgeDef
}

// NodeDef is a single node in a FlowDef.
type NodeDef struct {
	ID   string
	Type string // "task" | "fanout" | "fanin" | "choice" | "signal"

	// task
	Invoker string
	Target  string
	Subject string
	Timeout time.Duration
	Retry   *RetryPolicy

	// fanout
	Branches []string

	// fanin
	WaitFor    []string
	JoinPolicy string // "all" | "any" | "quorum:N"

	// choice
	Rules   []RuleDef
	OnError string // "" routes eval errors to the default rule; "fail" fails the execution

	// signal
	SignalName string
	OnTimeout  string
}

// EdgeDef connects two nodes in a FlowDef.
type EdgeDef struct {
	From string
	To   string
}

// RuleDef is one branch of a choice node.
type RuleDef struct {
	When    string
	Default bool
	To      string
}

// RetryPolicy controls task retries for a NodeDef.
type RetryPolicy struct {
	MaxAttempts int
	Backoff     string // "exponential" | "linear" | "fixed"
}

// ValidateFlowDef validates one or more FlowDefs against the full flow-graph
// rules — node/edge structure, a unique start node, choice defaults, fan-in join
// policy, retry bounds, etc. — without a NATS connection, so a builder can verify
// programmatic flows offline (e.g. in a `validate` command) and catch the same
// errors New would raise at startup. It returns the first validation error (which
// already names the offending flow).
func ValidateFlowDef(defs ...FlowDef) error {
	flows := make(map[string]*dsl.Flow, len(defs))

	for _, d := range defs {
		f, err := flowDefToDSL(d)
		if err != nil {
			return err
		}

		if _, dup := flows[f.Name]; dup {
			return fmt.Errorf("duplicate flow %q", f.Name)
		}

		flows[f.Name] = f
	}

	return compileChoiceRules(flows)
}

// flowDefToDSL converts a FlowDef into a validated *dsl.Flow.
func flowDefToDSL(f FlowDef) (*dsl.Flow, error) {
	nodes := make([]dsl.Node, len(f.Nodes))
	for i, n := range f.Nodes {
		rules := make([]dsl.Rule, len(n.Rules))
		for j, r := range n.Rules {
			rules[j] = dsl.Rule{When: r.When, Default: r.Default, To: r.To}
		}

		nodes[i] = dsl.Node{
			ID:         n.ID,
			Type:       n.Type,
			Invoker:    n.Invoker,
			Target:     n.Target,
			Subject:    n.Subject,
			Timeout:    dsl.Duration(n.Timeout),
			Branches:   append([]string(nil), n.Branches...),
			WaitFor:    append([]string(nil), n.WaitFor...),
			JoinPolicy: n.JoinPolicy,
			Rules:      rules,
			OnError:    n.OnError,
			SignalName: n.SignalName,
			OnTimeout:  n.OnTimeout,
		}

		if n.Retry != nil {
			nodes[i].Retry = &dsl.RetryPolicy{
				MaxAttempts: n.Retry.MaxAttempts,
				Backoff:     n.Retry.Backoff,
			}
		}
	}

	edges := make([]dsl.Edge, len(f.Edges))
	for i, e := range f.Edges {
		edges[i] = dsl.Edge{From: e.From, To: e.To}
	}

	df := &dsl.Flow{
		Version: f.Version,
		Name:    f.Name,
		Nodes:   nodes,
		Edges:   edges,
	}

	if err := df.Validate(); err != nil {
		return nil, err
	}

	return df, nil
}
