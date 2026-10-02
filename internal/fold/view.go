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

package fold

import (
	"encoding/json"

	"github.com/henomis/packtrail/internal/expr"
)

// Context is the view of an execution handed to a worker and to expressions.
type Context struct {
	Input    json.RawMessage            `json:"input"`
	Channels map[string]json.RawMessage `json:"channels"`
	Results  map[string]json.RawMessage `json:"results"`
	LastNode string                     `json:"last_node"`
	Visits   map[string]int             `json:"visits"`
	Signals  map[string]json.RawMessage `json:"signals"`
	Branches map[string]string          `json:"branches"`
	Counters map[string]float64         `json:"counters"`
	Errors   map[string]NodeError       `json:"errors"`
	Item     json.RawMessage            `json:"item,omitempty"`
	Index    *int                       `json:"index,omitempty"`
	Resume   json.RawMessage            `json:"resume,omitempty"`
}

// ContextView returns the context view of the state. For a task instance,
// item/index/resume are filled from t (t may be nil).
func (s *State) ContextView(t *Task) Context {
	c := Context{
		Input: s.Input, Channels: s.Channels, Results: s.Results, LastNode: s.LastNode, Visits: s.Visits,
		Signals: s.Signals, Branches: s.Branches, Counters: s.Counters, Errors: s.Errors,
	}

	if len(c.Input) == 0 {
		c.Input = json.RawMessage(`{}`)
	}

	if t != nil {
		c.Resume = t.Resume

		if m := s.Maps[t.Owner]; m != nil && t.Node == t.Owner {
			idx := t.Index
			c.Item, c.Index = t.Item, &idx
		}
	}

	return c
}

// Env returns the expression environment of the state (decoded JSON values).
func (s *State) Env(t *Task) map[string]any {
	b, err := json.Marshal(s.ContextView(t))
	if err != nil {
		return map[string]any{}
	}

	var env map[string]any
	if err = json.Unmarshal(b, &env); err != nil || env == nil {
		return map[string]any{}
	}

	if _, ok := env[expr.VarItem]; !ok {
		env[expr.VarItem] = nil
	}

	return env
}
