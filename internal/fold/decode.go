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
	"errors"
	"fmt"
)

// ErrNoValue means a decoded value is absent: the node has no result, the
// channel or signal no value, the execution no output yet.
var ErrNoValue = errors.New("packtrail: value not present")

func decodeRaw(what string, raw json.RawMessage, v any) error {
	if len(raw) == 0 {
		return fmt.Errorf("%w: %s", ErrNoValue, what)
	}

	return json.Unmarshal(raw, v)
}

// DecodeInput decodes the execution input into v.
func (s *State) DecodeInput(v any) error { return decodeRaw("input", s.Input, v) }

// DecodeOutput decodes the output of the finished execution into v.
func (s *State) DecodeOutput(v any) error { return decodeRaw("output", s.Output, v) }

// Result decodes the latest output of node into v.
func (s *State) Result(node string, v any) error {
	return decodeRaw("result of "+node, s.Results[node], v)
}

// Channel decodes the current value of a channel into v.
func (s *State) Channel(name string, v any) error {
	return decodeRaw("channel "+name, s.Channels[name], v)
}

// Signal decodes the payload of the last consumed signal name into v.
func (s *State) Signal(name string, v any) error {
	return decodeRaw("signal "+name, s.Signals[name], v)
}
