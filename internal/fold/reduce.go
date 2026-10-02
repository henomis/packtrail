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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/henomis/packtrail/flow"
)

var errReducer = errors.New("reducer")

// Reduce folds delta into cur with the named reducer:
//
//	replace  the delta replaces the value
//	append   an array delta is concatenated, any other value is appended
//	merge    an object delta is shallow-merged into the object value
//	sum      a number delta is added to the number value
//
// The same function validates writes in Decide (so an invalid write fails the
// node instead of the fold) and applies them in Apply.
func Reduce(reducer string, cur, delta json.RawMessage) (json.RawMessage, error) {
	switch reducer {
	case flow.ReducerReplace, "":
		return delta, nil
	case flow.ReducerAppend:
		return reduceAppend(cur, delta)
	case flow.ReducerMerge:
		return reduceMerge(cur, delta)
	case flow.ReducerSum:
		return reduceSum(cur, delta)
	default:
		return nil, fmt.Errorf("%w: unknown reducer %q", errReducer, reducer)
	}
}

func isNull(b json.RawMessage) bool {
	t := bytes.TrimSpace(b)
	return len(t) == 0 || string(t) == "null"
}

func reduceAppend(cur, delta json.RawMessage) (json.RawMessage, error) {
	var list []json.RawMessage

	if !isNull(cur) {
		if err := json.Unmarshal(cur, &list); err != nil {
			return nil, fmt.Errorf("%w: append: current value is not an array", errReducer)
		}
	}

	var items []json.RawMessage
	if err := json.Unmarshal(delta, &items); err == nil {
		list = append(list, items...)
	} else {
		list = append(list, delta)
	}

	if list == nil {
		list = []json.RawMessage{}
	}

	return json.Marshal(list)
}

func reduceMerge(cur, delta json.RawMessage) (json.RawMessage, error) {
	obj := map[string]json.RawMessage{}

	if !isNull(cur) {
		if err := json.Unmarshal(cur, &obj); err != nil || obj == nil {
			return nil, fmt.Errorf("%w: merge: current value is not an object", errReducer)
		}
	}

	var d map[string]json.RawMessage
	if err := json.Unmarshal(delta, &d); err != nil || d == nil {
		return nil, fmt.Errorf("%w: merge: delta is not an object", errReducer)
	}

	for k, v := range d {
		obj[k] = v
	}

	return json.Marshal(obj)
}

func reduceSum(cur, delta json.RawMessage) (json.RawMessage, error) {
	var a float64

	if !isNull(cur) {
		if err := json.Unmarshal(cur, &a); err != nil {
			return nil, fmt.Errorf("%w: sum: current value is not a number", errReducer)
		}
	}

	var b float64
	if err := json.Unmarshal(delta, &b); err != nil {
		return nil, fmt.Errorf("%w: sum: delta is not a number", errReducer)
	}

	return json.Marshal(a + b)
}
