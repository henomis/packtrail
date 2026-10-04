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

package worker

import (
	"math"
	"testing"

	"github.com/henomis/packtrail/internal/cmd"
	"github.com/henomis/packtrail/internal/wire"
)

// an attempt whose output or interrupt payload cannot be encoded
// ends as a fail; the usage it reported must still be counted.
func TestUsageKeptWhenAnswerCannotBeEncoded(t *testing.T) {
	u := map[string]float64{"tokens": 7}
	wj := wire.Job{ExecID: "e1", Key: "n", Generation: 1, Attempt: 1}
	ref := cmd.TaskRef{Key: "n", Generation: 1, Attempt: 1}

	for name, tc := range map[string]struct {
		res *Result
		err error
	}{
		"NaN output":           {&Result{Output: map[string]any{"score": math.NaN()}, Usage: u}, nil},
		"unencodable question": {nil, WithUsage(Interrupt(map[string]any{"ch": make(chan int)}), u)},
	} {
		c, err := resultCmd("id", wj, ref, tc.res, tc.err)
		if err != nil {
			t.Fatal(err)
		}

		var p cmd.FailData
		if c.Type != cmd.Fail || c.Payload(&p) != nil {
			t.Fatalf("%s: %s", name, c.Type)
		}

		if p.Usage["tokens"] != 7 {
			t.Errorf("%s: fail carries usage %v, want %v", name, p.Usage, u)
		}
	}
}
