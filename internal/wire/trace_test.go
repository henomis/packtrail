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

package wire

import "testing"

func TestTraceIDs(t *testing.T) {
	const (
		tid = "4bf92f3577b34da6a3ce929d0e0e4736"
		sid = "00f067aa0ba902b7"
	)

	for _, tc := range []struct {
		in string
		ok bool
	}{
		{"00-" + tid + "-" + sid + "-01", true},
		{"01-" + tid + "-" + sid + "-00-future", true},
		{"", false},
		{"garbage", false},
		{"00-" + tid + "-" + sid + "-01-extra", false},
		{"ff-" + tid + "-" + sid + "-01", false},
		{"00-00000000000000000000000000000000-" + sid + "-01", false},
		{"00-" + tid + "-0000000000000000-01", false},
		{"00-" + tid[:31] + "-" + sid + "-01", false},
		{"00-" + "4BF92F3577B34DA6A3CE929D0E0E4736" + "-" + sid + "-01", false},
	} {
		gotT, gotS, ok := TraceIDs(tc.in)
		if ok != tc.ok {
			t.Errorf("%q: ok %v, want %v", tc.in, ok, tc.ok)

			continue
		}

		if ok && (gotT != tid || gotS != sid) {
			t.Errorf("%q: got %s %s", tc.in, gotT, gotS)
		}
	}
}
