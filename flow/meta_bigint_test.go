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

import "testing"

// TestMetaBigIntRoundTrip: the registry stores def.JSON() and jobs take
// meta from ParseJSON of it; an integer above 2^53 must survive.
func TestMetaBigIntRoundTrip(t *testing.T) {
	f, err := Parse([]byte("name: m\nnodes: [{id: a, type: task, kind: k, meta: {channel: 1234567890123456789}}]"))
	if err != nil {
		t.Fatal(err)
	}

	b, _ := f.JSON()

	g, err := ParseJSON(b)
	if err != nil {
		t.Fatal(err)
	}

	h1, _ := f.Hash()
	h2, _ := g.Hash()

	if string(g.Node("a").MetaJSON()) != string(f.Node("a").MetaJSON()) || h1 != h2 {
		t.Fatalf("meta %s -> %s, hash %s -> %s", f.Node("a").MetaJSON(), g.Node("a").MetaJSON(), h1, h2)
	}
}
