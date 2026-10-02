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

package cmd

import (
	"encoding/json"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	c, err := New("id-1", Complete, "e1", CompleteData{
		TaskRef: TaskRef{Key: "n", Generation: 2, Attempt: 1},
		Output:  json.RawMessage(`{"x":1}`),
		Writes:  map[string]json.RawMessage{"c": json.RawMessage(`[1]`)},
	})
	if err != nil {
		t.Fatal(err)
	}

	b, _ := json.Marshal(c) //nolint:errchkjson // test value

	got, err := Decode(b)
	if err != nil {
		t.Fatal(err)
	}

	var d CompleteData
	if err = got.Payload(&d); err != nil {
		t.Fatal(err)
	}

	if d.Key != "n" || d.Generation != 2 || string(d.Writes["c"]) != "[1]" {
		t.Fatalf("payload = %+v", d)
	}
}

func TestValidate(t *testing.T) {
	bad := []Command{
		{Type: Start, ExecID: "e"},
		{ID: "x", Type: Start, ExecID: "a.b"},
		{ID: "x", Type: Start, ExecID: "*"},
		{ID: "x", Type: "nope", ExecID: "e"},
	}
	for _, c := range bad {
		if c.Validate() == nil {
			t.Errorf("Validate(%+v) = nil", c)
		}
	}

	if _, err := Decode([]byte("{")); err == nil {
		t.Fatal("bad json accepted")
	}
}
