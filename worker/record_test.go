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
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/henomis/packtrail/internal/cmd"
	"github.com/henomis/packtrail/internal/wire"
)

func TestSetRecordReplacesAndRejectsUnencodable(t *testing.T) {
	j := &Job{}

	if err := j.SetRecord(map[string]int{"a": 1}); err != nil {
		t.Fatal(err)
	}

	if err := j.SetRecord(map[string]int{"b": 2}); err != nil {
		t.Fatal(err)
	}

	if err := j.SetRecord(make(chan int)); err == nil {
		t.Fatal("unencodable record accepted")
	}

	if got := string(j.recorded()); got != `{"b":2}` {
		t.Fatalf("record %s, want the last encodable one", got)
	}
}

func TestRecordTruncation(t *testing.T) {
	for _, s := range []string{
		strings.Repeat("x", 5000),
		strings.Repeat(`"\`, 3000), // escaping doubles the head
		strings.Repeat("é€😀", 800), // multi-byte runes are never cut
	} {
		for _, limit := range []int{minMaxRecord, 1000, 4096} {
			j := &Job{record: recordSink{max: limit}}
			if err := j.SetRecord(map[string]string{"prompt": s}); err != nil {
				t.Fatal(err)
			}

			raw := j.recorded()
			if len(raw) > limit {
				t.Fatalf("limit %d: record is %d bytes", limit, len(raw))
			}

			var tr RecordTruncated
			if err := json.Unmarshal(raw, &tr); err != nil || !tr.Truncated || tr.Size <= limit {
				t.Fatalf("limit %d: %s (%v)", limit, raw, err)
			}

			full, _ := json.Marshal(map[string]string{"prompt": s})
			if !strings.HasPrefix(string(full), tr.Head) || len(tr.Head) == 0 || tr.Size != len(full) {
				t.Fatalf("limit %d: head %q is not a prefix of the record (size %d/%d)", limit, tr.Head, tr.Size, len(full))
			}
		}
	}

	// Under the cap: kept as is.
	j := &Job{record: recordSink{max: 1000}}
	_ = j.SetRecord("short")

	if string(j.recorded()) != `"short"` {
		t.Fatalf("record %s", j.recorded())
	}
}

func TestWithMaxRecordBytes(t *testing.T) {
	w := &Worker{}

	if err := WithMaxRecordBytes(-1)(w); err == nil {
		t.Fatal("negative cap accepted")
	}

	if _ = WithMaxRecordBytes(10)(w); w.maxRecord != minMaxRecord {
		t.Fatalf("cap %d, want it raised to %d", w.maxRecord, minMaxRecord)
	}
}

// The record rides on every command an attempt can end with, also when the
// result itself cannot be encoded.
func TestRecordOnEveryOutcome(t *testing.T) {
	wj := wire.Job{ExecID: "e1", Key: "a", Generation: 1, Attempt: 1}
	ref := cmd.TaskRef{Key: "a", Generation: 1, Attempt: 1}
	rec := json.RawMessage(`{"sent":"hi"}`)

	cases := map[string]struct {
		res *Result
		err error
	}{
		"complete":             {res: &Result{Output: map[string]any{"x": 1}}},
		"fail":                 {err: errors.New("boom")},
		"permanent with usage": {err: WithUsage(Permanent(errors.New("boom")), map[string]float64{"t": 1})},
		"interrupt with usage": {err: WithUsage(Interrupt("ask"), map[string]float64{"t": 1})},
		"unencodable output":   {res: &Result{Output: json.RawMessage(`{bad`)}},
	}

	for name, tc := range cases {
		c, err := resultCmd("id", wj, ref, tc.res, tc.err, rec)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}

		var d struct {
			Record json.RawMessage `json:"record"`
		}

		if err = json.Unmarshal(c.Data, &d); err != nil || string(d.Record) != string(rec) {
			t.Fatalf("%s: %s command record %s (%v)", name, c.Type, d.Record, err)
		}
	}
}
