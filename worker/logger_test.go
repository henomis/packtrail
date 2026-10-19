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

package worker_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/henomis/packtrail/worker"
)

func TestJobLoggerWithoutWorker(t *testing.T) {
	// A Job built by hand (a handler unit test) still has a logger.
	j := &worker.Job{ExecID: "e", Node: "n"}

	l := j.Logger()
	if l == nil || j.Logger() != l {
		t.Fatal("Logger must be non-nil and stable")
	}
}

func TestJobLoggerTraceKeys(t *testing.T) {
	const tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

	for _, tc := range []struct {
		traceparent string
		want        bool
	}{
		{tp, true},
		{"", false},
		{"not-a-traceparent", false},
	} {
		var buf bytes.Buffer

		prev := slog.Default()

		slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))

		j := &worker.Job{ExecID: "e", Node: "n", Traceparent: tc.traceparent}
		j.Logger().Info("hello")

		slog.SetDefault(prev)

		var line map[string]any
		if err := json.Unmarshal(buf.Bytes(), &line); err != nil {
			t.Fatal(err)
		}

		_, hasTrace := line["trace_id"]
		_, hasSpan := line["span_id"]

		if hasTrace != tc.want || hasSpan != tc.want {
			t.Fatalf("%q: trace_id %v span_id %v, want %v", tc.traceparent, hasTrace, hasSpan, tc.want)
		}

		if tc.want && (line["trace_id"] != "4bf92f3577b34da6a3ce929d0e0e4736" || line["span_id"] != "00f067aa0ba902b7") {
			t.Fatalf("keys %v", line)
		}
	}
}
