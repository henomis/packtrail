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

package logflags

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

func parse(t *testing.T, args ...string) *Flags {
	t.Helper()

	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	f := Register(fs, "LFTEST")

	if err := fs.Parse(args); err != nil {
		t.Fatal(err)
	}

	return f
}

func TestDefaultsAreInfoText(t *testing.T) {
	var buf bytes.Buffer

	l, err := parse(t).NewLogger(&buf)
	if err != nil {
		t.Fatal(err)
	}

	l.Debug("hidden")
	l.Info("shown", "k", "v")

	if out := buf.String(); strings.Contains(out, "hidden") || !strings.Contains(out, "msg=shown k=v") {
		t.Fatalf("output %q", out)
	}
}

func TestEnvThenFlag(t *testing.T) {
	t.Setenv("LFTEST_LOG_LEVEL", "debug")
	t.Setenv("LFTEST_LOG_FORMAT", "json")

	if f := parse(t); f.Level != "debug" || f.Format != "json" {
		t.Fatalf("env not used: %+v", f)
	}

	f := parse(t, "-log-level", "warn")

	var buf bytes.Buffer

	l, err := f.NewLogger(&buf)
	if err != nil {
		t.Fatal(err)
	}

	l.Info("hidden")
	l.Warn("shown", "exec", "x")

	var rec map[string]any
	if err = json.Unmarshal(buf.Bytes(), &rec); err != nil || rec["msg"] != "shown" || rec["exec"] != "x" {
		t.Fatalf("json output %q (%v)", buf.String(), err)
	}
}

func TestInvalid(t *testing.T) {
	for _, args := range [][]string{{"-log-level", "loud"}, {"-log-format", "xml"}} {
		if _, err := parse(t, args...).NewLogger(&bytes.Buffer{}); !errors.Is(err, ErrInvalid) {
			t.Fatalf("%v: err %v", args, err)
		}
	}
}

func TestTraceIDs(t *testing.T) {
	var buf bytes.Buffer

	l, err := parse(t, "-log-format", "json").NewLogger(&buf)
	if err != nil {
		t.Fatal(err)
	}

	tid, _ := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	sid, _ := trace.SpanIDFromHex("00f067aa0ba902b7")
	ctx := trace.ContextWithSpanContext(context.Background(),
		trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid}))

	l.InfoContext(ctx, "plain")
	l.With("trace_id", "bound").InfoContext(ctx, "bound")
	l.WithGroup("g").InfoContext(ctx, "grouped", "k", "v")

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 3 || !strings.Contains(lines[0], `"trace_id":"4bf92f3577b34da6a3ce929d0e0e4736"`) ||
		!strings.Contains(lines[0], `"span_id":"00f067aa0ba902b7"`) ||
		strings.Count(lines[1], "trace_id") != 1 || strings.Contains(lines[2], "trace_id") {
		t.Fatalf("lines:\n%s", buf.String())
	}
}
