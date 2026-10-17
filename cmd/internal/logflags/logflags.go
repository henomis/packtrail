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

// Package logflags gives the command-line tools the same logging flags:
// -log-level (debug, info, warn, error) and -log-format (text, json), each
// defaulting to an environment variable, and builds the slog.Logger they ask
// for. Logs go to stderr; stdout stays the command's output.
package logflags

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"go.opentelemetry.io/otel/trace"
)

const (
	formatText = "text"
	formatJSON = "json"
)

// ErrInvalid reports a log level or format that is not one of the known ones.
var ErrInvalid = errors.New("logflags: invalid value")

// Flags holds the parsed logging flags.
type Flags struct {
	Level  string
	Format string
}

// Register adds -log-level and -log-format to fs. Their defaults come from
// <envPrefix>_LOG_LEVEL and <envPrefix>_LOG_FORMAT, else info and text.
func Register(fs *flag.FlagSet, envPrefix string) *Flags {
	f := &Flags{}

	fs.StringVar(&f.Level, "log-level", envOr(envPrefix+"_LOG_LEVEL", "info"), "log level: debug, info, warn, error")
	fs.StringVar(&f.Format, "log-format", envOr(envPrefix+"_LOG_FORMAT", formatText), "log format: text, json")

	return f
}

// NewLogger builds the logger the flags describe, writing to w.
func (f *Flags) NewLogger(w io.Writer) (*slog.Logger, error) {
	var level slog.Level
	if err := level.UnmarshalText([]byte(f.Level)); err != nil {
		return nil, fmt.Errorf("%w: log level %q (want debug, info, warn or error)", ErrInvalid, f.Level)
	}

	opts := &slog.HandlerOptions{Level: level}

	var h slog.Handler

	switch strings.ToLower(f.Format) {
	case formatText:
		h = slog.NewTextHandler(w, opts)
	case formatJSON:
		h = slog.NewJSONHandler(w, opts)
	default:
		return nil, fmt.Errorf("%w: log format %q (want text or json)", ErrInvalid, f.Format)
	}

	return slog.New(traceHandler{Handler: h}), nil
}

// traceHandler adds trace_id and span_id to records logged with a context
// holding a span, so log lines and traces join. A trace_id already bound
// to the logger (a worker's job logger) is not repeated. Once the logger
// has a group, the ids would land inside it: they are left out.
type traceHandler struct {
	slog.Handler

	traceBound bool
	grouped    bool
}

func (h traceHandler) Handle(ctx context.Context, r slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() && !h.grouped {
		if !h.traceBound {
			r.AddAttrs(slog.String("trace_id", sc.TraceID().String()))
		}

		r.AddAttrs(slog.String("span_id", sc.SpanID().String()))
	}

	return h.Handler.Handle(ctx, r)
}

func (h traceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	bound := h.traceBound
	for _, a := range attrs {
		bound = bound || (a.Key == "trace_id" && !h.grouped)
	}

	return traceHandler{Handler: h.Handler.WithAttrs(attrs), traceBound: bound, grouped: h.grouped}
}

func (h traceHandler) WithGroup(name string) slog.Handler {
	return traceHandler{Handler: h.Handler.WithGroup(name), traceBound: h.traceBound, grouped: h.grouped || name != ""}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}

	return def
}
