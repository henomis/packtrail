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

package acceptance

import (
	"context"
	"log/slog"
	"sync"
	"testing"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/worker"
)

// recorder is a slog.Handler keeping every record with its attributes
// (including those bound with Logger.With) flattened to strings.
type recorder struct {
	level slog.Level
	mu    *sync.Mutex
	recs  *[]map[string]string
	attrs []slog.Attr
}

func newRecorder(level slog.Level) *recorder {
	return &recorder{level: level, mu: &sync.Mutex{}, recs: &[]map[string]string{}}
}

func (r *recorder) Enabled(_ context.Context, l slog.Level) bool { return l >= r.level }

func (r *recorder) Handle(_ context.Context, rec slog.Record) error {
	m := map[string]string{"msg": rec.Message, "level": rec.Level.String()}
	for _, a := range r.attrs {
		m[a.Key] = a.Value.String()
	}

	rec.Attrs(func(a slog.Attr) bool {
		m[a.Key] = a.Value.String()

		return true
	})

	r.mu.Lock()
	*r.recs = append(*r.recs, m)
	r.mu.Unlock()

	return nil
}

func (r *recorder) WithAttrs(as []slog.Attr) slog.Handler {
	c := *r
	c.attrs = append(append([]slog.Attr{}, r.attrs...), as...)

	return &c
}

func (r *recorder) WithGroup(string) slog.Handler { return r }

// find returns the records with message msg whose attributes include want.
func (r *recorder) find(msg string, want map[string]string) []map[string]string {
	r.mu.Lock()
	defer r.mu.Unlock()

	var out []map[string]string

next:
	for _, m := range *r.recs {
		if m["msg"] != msg {
			continue
		}

		for k, v := range want {
			if m[k] != v {
				continue next
			}
		}

		out = append(out, m)
	}

	return out
}

func TestLoggingDebugLinesShareKeys(t *testing.T) {
	rec := newRecorder(slog.LevelDebug)
	log := slog.New(rec)
	e := NewEnv(t, []string{m1Linear}, packtrail.WithLogger(log))

	const tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

	e.Worker("echo", func(ctx context.Context, j *worker.Job) (*worker.Result, error) {
		j.Logger().InfoContext(ctx, "handler ran")

		return Echo(ctx, j)
	}, worker.WithLogger(log))

	id := e.Start("linear", nil, packtrail.WithTraceparent(tp))
	e.Completed(id)

	job := map[string]string{"exec": id, "node": "b", "kind": "echo", "attempt": "1"}

	if got := rec.find("handler ran", map[string]string{
		"exec": id, "node": "b", "flow": "linear", "kind": "echo", "generation": "2", "attempt": "1",
		"trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
	}); len(got) != 1 {
		t.Fatalf("handler line through Job.Logger: %v", rec.find("handler ran", nil))
	}

	for _, msg := range []string{"packtrail: job dispatched", "worker: job started"} {
		if len(rec.find(msg, job)) == 0 {
			t.Fatalf("no %q line for %v", msg, job)
		}
	}

	ended := rec.find("worker: job ended", job)
	if len(ended) != 1 || ended[0]["outcome"] != "completed" || ended[0]["duration"] == "" {
		t.Fatalf("job ended: %v", ended)
	}

	if len(rec.find("packtrail: decision applied", map[string]string{"exec": id, "type": "start"})) != 1 {
		t.Fatalf("no decision applied line for the start")
	}
}

func TestLoggingDebugLinesHiddenAtInfo(t *testing.T) {
	rec := newRecorder(slog.LevelInfo)
	log := slog.New(rec)
	e := NewEnv(t, []string{m1Linear}, packtrail.WithLogger(log))
	e.Worker("echo", Echo, worker.WithLogger(log))
	e.Completed(e.Start("linear", nil))

	for _, msg := range []string{
		"packtrail: decision applied", "packtrail: job dispatched", "worker: job started", "worker: job ended",
	} {
		if got := rec.find(msg, nil); len(got) != 0 {
			t.Fatalf("%q logged at info: %v", msg, got)
		}
	}
}

func TestClientLoggerRejectsNil(t *testing.T) {
	s := NewEnv(t, nil).S
	if _, err := packtrail.NewClient(s.Connect(t), packtrail.WithClientLogger(nil)); err == nil {
		t.Fatal("nil logger accepted")
	}
}
