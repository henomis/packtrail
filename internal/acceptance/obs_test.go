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
	"strings"
	"testing"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/event"
	"github.com/henomis/packtrail/worker"
)

const obsGated = `
name: gated
nodes:
  - {id: w, type: await, signal: go, timeout: 1h, next: t}
  - {id: t, type: task, kind: echo}
`

// obsEnv is a deployment whose engine and worker report to an in-memory
// span recorder and metric reader. handlerSpans receives the span context
// each handler runs under.
type obsEnv struct {
	*Env

	spans        *tracetest.SpanRecorder
	tp           *sdktrace.TracerProvider
	reader       *sdkmetric.ManualReader
	handlerSpans chan trace.SpanContext
}

func newObsEnv(t *testing.T, flows ...string) *obsEnv {
	t.Helper()

	spans := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans))
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	o := &obsEnv{spans: spans, tp: tp, reader: reader, handlerSpans: make(chan trace.SpanContext, 16)}
	o.Env = NewEnv(t, flows, packtrail.WithTracerProvider(tp), packtrail.WithMeterProvider(mp))
	o.Worker("echo", func(ctx context.Context, j *worker.Job) (*worker.Result, error) {
		o.handlerSpans <- trace.SpanContextFromContext(ctx)

		res, err := Echo(ctx, j)
		if res != nil {
			res.Summary = map[string]any{"node": j.Node}
		}

		return res, err
	}, worker.WithTracerProvider(tp), worker.WithMeterProvider(mp))

	return o
}

// ended returns the recorded spans whose name starts with prefix.
func (o *obsEnv) ended(prefix string) []sdktrace.ReadOnlySpan {
	var out []sdktrace.ReadOnlySpan

	for _, s := range o.spans.Ended() {
		if strings.HasPrefix(s.Name(), prefix) {
			out = append(out, s)
		}
	}

	return out
}

func (o *obsEnv) byID(id trace.SpanID) sdktrace.ReadOnlySpan {
	for _, s := range o.spans.Ended() {
		if s.SpanContext().SpanID() == id {
			return s
		}
	}

	return nil
}

// TestTraceOfAnExecution checks one execution is one trace, each job under
// the dispatch that sent it, each dispatch under the decision that
// scheduled it, and the handler's context in the job's span.
func TestTraceOfAnExecution(t *testing.T) {
	o := newObsEnv(t, m1Linear)

	ctx, root := o.tp.Tracer("test").Start(o.Ctx, "caller")

	id, err := o.Client.Start(ctx, "linear", nil)
	if err != nil {
		t.Fatal(err)
	}

	root.End()
	o.Completed(id)

	tid := root.SpanContext().TraceID()

	jobs := o.ended("packtrail.job ")
	if len(jobs) != 3 {
		t.Fatalf("%d job spans", len(jobs))
	}

	for range 3 {
		hs := <-o.handlerSpans
		if hs.TraceID() != tid || o.byID(hs.SpanID()) == nil || !strings.HasPrefix(o.byID(hs.SpanID()).Name(), "packtrail.job ") {
			t.Fatalf("handler span %v is not a job span of trace %v", hs, tid)
		}
	}

	for _, j := range jobs {
		disp := o.byID(j.Parent().SpanID())
		if disp == nil || !strings.HasPrefix(disp.Name(), "packtrail.dispatch ") || j.SpanContext().TraceID() != tid {
			t.Fatalf("job %s: parent %v", j.Name(), disp)
		}

		dec := o.byID(disp.Parent().SpanID())
		if dec == nil || !strings.HasPrefix(dec.Name(), "packtrail.decide ") {
			t.Fatalf("dispatch %s: parent %v", disp.Name(), dec)
		}
	}

	start := o.ended("packtrail.start linear")
	if len(start) != 1 || start[0].Parent().SpanID() != root.SpanContext().SpanID() {
		t.Fatalf("start span %v", start)
	}

	evs, err := o.Client.History(o.Ctx, id)
	if err != nil {
		t.Fatal(err)
	}

	for _, ev := range evs {
		if sc := trace.SpanContextFromContext(ctxWithTP(ev.Trace)); sc.TraceID() != tid {
			t.Fatalf("event %s carries trace %q", ev.Type, ev.Trace)
		}
	}
}

// TestSignalJoinsTheExecutionTrace checks a signal sent from another trace
// is decided in the execution's trace, linked to the sender's, and the work
// it releases stays in the execution's trace.
func TestSignalJoinsTheExecutionTrace(t *testing.T) {
	o := newObsEnv(t, obsGated)

	ctx, root := o.tp.Tracer("test").Start(o.Ctx, "starter")

	id, err := o.Client.Start(ctx, "gated", nil)
	if err != nil {
		t.Fatal(err)
	}

	root.End()

	sctx, other := o.tp.Tracer("test").Start(o.Ctx, "approver")
	if err = o.Client.Signal(sctx, id, "go", nil); err != nil {
		t.Fatal(err)
	}

	other.End()
	o.Completed(id)

	exec, foreign := root.SpanContext().TraceID(), other.SpanContext().TraceID()

	if sig := o.ended("packtrail.signal"); len(sig) != 1 || sig[0].SpanContext().TraceID() != foreign {
		t.Fatalf("signal span %v", sig)
	}

	var linked bool

	for _, d := range o.ended("packtrail.decide signal") {
		if d.SpanContext().TraceID() != exec {
			t.Fatalf("signal decided in trace %v, want the execution's %v", d.SpanContext().TraceID(), exec)
		}

		for _, l := range d.Links() {
			linked = linked || l.SpanContext.TraceID() == foreign
		}
	}

	if !linked {
		t.Fatal("the signal's decision has no link to the sender's trace")
	}

	if jobs := o.ended("packtrail.job t"); len(jobs) != 1 || jobs[0].SpanContext().TraceID() != exec {
		t.Fatalf("job after the signal: %v", jobs)
	}
}

// TestForkStartsItsOwnTrace checks a fork is a trace of its own.
func TestForkStartsItsOwnTrace(t *testing.T) {
	o := newObsEnv(t, m1Linear)

	ctx, root := o.tp.Tracer("test").Start(o.Ctx, "caller")

	id, err := o.Client.Start(ctx, "linear", nil)
	if err != nil {
		t.Fatal(err)
	}

	root.End()
	o.Completed(id)

	evs, err := o.Client.History(o.Ctx, id)
	if err != nil {
		t.Fatal(err)
	}

	fctx, forker := o.tp.Tracer("test").Start(o.Ctx, "forker")

	fid, err := o.Client.Fork(fctx, id, evs[1].Seq)
	if err != nil {
		t.Fatal(err)
	}

	forker.End()

	st := o.Completed(fid)
	if sc := trace.SpanContextFromContext(ctxWithTP(st.Trace)); sc.TraceID() != forker.SpanContext().TraceID() {
		t.Fatalf("fork trace %q, want the forker's %v", st.Trace, forker.SpanContext().TraceID())
	}
}

// TestAttemptReportAndMetrics checks the worker's start time and summary
// reach the event log and the timeline, and the instruments count the jobs.
func TestAttemptReportAndMetrics(t *testing.T) {
	o := newObsEnv(t, m1Linear)
	id := o.Start("linear", nil)
	o.Completed(id)

	evs, err := o.Client.History(o.Ctx, id)
	if err != nil {
		t.Fatal(err)
	}

	for _, ev := range evs {
		if d, ok := ev.Data.(*event.NodeDone); ok && (d.Started.IsZero() || string(d.Summary) != `{"node":"`+d.Node+`"}`) {
			t.Fatalf("NodeCompleted %+v", d)
		}
	}

	tl, err := o.Client.Timeline(o.Ctx, id)
	if err != nil {
		t.Fatal(err)
	}

	if len(tl.Steps) != 3 || tl.Status != packtrail.StatusCompleted || tl.Steps[2].Started.IsZero() ||
		tl.Steps[2].Outcome != packtrail.OutcomeCompleted {
		t.Fatalf("timeline %+v", tl)
	}

	var rm metricdata.ResourceMetrics
	if err = o.reader.Collect(o.Ctx, &rm); err != nil {
		t.Fatal(err)
	}

	got := map[string]metricdata.Aggregation{}

	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			got[m.Name] = m.Data
		}
	}

	for _, name := range []string{
		"packtrail.engine.commands", "packtrail.engine.events.appended", "packtrail.dispatch.jobs",
		"packtrail.engine.command.duration", "packtrail.executions.started", "packtrail.executions.ended",
		"packtrail.execution.duration", "packtrail.job.duration", "packtrail.job.queue_wait",
		"packtrail.worker.jobs.active", "packtrail.dispatch.lag",
	} {
		if got[name] == nil {
			t.Errorf("no %s", name)
		}
	}

	jobs, ok := got["packtrail.worker.jobs"].(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("packtrail.worker.jobs: %T", got["packtrail.worker.jobs"])
	}

	var completed int64

	for _, dp := range jobs.DataPoints {
		if v, _ := dp.Attributes.Value("packtrail.job.outcome"); v.AsString() == "completed" {
			completed += dp.Value
		}
	}

	if completed != 3 {
		t.Fatalf("completed jobs %d, want 3", completed)
	}
}

func ctxWithTP(tp string) context.Context {
	return trace.ContextWithRemoteSpanContext(context.Background(), spanContextOf(tp))
}

// spanContextOf parses a W3C traceparent ("" or invalid: an invalid one).
func spanContextOf(tp string) trace.SpanContext {
	f := strings.Split(tp, "-")
	if len(f) != 4 { //nolint:mnd // version-trace-parent-flags.
		return trace.SpanContext{}
	}

	tid, err := trace.TraceIDFromHex(f[1])
	if err != nil {
		return trace.SpanContext{}
	}

	sid, err := trace.SpanIDFromHex(f[2])
	if err != nil {
		return trace.SpanContext{}
	}

	return trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid, Remote: true})
}
