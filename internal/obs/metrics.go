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

package obs

import (
	"context"
	"math"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/henomis/packtrail/internal/metrics"
)

// Units of counts.
const (
	unitJob       = "{job}"
	unitExecution = "{execution}"
)

// durationBuckets are the boundaries, in seconds, of command, job and queue
// durations: from a millisecond to an hour.
var durationBuckets = []float64{
	0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300, 900, 3600,
}

// executionBuckets are the boundaries, in seconds, of execution durations:
// from a second to thirty days.
var executionBuckets = []float64{
	1, 5, 15, 60, 300, 900, 3600, 4 * 3600, 12 * 3600, 86400, 3 * 86400, 7 * 86400, 30 * 86400,
}

// Engine holds the engine's synchronous instruments.
type Engine struct {
	commandDuration metric.Float64Histogram
	started         metric.Int64Counter
	ended           metric.Int64Counter
	execDuration    metric.Float64Histogram
}

// NewEngine creates the engine's instruments on m (nil: no-op).
func NewEngine(m metric.Meter) *Engine {
	if m == nil {
		m = noop.Meter{}
	}

	var errs []error

	e := &Engine{}

	e.commandDuration, errs = hist(m, errs, "packtrail.engine.command.duration",
		"Time to decide and store one command.", durationBuckets)
	e.started, errs = counter(m, errs, "packtrail.executions.started", "Executions started (forks included).",
		unitExecution)
	e.ended, errs = counter(m, errs, "packtrail.executions.ended", "Executions that reached a final status.",
		unitExecution)
	e.execDuration, errs = hist(m, errs, "packtrail.execution.duration",
		"Time from the start of an execution to its final status.", executionBuckets)

	if len(errs) > 0 {
		return NewEngine(noop.Meter{})
	}

	return e
}

// Command records the handling of one command.
func (e *Engine) Command(ctx context.Context, cmdType string, d time.Duration) {
	e.commandDuration.Record(ctx, d.Seconds(), metric.WithAttributes(AttrCommandType.String(cmdType)))
}

// Started records an execution started.
func (e *Engine) Started(ctx context.Context, flow string) {
	e.started.Add(ctx, 1, metric.WithAttributes(AttrFlow.String(flow)))
}

// Ended records an execution that reached status after d.
func (e *Engine) Ended(ctx context.Context, flow, status string, d time.Duration) {
	attrs := metric.WithAttributes(AttrFlow.String(flow), AttrStatus.String(status))
	e.ended.Add(ctx, 1, attrs)
	e.execDuration.Record(ctx, d.Seconds(), attrs)
}

// ObserveEngine registers the engine's observable instruments: its counters
// (read from counters, cheap) and the dispatcher's lag and stall (read from
// lag, which may do I/O). The registration ends with Unregister.
func ObserveEngine(m metric.Meter, counters func() metrics.Snapshot,
	lag func(context.Context) metrics.Snapshot,
) (metric.Registration, error) {
	type obsCounter struct {
		inst metric.Int64ObservableCounter
		val  func(metrics.Snapshot) int64
	}

	defs := []struct {
		name, desc, unit string
		val              func(metrics.Snapshot) int64
	}{
		{
			"packtrail.engine.commands", "Commands handled.", "{command}",
			func(s metrics.Snapshot) int64 { return s.Commands },
		},
		{
			"packtrail.engine.conflicts", "Appends retried after a concurrent append.", "{conflict}",
			func(s metrics.Snapshot) int64 { return s.Conflicts },
		},
		{
			"packtrail.engine.events.appended", "Events appended to execution logs.", "{event}",
			func(s metrics.Snapshot) int64 { return s.EventsAppended },
		},
		{
			"packtrail.engine.dead_letters", "Messages dead-lettered.", "{message}",
			func(s metrics.Snapshot) int64 { return s.DeadLetters },
		},
		{
			"packtrail.dispatch.jobs", "Jobs sent to workers.", unitJob,
			func(s metrics.Snapshot) int64 { return s.JobsDispatched },
		},
		{
			"packtrail.dispatch.cache_hits", "Task results served from the result cache.", unitJob,
			func(s metrics.Snapshot) int64 { return s.CacheHits },
		},
		{
			"packtrail.engine.timers", "Timers fired.", "{timer}",
			func(s metrics.Snapshot) int64 { return s.Timers },
		},
		{
			"packtrail.executions.archived", "Executions archived.", unitExecution,
			func(s metrics.Snapshot) int64 { return s.Archived },
		},
		{
			"packtrail.executions.deleted", "Executions deleted.", unitExecution,
			func(s metrics.Snapshot) int64 { return s.Deleted },
		},
	}

	cs := make([]obsCounter, 0, len(defs))
	insts := make([]metric.Observable, 0, len(defs))

	for _, d := range defs {
		c, err := m.Int64ObservableCounter(d.name, metric.WithDescription(d.desc), metric.WithUnit(d.unit))
		if err != nil {
			return nil, err
		}

		cs = append(cs, obsCounter{c, d.val})
		insts = append(insts, c)
	}

	lagG, err := m.Int64ObservableGauge("packtrail.dispatch.lag",
		metric.WithDescription("Events the dispatcher has not processed yet."), metric.WithUnit("{event}"))
	if err != nil {
		return nil, err
	}

	stallG, err := m.Float64ObservableGauge("packtrail.dispatch.stall",
		metric.WithDescription("How long a dispatcher partition with pending events has gone without progress."),
		metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}

	insts = append(insts, lagG, stallG)

	return m.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		s := counters()
		for _, c := range cs {
			o.ObserveInt64(c.inst, c.val(s))
		}

		l := lag(ctx)
		o.ObserveInt64(lagG, int64(min(l.ProjectionLag, math.MaxInt64))) //nolint:gosec // bounded above.

		for p, d := range l.DispatchStalls {
			o.ObserveFloat64(stallG, d.Seconds(), metric.WithAttributes(AttrPartition.Int(p)))
		}

		return nil
	}, insts...)
}

// Worker holds a worker's instruments.
type Worker struct {
	jobs      metric.Int64Counter
	duration  metric.Float64Histogram
	queueWait metric.Float64Histogram
	active    metric.Int64UpDownCounter
}

// NewWorker creates a worker's instruments on m (nil: no-op).
func NewWorker(m metric.Meter) *Worker {
	if m == nil {
		m = noop.Meter{}
	}

	var errs []error

	w := &Worker{}

	w.jobs, errs = counter(m, errs, "packtrail.worker.jobs", "Job attempts handled, by outcome.", unitJob)
	w.duration, errs = hist(m, errs, "packtrail.job.duration", "Time a handler ran.", durationBuckets)
	w.queueWait, errs = hist(m, errs, "packtrail.job.queue_wait",
		"Time from dispatch to the first delivery of a job.", durationBuckets)

	a, err := m.Int64UpDownCounter("packtrail.worker.jobs.active", metric.WithDescription("Jobs running now."),
		metric.WithUnit(unitJob))
	if err != nil {
		errs = append(errs, err)
	}

	w.active = a

	if len(errs) > 0 {
		return NewWorker(noop.Meter{})
	}

	return w
}

// Begin records a job starting after waiting wait in its queue (wait < 0:
// a redelivery, not recorded). The returned function records its end.
func (w *Worker) Begin(ctx context.Context, kind string, wait time.Duration) func(outcome string, d time.Duration) {
	k := metric.WithAttributes(AttrKind.String(kind))
	if wait >= 0 {
		w.queueWait.Record(ctx, wait.Seconds(), k)
	}

	w.active.Add(ctx, 1, k)

	return func(outcome string, d time.Duration) {
		w.active.Add(ctx, -1, k)

		attrs := metric.WithAttributes(AttrKind.String(kind), AttrOutcome.String(outcome))
		w.jobs.Add(ctx, 1, attrs)
		w.duration.Record(ctx, d.Seconds(), attrs)
	}
}

// hist creates a histogram of durations in seconds.
func hist(m metric.Meter, errs []error, name, desc string, buckets []float64) (metric.Float64Histogram, []error) {
	h, err := m.Float64Histogram(name, metric.WithDescription(desc), metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(buckets...))
	if err != nil {
		errs = append(errs, err)
	}

	return h, errs
}

func counter(m metric.Meter, errs []error, name, desc, unit string) (metric.Int64Counter, []error) {
	c, err := m.Int64Counter(name, metric.WithDescription(desc), metric.WithUnit(unit))
	if err != nil {
		errs = append(errs, err)
	}

	return c, errs
}
