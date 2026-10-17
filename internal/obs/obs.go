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

// Package obs is packtrail's OpenTelemetry instrumentation. It uses the OTel
// API only: the application chooses the SDK and exporters, and without one
// every span and instrument is a no-op. The wire format of trace context is
// the W3C traceparent header, as before.
package obs

import (
	"context"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// Scope is the instrumentation scope of packtrail's tracer and meter.
const Scope = "github.com/henomis/packtrail"

// Attribute keys of spans and metrics.
const (
	AttrExecID        = attribute.Key("packtrail.exec.id")
	AttrFlow          = attribute.Key("packtrail.flow.name")
	AttrFlowHash      = attribute.Key("packtrail.flow.hash")
	AttrNode          = attribute.Key("packtrail.node")
	AttrTaskKey       = attribute.Key("packtrail.task.key")
	AttrGeneration    = attribute.Key("packtrail.task.generation")
	AttrAttempt       = attribute.Key("packtrail.task.attempt")
	AttrKind          = attribute.Key("packtrail.worker.kind")
	AttrCommandType   = attribute.Key("packtrail.command.type")
	AttrCommandID     = attribute.Key("packtrail.command.id")
	AttrEvents        = attribute.Key("packtrail.events")
	AttrConflicts     = attribute.Key("packtrail.conflicts")
	AttrPartition     = attribute.Key("packtrail.partition")
	AttrOutcome       = attribute.Key("packtrail.job.outcome")
	AttrStatus        = attribute.Key("packtrail.execution.status")
	AttrSignal        = attribute.Key("packtrail.signal.name")
	AttrSchedule      = attribute.Key("packtrail.schedule.name")
	AttrSourceExecID  = attribute.Key("packtrail.source.exec.id")
	AttrDeliveries    = attribute.Key("packtrail.job.deliveries")
	AttrRetryable     = attribute.Key("packtrail.job.retryable")
	AttrMessagingDest = attribute.Key("messaging.destination.name")
)

// headerTraceparent is the W3C trace context header.
const headerTraceparent = "traceparent"

// propagator is fixed: traceparent is part of the wire protocol.
var propagator = propagation.TraceContext{}

// HeaderCarrier adapts a NATS header to OTel propagation.
type HeaderCarrier nats.Header

// Get implements propagation.TextMapCarrier.
func (h HeaderCarrier) Get(key string) string { return nats.Header(h).Get(key) }

// Set implements propagation.TextMapCarrier.
func (h HeaderCarrier) Set(key, value string) { nats.Header(h).Set(key, value) }

// Keys implements propagation.TextMapCarrier.
func (h HeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(h))
	for k := range h {
		keys = append(keys, k)
	}

	return keys
}

type stringCarrier struct{ tp string }

func (c *stringCarrier) Get(key string) string {
	if key == headerTraceparent {
		return c.tp
	}

	return ""
}

func (c *stringCarrier) Set(key, value string) {
	if key == headerTraceparent {
		c.tp = value
	}
}

func (c *stringCarrier) Keys() []string { return []string{headerTraceparent} }

// Traceparent is the W3C traceparent of the span in ctx, "" when ctx holds
// no valid span context.
func Traceparent(ctx context.Context) string {
	c := &stringCarrier{}
	propagator.Inject(ctx, c)

	return c.tp
}

// ContextWithTraceparent returns ctx with tp as its remote parent; ctx
// unchanged when tp is "" or invalid.
func ContextWithTraceparent(ctx context.Context, tp string) context.Context {
	if tp == "" {
		return ctx
	}

	return propagator.Extract(ctx, &stringCarrier{tp: tp})
}

// SpanContext is the span context tp carries; invalid when tp is not one.
func SpanContext(tp string) trace.SpanContext {
	return trace.SpanContextFromContext(ContextWithTraceparent(context.Background(), tp))
}

// Extract returns ctx with the trace context of a NATS header.
func Extract(ctx context.Context, h nats.Header) context.Context {
	return propagator.Extract(ctx, HeaderCarrier(h))
}

// Tracer is packtrail's tracer from tp, the global provider when nil.
func Tracer(tp trace.TracerProvider) trace.Tracer {
	if tp == nil {
		tp = otel.GetTracerProvider()
	}

	return tp.Tracer(Scope)
}

// Meter is packtrail's meter from mp, the global provider when nil.
func Meter(mp metric.MeterProvider) metric.Meter {
	if mp == nil {
		mp = otel.GetMeterProvider()
	}

	return mp.Meter(Scope)
}

// Fail marks span as failed by err (nil: nothing).
func Fail(span trace.Span, err error) {
	if err == nil {
		return
	}

	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}
