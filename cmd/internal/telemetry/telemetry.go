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

// Package telemetry sets up OpenTelemetry for the command-line tools, from
// the standard OTEL_* environment variables:
//
//   - traces and metrics are exported over OTLP/HTTP (protobuf) when
//     OTEL_EXPORTER_OTLP_ENDPOINT (or the per-signal variant) is set, unless
//     OTEL_TRACES_EXPORTER / OTEL_METRICS_EXPORTER is "none"; asking for
//     OTLP/gRPC (OTEL_EXPORTER_OTLP_PROTOCOL=grpc) is an error;
//   - metrics can also be served for Prometheus to scrape (Handler);
//   - OTEL_SERVICE_NAME, OTEL_RESOURCE_ATTRIBUTES and OTEL_TRACES_SAMPLER
//     apply as usual.
//
// Without any of that, nothing is exported and nothing is dialled.
package telemetry

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// shutdownTimeout bounds flushing telemetry on exit.
const shutdownTimeout = 5 * time.Second

// Config says what to set up.
type Config struct {
	// ServiceName is the default service.name (OTEL_SERVICE_NAME wins).
	ServiceName    string
	ServiceVersion string
	// Prometheus serves metrics for scraping through Telemetry.Handler.
	Prometheus bool
}

// Telemetry holds the providers a command passes to packtrail.
type Telemetry struct {
	TracerProvider trace.TracerProvider
	MeterProvider  metric.MeterProvider
	// Handler serves the metrics in the Prometheus format; nil unless
	// Config.Prometheus.
	Handler http.Handler

	shutdown []func(context.Context) error
}

// Setup builds the providers cfg and the environment ask for and installs
// them as the global ones, with the W3C trace context propagator.
func Setup(ctx context.Context, cfg Config) (*Telemetry, error) {
	t := &Telemetry{TracerProvider: otel.GetTracerProvider(), MeterProvider: otel.GetMeterProvider()}

	traces, metrics := otlpEnabled("TRACES"), otlpEnabled("METRICS")
	if !traces && !metrics && !cfg.Prometheus {
		return t, nil
	}

	if err := checkProtocol(); err != nil {
		return nil, err
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(attribute.String("service.name", cfg.ServiceName),
			attribute.String("service.version", cfg.ServiceVersion)),
		resource.WithFromEnv(), resource.WithTelemetrySDK())
	if err != nil {
		return nil, fmt.Errorf("telemetry: resource: %w", err)
	}

	if traces {
		exp, terr := otlptracehttp.New(ctx)
		if terr != nil {
			return nil, fmt.Errorf("telemetry: traces: %w", terr)
		}

		tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exp), sdktrace.WithResource(res))
		t.TracerProvider = tp
		t.shutdown = append(t.shutdown, tp.Shutdown)
	}

	if err = t.setupMetrics(ctx, cfg, res, metrics); err != nil {
		return nil, errors.Join(err, t.Shutdown())
	}

	// Installed only once everything is set up: a failure leaves the
	// globals as they were.
	if traces {
		otel.SetTracerProvider(t.TracerProvider)
	}

	if mp, ok := t.MeterProvider.(*sdkmetric.MeterProvider); ok {
		otel.SetMeterProvider(mp)
	}

	otel.SetTextMapPropagator(propagation.TraceContext{})

	return t, nil
}

// ErrProtocol reports an OTLP protocol other than HTTP/protobuf.
var ErrProtocol = errors.New("telemetry: only OTLP over HTTP (http/protobuf) is supported")

// checkProtocol refuses OTEL_EXPORTER_OTLP_PROTOCOL (or a per-signal one)
// asking for gRPC, rather than silently sending HTTP to a gRPC port.
func checkProtocol() error {
	for _, k := range []string{
		"OTEL_EXPORTER_OTLP_PROTOCOL", "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "OTEL_EXPORTER_OTLP_METRICS_PROTOCOL",
	} {
		if v := os.Getenv(k); v != "" && v != "http/protobuf" {
			return fmt.Errorf("%w: %s=%s", ErrProtocol, k, v)
		}
	}

	return nil
}

func (t *Telemetry) setupMetrics(ctx context.Context, cfg Config, res *resource.Resource, otlp bool) error {
	var readers []sdkmetric.Option

	if otlp {
		exp, err := otlpmetrichttp.New(ctx)
		if err != nil {
			return fmt.Errorf("telemetry: metrics: %w", err)
		}

		readers = append(readers, sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exp)))
	}

	if cfg.Prometheus {
		reg := prometheus.NewRegistry()

		exp, err := otelprom.New(otelprom.WithRegisterer(reg))
		if err != nil {
			return fmt.Errorf("telemetry: prometheus: %w", err)
		}

		readers = append(readers, sdkmetric.WithReader(exp))
		t.Handler = promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
	}

	if len(readers) == 0 {
		return nil
	}

	mp := sdkmetric.NewMeterProvider(append(readers, sdkmetric.WithResource(res))...)
	t.MeterProvider = mp
	t.shutdown = append(t.shutdown, mp.Shutdown)

	return nil
}

// Shutdown flushes and stops what Setup started.
func (t *Telemetry) Shutdown() error {
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	errs := make([]error, 0, len(t.shutdown))
	for _, f := range t.shutdown {
		errs = append(errs, f(ctx))
	}

	t.shutdown = nil

	return errors.Join(errs...)
}

// otlpEnabled reports whether OTLP export of signal (TRACES, METRICS) is
// configured: an endpoint is set and the exporter is not "none".
func otlpEnabled(signal string) bool {
	if os.Getenv("OTEL_"+signal+"_EXPORTER") == "none" {
		return false
	}

	return os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" || os.Getenv("OTEL_EXPORTER_OTLP_"+signal+"_ENDPOINT") != ""
}
