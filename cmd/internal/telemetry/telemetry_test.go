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

package telemetry

import (
	"context"
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
)

func TestNothingConfigured(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "")

	tel, err := Setup(context.Background(), Config{ServiceName: "t"})
	if err != nil {
		t.Fatal(err)
	}

	if tel.Handler != nil || len(tel.shutdown) != 0 || tel.TracerProvider != otel.GetTracerProvider() {
		t.Fatalf("something was set up: %+v", tel)
	}

	if err = tel.Shutdown(); err != nil {
		t.Fatal(err)
	}
}

func TestExporterNone(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")
	t.Setenv("OTEL_TRACES_EXPORTER", "none")
	t.Setenv("OTEL_METRICS_EXPORTER", "none")

	if otlpEnabled("TRACES") || otlpEnabled("METRICS") {
		t.Fatal("exporter none ignored")
	}

	t.Setenv("OTEL_TRACES_EXPORTER", "")

	if !otlpEnabled("TRACES") {
		t.Fatal("endpoint ignored")
	}
}

func TestPrometheus(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")

	tel, err := Setup(context.Background(), Config{ServiceName: "t", Prometheus: true})
	if err != nil {
		t.Fatal(err)
	}

	defer func() { _ = tel.Shutdown() }()

	c, err := tel.MeterProvider.Meter("t").Int64Counter("packtrail.test.things")
	if err != nil {
		t.Fatal(err)
	}

	c.Add(context.Background(), 3)

	srv := httptest.NewServer(tel.Handler)
	defer srv.Close()

	resp, err := srv.Client().Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}

	defer func() { _ = resp.Body.Close() }()

	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "packtrail_test_things_total{") || !strings.Contains(string(body), "} 3\n") {
		t.Fatalf("metrics:\n%s", body)
	}
}

func TestGRPCRefused(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")

	before := otel.GetTracerProvider()

	if _, err := Setup(context.Background(), Config{ServiceName: "t"}); !errors.Is(err, ErrProtocol) {
		t.Fatalf("err %v, want ErrProtocol", err)
	}

	if otel.GetTracerProvider() != before {
		t.Fatal("a failed setup changed the global tracer provider")
	}
}
