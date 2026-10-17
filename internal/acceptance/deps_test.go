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
	"os"
	"os/exec"
	"strings"
	"testing"
)

// TestNoForbiddenDependencies keeps the module lean: NATS, expr-lang, yaml,
// a JSON Schema validator and the OpenTelemetry API — nothing else, and
// nothing AI-specific. The command-line tools also bring the OpenTelemetry
// SDK, its OTLP and Prometheus exporters and what they need; the library
// never imports those (TestLibraryImportsOnlyOTelAPI).
func TestNoForbiddenDependencies(t *testing.T) {
	data, err := os.ReadFile("../../go.mod")
	if err != nil {
		t.Fatal(err)
	}

	allowed := []string{
		"github.com/nats-io/", "github.com/expr-lang/expr", "gopkg.in/yaml.v3",
		"github.com/santhosh-tekuri/jsonschema", "github.com/klauspost/compress", "github.com/minio/highwayhash",
		"github.com/google/go-tpm", "github.com/antithesishq/", "golang.org/x/", "github.com/nats-io/nuid",
		// The OpenTelemetry API.
		"go.opentelemetry.io/otel", "go.opentelemetry.io/auto/sdk", "github.com/go-logr/", "github.com/cespare/xxhash",
		// The CLIs only: SDK, exporters, Prometheus.
		"go.opentelemetry.io/proto/otlp", "github.com/prometheus/", "github.com/beorn7/perks",
		"github.com/munnerz/goautoneg", "google.golang.org/protobuf", "google.golang.org/grpc",
		"google.golang.org/genproto/", "github.com/grpc-ecosystem/grpc-gateway", "github.com/cenkalti/backoff",
		"github.com/google/uuid", "go.yaml.in/yaml",
	}

	inRequire := false

	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)

		switch {
		case strings.HasPrefix(line, "require ("):
			inRequire = true

			continue
		case line == ")":
			inRequire = false

			continue
		}

		if !inRequire || line == "" {
			continue
		}

		mod := strings.Fields(line)[0]
		ok := false

		for _, a := range allowed {
			if strings.HasPrefix(mod, a) {
				ok = true
			}
		}

		if !ok {
			t.Errorf("unexpected dependency %s", mod)
		}
	}
}

// TestLibraryImportsOnlyOTelAPI checks that the packages an application
// imports reach only the OpenTelemetry API: the SDK and exporters are the
// application's choice (the CLIs make theirs under cmd/).
func TestLibraryImportsOnlyOTelAPI(t *testing.T) {
	out, err := exec.Command("go", "list", "-deps", "-f", "{{if not .Standard}}{{.ImportPath}}{{end}}",
		"github.com/henomis/packtrail", "github.com/henomis/packtrail/worker", "github.com/henomis/packtrail/event",
		"github.com/henomis/packtrail/flow", "github.com/henomis/packtrail/packtrailtest").Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}

	forbidden := []string{
		"go.opentelemetry.io/otel/sdk", "go.opentelemetry.io/otel/exporters", "github.com/prometheus/",
		"google.golang.org/grpc", "google.golang.org/protobuf",
	}

	for _, pkg := range strings.Fields(string(out)) {
		for _, f := range forbidden {
			if strings.HasPrefix(pkg, f) {
				t.Errorf("library package graph reaches %s", pkg)
			}
		}
	}
}
