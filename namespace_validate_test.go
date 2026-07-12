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

package packtrail_test

import (
	"context"
	"strings"
	"testing"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/internal/natstest"
)

// TestNewValidatesNamespaceAndAsyncKind: the namespace prefixes every NATS
// resource name and an async invoker kind names its work-queue stream, so
// unsafe values are rejected up front with a clear error instead of an opaque
// NATS failure mid-bootstrap.
func TestNewValidatesNamespaceAndAsyncKind(t *testing.T) {
	srv := natstest.Start(t)

	noop := packtrail.InvokerFunc(func(_ context.Context, _ packtrail.Request) (packtrail.Result, error) {
		return packtrail.Result{Status: packtrail.StatusOK}, nil
	})

	for _, ns := range []string{"bad ns", "dotted.ns", "wild*"} {
		if _, err := packtrail.New(srv.NC, packtrail.WithNamespace(ns), packtrail.WithFlow([]byte(ttlFlow))); err == nil ||
			!strings.Contains(err.Error(), "invalid namespace") {
			t.Errorf("New(namespace %q) err = %v, want invalid-namespace rejection", ns, err)
		}
	}

	if _, err := packtrail.New(srv.NC, packtrail.WithFlow([]byte(ttlFlow)),
		packtrail.WithAsyncInvoker("bad kind", noop)); err == nil ||
		!strings.Contains(err.Error(), "invalid async invoker kind") {
		t.Errorf("New(async kind) err = %v, want invalid-kind rejection", err)
	}

	// Well-formed values are accepted.
	if _, err := packtrail.New(srv.NC, packtrail.WithNamespace("acme-prod_1"),
		packtrail.WithFlow([]byte(ttlFlow)), packtrail.WithAsyncInvoker("agent", noop)); err != nil {
		t.Fatalf("New(valid names): %v", err)
	}
}
