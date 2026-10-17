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
	"testing"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/trace"
)

const tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

func TestTraceparentRoundTrip(t *testing.T) {
	ctx := ContextWithTraceparent(context.Background(), tp)
	if got := Traceparent(ctx); got != tp {
		t.Fatalf("Traceparent = %q, want %q", got, tp)
	}

	h := nats.Header{}
	HeaderCarrier(h).Set("traceparent", Traceparent(ctx))

	if sc := trace.SpanContextFromContext(Extract(context.Background(), h)); sc.TraceID().String() !=
		"4bf92f3577b34da6a3ce929d0e0e4736" || !sc.IsRemote() {
		t.Fatalf("extracted %v", sc)
	}
}

func TestNoTraceparent(t *testing.T) {
	for _, s := range []string{"", "garbage", "00-00000000000000000000000000000000-00f067aa0ba902b7-01"} {
		if got := Traceparent(ContextWithTraceparent(context.Background(), s)); got != "" {
			t.Errorf("%q: Traceparent = %q", s, got)
		}

		if SpanContext(s).IsValid() {
			t.Errorf("%q: valid span context", s)
		}
	}
}
