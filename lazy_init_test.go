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
	"errors"
	"testing"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/internal/natstest"
)

// TestNewIsPureAndInitIsLazy verifies the construction contract: New performs
// no NATS I/O (no buckets exist after it), and the first operation that needs
// NATS provisions everything implicitly via Init.
func TestNewIsPureAndInitIsLazy(t *testing.T) {
	srv := natstest.Start(t)
	ctx := context.Background()

	s, err := packtrail.New(srv.NC, packtrail.WithFlow([]byte(ttlFlow)))
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	js, err := jetstream.New(srv.NC)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}

	if _, err = js.KeyValue(ctx, "packtrail-executions"); err == nil {
		t.Fatal("executions bucket exists right after New; New must not touch NATS")
	}

	// First operation lazily provisions and then behaves normally.
	if _, err = s.Get(ctx, "no-such-execution"); !errors.Is(err, packtrail.ErrNotFound) {
		t.Fatalf("first Get = %v, want ErrNotFound after lazy init", err)
	}

	if _, err = js.KeyValue(ctx, "packtrail-executions"); err != nil {
		t.Fatalf("executions bucket missing after first use: %v", err)
	}

	// Explicit Init on an already-initialized server is a cheap no-op.
	if err = s.Init(ctx); err != nil {
		t.Fatalf("re-init: %v", err)
	}
}
