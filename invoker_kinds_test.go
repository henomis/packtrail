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
	"time"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/internal/natstest"
)

const ghostKindFlow = `
version: "1.0"
name: ghost-kind
nodes:
  - {id: a, type: task, invoker: ghost, target: agent-a}
`

// TestNewValidatesInvokerKinds: a task node naming an unregistered invoker kind
// is a construction error at New, not a runtime failure on the first execution
// to reach that node.
func TestNewValidatesInvokerKinds(t *testing.T) {
	srv := natstest.Start(t)

	noop := packtrail.InvokerFunc(func(_ context.Context, _ packtrail.Request) (packtrail.Result, error) {
		return packtrail.Result{Status: packtrail.StatusOK}, nil
	})

	if _, err := packtrail.New(srv.NC, packtrail.WithFlow([]byte(ghostKindFlow))); err == nil ||
		!strings.Contains(err.Error(), `invoker kind "ghost"`) {
		t.Fatalf("New(unregistered kind) err = %v, want unregistered-kind rejection", err)
	}

	// The same flow constructs once the kind is registered — synchronously...
	if _, err := packtrail.New(srv.NC, packtrail.WithFlow([]byte(ghostKindFlow)),
		packtrail.WithInvoker("ghost", noop)); err != nil {
		t.Fatalf("New(WithInvoker): %v", err)
	}

	// ...or as an async invoker kind.
	if _, err := packtrail.New(srv.NC, packtrail.WithFlow([]byte(ghostKindFlow)),
		packtrail.WithAsyncInvoker("ghost", noop)); err != nil {
		t.Fatalf("New(WithAsyncInvoker): %v", err)
	}

	// The built-in nats-task kind (explicit or defaulted via subject) needs no
	// registration.
	if _, err := packtrail.New(srv.NC, packtrail.WithFlow([]byte(`
version: "1.0"
name: builtin-kind
nodes:
  - {id: a, type: task, subject: "tasks.a"}
`))); err != nil {
		t.Fatalf("New(built-in kind): %v", err)
	}
}

// TestNewRejectsInvokerKindCollisions verifies ambiguous kinds are rejected up
// front instead of silently dropping an invoker or dispatcher.
func TestNewRejectsInvokerKindCollisions(t *testing.T) {
	srv := natstest.Start(t)

	noop := packtrail.InvokerFunc(func(_ context.Context, _ packtrail.Request) (packtrail.Result, error) {
		return packtrail.Result{Status: packtrail.StatusOK}, nil
	})

	flow := packtrail.WithFlow([]byte(ghostKindFlow))

	if _, err := packtrail.New(srv.NC, flow,
		packtrail.WithInvoker("ghost", noop),
		packtrail.WithInvoker("ghost", noop)); err == nil ||
		!strings.Contains(err.Error(), "registered twice") {
		t.Errorf("duplicate sync kind: err = %v, want rejection", err)
	}

	if _, err := packtrail.New(srv.NC, flow,
		packtrail.WithAsyncInvoker("ghost", noop),
		packtrail.WithAsyncInvoker("ghost", noop)); err == nil ||
		!strings.Contains(err.Error(), "registered twice") {
		t.Errorf("duplicate async kind: err = %v, want rejection", err)
	}

	if _, err := packtrail.New(srv.NC, flow,
		packtrail.WithInvoker("ghost", noop),
		packtrail.WithAsyncInvoker("ghost", noop)); err == nil ||
		!strings.Contains(err.Error(), "both WithInvoker and WithAsyncInvoker") {
		t.Errorf("sync+async kind: err = %v, want rejection", err)
	}

	if _, err := packtrail.New(srv.NC, flow, packtrail.WithInvoker("ghost", noop),
		packtrail.WithAsyncInvoker(packtrail.NATSTaskKind, noop)); err == nil ||
		!strings.Contains(err.Error(), "built-in") {
		t.Errorf("async nats-task kind: err = %v, want rejection", err)
	}
}

func TestNewRejectsNilInvokers(t *testing.T) {
	srv := natstest.Start(t)
	flow := packtrail.WithFlow([]byte(ghostKindFlow))

	if _, err := packtrail.New(srv.NC, flow, packtrail.WithInvoker("ghost", nil)); err == nil ||
		!strings.Contains(err.Error(), "nil Invoker") {
		t.Fatalf("New(WithInvoker nil) err = %v, want nil-invoker rejection", err)
	}

	if _, err := packtrail.New(srv.NC, flow, packtrail.WithAsyncInvoker("ghost", nil)); err == nil ||
		!strings.Contains(err.Error(), "nil Invoker") {
		t.Fatalf("New(WithAsyncInvoker nil) err = %v, want nil-invoker rejection", err)
	}
}

func TestWithInvokerOverridesBuiltinNATSTask(t *testing.T) {
	srv := natstest.Start(t)

	custom := packtrail.InvokerFunc(func(_ context.Context, req packtrail.Request) (packtrail.Result, error) {
		return packtrail.Result{Status: packtrail.StatusOK, Payload: req.Payload}, nil
	})

	s, err := packtrail.New(srv.NC,
		packtrail.WithFlow([]byte(`
version: "1.0"
name: override-nats-task
nodes:
  - {id: a, type: task, invoker: nats-task, target: ignored}
`)),
		packtrail.WithInvoker(packtrail.NATSTaskKind, custom),
	)
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = s.Run(ctx) }()

	id, err := s.Start(ctx, "override-nats-task", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	if ok := poll(t, 10*time.Second, func() bool {
		ex, getErr := s.Get(ctx, id)
		return getErr == nil && ex.Status == packtrail.ExecCompleted
	}); !ok {
		t.Fatal("execution did not complete through overridden nats-task invoker")
	}
}
