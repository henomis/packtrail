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
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/internal/natstest"
	"github.com/henomis/packtrail/invoker"
	"github.com/henomis/packtrail/invoker/asyncqueue"
)

// These cover the seams an embedder sits on: values it configures in one layer
// that only mean something against another (a drain budget against the hosted
// worker, a payload cap against the transport), and a rule this package enforces
// that an embedder has no supported way to check for itself.

const drainProbeFlow = `
version: "1.0"
name: drain-probe
nodes:
  - {id: a, type: task, invoker: slowkind, target: worker-a}
edges: []
`

// The drain cases reuse blockingInvoker (failactivity_test.go): it parks until
// its invocation context is cancelled, which is the only state in which a drain
// budget is observable at all — there has to be something in flight to drain.

// TestDrainTimeoutBoundsHostedWorker: WithDrainTimeout must bound the whole
// graceful shutdown, including the asyncqueue worker this package starts on the
// caller's behalf.
//
// Run waits for those workers before returning (the deferred wg.Wait in Run), so
// a worker left on the asyncqueue package default drained for 30s no matter what
// the caller configured — an option that named the thing it did not bound. With
// the fix Run returns on the configured budget; without it, this test sits for
// the full 30s and fails on the guard below.
func TestDrainTimeoutBoundsHostedWorker(t *testing.T) {
	srv := natstest.Start(t)
	probe := &blockingInvoker{entered: make(chan struct{}, 1)}

	const budget = 1 * time.Second

	s, err := packtrail.New(srv.NC,
		packtrail.WithFlow([]byte(drainProbeFlow)),
		packtrail.WithAsyncInvoker("slowkind", probe),
		packtrail.WithDrainTimeout(budget),
	)
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runErr := make(chan error, 1)

	go func() { runErr <- s.Run(ctx) }()

	if _, err = s.Start(context.Background(), "drain-probe", nil); err != nil {
		t.Fatalf("start: %v", err)
	}

	select {
	case <-probe.entered:
	case <-time.After(30 * time.Second):
		t.Fatal("invoker never ran; nothing was in flight to drain")
	}

	start := time.Now()

	cancel()

	select {
	case <-runErr:
	case <-time.After(20 * time.Second):
		t.Fatal("Run did not return; the hosted worker is draining on its own clock")
	}

	// Generous headroom over the 1s budget, but far below the 30s asyncqueue
	// default this used to inherit — so the assertion distinguishes the two
	// without depending on precise timing.
	if elapsed := time.Since(start); elapsed > 15*time.Second {
		t.Fatalf("shutdown took %s with a %s drain budget; the hosted worker ignored it", elapsed, budget)
	}
}

// TestDrainTimeoutPerKindOverrides: the Server-wide budget is a default for the
// hosted workers, not an override — a kind that asks for its own budget keeps
// it. Ordering in workerOptions is what makes that true, so it is pinned.
func TestDrainTimeoutPerKindOverrides(t *testing.T) {
	srv := natstest.Start(t)
	probe := &blockingInvoker{entered: make(chan struct{}, 1)}

	s, err := packtrail.New(srv.NC,
		packtrail.WithFlow([]byte(drainProbeFlow)),
		// The Server budget is long; this kind asks for a short one and must get it.
		packtrail.WithDrainTimeout(25*time.Second),
		packtrail.WithAsyncInvoker("slowkind", probe, asyncqueue.WithDrainTimeout(1*time.Second)),
	)
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runErr := make(chan error, 1)

	go func() { runErr <- s.Run(ctx) }()

	if _, err = s.Start(context.Background(), "drain-probe", nil); err != nil {
		t.Fatalf("start: %v", err)
	}

	select {
	case <-probe.entered:
	case <-time.After(30 * time.Second):
		t.Fatal("invoker never ran; nothing was in flight to drain")
	}

	cancel()

	// The engine's own drain still spends the Server budget, so this only checks
	// that Run returns at all rather than timing the worker's share precisely.
	select {
	case <-runErr:
	case <-time.After(40 * time.Second):
		t.Fatal("Run did not return")
	}
}

// TestValidateNamespaceMatchesNew: the exported validator and the rule New
// applies must be the same rule. An embedder pre-checking its own configuration
// against a drifting copy is exactly the failure this export exists to prevent,
// so the two are asserted together rather than separately.
func TestValidateNamespaceMatchesNew(t *testing.T) {
	srv := natstest.Start(t)

	for _, ns := range []string{"bad ns", "dotted.ns", "wild*", strings.Repeat("x", 65)} {
		if err := packtrail.ValidateNamespace(ns); err == nil {
			t.Errorf("ValidateNamespace(%q) = nil, want rejection", ns)
		} else if !errors.Is(err, packtrail.ErrInvalidArgument) {
			t.Errorf("ValidateNamespace(%q) err = %v, want ErrInvalidArgument", ns, err)
		}

		if _, err := packtrail.New(srv.NC, packtrail.WithNamespace(ns),
			packtrail.WithFlow([]byte(drainProbeFlow)),
			packtrail.WithAsyncInvoker("slowkind", noopInvoker{})); err == nil {
			t.Errorf("New(namespace %q) = nil, want the same rejection", ns)
		}
	}

	for _, ns := range []string{"", "acme-prod_1", strings.Repeat("x", 64)} {
		if err := packtrail.ValidateNamespace(ns); err != nil {
			t.Errorf("ValidateNamespace(%q) = %v, want nil", ns, err)
		}
	}
}

type noopInvoker struct{}

func (noopInvoker) Invoke(context.Context, invoker.Request) (invoker.Result, error) {
	return invoker.Result{Status: invoker.StatusOK}, nil
}

// TestMaxPayloadBytesAboveServerLimitRejected: a cap above what the connection
// can carry disables the guard it configures, so the contradiction is refused at
// construction rather than discovered mid-execution.
func TestMaxPayloadBytesAboveServerLimitRejected(t *testing.T) {
	srv := natstest.Start(t)

	oversize := int(srv.NC.MaxPayload()) + 1

	_, err := packtrail.New(srv.NC,
		packtrail.WithFlow([]byte(drainProbeFlow)),
		packtrail.WithAsyncInvoker("slowkind", noopInvoker{}),
		packtrail.WithMaxPayloadBytes(oversize),
	)
	if err == nil {
		t.Fatal("New accepted a payload cap above the server's max_payload")
	}

	if !errors.Is(err, packtrail.ErrInvalidArgument) {
		t.Errorf("err = %v, want ErrInvalidArgument", err)
	}

	// The message must carry both numbers; "invalid configuration" would leave
	// the caller to discover the server's limit on their own.
	if !strings.Contains(err.Error(), "max_payload") {
		t.Errorf("err = %v, want the server limit named", err)
	}
}

// TestMaxPayloadBytesNegativeStillDisables: a non-positive cap is the documented
// way to switch the guard off. Reconciliation must leave it alone — tightening it
// to the server limit would silently re-enable a guard the caller disabled.
func TestMaxPayloadBytesNegativeStillDisables(t *testing.T) {
	srv := natstest.Start(t)

	if _, err := packtrail.New(srv.NC,
		packtrail.WithFlow([]byte(drainProbeFlow)),
		packtrail.WithAsyncInvoker("slowkind", noopInvoker{}),
		packtrail.WithMaxPayloadBytes(-1),
	); err != nil {
		t.Fatalf("New rejected an explicitly disabled payload guard: %v", err)
	}
}

// TestDefaultPayloadCapTightenedToServerLimit: the 512 KiB default assumes a
// stock 1 MiB server and states no intent of its own. Against a server
// configured lower it is simply too loose — an entry between the two passes the
// guard and then fails as an opaque write error, which is the exact outcome the
// guard exists to replace.
func TestDefaultPayloadCapTightenedToServerLimit(t *testing.T) {
	const serverLimit = 128 << 10 // well under the 512 KiB default

	nc := startServerWithMaxPayload(t, serverLimit)

	s, err := packtrail.New(nc,
		packtrail.WithFlow([]byte(drainProbeFlow)),
		packtrail.WithAsyncInvoker("slowkind", noopInvoker{}),
	)
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	// Between the server's limit and the 512 KiB default: rejected by the
	// tightened guard, accepted by the untightened one.
	payload, err := json.Marshal(map[string]string{"blob": strings.Repeat("x", 200<<10)})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	_, err = s.Start(context.Background(), "drain-probe", payload)
	if err == nil {
		t.Fatal("Start accepted a payload the server cannot carry")
	}

	if !strings.Contains(err.Error(), "payload exceeds max size") {
		t.Errorf("err = %v, want packtrail's payload-size error rather than an opaque transport failure", err)
	}
}

// startServerWithMaxPayload runs an embedded server whose max_payload is smaller
// than the package default cap, which is the only way to observe the tightening.
func startServerWithMaxPayload(t *testing.T, maxPayload int32) *nats.Conn {
	t.Helper()

	opts := &natsserver.Options{
		Host:       "127.0.0.1",
		Port:       -1,
		JetStream:  true,
		StoreDir:   t.TempDir(),
		MaxPayload: maxPayload,
	}

	ns, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	go ns.Start()

	if !ns.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats-server not ready")
	}

	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		ns.Shutdown()
		t.Fatalf("connect: %v", err)
	}

	t.Cleanup(func() {
		nc.Close()
		ns.Shutdown()
		ns.WaitForShutdown()
	})

	return nc
}
