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
	"github.com/henomis/packtrail/invoker"
	"github.com/henomis/packtrail/invoker/asyncqueue"
)

// asyncNoTimeoutFlow declares no per-node timeout, which is the common case: the
// author expects the invoker's ceiling to govern.
const asyncNoTimeoutFlow = `
version: "1.0"
name: slow
nodes:
  - {id: a, type: task, invoker: slowkind, target: worker-a}
edges: []
`

const asyncLongTimeoutFlow = `
version: "1.0"
name: slow
nodes:
  - {id: a, type: task, invoker: slowkind, target: worker-a, timeout: 1h}
edges: []
`

// deadlineProbe reports the deadline its invocation context carries, so a test
// can see the budget the node actually ran under.
type deadlineProbe struct{ seen chan time.Duration }

func (d *deadlineProbe) Invoke(ctx context.Context, _ invoker.Request) (invoker.Result, error) {
	budget := time.Duration(0)
	if dl, ok := ctx.Deadline(); ok {
		budget = time.Until(dl)
	}

	select {
	case d.seen <- budget:
	default:
	}

	return invoker.Result{Status: invoker.StatusOK}, nil
}

// TestAsyncNodeRunsAtActivityTimeout: a node that declares no timeout must run at
// its async invoker's activity timeout, not at the engine's WithDefaultTimeout.
//
// The engine substituted DefaultTimeout (30s) before dispatch, stamping it on
// the job as the node's own budget, and the worker can only tighten a deadline
// it is handed. WithActivityTimeout was therefore unreachable for every node
// without an explicit timeout: an agent call configured for 30 minutes ran for
// 30 seconds, and neither setting said so.
func TestAsyncNodeRunsAtActivityTimeout(t *testing.T) {
	srv := natstest.Start(t)

	const (
		activity      = 20 * time.Minute
		engineDefault = 30 * time.Second
	)

	probe := &deadlineProbe{seen: make(chan time.Duration, 1)}

	s, err := packtrail.New(srv.NC,
		packtrail.WithNamespace("tmo"),
		packtrail.WithFlow([]byte(asyncNoTimeoutFlow)),
		packtrail.WithDefaultTimeout(engineDefault),
		packtrail.WithAsyncInvoker("slowkind", probe, asyncqueue.WithActivityTimeout(activity)),
	)
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	go func() { _ = s.Run(ctx) }()

	if _, err = s.Start(ctx, "slow", nil); err != nil {
		t.Fatalf("start: %v", err)
	}

	select {
	case got := <-probe.seen:
		// Allow for queue latency; what matters is which of the two numbers it is.
		if got <= engineDefault {
			t.Fatalf("async node ran with a %s budget, want ~%s: the engine's default timeout "+
				"is still being stamped on the dispatch", got, activity)
		}
	case <-ctx.Done():
		t.Fatal("async node was never invoked")
	}
}

// TestNewRejectsNodeTimeoutAboveCeiling: a node asking for longer than its
// invoker's ceiling is a contradiction, and must fail construction rather than
// being capped at run time with only a log line.
func TestNewRejectsNodeTimeoutAboveCeiling(t *testing.T) {
	srv := natstest.Start(t)

	probe := &deadlineProbe{seen: make(chan time.Duration, 1)}

	_, err := packtrail.New(srv.NC,
		packtrail.WithNamespace("tmo2"),
		packtrail.WithFlow([]byte(asyncLongTimeoutFlow)),
		packtrail.WithAsyncInvoker("slowkind", probe, asyncqueue.WithActivityTimeout(5*time.Minute)),
	)
	if err == nil {
		t.Fatal("New accepted a node timeout above the invoker's activity timeout")
	}

	for _, want := range []string{"1h0m0s", "5m0s", "slowkind"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q — an operator needs both numbers to fix it", err, want)
		}
	}
}

// TestNewAllowsNodeTimeoutWithinCeiling guards the other side: a node tightening
// the ceiling is the documented way to use it and must still construct.
func TestNewAllowsNodeTimeoutWithinCeiling(t *testing.T) {
	srv := natstest.Start(t)

	probe := &deadlineProbe{seen: make(chan time.Duration, 1)}

	if _, err := packtrail.New(srv.NC,
		packtrail.WithNamespace("tmo3"),
		packtrail.WithFlow([]byte(asyncLongTimeoutFlow)),
		packtrail.WithAsyncInvoker("slowkind", probe, asyncqueue.WithActivityTimeout(2*time.Hour)),
	); err != nil {
		t.Fatalf("New rejected a node timeout inside the ceiling: %v", err)
	}
}

func TestActivityTimeoutReportsDefault(t *testing.T) {
	if got := asyncqueue.ActivityTimeout(); got != 5*time.Minute {
		t.Errorf("ActivityTimeout() = %s, want the 5m default", got)
	}

	if got := asyncqueue.ActivityTimeout(asyncqueue.WithActivityTimeout(time.Hour)); got != time.Hour {
		t.Errorf("ActivityTimeout(1h) = %s, want 1h", got)
	}
}
