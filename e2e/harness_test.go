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

// Package e2e_test runs realistic, multi-feature workflows end to end through
// the public API only (packtrail, worker, flow), the way an application uses
// packtrail, and checks every execution against the invariants of the event
// model (invariants_test.go) on top of each scenario's expected outcome.
//
// Every process (engine, worker, client) has its own NATS connection, so a
// test can crash one — close its connection under it — as well as stop it.
// Run with: go test ./e2e/ (add -short to skip the chaos runs).
package e2e_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/internal/natstest"
	"github.com/henomis/packtrail/worker"
)

const (
	testTimeout = 3 * time.Minute
	partitions  = 4
	// stallTimeout bounds how long wait waits for one execution.
	stallTimeout = time.Minute
)

// cluster is one namespace served by engine and worker processes.
type cluster struct {
	t   *testing.T
	s   *natstest.Server
	ctx context.Context //nolint:containedctx // test-scoped context.
	// c is a standalone client (its own connection, like an API process).
	c *packtrail.Client

	opts []packtrail.Option

	mu      sync.Mutex
	engines []*proc
	workers []*proc
}

// proc is a running engine or worker process.
type proc struct {
	name   string
	nc     *nats.Conn
	cancel context.CancelFunc
	done   chan struct{}
}

func newCluster(t *testing.T, flows []string, opts ...packtrail.Option) *cluster {
	t.Helper()

	s := natstest.Start(t)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	t.Cleanup(cancel)

	base := make([]packtrail.Option, 0, 4+len(flows)+len(opts)) //nolint:mnd // the four below.
	base = append(base,
		packtrail.WithPartitions(partitions),
		packtrail.WithDrainTimeout(2*time.Second),
		packtrail.WithAckWait(time.Second),
		packtrail.WithPullExpiry(time.Second),
	)

	for _, f := range flows {
		base = append(base, packtrail.WithFlowYAML([]byte(f)))
	}

	cl := &cluster{t: t, s: s, ctx: ctx, opts: append(base, opts...)}
	t.Cleanup(cl.stopAll)

	cl.startEngine()

	c, err := packtrail.NewClient(s.Connect(t))
	if err != nil {
		t.Fatal(err)
	}

	cl.c = c

	return cl
}

// with returns a view of the cluster reporting to t (a subtest). It shares
// the processes but must not start or stop any.
func (cl *cluster) with(t *testing.T) *cluster {
	return &cluster{t: t, s: cl.s, ctx: cl.ctx, c: cl.c, opts: cl.opts}
}

// startEngine starts an engine process and waits until it pulls.
func (cl *cluster) startEngine() *proc {
	cl.t.Helper()

	nc := cl.s.Connect(cl.t)

	eng, err := packtrail.New(nc, cl.opts...)
	if err != nil {
		cl.t.Fatal(err)
	}

	p := cl.spawn("engine", nc, eng.Run)

	select {
	case <-eng.Ready():
	case <-p.done:
		cl.t.Fatal("engine stopped before it was ready")
	case <-cl.ctx.Done():
		cl.t.Fatal("engine never ready")
	}

	cl.mu.Lock()
	cl.engines = append(cl.engines, p)
	cl.mu.Unlock()

	return p
}

// worker starts a worker process for kind and waits until it pulls.
func (cl *cluster) worker(kind string, h worker.Handler, opts ...worker.Option) *proc {
	cl.t.Helper()

	nc := cl.s.Connect(cl.t)

	opts = append([]worker.Option{
		worker.WithDrainTimeout(time.Second), worker.WithAckWait(2 * time.Second),
		worker.WithLiveCheck(500 * time.Millisecond),
	}, opts...)

	w, err := worker.New(nc, kind, h, opts...)
	if err != nil {
		cl.t.Fatal(err)
	}

	p := cl.spawn("worker "+kind, nc, w.Run)

	select {
	case <-w.Ready():
	case <-p.done:
		cl.t.Fatalf("worker %s stopped before it was ready", kind)
	case <-cl.ctx.Done():
		cl.t.Fatalf("worker %s never ready", kind)
	}

	cl.mu.Lock()
	cl.workers = append(cl.workers, p)
	cl.mu.Unlock()

	return p
}

func (cl *cluster) spawn(name string, nc *nats.Conn, run func(context.Context) error) *proc {
	ctx, cancel := context.WithCancel(cl.ctx)
	p := &proc{name: name, nc: nc, cancel: cancel, done: make(chan struct{})}

	go func() {
		defer close(p.done)

		if err := run(ctx); err != nil && ctx.Err() == nil {
			cl.t.Logf("%s stopped: %v", name, err)
		}
	}()

	return p
}

// stop shuts p down gracefully (it drains).
func (p *proc) stop() {
	p.cancel()
	<-p.done
}

// crash kills p: its connection closes under it, nothing is drained or acked.
func (p *proc) crash() {
	p.nc.Close()
	p.cancel()
	<-p.done
}

func (cl *cluster) stopAll() {
	cl.mu.Lock()
	ps := append(append([]*proc{}, cl.workers...), cl.engines...)
	cl.mu.Unlock()

	for _, p := range ps {
		p.cancel()
	}

	for _, p := range ps {
		<-p.done
	}
}

// start starts flowName and returns the execution id.
func (cl *cluster) start(flowName string, input any, opts ...packtrail.StartOption) string {
	cl.t.Helper()

	id, err := cl.c.Start(cl.ctx, flowName, input, opts...)
	if err != nil {
		cl.t.Fatal(err)
	}

	return id
}

// wait waits until id finishes.
func (cl *cluster) wait(id string) *packtrail.State {
	cl.t.Helper()

	// No scenario takes this long: past it, the execution is stuck.
	ctx, cancel := context.WithTimeout(cl.ctx, stallTimeout)
	defer cancel()

	st, err := cl.c.Wait(ctx, id)
	if err != nil {
		cl.t.Fatalf("wait %s: %v%s", id, err, cl.diagnose(id))
	}

	return st
}

// waitStatus polls until id has status want.
func (cl *cluster) waitStatus(id string, want packtrail.Status) *packtrail.State {
	cl.t.Helper()

	for {
		st, err := cl.c.Get(cl.ctx, id)
		if err == nil && st.Status == want {
			return st
		}

		if cl.ctx.Err() != nil {
			cl.t.Fatalf("%s never reached %s: %+v %v", id, want, st, err)
		}

		time.Sleep(20 * time.Millisecond)
	}
}

// eventually polls cond until it holds.
func (cl *cluster) eventually(what string, cond func() bool) {
	cl.t.Helper()

	for !cond() {
		if cl.ctx.Err() != nil {
			cl.t.Fatalf("never: %s", what)
		}

		time.Sleep(20 * time.Millisecond)
	}
}
