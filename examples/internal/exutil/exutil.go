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

// Package exutil holds the few lines every example repeats.
package exutil

import (
	"context"
	"log"
	"sync"

	"github.com/nats-io/nats.go"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/worker"
)

// Group runs an engine and workers and stops them together: Stop cancels
// them and waits until they drained, so the connection can be closed after.
type Group struct {
	ctx    context.Context //nolint:containedctx // lifetime of the example.
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewGroup returns a group whose members run until Stop (or ctx ends).
func NewGroup(ctx context.Context) *Group {
	g := &Group{}
	g.ctx, g.cancel = context.WithCancel(ctx)

	return g
}

// Stop cancels every member and waits for them.
func (g *Group) Stop() {
	g.cancel()
	g.wg.Wait()
}

// Engine creates and runs an engine, and returns once it is ready: the
// namespace is provisioned and its consumers (cron and triggers included)
// are pulling.
func (g *Group) Engine(nc *nats.Conn, opts ...packtrail.Option) *packtrail.Engine {
	eng, err := packtrail.New(nc, opts...)
	if err != nil {
		log.Fatal(err)
	}

	done := make(chan error, 1)

	g.wg.Go(func() {
		err := eng.Run(g.ctx)
		if err != nil {
			log.Print(err)
		}

		done <- err
	})

	select {
	case <-eng.Ready():
	case err := <-done:
		log.Fatalf("engine stopped before it was ready: %v", err)
	case <-g.ctx.Done():
		log.Fatal("engine never ready")
	}

	return eng
}

// Serve runs a worker for kind.
func (g *Group) Serve(nc *nats.Conn, ns, kind string, h worker.Handler, opts ...worker.Option) {
	w, err := worker.New(nc, kind, h, append([]worker.Option{worker.WithNamespace(ns)}, opts...)...)
	if err != nil {
		log.Fatal(err)
	}

	g.wg.Go(func() {
		if err := w.Run(g.ctx); err != nil {
			log.Print(err)
		}
	})
}

// Run starts flow and waits for the final state.
func Run(ctx context.Context, c *packtrail.Client, flow string, input any) *packtrail.State {
	id, err := c.Start(ctx, flow, input)
	if err != nil {
		log.Fatal(err)
	}

	st, err := c.Wait(ctx, id)
	if err != nil {
		log.Fatal(err)
	}

	return st
}
