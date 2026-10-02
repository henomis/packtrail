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
	"context"
	"os"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/event"
	"github.com/henomis/packtrail/internal/natstest"
	"github.com/henomis/packtrail/worker"
)

// BenchmarkLinearThroughput measures completed 3-step executions per second
// (docs/bench.md). Completions are counted from the events streams by one
// consumer per namespace, so the measure is not bounded by the client.
// Environment: PT_BENCH_PARTITIONS (default 64), PT_BENCH_ENGINES (1 per
// namespace, each owning a share of the partitions), PT_BENCH_WORKERS (1
// process per namespace), PT_BENCH_SLOTS (64 per process), PT_BENCH_CLUSTER
// (n > 1: an n-node cluster with R=n, connections spread over the nodes) and
// PT_BENCH_NAMESPACES (independent deployments, each with its own events
// stream; starts are spread over them).
func BenchmarkLinearThroughput(b *testing.B) {
	e := newBenchEnv(b)

	var count atomic.Int64

	done := make(chan struct{})

	for _, ns := range e.Namespaces {
		js, err := jetstream.New(e.NC)
		if err != nil {
			b.Fatal(err)
		}

		cons, err := js.OrderedConsumer(e.Ctx, ns+"-events", jetstream.OrderedConsumerConfig{
			FilterSubjects: []string{ns + ".ev.>"}, HeadersOnly: true, DeliverPolicy: jetstream.DeliverNewPolicy,
		})
		if err != nil {
			b.Fatal(err)
		}

		cc, err := cons.Consume(func(m jetstream.Msg) {
			if slices.Contains(event.Types(m.Headers()), event.ExecutionCompleted) {
				if count.Add(1) == int64(b.N) {
					close(done)
				}
			}
		})
		if err != nil {
			b.Fatal(err)
		}

		defer cc.Stop()
	}

	b.ResetTimer()

	// Starts are published by several goroutines: one sequential publisher
	// waiting for every ack (an R3 round trip) would bound the measure.
	var (
		wg     sync.WaitGroup
		failed atomic.Value
		next   atomic.Int64
	)

	for range benchStarters {
		wg.Go(func() {
			for i := next.Add(1) - 1; i < int64(b.N); i = next.Add(1) - 1 {
				if _, err := e.Clients[i%int64(len(e.Clients))].Start(e.Ctx, "linear", nil); err != nil {
					failed.Store(err)

					return
				}
			}
		})
	}

	wg.Wait()

	if err, ok := failed.Load().(error); ok {
		b.Fatal(err)
	}

	b.ReportMetric(b.Elapsed().Seconds(), "start-phase-s")

	select {
	case <-done:
	case <-time.After(5 * time.Minute):
		b.Fatalf("only %d of %d completed", count.Load(), b.N)
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "exec/s")
	b.ReportMetric(e.Setup.Seconds(), "setup-s")
}

// BenchmarkStepLatency measures the end-to-end latency of one execution of a
// 3-step flow run alone (start → completed), reported per step.
func BenchmarkStepLatency(b *testing.B) {
	e := newBenchEnv(b)

	b.ResetTimer()

	var total time.Duration

	for range b.N {
		start := time.Now()

		id, err := e.Clients[0].Start(e.Ctx, "linear", nil)
		if err != nil {
			b.Fatal(err)
		}

		if _, err = e.Clients[0].Wait(e.Ctx, id); err != nil {
			b.Fatal(err)
		}

		total += time.Since(start)
	}

	b.ReportMetric(float64(total.Microseconds())/float64(b.N)/3, "µs/step")
}

// benchStarters is the number of goroutines publishing starts.
const benchStarters = 32

type benchEnv struct {
	// Setup is how long deployment and warm-up took: on a cluster it grows
	// with the number of replicated consumers (two per partition).
	Setup      time.Duration
	Ctx        context.Context //nolint:containedctx // benchmark-scoped.
	Clients    []*packtrail.Client
	Namespaces []string
	NC         *nats.Conn
}

func newBenchEnv(b *testing.B) *benchEnv {
	b.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	b.Cleanup(cancel)

	start := time.Now()

	var (
		connect  func(i int) *nats.Conn
		replicas = envInt("PT_BENCH_CLUSTER", 1)
		conns    int
	)

	if replicas > 1 {
		c := natstest.StartCluster(b, replicas)
		connect = func(i int) *nats.Conn { return c.Connect(b, i) }
	} else {
		s := natstest.Start(b)
		connect = func(int) *nats.Conn { return s.Connect(b) }
	}

	next := func() *nats.Conn {
		conns++

		return connect(conns)
	}

	e := &benchEnv{Ctx: ctx, NC: next()}

	for n := range envInt("PT_BENCH_NAMESPACES", 1) {
		ns := "packtrail"
		if n > 0 {
			ns = "bench" + strconv.Itoa(n)
		}

		e.Namespaces = append(e.Namespaces, ns)
		e.Clients = append(e.Clients, startBenchNamespace(ctx, b, ns, replicas, next))
	}

	e.warmUp(b)
	e.Setup = time.Since(start)

	return e
}

// warmUp runs executions over every partition before the timer starts: on a
// cluster each durable consumer is a Raft group, and creating a few hundred of
// them takes seconds that would otherwise be measured as throughput.
func (e *benchEnv) warmUp(b *testing.B) {
	b.Helper()

	var wg sync.WaitGroup

	slots := make(chan struct{}, benchStarters)

	for _, c := range e.Clients {
		for range 4 * envInt("PT_BENCH_PARTITIONS", 64) {
			wg.Go(func() {
				slots <- struct{}{}
				defer func() { <-slots }()

				id, err := c.Start(e.Ctx, "linear", nil)
				if err == nil {
					_, err = c.Wait(e.Ctx, id)
				}

				if err != nil {
					b.Error(err)
				}
			})
		}
	}

	wg.Wait()
}

// startBenchNamespace deploys one namespace: its engines and workers.
func startBenchNamespace(ctx context.Context, b *testing.B, ns string, replicas int,
	next func() *nats.Conn,
) *packtrail.Client {
	b.Helper()

	parts, engines, slots := envInt("PT_BENCH_PARTITIONS", 64), envInt("PT_BENCH_ENGINES", 1), envInt("PT_BENCH_SLOTS", 64)

	var client *packtrail.Client

	for i := range engines {
		opts := []packtrail.Option{
			packtrail.WithNamespace(ns), packtrail.WithFlowYAML([]byte(m1Linear)), packtrail.WithPartitions(parts),
		}

		if replicas > 1 {
			opts = append(opts, packtrail.WithReplicas(replicas))
		}

		if engines > 1 {
			var own []int

			for p := i; p < parts; p += engines {
				own = append(own, p)
			}

			opts = append(opts, packtrail.WithOwnedPartitions(own...))
		}

		en, err := packtrail.New(next(), opts...)
		if err != nil {
			b.Fatal(err)
		}

		if err = en.Init(ctx); err != nil {
			b.Fatal(err)
		}

		go func() { _ = en.Run(ctx) }()

		if i == 0 {
			client = en.Client()
		}
	}

	for range envInt("PT_BENCH_WORKERS", 1) {
		w, err := worker.New(next(), "echo", Echo, worker.WithNamespace(ns), worker.WithConcurrency(slots))
		if err != nil {
			b.Fatal(err)
		}

		go func() { _ = w.Run(ctx) }()
	}

	return client
}

func envInt(k string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(k)); err == nil && v > 0 {
		return v
	}

	return def
}
