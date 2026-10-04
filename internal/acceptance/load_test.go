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
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/worker"
)

// TestM4Load runs many executions across 3 engines × 16 partitions while
// engines are killed and replaced in rotation. PT_LOAD sets the number of
// executions (default 300).
func TestM4Load(t *testing.T) {
	if testing.Short() {
		t.Skip("load test")
	}

	n := 300
	if v, err := strconv.Atoi(os.Getenv("PT_LOAD")); err == nil && v > 0 {
		n = v
	}

	e := NewEnv(t, []string{m1Linear, fmt.Sprintf(m1Fan, "all", "all")}, packtrail.WithPartitions(16))
	e.StartEngine()
	e.StartEngine()
	e.Worker("echo", Echo, worker.WithConcurrency(64))
	e.Worker("branch", branchWorker(nil, nil), worker.WithConcurrency(64))

	stop := make(chan struct{})

	var wg sync.WaitGroup

	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(700 * time.Millisecond):
				e.RestartEngine()
				e.StartEngine()
				e.StartEngine()
			}
		}
	})

	start := time.Now()
	ids := make([]string, 0, n)

	for i := range n {
		f := "linear"
		if i%3 == 0 {
			f = "fan-all"
		}

		ids = append(ids, e.Start(f, map[string]any{"i": i}))
	}

	for _, id := range ids {
		e.Completed(id)
	}

	close(stop)
	wg.Wait()

	elapsed := time.Since(start)
	t.Logf("load: %d executions in %v (%.0f exec/s) with rotating engine kills", n, elapsed,
		float64(n)/elapsed.Seconds())
}

// TestM4ManyWaiting parks many executions on an await and wakes them all:
// waiting costs no process memory (durable timers, no in-memory state).
// PT_WAITING sets the count (default 500).
func TestM4ManyWaiting(t *testing.T) {
	if testing.Short() {
		t.Skip("load test")
	}

	n := 500
	if v, err := strconv.Atoi(os.Getenv("PT_WAITING")); err == nil && v > 0 {
		n = v
	}

	e := NewEnv(t, []string{`
name: parked
nodes:
  - {id: w, type: await, signal: wake, timeout: 24h, next: t}
  - {id: t, type: task, kind: echo}
`}, packtrail.WithPartitions(16))
	e.Worker("echo", Echo, worker.WithConcurrency(64))

	start := time.Now()
	ids := make([]string, n)

	for i := range ids {
		ids[i] = e.Start("parked", nil)
	}

	e.Eventually(func() bool {
		l, err := e.Client.List(e.Ctx, packtrail.ListFilter{Status: packtrail.StatusWaiting, Limit: n + 1})

		return err == nil && len(l) == n
	}, func() string { return "not all executions parked" })

	parked := time.Since(start)

	// Restart everything while they wait: nothing is lost.
	e.RestartEngine()

	for _, id := range ids {
		if err := e.Client.Signal(e.Ctx, id, "wake", nil); err != nil {
			t.Fatal(err)
		}
	}

	for _, id := range ids {
		e.Completed(id)
	}

	t.Logf("waiting: %d parked in %v, all woken and completed in %v", n, parked, time.Since(start))
}
