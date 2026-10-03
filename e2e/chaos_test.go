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

package e2e_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/worker"
)

// slowly delays a handler a little, so crashes catch jobs in flight.
func slowly(h worker.Handler) worker.Handler {
	return func(ctx context.Context, j *worker.Job) (*worker.Result, error) {
		select {
		case <-time.After(time.Duration(rand.IntN(40)) * time.Millisecond): //nolint:gosec // test jitter.
		case <-ctx.Done():
			return nil, ctx.Err()
		}

		return h(ctx, j)
	}
}

// fleet keeps two worker processes per kind and replaces crashed ones.
type fleet struct {
	cl       *cluster
	handlers map[string]worker.Handler

	mu    sync.Mutex
	procs map[string][]*proc
}

func newFleet(cl *cluster, handlers ...map[string]worker.Handler) *fleet {
	fl := &fleet{cl: cl, handlers: map[string]worker.Handler{}, procs: map[string][]*proc{}}

	for _, hs := range handlers {
		for k, h := range hs {
			fl.handlers[k] = slowly(h)
		}
	}

	for k := range fl.handlers {
		fl.add(k)
		fl.add(k)
	}

	return fl
}

func (fl *fleet) add(kind string) {
	p := fl.cl.worker(kind, fl.handlers[kind], worker.WithConcurrency(8))

	fl.mu.Lock()
	fl.procs[kind] = append(fl.procs[kind], p)
	fl.mu.Unlock()
}

// crashOne kills a worker process of a random kind mid-work and starts a
// replacement.
func (fl *fleet) crashOne() string {
	fl.mu.Lock()

	kinds := keys(fl.procs)
	kind := kinds[rand.IntN(len(kinds))] //nolint:gosec // test choice.
	ps := fl.procs[kind]
	victim := ps[0]
	fl.procs[kind] = ps[1:]

	fl.mu.Unlock()

	victim.crash()
	fl.add(kind)

	return kind
}

// TestChaos runs many orders and agent loops while engines crash and are
// replaced, NATS restarts, and worker processes crash mid-job. None of that
// may change an outcome: every execution must end exactly as it would have
// alone, satisfy every invariant, and leave no dead letter or quarantine.
func TestChaos(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos run skipped in -short mode")
	}

	cl := newCluster(t, []string{orderFlow, invoiceFlow, fmt.Sprintf(agentFlow, 1_000_000)},
		packtrail.WithHistoryLimit(80))

	engines := []*proc{cl.engines[0], cl.startEngine(), cl.startEngine()}
	fl := newFleet(cl, orderHandlers(), agentHandlers())

	// The workload: three rounds of every order variant, and agent loops.
	type run struct {
		oc orderCase
		id string
	}

	var runs []run

	for round := range 3 {
		for _, oc := range orderCases(fmt.Sprintf("chaos%d-%d-", time.Now().UnixNano(), round)) {
			runs = append(runs, run{oc: oc, id: cl.runOrder(oc)})
		}
	}

	agents := make([]string, 6)
	for i := range agents {
		agents[i] = cl.start("agent", agentTask{Steps: 80 + i, AskAt: -1})
	}

	// The chaos, while the workload runs.
	chaosDone := make(chan struct{})

	go func() {
		defer close(chaosDone)

		steps := []func() string{
			func() string { return "crash worker " + fl.crashOne() },
			func() string {
				i := rand.IntN(len(engines)) //nolint:gosec // test choice.
				engines[i].crash()
				engines[i] = cl.startEngine()

				return fmt.Sprintf("crash engine %d", i)
			},
			func() string { return "crash worker " + fl.crashOne() },
			func() string { cl.s.Restart(t); return "restart NATS" },
			func() string { return "crash worker " + fl.crashOne() },
			func() string {
				engines[0].stop()
				engines[0] = cl.startEngine()

				return "stop engine 0"
			},
		}

		for _, step := range steps {
			time.Sleep(300 * time.Millisecond)
			t.Log("chaos:", step())
		}
	}()

	<-chaosDone

	for _, r := range runs {
		t.Run("order/"+r.oc.name+"/"+r.id, func(t *testing.T) { cl.with(t).verifyOrder(r.oc, r.id) })
	}

	for i, id := range agents {
		st := cl.wait(id)
		cl.check(id)

		steps := 80 + i
		if st.Status != packtrail.StatusCompleted || st.Visits["plan"] != steps+1 || st.Visits["tool"] != steps {
			t.Fatalf("agent %s: %s visits %v (%s)", id, st.Status, st.Visits, st.Error)
		}

		var notes []string
		if st.Channel("scratch", &notes) != nil || len(notes) != steps {
			t.Fatalf("agent %s: scratch %d entries, want %d", id, len(notes), steps)
		}
	}

	if dl, err := cl.c.DeadLetters(cl.ctx, 100); err != nil || len(dl) != 0 {
		t.Fatalf("dead letters after chaos: %+v %v", dl, err)
	}

	if q, err := cl.c.Quarantined(cl.ctx); err != nil || len(q) != 0 {
		t.Fatalf("quarantined after chaos: %v %v", q, err)
	}
}
