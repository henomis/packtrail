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
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/event"
	"github.com/henomis/packtrail/worker"
)

// quoteFlow asks three carriers in parallel and books as soon as the join
// policy is met: the remaining carriers are no longer wanted.
const quoteFlow = `
name: quote-%s
nodes:
  - {id: ask, type: fanout, branches: [carrier-a, carrier-b, carrier-c], next: pick}
  - {id: carrier-a, type: task, kind: carrier}
  - {id: carrier-b, type: task, kind: carrier}
  - {id: carrier-c, type: task, kind: carrier}
  - {id: pick, type: join, policy: "%s", next: book}
  - {id: book, type: task, kind: book}
`

// quoteRequest says in which order carriers answer, how many answers the
// join needs, and which loser ignores cancellation and answers late anyway.
type quoteRequest struct {
	Order    []string `json:"order"`
	Need     int      `json:"need"`
	Stubborn string   `json:"stubborn"`
}

// carrierLog records how losing carriers ended.
type carrierLog struct {
	mu       sync.Mutex
	stopped  map[string]error // exec/node -> cancel cause
	answered map[string]bool  // exec/node of stubborn losers that answered late
}

func (l *carrierLog) record(key string, cause error, late bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if late {
		l.answered[key] = true
	} else {
		l.stopped[key] = cause
	}
}

// get returns whether key was stopped (and its cause) or answered late.
func (l *carrierLog) get(key string) (stopped, late bool, cause error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	cause, stopped = l.stopped[key]

	return stopped, l.answered[key], cause
}

// TestFirstResponderWins runs quote races with join policies any and
// quorum:2: the execution books with exactly the winning quotes, the losers
// still running are stopped with ErrCancelled, and a loser that answers late
// anyway changes nothing.
func TestFirstResponderWins(t *testing.T) {
	cl := newCluster(t, []string{fmt.Sprintf(quoteFlow, "any", "any"), fmt.Sprintf(quoteFlow, "quorum", "quorum:2")})

	log := &carrierLog{stopped: map[string]error{}, answered: map[string]bool{}}

	cl.worker("carrier", func(ctx context.Context, j *worker.Job) (*worker.Result, error) {
		var r quoteRequest
		if err := j.Input(&r); err != nil {
			return nil, worker.Permanent(err)
		}

		rank := slices.Index(r.Order, j.Node)
		key := j.ExecID + "/" + j.Node

		if rank < r.Need {
			time.Sleep(time.Duration(rank+1) * 100 * time.Millisecond)

			return &worker.Result{Output: map[string]any{"price": 100 + rank}}, nil
		}

		if j.Node == r.Stubborn {
			time.Sleep(time.Second) // ignores ctx: answers after the race is over

			log.record(key, nil, true)

			return &worker.Result{Output: map[string]any{"price": 1}}, nil
		}

		<-ctx.Done()
		log.record(key, context.Cause(ctx), false)

		return nil, ctx.Err()
	}, worker.WithConcurrency(32))

	cl.worker("book", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		var quotes []string

		for node := range j.Context.Results {
			if strings.HasPrefix(node, "carrier-") {
				quotes = append(quotes, node)
			}
		}

		sort.Strings(quotes)

		return &worker.Result{Output: map[string]any{"quotes": quotes}}, nil
	})

	type race struct {
		id string
		r  quoteRequest
	}

	orders := [][]string{
		{"carrier-a", "carrier-b", "carrier-c"},
		{"carrier-c", "carrier-a", "carrier-b"},
		{"carrier-b", "carrier-c", "carrier-a"},
	}

	races := make([]race, 0, 2*len(orders)) //nolint:mnd // two policies per order.

	for _, order := range orders {
		for flow, need := range map[string]int{"quote-any": 1, "quote-quorum": 2} {
			r := quoteRequest{Order: order, Need: need, Stubborn: order[2]}
			races = append(races, race{id: cl.start(flow, r), r: r})
		}
	}

	for _, rc := range races {
		st := cl.wait(rc.id)
		f := cl.check(rc.id)

		winners := slices.Sorted(slices.Values(rc.r.Order[:rc.r.Need]))
		losers := rc.r.Order[rc.r.Need:]

		var booked struct{ Quotes []string }
		if st.Status != packtrail.StatusCompleted || st.Result("book", &booked) != nil ||
			!slices.Equal(booked.Quotes, winners) {
			t.Fatalf("%s %+v: %s booked %v, want %v", rc.id, rc.r, st.Status, booked.Quotes, winners)
		}

		for _, l := range losers {
			if st.Results[l] != nil || f.completed[l] != 0 {
				t.Fatalf("%s: loser %s has a result", rc.id, l)
			}
		}

		if f.count[event.NodeCancelled] != len(losers) {
			t.Fatalf("%s: %d branches cancelled, want %d", rc.id, f.count[event.NodeCancelled], len(losers))
		}

		// Running losers are stopped; the stubborn one answers late.
		for _, l := range losers {
			key := rc.id + "/" + l

			cl.eventually("loser "+key+" ended", func() bool {
				stopped, late, _ := log.get(key)

				return stopped || late
			})

			stopped, _, cause := log.get(key)
			if l != rc.r.Stubborn && (!stopped || !errors.Is(cause, worker.ErrCancelled)) {
				t.Fatalf("%s stopped by %v, want ErrCancelled", key, cause)
			}
		}
	}

	// The late answers arrived after every execution ended: nothing changed.
	time.Sleep(500 * time.Millisecond)

	for _, rc := range races {
		if f := cl.check(rc.id); f.completed[rc.r.Stubborn] != 0 {
			t.Fatalf("%s: the late answer of %s was recorded", rc.id, rc.r.Stubborn)
		}
	}
}

// campaignFlow runs a mailing as a child that is abandoned when the campaign
// closes (it must still go out); auditFlow runs one that is cancelled with
// its parent (the default). Fan-out branches must be tasks, so each parent
// runs one child.
const campaignFlow = `
name: campaign
nodes:
  - {id: mail, type: subflow, flow: mailing, input: input, on_parent_close: abandon}
`

const auditFlow = `
name: audit
nodes:
  - {id: check, type: subflow, flow: mailing, input: input}
`

const mailingFlow = `
name: mailing
nodes:
  - {id: hold, type: await, signal: send, timeout: 1h, next: send}
  - {id: send, type: task, kind: mailer}
`

// childOf waits until parent runs its child and returns the child id.
func (cl *cluster) childOf(parent string) string {
	cl.t.Helper()

	st := cl.waitUntil(parent, "running its child", func(st *packtrail.State) bool { return len(st.Children) == 1 })
	for _, c := range st.Children {
		return c.ChildID
	}

	return ""
}

// TestAbandonedChildOutlivesParent cancels two parents while their children
// wait: the child with on_parent_close: cancel is cancelled with its parent,
// the abandoned one carries on and completes later, and its completion does
// not touch its cancelled parent.
func TestAbandonedChildOutlivesParent(t *testing.T) {
	cl := newCluster(t, []string{campaignFlow, auditFlow, mailingFlow})

	cl.worker("mailer", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		return &worker.Result{Output: map[string]any{"sent": j.ExecID}}, nil
	})

	ends, err := cl.c.WatchTerminal(cl.ctx, 0)
	if err != nil {
		t.Fatal(err)
	}

	campaign := cl.start("campaign", map[string]any{"list": "spring"})
	audit := cl.start("audit", map[string]any{"list": "spring"})
	mail, check := cl.childOf(campaign), cl.childOf(audit)

	cl.parkedAt(mail, "hold")
	cl.parkedAt(check, "hold")

	for _, p := range []string{campaign, audit} {
		if err = cl.c.Cancel(cl.ctx, p, "campaign withdrawn"); err != nil {
			t.Fatal(err)
		}
	}

	for _, x := range []string{campaign, audit, check} {
		if st := cl.wait(x); st.Status != packtrail.StatusCancelled {
			t.Fatalf("%s: %s, want cancelled", x, st.Status)
		}
	}

	// The abandoned mailing still waits, and can still be sent.
	st, err := cl.c.Get(cl.ctx, mail)
	if err != nil || st.Status != packtrail.StatusWaiting || st.Parent == nil || st.Parent.ExecID != campaign {
		t.Fatalf("abandoned child: %+v %v", st, err)
	}

	if err = cl.c.Signal(cl.ctx, mail, "send", nil); err != nil {
		t.Fatal(err)
	}

	if st = cl.wait(mail); st.Status != packtrail.StatusCompleted {
		t.Fatalf("abandoned child: %s (%s)", st.Status, st.Error)
	}

	for _, x := range []string{campaign, audit, check, mail} {
		cl.check(x)
	}

	if st = cl.wait(campaign); st.Status != packtrail.StatusCancelled {
		t.Fatalf("parent changed after its abandoned child ended: %s", st.Status)
	}

	seen := map[string]packtrail.Status{}
	for len(seen) < 4 {
		select {
		case e := <-ends:
			seen[e.ExecID] = e.Status
		case <-time.After(20 * time.Second):
			t.Fatalf("WatchTerminal reported %v", seen)
		}
	}

	want := map[string]packtrail.Status{
		campaign: packtrail.StatusCancelled, audit: packtrail.StatusCancelled,
		check: packtrail.StatusCancelled, mail: packtrail.StatusCompleted,
	}
	if !maps.Equal(seen, want) {
		t.Fatalf("WatchTerminal reported %v, want %v", seen, want)
	}
}
