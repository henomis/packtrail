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
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/event"
	"github.com/henomis/packtrail/worker"
)

// orderFlow is an order saga: validation with retries, a choice, a human
// approval for large orders (await with timeout), three parallel steps joined
// with compensation on failure, packing every item (map with flaky items and
// retries), an invoice subflow, and a final notification.
const orderFlow = `
name: order
channels:
  log:      {reducer: append}
  total:    {reducer: sum, default: 0}
  reserved: {reducer: merge}
search_attributes: {customer: input.customer}
nodes:
  - id: validate
    type: task
    kind: validate
    retry: {max_attempts: 3, delay: 50ms}
    output_schema: {type: object, required: [ok, amount]}
    next: route
  - id: route
    type: choice
    rules:
      - {when: "results.validate.amount > 1000", to: review}
      - {default: true, to: prepare}
  - {id: review, type: await, signal: approval, timeout: 2s, on_timeout: reject, next: decide}
  - id: decide
    type: choice
    rules:
      - {when: "signals.approval.ok == true", to: prepare}
      - {default: true, to: reject}
  - {id: prepare, type: fanout, branches: [reserve, charge, label], next: settle}
  - {id: reserve, type: task, kind: reserve}
  - {id: charge, type: task, kind: charge, retry: {max_attempts: 2, delay: 50ms}}
  - {id: label, type: task, kind: label}
  - {id: settle, type: join, policy: all, on_failure: compensate, next: pack}
  - id: pack
    type: map
    kind: pack
    over: input.items
    max_parallel: 3
    retry: {max_attempts: 4, delay: 20ms}
    on_failure: compensate
    next: invoice
  - {id: invoice, type: subflow, flow: invoice, input: input, next: notify}
  - {id: notify, type: task, kind: notify}
  - {id: compensate, type: task, kind: compensate}
  - {id: reject, type: task, kind: reject}
`

// invoiceFlow has no channels, so its output (the parent's results.invoice)
// is the result of its last node.
const invoiceFlow = `
name: invoice
nodes:
  - {id: compute, type: task, kind: invoice-compute, next: emit}
  - {id: emit, type: task, kind: invoice-emit}
`

type item struct {
	SKU   string  `json:"sku"`
	Price float64 `json:"price"`
	// Fragile items fail their first two packing attempts.
	Fragile bool `json:"fragile,omitempty"`
	// Broken items never pack.
	Broken bool `json:"broken,omitempty"`
}

type order struct {
	ID       string  `json:"id"`
	Customer string  `json:"customer"`
	Amount   float64 `json:"amount"`
	Items    []item  `json:"items"`
	// Flaky validation fails its first attempt.
	Flaky bool `json:"flaky,omitempty"`
	// Card "declined" fails the charge permanently.
	Card string `json:"card,omitempty"`
	// Gated validation waits until the test opens the order's gate.
	Gated bool `json:"gated,omitempty"`
}

// fixedCards lists orders whose declined card the charge worker accepts
// (the "bug fix" a rerun picks up).
var fixedCards sync.Map

// gates hold back the validation of gated orders, by order id.
var gates sync.Map

func gate(id string) chan struct{} {
	ch, _ := gates.LoadOrStore(id, make(chan struct{}))

	return ch.(chan struct{})
}

// outcome is how an order should end.
type outcome int

const (
	shipped     outcome = iota // notify ran
	rejected                   // reject ran (denied or timed out)
	compensated                // compensate ran
)

func (o order) total() float64 {
	t := 0.0
	for _, it := range o.Items {
		t += it.Price
	}

	return t
}

// orderHandlers serves every kind the order and invoice flows use. Workers
// decide from the order alone, so the expected outcome is exact.
func orderHandlers() map[string]worker.Handler {
	h := map[string]worker.Handler{}

	h["validate"] = func(ctx context.Context, j *worker.Job) (*worker.Result, error) {
		var o order
		if err := j.Input(&o); err != nil {
			return nil, worker.Permanent(err)
		}

		if o.Flaky && j.Attempt == 1 {
			return nil, errors.New("validation service unavailable")
		}

		if o.Gated {
			select {
			case <-gate(o.ID):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		return &worker.Result{
			Output: map[string]any{"ok": true, "amount": o.Amount},
			Writes: map[string]any{"log": "validate"},
		}, nil
	}

	h["reserve"] = func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		var o order
		if err := j.Input(&o); err != nil {
			return nil, worker.Permanent(err)
		}

		held := map[string]any{}
		for _, it := range o.Items {
			held[it.SKU] = true
		}

		return &worker.Result{Output: map[string]any{"held": len(held)}, Writes: map[string]any{
			"reserved": held, "log": "reserve",
		}}, nil
	}

	h["charge"] = func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		var o order
		if err := j.Input(&o); err != nil {
			return nil, worker.Permanent(err)
		}

		if _, fixed := fixedCards.Load(o.ID); o.Card == "declined" && !fixed {
			return nil, worker.Permanent(errors.New("card declined"))
		}

		return &worker.Result{
			Output: map[string]any{"charged": o.Amount}, Writes: map[string]any{"log": "charge"},
			Usage: map[string]float64{"charges": 1},
		}, nil
	}

	h["label"] = func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		return &worker.Result{
			Output: map[string]any{"label": "L-" + j.ExecID}, Writes: map[string]any{"log": "label"},
		}, nil
	}

	h["pack"] = func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		var it item
		if err := j.Item(&it); err != nil {
			return nil, worker.Permanent(err)
		}

		if it.Broken || (it.Fragile && j.Attempt <= 2) {
			return nil, fmt.Errorf("cannot pack %s (attempt %d)", it.SKU, j.Attempt)
		}

		return &worker.Result{
			Output: map[string]any{"sku": it.SKU, "index": *j.Context.Index},
			Writes: map[string]any{"total": it.Price, "log": "pack:" + it.SKU},
		}, nil
	}

	h["invoice-compute"] = func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		var o order
		if err := j.Input(&o); err != nil {
			return nil, worker.Permanent(err)
		}

		return &worker.Result{
			Output: map[string]any{"total": o.total()},
			Usage:  map[string]float64{"invoices": 1},
		}, nil
	}

	h["invoice-emit"] = func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		var (
			o    order
			calc struct{ Total float64 }
		)

		if err := j.Input(&o); err != nil {
			return nil, worker.Permanent(err)
		}

		if err := j.Result("compute", &calc); err != nil {
			return nil, worker.Permanent(err)
		}

		return &worker.Result{Output: map[string]any{"number": "INV-" + o.ID, "total": calc.Total}}, nil
	}

	h["notify"] = func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		var inv struct{ Number string }
		if err := j.Result("invoice", &inv); err != nil {
			return nil, worker.Permanent(err)
		}

		return &worker.Result{Output: map[string]any{"status": "shipped", "invoice": inv.Number}}, nil
	}

	h["compensate"] = func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		failed := j.Context.LastNode
		cause := j.Context.Errors[failed]

		return &worker.Result{
			Output: map[string]any{"status": "compensated", "failed": failed, "reason": cause.Reason, "error": cause.Error},
			Writes: map[string]any{"log": "compensate"},
		}, nil
	}

	h["reject"] = func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		timedOut := j.Context.Signals["approval"] == nil

		return &worker.Result{Output: map[string]any{"status": "rejected", "timed_out": timedOut}}, nil
	}

	return h
}

// orderWorkers starts one worker process per kind of the order flows.
func orderWorkers(cl *cluster, opts ...worker.Option) {
	for kind, h := range orderHandlers() {
		cl.worker(kind, h, opts...)
	}
}

// orderCase is one order and how it must end.
type orderCase struct {
	name string
	o    order
	want outcome
	// approve: "yes", "no" or "" (no signal: the review times out).
	approve string
	// early sends the approval right after start, before the await exists.
	early bool
	// failedAt is the node compensation must name.
	failedAt string
}

func orderCases(prefix string) []orderCase {
	items := []item{{SKU: "A", Price: 10}, {SKU: "B", Price: 20, Fragile: true}, {SKU: "C", Price: 5}, {SKU: "D", Price: 7.5}}

	return []orderCase{
		{name: "small", o: order{ID: prefix + "small", Customer: "ann", Amount: 42, Items: items, Flaky: true}, want: shipped},
		{
			name: "approved-early", o: order{ID: prefix + "big-early", Customer: "bob", Amount: 5000, Items: items[:2], Gated: true},
			want: shipped, approve: "yes", early: true,
		},
		{
			name: "approved-late", o: order{ID: prefix + "big-late", Customer: "bob", Amount: 2500, Items: items[1:]},
			want: shipped, approve: "yes",
		},
		{name: "denied", o: order{ID: prefix + "denied", Customer: "cy", Amount: 9000, Items: items}, want: rejected, approve: "no"},
		{name: "timed-out", o: order{ID: prefix + "timeout", Customer: "cy", Amount: 1001, Items: items}, want: rejected},
		{
			name: "declined", o: order{ID: prefix + "declined", Customer: "dee", Amount: 30, Items: items, Card: "declined"},
			want: compensated, failedAt: "settle",
		},
		{
			name: "broken-item", o: order{ID: prefix + "broken", Customer: "eve", Amount: 50,
				Items: []item{{SKU: "X", Price: 1}, {SKU: "Y", Price: 2, Broken: true}, {SKU: "Z", Price: 3}}},
			want: compensated, failedAt: "pack",
		},
	}
}

// run starts the case; the approval, if any, is sent (twice, with one id)
// when the order reaches its review, or right away when early.
func (cl *cluster) runOrder(oc orderCase) string {
	cl.t.Helper()

	id := cl.start("order", oc.o, packtrail.WithExecutionID(oc.o.ID))

	if oc.approve == "" {
		return id
	}

	send := func() {
		for range 2 {
			err := cl.c.Signal(cl.ctx, id, "approval", map[string]any{"ok": oc.approve == "yes"},
				packtrail.WithSignalID("approval-"+id))
			if err != nil {
				cl.t.Errorf("signal %s: %v", id, err)
			}
		}
	}

	if oc.early {
		// Signal needs the execution to exist; it is still far from its review.
		cl.eventually("order "+id+" exists", func() bool {
			_, err := cl.c.Get(cl.ctx, id)

			return err == nil
		})
		send()
		close(gate(id))

		return id
	}

	go func() {
		for cl.ctx.Err() == nil {
			st, err := cl.c.Get(cl.ctx, id)
			if err == nil && (st.Awaits["review"] != nil || st.Status.Terminal()) {
				break
			}

			time.Sleep(10 * time.Millisecond)
		}

		send()
	}()

	return id
}

// verifyOrder checks the final state of a case against its expectation.
func (cl *cluster) verifyOrder(oc orderCase, id string) {
	cl.t.Helper()

	t := cl.t
	st := cl.wait(id)
	f := cl.check(id)

	if st.Status != packtrail.StatusCompleted {
		t.Fatalf("%s: status %s (%s: %s)", oc.name, st.Status, st.Reason, st.Error)
	}

	// A flow with channels outputs its channels; the outcome is the result
	// of the node it ended on.
	if !jsonEqual(st.Output, mustJSON(st.Channels)) {
		t.Fatalf("%s: output %s, want the channels %v", oc.name, st.Output, st.Channels)
	}

	var out map[string]any
	if err := json.Unmarshal(st.Results[st.LastNode], &out); err != nil {
		t.Fatalf("%s: result of %s: %s", oc.name, st.LastNode, st.Results[st.LastNode])
	}

	var logs []string

	_ = json.Unmarshal(st.Channels["log"], &logs)

	switch oc.want {
	case shipped:
		cl.verifyShipped(oc, st, out, logs, f)
	case rejected:
		if out["status"] != "rejected" || out["timed_out"] != (oc.approve == "") {
			t.Fatalf("%s: output %v", oc.name, out)
		}

		if f.count[event.AwaitTimedOut] != boolInt(oc.approve == "") || st.Results["prepare"] != nil {
			t.Fatalf("%s: timeouts %d, prepare %s", oc.name, f.count[event.AwaitTimedOut], st.Results["prepare"])
		}
	case compensated:
		if out["status"] != "compensated" || out["failed"] != oc.failedAt {
			t.Fatalf("%s: output %v, want compensation of %s", oc.name, out, oc.failedAt)
		}

		if st.Results["notify"] != nil || st.Results["invoice"] != nil {
			t.Fatalf("%s: went on after the failure: %v", oc.name, keys(st.Results))
		}

		if !slices.Contains(logs, "compensate") {
			t.Fatalf("%s: log %v", oc.name, logs)
		}
	}

	// A signal sent twice with one id is consumed once.
	if oc.approve != "" && f.count[event.SignalReceived] != 1 {
		t.Fatalf("%s: %d signals received, want 1 (deduplicated)", oc.name, f.count[event.SignalReceived])
	}

	// An early signal was buffered: received before the await opened.
	if oc.early && indexOf(f, event.SignalReceived) > indexOf(f, event.AwaitStarted) {
		t.Fatalf("%s: the signal arrived after the await opened; the test did not exercise buffering", oc.name)
	}
}

func indexOf(f *facts, t event.Type) int {
	for _, ev := range f.events {
		if ev.Type == t {
			return ev.Index
		}
	}

	return -1
}

func mustJSON(v any) json.RawMessage {
	b, _ := json.Marshal(v) //nolint:errchkjson // test values always encode.

	return b
}

func (cl *cluster) verifyShipped(oc orderCase, st *packtrail.State, out map[string]any, logs []string, f *facts) {
	t := cl.t

	if out["status"] != "shipped" || out["invoice"] != "INV-"+oc.o.ID {
		t.Fatalf("%s: output %v", oc.name, out)
	}

	var total float64
	if err := json.Unmarshal(st.Channels["total"], &total); err != nil || total != oc.o.total() {
		t.Fatalf("%s: total %s, want %g", oc.name, st.Channels["total"], oc.o.total())
	}

	var packed []struct {
		SKU   string
		Index int
	}
	if err := json.Unmarshal(st.Results["pack"], &packed); err != nil || len(packed) != len(oc.o.Items) {
		t.Fatalf("%s: pack result %s", oc.name, st.Results["pack"])
	}

	for i, p := range packed {
		if p.SKU != oc.o.Items[i].SKU || p.Index != i {
			t.Fatalf("%s: pack result %d is %+v, want %s in item order", oc.name, i, p, oc.o.Items[i].SKU)
		}
	}

	var reserved map[string]bool
	if err := json.Unmarshal(st.Channels["reserved"], &reserved); err != nil || len(reserved) != len(oc.o.Items) {
		t.Fatalf("%s: reserved %s", oc.name, st.Channels["reserved"])
	}

	// Every step logged exactly once, packing once per item.
	want := make([]string, 0, 4+len(oc.o.Items)) //nolint:mnd // the four steps below.
	want = append(want, "validate", "reserve", "charge", "label")

	for _, it := range oc.o.Items {
		want = append(want, "pack:"+it.SKU)
	}

	got := slices.Clone(logs)
	sort.Strings(got)
	sort.Strings(want)

	if !slices.Equal(got, want) {
		t.Fatalf("%s: log %v, want %v", oc.name, logs, want)
	}

	var inv struct {
		Number string
		Total  float64
	}
	if err := json.Unmarshal(st.Results["invoice"], &inv); err != nil || inv.Total != oc.o.total() {
		t.Fatalf("%s: invoice %s", oc.name, st.Results["invoice"])
	}

	// Counters: the charge, plus the child's invoice usage folded into the parent.
	if st.Counters["charges"] != 1 || st.Counters["invoices"] != 1 {
		t.Fatalf("%s: counters %v", oc.name, st.Counters)
	}

	fragile := 0

	for _, it := range oc.o.Items {
		if it.Fragile {
			fragile++
		}
	}

	if f.retried["pack"] != 2*fragile || f.retried["validate"] != boolInt(oc.o.Flaky) {
		t.Fatalf("%s: retries %v, want pack %d validate %d", oc.name, f.retried, 2*fragile, boolInt(oc.o.Flaky))
	}

	// The invoice child ran to completion on its own and is linked to us.
	kids := 0

	for _, ev := range f.events {
		if d, ok := ev.Data.(*event.Child); ok {
			kids++

			cst := cl.wait(d.ChildID)
			cl.check(d.ChildID)

			if cst.Status != packtrail.StatusCompleted || cst.Parent == nil || cst.Parent.ExecID != oc.o.ID {
				t.Fatalf("%s: child %s %s parent %+v", oc.name, d.ChildID, cst.Status, cst.Parent)
			}
		}
	}

	if kids != 1 {
		t.Fatalf("%s: %d children, want 1", oc.name, kids)
	}
}

func boolInt(b bool) int {
	if b {
		return 1
	}

	return 0
}

// TestOrderSaga runs every order variant concurrently on two engines.
func TestOrderSaga(t *testing.T) {
	cl := newCluster(t, []string{orderFlow, invoiceFlow})
	cl.startEngine()
	orderWorkers(cl)

	cases := orderCases(fmt.Sprintf("saga%d-", time.Now().UnixNano()))
	ids := make([]string, len(cases))

	for i, oc := range cases {
		ids[i] = cl.runOrder(oc)
	}

	for i, oc := range cases {
		t.Run(oc.name, func(t *testing.T) {
			cl.with(t).verifyOrder(oc, ids[i])
		})
	}

	// The index agrees: every order is listed as completed, findable by customer.
	cl.eventually("orders indexed", func() bool {
		l, err := cl.c.List(cl.ctx, packtrail.ListFilter{Flow: "order", Status: packtrail.StatusCompleted})

		return err == nil && len(l) == len(cases)
	})

	l, err := cl.c.List(cl.ctx, packtrail.ListFilter{Flow: "order", Attr: "customer", Value: "bob"})
	if err != nil || len(l) != 2 {
		t.Fatalf("orders of bob: %v %v", l, err)
	}
}
