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
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail"
)

// Durable (stream) triggers: the application owns stream ORDERS; two flows
// start from its messages.
const (
	trOrder = `
name: on-order
triggers:
  - {subject: orders.js.created, stream: ORDERS}
nodes:
  - {id: handle, type: task, kind: echo}
`
	trAudit = `
name: audit
triggers:
  - {subject: orders.js.created, stream: ORDERS}
  - {subject: orders.core.created}
nodes:
  - {id: record, type: task, kind: echo}
`
	trCore = `
name: on-core
triggers:
  - {subject: orders.core.created}
nodes:
  - {id: handle, type: task, kind: echo}
`
)

func createOrders(t *testing.T, e *Env, cfg jetstream.StreamConfig) {
	t.Helper()

	cfg.Name, cfg.Subjects = "ORDERS", []string{"orders.js.>"}

	if _, err := e.S.JS.CreateStream(e.Ctx, cfg); err != nil {
		t.Fatal(err)
	}
}

// storeOrder stores one message in ORDERS, retrying until the stream
// acknowledges it (the Msg-Id makes a retry of a stored message a no-op).
// Safe from any goroutine.
func storeOrder(ctx context.Context, js jetstream.JetStream, msgID string, body []byte) (uint64, error) {
	for {
		m := nats.NewMsg("orders.js.created")
		m.Data = body

		if msgID != "" {
			m.Header.Set("Nats-Msg-Id", msgID)
		}

		// Bound each attempt: a publish sent as the server goes away is
		// never answered.
		pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		ack, err := js.PublishMsg(pctx, m)

		cancel()

		if err == nil {
			return ack.Sequence, nil
		}

		if ctx.Err() != nil {
			return 0, fmt.Errorf("publish %s: %w", msgID, err)
		}

		time.Sleep(50 * time.Millisecond)
	}
}

// publishOrder is storeOrder for the test goroutine.
func publishOrder(t *testing.T, e *Env, js jetstream.JetStream, msgID string, body []byte) uint64 {
	t.Helper()

	seq, err := storeOrder(e.Ctx, js, msgID, body)
	if err != nil {
		t.Fatal(err)
	}

	return seq
}

// restartReady replaces every engine and waits until the new one pulls from all its
// consumers and triggers.
func (e *Env) restartReady() {
	e.T.Helper()

	e.KillEngines()
	eng := e.StartEngine()

	select {
	case <-eng.Ready():
	case <-e.Ctx.Done():
		e.T.Fatal("engine never ready")
	}
}

// countFlow is how many executions of flow the index lists.
func countFlow(e *Env, flow string) int {
	l, err := e.Client.List(e.Ctx, packtrail.ListFilter{Flow: flow, Limit: 1000})
	if err != nil {
		return -1
	}

	return len(l)
}

// TestTriggerStreamSurvivesEngineOutage: messages stored while no engine
// runs start their executions once one is back — what a core trigger would
// lose. Ids come from the Msg-Id, or the stream sequence without one.
func TestTriggerStreamSurvivesEngineOutage(t *testing.T) {
	e := NewEnv(t, []string{trOrder})
	e.Worker("echo", Echo)
	createOrders(t, e, jetstream.StreamConfig{})
	e.restartReady()
	e.KillEngines()

	const n = 10

	want := map[string]string{} // exec id → input

	for i := range n {
		body := fmt.Sprintf(`{"order":%d}`, i)

		if i%2 == 0 {
			publishOrder(t, e, e.S.JS, "o"+strconv.Itoa(i), []byte(body))
			want["on-order-o"+strconv.Itoa(i)] = body

			continue
		}

		seq := publishOrder(t, e, e.S.JS, "", []byte(body))
		want["on-order-ORDERS-"+strconv.FormatUint(seq, 10)] = body
	}

	e.StartEngine()

	for id, body := range want {
		if st := e.Completed(id); string(st.Input) != body {
			t.Fatalf("%s input %s, want %s", id, st.Input, body)
		}
	}

	e.Eventually(func() bool { return countFlow(e, "on-order") == n },
		func() string { return fmt.Sprintf("%d executions, want %d", countFlow(e, "on-order"), n) })
}

// TestTriggerStreamAcrossCrashAndNATSRestart: while messages keep arriving,
// the engine is replaced and NATS restarts; every stored message starts
// exactly one execution.
func TestTriggerStreamAcrossCrashAndNATSRestart(t *testing.T) {
	e := NewEnv(t, []string{trOrder})
	e.Worker("echo", Echo)
	createOrders(t, e, jetstream.StreamConfig{})
	e.restartReady()

	const n = 60

	pub := e.NC()

	js, err := jetstream.New(pub)
	if err != nil {
		t.Fatal(err)
	}

	published := make(chan error, 1)

	go func() {
		for i := range n {
			if _, perr := storeOrder(e.Ctx, js, "o"+strconv.Itoa(i), fmt.Appendf(nil, `{"order":%d}`, i)); perr != nil {
				published <- perr

				return
			}

			time.Sleep(15 * time.Millisecond)
		}

		published <- nil
	}()

	time.Sleep(200 * time.Millisecond)
	e.RestartEngine()
	time.Sleep(200 * time.Millisecond)
	e.S.Restart(t)

	if err = <-published; err != nil {
		t.Fatal(err)
	}

	for i := range n {
		e.Completed("on-order-o" + strconv.Itoa(i))
	}

	e.Eventually(func() bool { return countFlow(e, "on-order") == n },
		func() string { return fmt.Sprintf("%d executions, want %d", countFlow(e, "on-order"), n) })
}

// TestTriggerRepublishedMessageStartsOnce: a message published again after
// the stream's duplicate window is a second stream message, but it carries
// the same Msg-Id, so it starts nothing new.
func TestTriggerRepublishedMessageStartsOnce(t *testing.T) {
	e := NewEnv(t, []string{trOrder})
	e.Worker("echo", Echo)
	createOrders(t, e, jetstream.StreamConfig{Duplicates: 100 * time.Millisecond})
	e.restartReady()

	first := publishOrder(t, e, e.S.JS, "x", []byte(`{"v":1}`))
	e.Completed("on-order-x")

	time.Sleep(300 * time.Millisecond)

	if second := publishOrder(t, e, e.S.JS, "x", []byte(`{"v":2}`)); second == first {
		t.Fatal("the stream deduplicated the second publish: the test needs two stream messages")
	}

	// Let the trigger take the second message, then check nothing changed.
	e.Eventually(func() bool {
		info, ierr := e.S.JS.Consumer(e.Ctx, "ORDERS", "packtrail-trigger-on-order-0")
		if ierr != nil {
			return false
		}

		ci, ierr := info.Info(e.Ctx)

		return ierr == nil && ci.NumPending == 0 && ci.NumAckPending == 0
	}, func() string { return "the trigger never took the second message" })

	if c := countFlow(e, "on-order"); c != 1 {
		t.Fatalf("%d executions, want 1", c)
	}

	if st := e.Completed("on-order-x"); string(st.Input) != `{"v":1}` {
		t.Fatalf("input %s: the republished message replaced the first", st.Input)
	}
}

// TestTriggerOneMessageStartsEveryFlow: every flow triggered by a message
// runs once, on a stream and on a core subject — the Msg-Id alone is not the
// execution id.
func TestTriggerOneMessageStartsEveryFlow(t *testing.T) {
	e := NewEnv(t, []string{trOrder, trAudit, trCore})
	e.Worker("echo", Echo)
	createOrders(t, e, jetstream.StreamConfig{})
	e.restartReady()

	publishOrder(t, e, e.S.JS, "m1", []byte(`{"order":1}`))

	m := nats.NewMsg("orders.core.created")
	m.Header.Set("Nats-Msg-Id", "m2")
	m.Data = []byte(`{"order":2}`)

	nc := e.NC()
	if err := nc.PublishMsg(m); err != nil {
		t.Fatal(err)
	}

	for _, id := range []string{"on-order-m1", "audit-m1", "on-core-m2", "audit-m2"} {
		e.Completed(id)
	}

	assertFlowCounts(e, map[string]int{"on-order": 1, "audit": 2, "on-core": 1})
}

// assertFlowCounts waits until the index lists want executions per flow.
func assertFlowCounts(e *Env, want map[string]int) {
	e.T.Helper()

	got := map[string]int{}

	e.Eventually(func() bool {
		for f, n := range want {
			if got[f] = countFlow(e, f); got[f] != n {
				return false
			}
		}

		return true
	}, func() string { return fmt.Sprintf("executions per flow %v, want %v", got, want) })
}

// TestTriggerStreamCreatedAfterEngine: a trigger whose stream does not exist
// yet keeps the engine unready and is picked up once the stream is created,
// without a restart.
func TestTriggerStreamCreatedAfterEngine(t *testing.T) {
	e := NewEnv(t, []string{trOrder})
	e.Worker("echo", Echo)

	time.Sleep(1500 * time.Millisecond) // at least one failed setup

	select {
	case <-e.Engine.Ready():
		t.Fatal("ready while a trigger's stream is missing")
	default:
	}

	createOrders(t, e, jetstream.StreamConfig{})
	publishOrder(t, e, e.S.JS, "late", []byte(`{}`))
	e.Completed("on-order-late")

	select {
	case <-e.Engine.Ready():
	case <-e.Ctx.Done():
		t.Fatal("never ready after the stream was created")
	}
}

// TestTriggerInputWrapping: an object body is the input; anything else is
// wrapped under "data".
func TestTriggerInputWrapping(t *testing.T) {
	e := NewEnv(t, []string{trOrder})
	e.Worker("echo", Echo)
	createOrders(t, e, jetstream.StreamConfig{})
	e.restartReady()

	cases := map[string]string{
		`{"a":1}`: `{"a":1}`,
		`[1,2]`:   `{"data":[1,2]}`,
		`42`:      `{"data":42}`,
		`hello`:   `{"data":"hello"}`,
		``:        `{"data":""}`,
	}

	i := 0
	for body, want := range cases {
		id := "w" + strconv.Itoa(i)
		i++

		publishOrder(t, e, e.S.JS, id, []byte(body))

		st := e.Completed("on-order-" + id)

		var got, exp any
		if json.Unmarshal(st.Input, &got) != nil || json.Unmarshal([]byte(want), &exp) != nil ||
			fmt.Sprint(got) != fmt.Sprint(exp) {
			t.Fatalf("body %q: input %s, want %s", body, st.Input, want)
		}
	}
}
