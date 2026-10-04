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

package engine

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail/flow"
	"github.com/henomis/packtrail/internal/infra"
	"github.com/henomis/packtrail/internal/metrics"
	"github.com/henomis/packtrail/internal/names"
	"github.com/henomis/packtrail/internal/natstest"
	"github.com/henomis/packtrail/internal/wire"
)

// TestTriggerDeadLettersAfterMaxDeliver: a stream trigger message that
// cannot be started (here: the command stream is gone) is dead-lettered
// once its deliveries are spent, instead of being dropped by the server.
func TestTriggerDeadLettersAfterMaxDeliver(t *testing.T) {
	s := natstest.Start(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	in, err := infra.New(s.NC, names.New(""), nil)
	if err != nil {
		t.Fatal(err)
	}

	if err = in.Provision(ctx, 1); err != nil {
		t.Fatal(err)
	}

	if _, err = s.JS.CreateStream(ctx, jetstream.StreamConfig{Name: "ORDERS", Subjects: []string{"orders.>"}}); err != nil {
		t.Fatal(err)
	}

	if err = s.JS.DeleteStream(ctx, in.Names.StreamCmd); err != nil {
		t.Fatal(err)
	}

	def, err := flow.Parse([]byte("name: trig\ntriggers: [{subject: orders.created, stream: ORDERS}]\nnodes: [{id: a, type: task, kind: k}]"))
	if err != nil {
		t.Fatal(err)
	}

	e := &Engine{In: in, Metrics: &metrics.M{}, triggerMaxDeliver: 2}

	rctx, stop := context.WithCancel(ctx)
	done := make(chan struct{})

	go func() {
		defer close(done)

		_ = e.RunTrigger(rctx, def, 0, def.Triggers[0], func() {})
	}()

	defer func() { stop(); <-done }()

	m := nats.NewMsg("orders.created")
	m.Header.Set("Nats-Msg-Id", "o1")
	m.Data = []byte(`{"order":1}`)

	if _, err = s.JS.PublishMsg(ctx, m); err != nil {
		t.Fatal(err)
	}

	dlq, err := s.JS.Stream(ctx, in.Names.StreamDLQ)
	if err != nil {
		t.Fatal(err)
	}

	var raw *jetstream.RawStreamMsg

	for raw == nil {
		raw, _ = dlq.GetLastMsgForSubject(ctx, in.Names.DLQSubject(wire.DLQTrigger, names.TriggerMsgExecID("trig", "o1")))

		select {
		case <-ctx.Done():
			t.Fatal("no trigger dead letter")
		case <-time.After(50 * time.Millisecond):
		}
	}

	var d wire.DeadLetter
	if err = json.Unmarshal(raw.Data, &d); err != nil {
		t.Fatal(err)
	}

	if d.Kind != wire.DLQTrigger || d.Key != names.TriggerMsgExecID("trig", "o1") || d.Subject != "orders.created" ||
		string(d.Body) != `{"order":1}` || d.Deliveries != 2 {
		t.Fatalf("dead letter %+v", d)
	}

	if e.Metrics.DeadLetters.Load() != 1 {
		t.Fatalf("dead letters metric %d", e.Metrics.DeadLetters.Load())
	}

	// Terminated: nothing left to deliver.
	cons, err := s.JS.Consumer(ctx, "ORDERS", in.Names.Prefix+"-trigger-trig-0")
	if err != nil {
		t.Fatal(err)
	}

	info, err := cons.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if info.NumPending != 0 || info.NumAckPending != 0 || info.NumRedelivered != 0 {
		t.Fatalf("message still pending: %+v", info)
	}
}
