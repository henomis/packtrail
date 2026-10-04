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

// TestTriggerDeadLetterPerFlow: two flows triggered by the same stream
// message that both cannot start must each leave a `trigger` dead letter.
func TestTriggerDeadLetterPerFlow(t *testing.T) {
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

	e := &Engine{In: in, Metrics: &metrics.M{}, triggerMaxDeliver: 2}
	rctx, stop := context.WithCancel(ctx)

	dones := make([]chan struct{}, 0, 2)

	for _, name := range []string{"trig1", "trig2"} {
		def, perr := flow.Parse([]byte("name: " + name +
			"\ntriggers: [{subject: orders.created, stream: ORDERS}]\nnodes: [{id: a, type: task, kind: k}]"))
		if perr != nil {
			t.Fatal(perr)
		}

		done := make(chan struct{})
		dones = append(dones, done)

		go func() {
			defer close(done)

			_ = e.RunTrigger(rctx, def, 0, def.Triggers[0], func() {})
		}()
	}

	defer func() {
		stop()

		for _, d := range dones {
			<-d
		}
	}()

	m := nats.NewMsg("orders.created")
	m.Header.Set("Nats-Msg-Id", "o1")
	m.Data = []byte(`{"order":1}`)

	if _, err = s.JS.PublishMsg(ctx, m); err != nil {
		t.Fatal(err)
	}

	// Wait until both consumers have terminated the message.
	for _, name := range []string{"trig1", "trig2"} {
		for {
			cons, cerr := s.JS.Consumer(ctx, "ORDERS", in.Names.Prefix+"-trigger-"+name+"-0")
			if cerr == nil {
				info, ierr := cons.Info(ctx)
				if ierr == nil && info.AckFloor.Stream == 1 {
					break
				}
			}

			select {
			case <-ctx.Done():
				t.Fatalf("%s never terminated the message", name)
			case <-time.After(50 * time.Millisecond):
			}
		}
	}

	dlq, err := s.JS.Stream(ctx, in.Names.StreamDLQ)
	if err != nil {
		t.Fatal(err)
	}

	info, err := dlq.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("dead letters metric %d, dead letters stored %d", e.Metrics.DeadLetters.Load(), info.State.Msgs)

	for _, key := range []string{"trig1-o1", "trig2-o1"} {
		if _, gerr := dlq.GetLastMsgForSubject(ctx, in.Names.DLQSubject(wire.DLQTrigger, key)); gerr != nil {
			t.Errorf("no trigger dead letter for %s: %v (message terminated, never started)", key, gerr)
		}
	}
}
