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
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/internal/names"
	"github.com/henomis/packtrail/internal/wire"
)

// TestTriggerRedriveStartsOnlyItsFlow: redriving a trigger dead letter starts
// its own flow with the id the message was meant to get. The message is not
// republished, so the other flow triggered by its subject and the
// application's stream do not see it again; a second redrive of the same
// record starts nothing twice.
func TestTriggerRedriveStartsOnlyItsFlow(t *testing.T) {
	e := NewEnv(t, []string{trOrder, trAudit})
	e.Worker("echo", Echo)
	createOrders(t, e, jetstream.StreamConfig{})
	e.restartReady()

	id := packtrail.TriggerExecID("on-order", "lost")
	record := wire.DeadLetter{
		Kind: wire.DLQTrigger, Key: id, Flow: "on-order", Reason: "delivery attempts exhausted",
		Subject: "orders.js.created", Header: map[string][]string{"Nats-Msg-Id": {"lost"}}, Body: []byte(`{"order":7}`),
	}

	for range 2 { // the record, and a copy of it redriven later
		b, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}

		if _, err = e.S.JS.Publish(e.Ctx, names.New("").DLQSubject(wire.DLQTrigger, id), b); err != nil {
			t.Fatal(err)
		}
	}

	letters, err := e.Client.DeadLetters(e.Ctx, 10)
	if err != nil || len(letters) != 2 {
		t.Fatalf("dead letters %v, %v", letters, err)
	}

	for _, d := range letters {
		if err = e.Client.Redrive(e.Ctx, d.Seq); err != nil {
			t.Fatal(err)
		}
	}

	if st := e.Completed(id); string(st.Input) != `{"order":7}` {
		t.Fatalf("redriven start input %s", st.Input)
	}

	time.Sleep(500 * time.Millisecond) // time for a wrong start to show

	assertFlowCounts(e, map[string]int{"on-order": 1, "audit": 0})

	orders, err := e.S.JS.Stream(e.Ctx, "ORDERS")
	if err != nil {
		t.Fatal(err)
	}

	if info, ierr := orders.Info(e.Ctx); ierr != nil || info.State.Msgs != 0 {
		t.Fatalf("the redrive republished to the application's stream: %v, %v", info, ierr)
	}
}
