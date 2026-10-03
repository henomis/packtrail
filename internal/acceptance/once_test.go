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
	"testing"
	"time"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/internal/natstest"
)

// TestClientAttachRetries: a standalone client used before the namespace
// exists fails, and works once an engine has provisioned it; a call made with
// an expired context does not break it either.
func TestClientAttachRetries(t *testing.T) {
	s := natstest.Start(t)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
	defer cancel()

	c, err := packtrail.NewClient(s.Connect(t))
	if err != nil {
		t.Fatal(err)
	}

	dead, stop := context.WithCancel(ctx)
	stop()

	if _, err = c.Flows(dead); err == nil {
		t.Fatal("Flows with a cancelled context succeeded")
	}

	if _, err = c.Flows(ctx); err == nil {
		t.Fatal("Flows before the namespace exists succeeded")
	}

	eng, err := packtrail.New(s.Connect(t), packtrail.WithFlowYAML([]byte(wtOne)), packtrail.WithPartitions(1))
	if err != nil {
		t.Fatal(err)
	}

	if err = eng.Init(ctx); err != nil {
		t.Fatal(err)
	}

	flows, err := c.Flows(ctx)
	if err != nil {
		t.Fatalf("client still failing after the namespace was provisioned: %v", err)
	}

	if len(flows) != 1 {
		t.Fatalf("flows %+v", flows)
	}
}

// TestEngineRunAgainKeepsQuarantineControl: a second Run of the same engine
// still hears quarantine releases, so a released execution continues.
func TestEngineRunAgainKeepsQuarantineControl(t *testing.T) {
	s := natstest.Start(t)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
	defer cancel()

	eng, err := packtrail.New(s.Connect(t), packtrail.WithFlowYAML([]byte(wtAwait)), packtrail.WithPartitions(1))
	if err != nil {
		t.Fatal(err)
	}

	first, stopFirst := context.WithCancel(ctx)
	firstDone := make(chan struct{})

	go func() { defer close(firstDone); _ = eng.Run(first) }()

	waitReady(t, "first run", eng.Ready())
	stopFirst()
	<-firstDone

	second, stopSecond := context.WithCancel(ctx)
	defer stopSecond()

	go func() { _ = eng.Run(second) }()

	waitReady(t, "second run", eng.Ready())

	if !controlSubscribed(t, s) {
		t.Fatal("the second run does not listen to quarantine control notices")
	}
}

// controlSubscribed reports whether some connection holds the
// dispatcher's quarantine control subscriptions, by asking the server.
func controlSubscribed(t *testing.T, s *natstest.Server) bool {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)

	for time.Now().Before(deadline) {
		if s.HasSubscription("packtrail.ctl.quarantine") && s.HasSubscription("packtrail.ctl.release") {
			return true
		}

		time.Sleep(20 * time.Millisecond)
	}

	return false
}
