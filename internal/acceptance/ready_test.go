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
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/internal/names"
	"github.com/henomis/packtrail/internal/natstest"
	"github.com/henomis/packtrail/worker"
)

const readyFlow = `
name: on-ping
triggers:
  - {subject: ping.core}
  - {subject: ping.js.x, stream: PINGS}
nodes:
  - {id: a, type: task, kind: echo}
`

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func waitReady(t *testing.T, what string, ch <-chan struct{}) {
	t.Helper()

	select {
	case <-ch:
	case <-time.After(20 * time.Second):
		t.Fatalf("%s never ready", what)
	}
}

// pulling checks that durable has been created and has a pull request
// waiting on the server.
func pulling(ctx context.Context, t *testing.T, js jetstream.JetStream, stream, durable string) {
	t.Helper()

	c, err := js.Consumer(ctx, stream, durable)
	if err != nil {
		t.Fatalf("consumer %s: %v", durable, err)
	}

	// The pull request has been sent; the server registers it right after.
	deadline := time.Now().Add(2 * time.Second)

	for {
		info, ierr := c.Info(ctx)
		if ierr == nil && info.NumWaiting > 0 {
			return
		}

		if time.Now().After(deadline) {
			t.Fatalf("consumer %s has no waiting pull request", durable)
		}

		time.Sleep(10 * time.Millisecond)
	}
}

// TestEngineAndWorkerReady: Ready closes only once Run has provisioned and
// every consumer pulls; a core trigger message published right after it is
// not lost, so no start-up sleep is needed.
func TestEngineAndWorkerReady(t *testing.T) {
	s := natstest.Start(t)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
	defer cancel()

	if _, err := s.JS.CreateStream(ctx, jetstream.StreamConfig{Name: "PINGS", Subjects: []string{"ping.js.>"}}); err != nil {
		t.Fatal(err)
	}

	const partitions = 3

	eng, err := packtrail.New(s.Connect(t), packtrail.WithFlowYAML([]byte(readyFlow)),
		packtrail.WithPartitions(partitions))
	if err != nil {
		t.Fatal(err)
	}

	w, err := worker.New(s.Connect(t), "echo", Echo)
	if err != nil {
		t.Fatal(err)
	}

	if isClosed(eng.Ready()) || isClosed(w.Ready()) {
		t.Fatal("ready before Run")
	}

	runCtx, stop := context.WithCancel(ctx)
	defer stop()

	go func() { _ = eng.Run(runCtx) }()

	waitReady(t, "engine", eng.Ready())

	n := names.New(names.Default)
	for p := range partitions {
		pulling(ctx, t, s.JS, n.StreamCmd, n.DurEngine(p))
		pulling(ctx, t, s.JS, n.StreamEvents, n.DurDispatch(p))
	}

	pulling(ctx, t, s.JS, n.StreamCmd, n.DurCron())
	pulling(ctx, t, s.JS, "PINGS", n.Prefix+"-trigger-on-ping-1")

	// A core trigger is at-most-once: without its subscription the message
	// would be dropped.
	if err = s.NC.Publish("ping.core", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}

	go func() { _ = w.Run(runCtx) }()

	waitReady(t, "worker", w.Ready())
	pulling(ctx, t, s.JS, n.StreamWork, n.DurWorker("echo"))

	c := eng.Client()

	for {
		l, lerr := c.List(ctx, packtrail.ListFilter{Flow: "on-ping", Status: packtrail.StatusCompleted})
		if lerr == nil && len(l) == 1 {
			break
		}

		if ctx.Err() != nil {
			t.Fatal("the core trigger message was lost")
		}

		time.Sleep(20 * time.Millisecond)
	}
}

// TestReadyStaysOpenWhenRunFails: a Run that cannot start never reports ready.
func TestReadyStaysOpenWhenRunFails(t *testing.T) {
	s := natstest.Start(t)

	eng, err := packtrail.New(s.Connect(t))
	if err != nil {
		t.Fatal(err)
	}

	nc := s.Connect(t)

	w, err := worker.New(nc, "echo", Echo)
	if err != nil {
		t.Fatal(err)
	}

	done, cancel := context.WithCancel(context.Background())
	cancel()

	if err = eng.Run(done); err == nil {
		t.Fatal("engine Run with a cancelled context succeeded")
	}

	nc.Close()

	if err = w.Run(context.Background()); err == nil {
		t.Fatal("worker Run on a closed connection succeeded")
	}

	if isClosed(eng.Ready()) || isClosed(w.Ready()) {
		t.Fatal("ready after a failed Run")
	}
}

// TestEngineReadyWithoutConsumers: an engine with nothing to consume is ready
// once provisioned.
func TestEngineReadyWithoutConsumers(t *testing.T) {
	s := natstest.Start(t)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
	defer cancel()

	eng, err := packtrail.New(s.Connect(t), packtrail.WithoutCommands(), packtrail.WithoutDispatcher())
	if err != nil {
		t.Fatal(err)
	}

	go func() { _ = eng.Run(ctx) }()

	waitReady(t, "engine", eng.Ready())
}

// TestReadyFollowsRun: Ready reports a Run that is up, not one that was: it is
// open again once Run returns, and a second Run must pull before it closes.
func TestReadyFollowsRun(t *testing.T) {
	s := natstest.Start(t)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
	defer cancel()

	eng, err := packtrail.New(s.Connect(t), packtrail.WithPartitions(1))
	if err != nil {
		t.Fatal(err)
	}

	w, err := worker.New(s.Connect(t), "echo", Echo)
	if err != nil {
		t.Fatal(err)
	}

	for round := range 2 {
		rctx, stop := context.WithCancel(ctx)
		engDone, wDone := make(chan struct{}), make(chan struct{})

		go func() { defer close(engDone); _ = eng.Run(rctx) }()

		waitReady(t, fmt.Sprintf("engine round %d", round), eng.Ready())

		// A worker needs the namespace the engine provisions.
		go func() { defer close(wDone); _ = w.Run(rctx) }()

		waitReady(t, fmt.Sprintf("worker round %d", round), w.Ready())

		stop()
		<-engDone
		<-wDone

		if isClosed(eng.Ready()) || isClosed(w.Ready()) {
			t.Fatalf("round %d: still ready after Run returned", round)
		}
	}
}
