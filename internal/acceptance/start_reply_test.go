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
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/internal/natstest"
)

// blip is a TCP proxy in front of the server whose clients can be cut off
// (a network blip) and let back in.
type blip struct {
	ln     net.Listener
	target string
	down   atomic.Bool

	mu    sync.Mutex
	conns []net.Conn
}

func newBlip(t *testing.T, target string) *blip {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	b := &blip{ln: ln, target: target}

	t.Cleanup(func() { _ = ln.Close(); b.cut() })

	go b.serve()

	return b
}

func (b *blip) url() string { return "nats://" + b.ln.Addr().String() }

func (b *blip) serve() {
	for {
		c, err := b.ln.Accept()
		if err != nil {
			return
		}

		if b.down.Load() {
			_ = c.Close()

			continue
		}

		s, err := net.Dial("tcp", b.target)
		if err != nil {
			_ = c.Close()

			continue
		}

		b.mu.Lock()
		b.conns = append(b.conns, c, s)
		b.mu.Unlock()

		go func() { _, _ = io.Copy(s, c); _ = s.Close() }()
		go func() { _, _ = io.Copy(c, s); _ = c.Close() }()
	}
}

func (b *blip) cut() {
	b.down.Store(true)

	b.mu.Lock()
	defer b.mu.Unlock()

	for _, c := range b.conns {
		_ = c.Close()
	}

	b.conns = nil
}

// TestStartReplyLostToReconnect: the start is stored and decided, but the
// engine's reply reaches the client while its connection is down (a short
// network blip). Nothing answers again; Start must still return once the
// execution exists.
func TestStartReplyLostToReconnect(t *testing.T) {
	s := natstest.Start(t)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
	defer cancel()

	eng, err := packtrail.New(s.Connect(t), packtrail.WithFlowYAML([]byte(wtAwait)), packtrail.WithPartitions(1))
	if err != nil {
		t.Fatal(err)
	}

	if err = eng.Init(ctx); err != nil {
		t.Fatal(err)
	}

	p := newBlip(t, strings.TrimPrefix(s.URL(), "nats://"))

	nc, err := nats.Connect(p.url(), nats.MaxReconnects(-1), nats.ReconnectWait(50*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(nc.Close)

	c, err := packtrail.NewClient(nc)
	if err != nil {
		t.Fatal(err)
	}

	type result struct {
		id  string
		err error
	}

	done := make(chan result, 1)

	go func() {
		sctx, scancel := context.WithTimeout(ctx, 15*time.Second) // stands for "no deadline"
		defer scancel()

		id, serr := c.Start(sctx, "hold", nil, packtrail.WithExecutionID("blip"))
		done <- result{id, serr}
	}()

	time.Sleep(500 * time.Millisecond) // the start command is stored; Start waits for the reply

	p.cut() // network blip: the client is disconnected

	runCtx, stop := context.WithCancel(ctx)
	defer stop()

	go func() { _ = eng.Run(runCtx) }()

	// The engine decides the start (and replies into the void).
	deadline := time.Now().Add(10 * time.Second)

	for {
		if st, gerr := eng.Client().Get(ctx, "blip"); gerr == nil && st.Status == packtrail.StatusWaiting {
			break
		}

		if time.Now().After(deadline) {
			t.Fatal("the start was never decided")
		}

		time.Sleep(20 * time.Millisecond)
	}

	time.Sleep(200 * time.Millisecond)
	p.down.Store(false) // the blip ends; the client reconnects

	for !nc.IsConnected() {
		if time.Now().After(deadline.Add(10 * time.Second)) {
			t.Fatal("client never reconnected")
		}

		time.Sleep(20 * time.Millisecond)
	}

	t.Logf("client reconnected (%d reconnects)", nc.Stats().Reconnects)

	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("Start of an execution that exists: %q, %v", r.id, r.err)
		}
	case <-time.After(5 * time.Second):
		r := <-done // the 15s ctx

		t.Fatalf("Start still blocked 5s after reconnecting although execution blip exists; it returned %q, %v "+
			"only when its ctx ended", r.id, r.err)
	}
}

// TestStartTimeoutKeepsID: a Start whose ctx ends before an engine answers
// returns its id with the error; retrying with that id starts one execution.
func TestStartTimeoutKeepsID(t *testing.T) {
	s := natstest.Start(t)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
	defer cancel()

	eng, err := packtrail.New(s.Connect(t), packtrail.WithFlowYAML([]byte(wtAwait)), packtrail.WithPartitions(1))
	if err != nil {
		t.Fatal(err)
	}

	if err = eng.Init(ctx); err != nil {
		t.Fatal(err)
	}

	c := eng.Client()

	var id string

	for range 2 { // the call, and the caller's retry with the id it got
		short, stop := context.WithTimeout(ctx, 300*time.Millisecond)

		var opts []packtrail.StartOption
		if id != "" {
			opts = append(opts, packtrail.WithExecutionID(id))
		}

		got, serr := c.Start(short, "hold", nil, opts...)

		stop()

		if serr == nil || got == "" || (id != "" && got != id) {
			t.Fatalf("Start without an engine: id %q (first %q), err %v", got, id, serr)
		}

		id = got
	}

	runCtx, stopRun := context.WithCancel(ctx)
	defer stopRun()

	go func() { _ = eng.Run(runCtx) }()

	if _, err = c.WaitUntil(ctx, id, func(st *packtrail.State) bool {
		return st.Status == packtrail.StatusWaiting
	}); err != nil {
		t.Fatal(err)
	}

	// The visibility index follows the log; give a duplicate time to show.
	var started []packtrail.Summary

	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
		if started, err = c.List(ctx, packtrail.ListFilter{Flow: "hold"}); err != nil || len(started) > 1 {
			break
		}

		time.Sleep(50 * time.Millisecond)
	}

	if err != nil || len(started) != 1 || started[0].ExecID != id {
		t.Fatalf("executions %v (err %v), want only %s", started, err, id)
	}
}
