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

// Package natstest starts an embedded, in-process nats-server with JetStream
// enabled for use in tests. It is a real server (no client mocks), satisfying
// the requirement that all tests run against a genuine nats-server.
package natstest

import (
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Server is a running embedded nats-server with a connected client.
type Server struct {
	NS *natsserver.Server
	NC *nats.Conn
	JS jetstream.JetStream
}

const natsReadyTimeout = 10 * time.Second

// Start launches an embedded JetStream-enabled nats-server on a random port,
// returns a connected client and a JetStream context, and registers cleanup
// with the test.
func Start(t testing.TB) *Server {
	t.Helper()
	opts := &natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1, // random free port
		JetStream: true,
		StoreDir:  t.TempDir(),
	}

	ns, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	go ns.Start()

	if !ns.ReadyForConnections(natsReadyTimeout) {
		t.Fatal("nats-server not ready")
	}

	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		ns.Shutdown()
		t.Fatalf("connect: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		ns.Shutdown()
		t.Fatalf("jetstream: %v", err)
	}

	s := &Server{NS: ns, NC: nc, JS: js}

	t.Cleanup(func() {
		nc.Close()
		ns.Shutdown()
		ns.WaitForShutdown()
	})

	return s
}

// Connect opens a second, independent connection to the same server, closed when
// the test ends. Use it where the point under test is that two separate
// processes share only a NATS cluster: reusing s.NC would hide a dependency on
// in-process state that a real deployment does not have.
func (s *Server) Connect(t testing.TB) *nats.Conn {
	t.Helper()

	nc, err := nats.Connect(s.NS.ClientURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	t.Cleanup(nc.Close)

	return nc
}
