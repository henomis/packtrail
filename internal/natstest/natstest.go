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
// enabled for use in tests. It is a real server (no client mocks): packtrail's
// correctness is about NATS semantics, so every test runs against the genuine
// article. Restart supports fault-injection tests that bounce the server while
// keeping its storage.
package natstest

import (
	"context"
	"net"
	"net/url"
	"strconv"
	"sync"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Server is a running embedded nats-server with a connected client.
type Server struct {
	NC *nats.Conn
	JS jetstream.JetStream

	mu   sync.Mutex
	ns   *natsserver.Server
	opts natsserver.Options
}

const natsReadyTimeout = 10 * time.Second

// Start launches an embedded JetStream-enabled nats-server on a random port,
// returns a connected client and a JetStream context, and registers cleanup
// with the test.
func Start(t testing.TB) *Server {
	t.Helper()

	s := &Server{opts: natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
		NoLog:     true,
		NoSigs:    true,
	}}

	s.boot(t)

	// Pin the port so a restarted server is reachable at the same URL.
	if addr, ok := s.ns.Addr().(*net.TCPAddr); ok {
		s.opts.Port = addr.Port
	}

	s.NC = s.Connect(t)

	js, err := jetstream.New(s.NC)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}

	s.JS = js

	t.Cleanup(func() {
		s.mu.Lock()
		defer s.mu.Unlock()

		s.ns.Shutdown()
		s.ns.WaitForShutdown()
	})

	return s
}

func (s *Server) boot(t testing.TB) {
	t.Helper()

	opts := s.opts

	ns, err := natsserver.NewServer(&opts)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	go ns.Start()

	if !ns.ReadyForConnections(natsReadyTimeout) {
		t.Fatal("nats-server not ready")
	}

	s.ns = ns
}

// URL returns the client URL of the server.
func (s *Server) URL() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.ns.ClientURL()
}

// Connect opens a second, independent connection to the same server, closed
// when the test ends. Use it where the point under test is that two separate
// processes share only a NATS cluster. The connection reconnects forever, so it
// survives Restart.
func (s *Server) Connect(t testing.TB) *nats.Conn {
	t.Helper()

	nc, err := nats.Connect(s.URL(),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(50*time.Millisecond), //nolint:mnd // test-only tuning.
	)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	t.Cleanup(nc.Close)

	return nc
}

// Restart shuts the server down and boots it again on the same port with the
// same storage directory: JetStream state survives, connections reconnect.
func (s *Server) Restart(t testing.TB) {
	t.Helper()

	s.mu.Lock()
	defer s.mu.Unlock()

	s.ns.Shutdown()
	s.ns.WaitForShutdown()
	s.boot(t)
}

// Cluster is a running embedded JetStream cluster.
type Cluster struct {
	NC      *nats.Conn
	JS      jetstream.JetStream
	servers []*natsserver.Server
}

// StartCluster launches an n-node JetStream cluster (for replication tests)
// and waits until it has a meta leader.
func StartCluster(t testing.TB, n int) *Cluster {
	t.Helper()

	clusterPorts := make([]int, n)
	for i := range clusterPorts {
		clusterPorts[i] = freePort(t)
	}

	c := &Cluster{}

	for i := range n {
		opts := &natsserver.Options{
			ServerName: "s" + strconv.Itoa(i), Host: "127.0.0.1", Port: -1, JetStream: true,
			StoreDir: t.TempDir(), NoLog: true, NoSigs: true,
			Cluster: natsserver.ClusterOpts{Name: "pt", Host: "127.0.0.1", Port: clusterPorts[i]},
		}

		for j, p := range clusterPorts {
			if j != i {
				u, _ := url.Parse("nats://127.0.0.1:" + strconv.Itoa(p))
				opts.Routes = append(opts.Routes, u)
			}
		}

		ns, err := natsserver.NewServer(opts)
		if err != nil {
			t.Fatalf("new server: %v", err)
		}

		go ns.Start()

		c.servers = append(c.servers, ns)
	}

	t.Cleanup(func() {
		for _, ns := range c.servers {
			ns.Shutdown()
			ns.WaitForShutdown()
		}
	})

	for _, ns := range c.servers {
		if !ns.ReadyForConnections(natsReadyTimeout) {
			t.Fatal("cluster server not ready")
		}
	}

	nc, err := nats.Connect(c.servers[0].ClientURL())
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(nc.Close)

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}

	c.NC, c.JS = nc, js

	c.waitReady(t, n)

	return c
}

// Connect returns a new connection to node i (modulo the cluster size), closed
// at cleanup: spreading connections over the nodes spreads the load.
func (c *Cluster) Connect(t testing.TB, i int) *nats.Conn {
	t.Helper()

	nc, err := nats.Connect(c.servers[i%len(c.servers)].ClientURL())
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(nc.Close)

	return nc
}

// waitReady blocks until every peer can take a replica: it places a probe
// stream with n replicas.
func (c *Cluster) waitReady(t testing.TB, n int) {
	t.Helper()

	deadline := time.Now().Add(natsReadyTimeout)
	probe := jetstream.StreamConfig{Name: "natstest-probe", Subjects: []string{"natstest.probe"}, Replicas: n}

	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)

		_, err := c.JS.CreateStream(ctx, probe)
		if err == nil {
			_ = c.JS.DeleteStream(ctx, probe.Name)

			cancel()

			return
		}

		cancel()

		if time.Now().After(deadline) {
			t.Fatalf("cluster not ready for %d replicas: %v", n, err)
		}

		time.Sleep(100 * time.Millisecond) //nolint:mnd // poll interval.
	}
}

func freePort(t testing.TB) int {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	defer func() { _ = l.Close() }()

	return l.Addr().(*net.TCPAddr).Port //nolint:errcheck,forcetypeassert // a TCP listener.
}
