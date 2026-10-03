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

// Package packtrailtest runs an embedded, in-process nats-server with
// JetStream enabled, for testing applications built on packtrail. It is a
// real server, so engines, workers and clients behave exactly as in
// production; Restart bounces it while keeping its storage, to test how an
// application rides out a NATS outage.
package packtrailtest

import (
	"testing"

	"github.com/nats-io/nats.go"

	"github.com/henomis/packtrail/internal/natstest"
)

// Server is a running embedded nats-server, shut down when the test ends.
type Server struct {
	s *natstest.Server
}

// Start launches a JetStream-enabled server on a random loopback port, with
// its storage in a temporary directory of the test.
func Start(t testing.TB) *Server {
	t.Helper()

	return &Server{s: natstest.Start(t)}
}

// URL returns the client URL of the server. It does not change on Restart.
func (s *Server) URL() string { return s.s.URL() }

// Connect opens a new connection to the server, closed when the test ends.
// Give each engine, worker and client its own connection to model separate
// processes. The connection reconnects forever, so it survives Restart.
func (s *Server) Connect(t testing.TB) *nats.Conn {
	t.Helper()

	return s.s.Connect(t)
}

// Restart shuts the server down and boots it again on the same port with the
// same storage: JetStream state survives and connections reconnect.
func (s *Server) Restart(t testing.TB) {
	t.Helper()

	s.s.Restart(t)
}
