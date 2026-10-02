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

// Package devserver gives the examples a NATS connection to the server named by
// $NATS_URL (default nats://127.0.0.1:4222), which must have JetStream enabled.
package devserver

import (
	"log"
	"os"

	"github.com/nats-io/nats.go"
)

// Connect returns a connection and a function that releases it.
func Connect() (*nats.Conn, func()) {
	url := os.Getenv("NATS_URL")
	if url == "" {
		url = nats.DefaultURL
	}

	nc, err := nats.Connect(url)
	if err != nil {
		log.Fatalf("connect %s: %v\n\nStart a JetStream-enabled NATS server, for example:\n\n"+
			"\tdocker run --rm -p 4222:4222 nats:2.14.2 -js\n\n"+
			"or set NATS_URL to point to one.", url, err)
	}

	return nc, nc.Close
}
