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

// Command packtrail-ui is a read-only observability dashboard for a packtrail
// deployment. It connects to the same NATS cluster, reads execution state and
// the flow registry, tails the live event stream, and serves a small web UI.
// It never drives executions — it is purely an observer.
package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/henomis/packtrail"
)

const (
	readHeaderTimeout = 5 * time.Second
	shutdownTimeout   = 5 * time.Second
)

func main() {
	var (
		addr = flag.String("addr", envOr("PACKTRAIL_UI_ADDR", ":8088"),
			"HTTP listen address [$PACKTRAIL_UI_ADDR]. There is no built-in authentication: "+
				"anything reachable at this address can read every execution's payloads, "+
				"history, errors and dead-letters. Bind to a loopback/private address, or "+
				"put a reverse proxy with auth in front, before exposing beyond a trusted network.")
		namespace = flag.String(
			"namespace",
			envOr("PACKTRAIL_NAMESPACE", "packtrail"),
			"packtrail namespace prefix to observe [$PACKTRAIL_NAMESPACE]",
		)
	)

	flag.Parse()

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))

	url := envOr("NATS_URL", nats.DefaultURL)

	nc, err := nats.Connect(url, nats.Name("packtrail-ui"))
	if err != nil {
		slog.Error("connect NATS", "url", url, "err", err)
		os.Exit(1)
	}
	defer nc.Close()

	srv, err := packtrail.New(nc, packtrail.WithNamespace(*namespace))
	if err != nil {
		slog.Error("packtrail server", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	api := newAPI(srv)
	httpSrv := &http.Server{
		Addr:              *addr,
		Handler:           api.routes(),
		ReadHeaderTimeout: readHeaderTimeout,
	}

	go func() {
		<-ctx.Done()

		sh, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()

		_ = httpSrv.Shutdown(sh)
	}()

	slog.Warn("packtrail-ui has no built-in authentication; anything that can reach addr can read " +
		"every execution's payloads, history, errors and dead-letters. Bind to a loopback/private " +
		"address or put an authenticating reverse proxy in front before exposing it beyond a trusted network.")
	slog.Info("packtrail-ui listening", "addr", *addr, "namespace", *namespace, "nats", url)

	err = httpSrv.ListenAndServe()
	if err != nil && err != http.ErrServerClosed {
		slog.Error("http server", "err", err)
		os.Exit(1)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}

	return def
}
