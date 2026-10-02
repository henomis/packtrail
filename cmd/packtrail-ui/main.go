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

// Command packtrail-ui is the debugging dashboard: execution list, flow graph
// coloured by node state, the event timeline with the state at any sequence
// (time travel), and actions — signal, resume, cancel, fork, rerun, dead-letter
// redrive. It is a client of the deployment: it needs only NATS.
//
// It serves every namespace provisioned on the NATS account (switchable from
// the header), or only those listed with -namespaces; -namespace is the one
// selected first.
//
// There is no built-in authentication and the UI can drive executions: it
// binds to loopback by default; put an authenticating reverse proxy in front
// before exposing it.
package main

import (
	"context"
	"embed"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/henomis/packtrail/internal/names"
)

//go:embed web
var webFS embed.FS

const (
	readHeaderTimeout = 5 * time.Second
	shutdownTimeout   = 5 * time.Second
)

func main() {
	addr := flag.String("addr", envOr("PACKTRAIL_UI_ADDR", "127.0.0.1:8088"),
		"HTTP listen address; there is no built-in authentication and the UI can drive executions")
	ns := flag.String("namespace", envOr("PACKTRAIL_NAMESPACE", "packtrail"), "namespace selected first")
	allowFlag := flag.String("namespaces", os.Getenv("PACKTRAIL_UI_NAMESPACES"),
		"comma-separated namespaces the UI may serve (default: every namespace on the NATS account)")

	flag.Parse()

	allow, err := parseAllow(*allowFlag)
	if err != nil {
		slog.Error("namespaces", "err", err)
		os.Exit(1)
	}

	url := envOr("NATS_URL", nats.DefaultURL)

	nc, err := nats.Connect(url, nats.Name("packtrail-ui"))
	if err != nil {
		slog.Error("connect NATS", "url", url, "err", err)
		os.Exit(1)
	}
	defer nc.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	static, _ := fs.Sub(webFS, "web")
	srv := &http.Server{
		Addr: *addr, Handler: newAPI(nc, *ns, allow).routes(http.FileServerFS(static)), ReadHeaderTimeout: readHeaderTimeout,
	}

	go func() {
		<-ctx.Done()

		sh, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()

		_ = srv.Shutdown(sh)
	}()

	slog.Warn("packtrail-ui has no authentication and can drive executions; keep it on a trusted address")
	slog.Info("packtrail-ui listening", "addr", *addr, "namespace", *ns, "allow", allow, "nats", url)

	if err = srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("http", "err", err)
		os.Exit(1)
	}
}

// parseAllow parses the -namespaces allowlist; nil means no restriction.
func parseAllow(s string) ([]string, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}

	var out []string

	for p := range strings.SplitSeq(s, ",") {
		p = strings.TrimSpace(p)
		if p == "" || slices.Contains(out, p) {
			continue
		}

		if !names.ValidPrefix(p) {
			return nil, fmt.Errorf("invalid namespace %q: must match [A-Za-z0-9_-]{1,64}", p)
		}

		out = append(out, p)
	}

	slices.Sort(out)

	return out, nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}

	return def
}
