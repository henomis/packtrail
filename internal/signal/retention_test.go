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

package signal_test

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail/internal/names"
	"github.com/henomis/packtrail/internal/natstest"
	"github.com/henomis/packtrail/internal/signal"
)

// defaultRetention mirrors the unexported constant in the signal package: the
// MaxAge EnsureStream applies when it creates the stream and nothing configured
// one.
const defaultRetention = 7 * 24 * time.Hour

// streamConfig reads the live signals-stream configuration.
func streamConfig(ctx context.Context, t *testing.T, js jetstream.JetStream, n names.Names) jetstream.StreamConfig {
	t.Helper()

	stream, err := js.Stream(ctx, n.StreamSignals)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}

	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("info: %v", err)
	}

	return info.Config
}

// A Signals that was never given a retention must not overwrite one that is
// already in place.
//
// EnsureStream issues a CreateOrUpdate, and every namespace-only participant
// provisions: reading state, starting a flow and sending a signal all run the
// same Init. Before this, each of them asserted the default MaxAge, so a single
// read-only CLI command against a fleet's namespace silently retuned that
// fleet's signal retention — and the consequence (undelivered signals expiring
// early during an outage) surfaced much later, nowhere near the cause.
func TestUnmanagedRetentionPreservesExistingMaxAge(t *testing.T) {
	ctx := context.Background()
	srv := natstest.Start(t)
	n := names.New("")

	// The fleet provisions with a tuned retention.
	fleet := signal.New(srv.JS, n)
	fleet.SetRetention(48 * time.Hour)

	if err := fleet.EnsureStream(ctx); err != nil {
		t.Fatalf("fleet ensure stream: %v", err)
	}

	// A namespace-only client provisions the same namespace, configuring nothing.
	client := signal.New(srv.JS, n)
	if err := client.EnsureStream(ctx); err != nil {
		t.Fatalf("client ensure stream: %v", err)
	}

	if got := streamConfig(ctx, t, srv.JS, n).MaxAge; got != 48*time.Hour {
		t.Fatalf("MaxAge = %v after a client provisioned; want the fleet's 48h left intact", got)
	}
}

// The dedup window is derived from retention, so it has to follow the MaxAge
// that actually won — otherwise an inherited short retention would be paired
// with a dedup window outliving the messages it deduplicates.
func TestUnmanagedRetentionPreservesCappedDedupWindow(t *testing.T) {
	ctx := context.Background()
	srv := natstest.Start(t)
	n := names.New("")

	fleet := signal.New(srv.JS, n)
	fleet.SetRetention(time.Second)

	if err := fleet.EnsureStream(ctx); err != nil {
		t.Fatalf("fleet ensure stream: %v", err)
	}

	client := signal.New(srv.JS, n)
	if err := client.EnsureStream(ctx); err != nil {
		t.Fatalf("client ensure stream: %v", err)
	}

	cfg := streamConfig(ctx, t, srv.JS, n)

	if cfg.MaxAge != time.Second {
		t.Errorf("MaxAge = %v, want 1s preserved", cfg.MaxAge)
	}

	if cfg.Duplicates != time.Second {
		t.Errorf("Duplicates = %v, want 1s — the window must follow the inherited MaxAge", cfg.Duplicates)
	}
}

// Leaving retention unmanaged must not mean "no retention": with no stream to
// inherit from, creating one still applies the default.
func TestUnmanagedRetentionAppliesDefaultOnCreate(t *testing.T) {
	ctx := context.Background()
	srv := natstest.Start(t)
	n := names.New("")

	if err := signal.New(srv.JS, n).EnsureStream(ctx); err != nil {
		t.Fatalf("ensure stream: %v", err)
	}

	if got := streamConfig(ctx, t, srv.JS, n).MaxAge; got != defaultRetention {
		t.Fatalf("MaxAge = %v on a freshly created stream, want the %v default", got, defaultRetention)
	}
}

// Inheriting applies only to the unmanaged case: an embedder that does configure
// retention is still authoritative, including when it lowers what is there.
func TestConfiguredRetentionStillOverrides(t *testing.T) {
	ctx := context.Background()
	srv := natstest.Start(t)
	n := names.New("")

	first := signal.New(srv.JS, n)
	first.SetRetention(48 * time.Hour)

	if err := first.EnsureStream(ctx); err != nil {
		t.Fatalf("first ensure stream: %v", err)
	}

	second := signal.New(srv.JS, n)
	second.SetRetention(time.Hour)

	if err := second.EnsureStream(ctx); err != nil {
		t.Fatalf("second ensure stream: %v", err)
	}

	if got := streamConfig(ctx, t, srv.JS, n).MaxAge; got != time.Hour {
		t.Fatalf("MaxAge = %v, want the explicitly configured 1h to win", got)
	}
}

// A negative retention disables the age limit, and that is a configured choice —
// it must survive a later unmanaged provisioning rather than being read back as
// "nothing set" and replaced with the default.
func TestDisabledRetentionSurvivesUnmanagedProvisioning(t *testing.T) {
	ctx := context.Background()
	srv := natstest.Start(t)
	n := names.New("")

	fleet := signal.New(srv.JS, n)
	fleet.SetRetention(-1)

	if err := fleet.EnsureStream(ctx); err != nil {
		t.Fatalf("fleet ensure stream: %v", err)
	}

	client := signal.New(srv.JS, n)
	if err := client.EnsureStream(ctx); err != nil {
		t.Fatalf("client ensure stream: %v", err)
	}

	if got := streamConfig(ctx, t, srv.JS, n).MaxAge; got != 0 {
		t.Fatalf("MaxAge = %v, want 0 (no age limit) preserved", got)
	}
}
