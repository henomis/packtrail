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

package infra

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail/internal/names"
	"github.com/henomis/packtrail/internal/natstest"
)

func TestProvisionReplicas(t *testing.T) {
	s := natstest.Start(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	in, err := New(s.NC, names.New("rep"), nil)
	if err != nil {
		t.Fatal(err)
	}

	in.Replicas = 1

	if err = in.Provision(ctx, 2); err != nil {
		t.Fatal(err)
	}

	st, err := s.JS.Stream(ctx, "rep-events")
	if err != nil || st.CachedInfo().Config.Replicas != 1 {
		t.Fatalf("events replicas: %v", err)
	}

	// A single server cannot host R3: provisioning must fail loudly, not
	// silently keep R1.
	in.Replicas = 3
	if err = in.Provision(ctx, 2); err == nil {
		t.Fatal("R3 on a single server was accepted")
	}

	// The partition count is permanent.
	in.Replicas = 1
	if err = in.Provision(ctx, 4); err == nil {
		t.Fatal("partition change accepted")
	}
}

// TestProvisionReplicasOnCluster: every resource — the events stream above
// all — gets the requested replicas, and an engine started without the
// option keeps the deployed replication instead of scaling it down (F2-01).
func TestProvisionReplicasOnCluster(t *testing.T) {
	c := natstest.StartCluster(t, 3)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	in, err := New(c.NC, names.New("cl"), nil)
	if err != nil {
		t.Fatal(err)
	}

	in.Replicas = 3
	if err = in.Provision(ctx, 2); err != nil {
		t.Fatal(err)
	}

	check := func(want int) {
		t.Helper()

		for _, name := range []string{
			"cl-events", "cl-cmd", "cl-work", "cl-dlq", "KV_cl-snapshots", "KV_cl-index",
			"OBJ_cl-blobs", "OBJ_cl-archive",
		} {
			st, serr := c.JS.Stream(ctx, name)
			if serr != nil {
				t.Fatalf("%s: %v", name, serr)
			}

			if got := st.CachedInfo().Config.Replicas; got != want {
				t.Errorf("%s: replicas %d, want %d", name, got, want)
			}
		}
	}

	check(3)

	// A second engine without WithReplicas must not scale the deployment down.
	other, err := New(c.NC, names.New("cl"), nil)
	if err != nil {
		t.Fatal(err)
	}

	if err = other.Provision(ctx, 0); err != nil {
		t.Fatal(err)
	}

	check(3)
}

// TestProvisionUpdatesAnyDifference: provisioning compares the whole
// configuration, so a deployed resource that differs in any field we set is
// updated — not only in a hand-picked list (F4-02).
func TestProvisionUpdatesAnyDifference(t *testing.T) {
	s := natstest.Start(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	in, err := New(s.NC, names.New("cmp"), nil)
	if err != nil {
		t.Fatal(err)
	}

	if err = in.Provision(ctx, 2); err != nil {
		t.Fatal(err)
	}

	ev, err := s.JS.Stream(ctx, "cmp-events")
	if err != nil {
		t.Fatal(err)
	}

	drifted := ev.CachedInfo().Config
	drifted.Description = "edited by hand"

	if _, err = s.JS.UpdateStream(ctx, drifted); err != nil {
		t.Fatal(err)
	}

	kvs, err := s.JS.Stream(ctx, "KV_cmp-snapshots")
	if err != nil {
		t.Fatal(err)
	}

	kvDrift := kvs.CachedInfo().Config
	kvDrift.Compression = jetstream.NoCompression

	if _, err = s.JS.UpdateStream(ctx, kvDrift); err != nil {
		t.Fatal(err)
	}

	if err = in.Provision(ctx, 0); err != nil {
		t.Fatal(err)
	}

	if ev, err = s.JS.Stream(ctx, "cmp-events"); err != nil || ev.CachedInfo().Config.Description == "edited by hand" {
		t.Fatalf("events stream description not restored: %v", err)
	}

	if kvs, err = s.JS.Stream(ctx, "KV_cmp-snapshots"); err != nil ||
		kvs.CachedInfo().Config.Compression != jetstream.S2Compression {
		t.Fatalf("snapshot bucket compression not restored: %v", err)
	}
}

// TestProvisionIsIdempotent: right after provisioning, every resource is seen
// as up to date, so the next engine start sends no updates (a no-op update
// raced consumer creation inside nats-server).
func TestProvisionIsIdempotent(t *testing.T) {
	s := natstest.Start(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	in, err := New(s.NC, names.New("idem"), nil)
	if err != nil {
		t.Fatal(err)
	}

	if err = in.Provision(ctx, 2); err != nil {
		t.Fatal(err)
	}

	for _, cfg := range in.streamConfigs() {
		if cfg.Replicas, err = in.replicasFor(ctx, cfg.Name); err != nil {
			t.Fatal(err)
		}

		st, serr := s.JS.Stream(ctx, cfg.Name)
		if serr != nil || !streamUpToDate(cfg, st.CachedInfo().Config) {
			t.Errorf("stream %s seen as changed after provisioning", cfg.Name)
		}
	}

	for _, cfg := range in.kvConfigs() {
		if cfg.Replicas, err = in.replicasFor(ctx, "KV_"+cfg.Bucket); err != nil {
			t.Fatal(err)
		}

		if !in.kvUpToDate(ctx, cfg) {
			t.Errorf("bucket %s seen as changed after provisioning", cfg.Bucket)
		}
	}

	for _, b := range []string{in.Names.ObjectBlobs, in.Names.ObjectArchive} {
		cfg := jetstream.ObjectStoreConfig{Bucket: b, Storage: jetstream.FileStorage, Compression: true, Replicas: 1}
		if !in.objectUpToDate(ctx, cfg) {
			t.Errorf("object store %s seen as changed after provisioning", b)
		}
	}
}
