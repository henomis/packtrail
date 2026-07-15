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

package packtrail_test

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/internal/natstest"
)

const ttlFlow = `
version: "1.0"
name: ttl-flow
nodes:
  - {id: a, type: task, subject: "tasks.a"}
edges: []
`

// TestResultCacheBucketHasTTL verifies the result-cache bucket is created with
// an entry TTL, so cached (execution, node, attempt) results expire instead of
// accumulating forever: 24h by default, custom via WithResultCacheTTL.
func TestResultCacheBucketHasTTL(t *testing.T) {
	cases := []struct {
		name    string
		opt     packtrail.Option
		wantTTL time.Duration
	}{
		{"default", packtrail.WithResultCache(), 24 * time.Hour},
		{"custom", packtrail.WithResultCacheTTL(time.Hour), time.Hour},
		{"disabled expiry", packtrail.WithResultCacheTTL(-1), 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := natstest.Start(t)
			ctx := context.Background()

			s, err := packtrail.New(srv.NC, packtrail.WithFlow([]byte(ttlFlow)), tc.opt)
			if err != nil {
				t.Fatalf("new: %v", err)
			}

			// New is pure; provisioning (including the cache bucket) happens at Init.
			if err = s.Init(ctx); err != nil {
				t.Fatalf("init: %v", err)
			}

			js, err := jetstream.New(srv.NC)
			if err != nil {
				t.Fatalf("jetstream: %v", err)
			}

			kv, err := js.KeyValue(ctx, "packtrail-result-cache")
			if err != nil {
				t.Fatalf("result-cache bucket missing: %v", err)
			}

			status, err := kv.Status(ctx)
			if err != nil {
				t.Fatalf("bucket status: %v", err)
			}

			if got := status.TTL(); got != tc.wantTTL {
				t.Fatalf("bucket TTL = %v, want %v", got, tc.wantTTL)
			}
		})
	}
}
