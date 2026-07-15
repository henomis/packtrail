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

package runtime

import (
	"context"
	"testing"

	"github.com/henomis/packtrail/internal/store"
)

// TestRevertClaim verifies CompleteActivity's claim-revert (F-030): a still-running
// execution at the claimed node/attempt is flipped back to waiting so a redelivered
// completion can re-settle it, but an execution settleTask already advanced (or one
// that became terminal) is never rewound.
func TestRevertClaim(t *testing.T) {
	h := newHarness(t, linearFlow, Config{})
	ctx := context.Background()

	cases := []struct {
		name       string
		seed       *store.Execution
		node       string
		attempt    int
		wantStatus string
	}{
		{
			name: "reverts a running claim at node/attempt",
			seed: &store.Execution{
				ID: "revert-hit", FlowName: "linear", Status: store.StatusRunning, CurrentNode: "a", Attempt: 0,
			},
			node: "a", attempt: 0, wantStatus: store.StatusWaiting,
		},
		{
			name: "leaves an advanced execution (moved to next node)",
			seed: &store.Execution{
				ID: "revert-advanced", FlowName: "linear", Status: store.StatusRunning, CurrentNode: "b", Attempt: 0,
			},
			node: "a", attempt: 0, wantStatus: store.StatusRunning,
		},
		{
			name: "leaves a different attempt (a retry was scheduled)",
			seed: &store.Execution{
				ID: "revert-attempt", FlowName: "linear", Status: store.StatusRunning, CurrentNode: "a", Attempt: 1,
			},
			node: "a", attempt: 0, wantStatus: store.StatusRunning,
		},
		{
			name: "leaves a terminal execution untouched",
			seed: &store.Execution{
				ID: "revert-failed", FlowName: "linear", Status: store.StatusFailed, CurrentNode: "a", Attempt: 0,
			},
			node: "a", attempt: 0, wantStatus: store.StatusFailed,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := h.store.Create(ctx, tc.seed); err != nil {
				t.Fatalf("create: %v", err)
			}

			h.engine.revertClaim(ctx, tc.seed.ID, tc.node, tc.attempt)

			got, err := h.store.Get(ctx, tc.seed.ID)
			if err != nil {
				t.Fatalf("get: %v", err)
			}

			if got.Status != tc.wantStatus {
				t.Fatalf("status = %q, want %q", got.Status, tc.wantStatus)
			}
		})
	}
}
