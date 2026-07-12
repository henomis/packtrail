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
	"testing"
	"time"

	"github.com/henomis/packtrail/internal/dsl"
)

// Exponential backoff must never produce a delay outside (0, maxDelay], even for
// a large attempt where base << (attempt-1) overflows int64. An overflow that
// wrapped into (0, maxDelay] would slip past the clamp and turn the intended long
// backoff into a spuriously short one; the shift must saturate to maxDelay instead.
func TestBackoffExponentialSaturates(t *testing.T) {
	node := &dsl.Node{Retry: &dsl.RetryPolicy{Backoff: backoffKindExponential}}

	base := time.Second
	maxDelay := 60 * time.Second

	for attempt := 1; attempt <= 200; attempt++ {
		d := backoff(node, attempt, base, maxDelay)
		if d <= 0 || d > maxDelay {
			t.Fatalf("attempt %d: backoff %v out of (0, %v] — overflow leaked past the clamp", attempt, d, maxDelay)
		}
	}

	// A high attempt that would overflow must land on maxDelay (saturated), not a
	// wrapped small value.
	for _, attempt := range []int{40, 50, 63, 64, 100} {
		if d := backoff(node, attempt, base, maxDelay); d != maxDelay {
			t.Fatalf("attempt %d: backoff %v, want saturated %v", attempt, d, maxDelay)
		}
	}
}

// The early exponential attempts still grow normally up to the cap.
func TestBackoffExponentialGrows(t *testing.T) {
	node := &dsl.Node{Retry: &dsl.RetryPolicy{Backoff: backoffKindExponential}}

	base := time.Second
	maxDelay := 60 * time.Second

	want := []time.Duration{base, 2 * base, 4 * base, 8 * base, 16 * base, 32 * base}
	for i, w := range want {
		if d := backoff(node, i+1, base, maxDelay); d != w {
			t.Fatalf("attempt %d: backoff %v, want %v", i+1, d, w)
		}
	}
}
