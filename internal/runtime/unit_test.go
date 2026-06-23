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
	"errors"
	"testing"
	"time"

	"github.com/henomis/packtrail/internal/dsl"
	"github.com/henomis/packtrail/invoker"
)

func TestSettleResult(t *testing.T) {
	// A transport error normalises to a retry carrying the error text.
	got := settleResult(invoker.Result{Status: invoker.StatusOK}, errors.New("boom"))
	if got.Status != invoker.StatusRetry || got.Error != "boom" {
		t.Fatalf("settleResult(err) = %+v, want retry/boom", got)
	}

	// With no transport error the result passes through unchanged.
	in := invoker.Result{Status: invoker.StatusOK, Payload: []byte(`{"x":1}`)}
	if got = settleResult(in, nil); got.Status != invoker.StatusOK || string(got.Payload) != `{"x":1}` {
		t.Fatalf("settleResult(ok) = %+v, want passthrough", got)
	}
}

func TestRetryReason(t *testing.T) {
	if got := retryReason(invoker.Result{}, errors.New("transport")); got != "transport" {
		t.Errorf("retryReason(callErr) = %q, want transport", got)
	}

	if got := retryReason(invoker.Result{Error: "explicit"}, nil); got != "explicit" {
		t.Errorf("retryReason(res.Error) = %q, want explicit", got)
	}

	if got := retryReason(invoker.Result{}, nil); got != "retry requested" {
		t.Errorf("retryReason(empty) = %q, want 'retry requested'", got)
	}
}

func TestBackoff(t *testing.T) {
	const (
		base = 100 * time.Millisecond
		cap  = 10 * time.Second
	)

	cases := []struct {
		name    string
		kind    string
		attempt int
		want    time.Duration
	}{
		{"fixed", "fixed", 1, base},
		{"fixed default empty", "", 3, base},
		{"linear attempt 1", "linear", 1, base},
		{"linear attempt 3", "linear", 3, 3 * base},
		{"exponential attempt 1", "exponential", 1, base},
		{"exponential attempt 4", "exponential", 4, base << 3},
		{"capped at max", "exponential", 30, cap}, // 100ms<<29 overflows past cap
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			node := &dsl.Node{Retry: &dsl.RetryPolicy{Backoff: c.kind}}
			if got := backoff(node, c.attempt, base, cap); got != c.want {
				t.Errorf("backoff(%s, %d) = %v, want %v", c.kind, c.attempt, got, c.want)
			}
		})
	}

	// A node with no retry policy uses the fixed default.
	if got := backoff(&dsl.Node{}, 1, base, cap); got != base {
		t.Errorf("backoff(no policy) = %v, want %v", got, base)
	}
}
