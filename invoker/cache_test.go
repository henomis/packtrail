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

package invoker_test

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail/internal/natstest"
	"github.com/henomis/packtrail/invoker"
)

// TestCacheDedupesSameAttempt verifies the idempotency contract: a redelivery of
// the same (execution, node, attempt) is served from the cache and never reaches
// the delegate, while a new attempt does re-invoke — exactly what a retry needs.
func TestCacheDedupesSameAttempt(t *testing.T) {
	ctx := context.Background()
	srv := natstest.Start(t)

	kv, err := srv.JS.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "test-cache"})
	if err != nil {
		t.Fatalf("kv: %v", err)
	}

	var calls atomic.Int32

	delegate := invoker.Func(func(_ context.Context, req invoker.Request) (invoker.Result, error) {
		calls.Add(1)
		return invoker.Result{Status: invoker.StatusOK, Payload: req.Payload}, nil
	})
	cache := invoker.NewCache(kv, delegate)

	req := invoker.Request{
		ExecutionID: "exec-1", NodeID: "triage", Attempt: 0,
		Payload: json.RawMessage(`{"v":1}`),
	}

	// First call hits the delegate; a redelivery of the same attempt is cached.
	for i := range 3 {
		res, invokeErr := cache.Invoke(ctx, req)
		if invokeErr != nil {
			t.Fatalf("invoke %d: %v", i, invokeErr)
		}

		if res.Status != invoker.StatusOK || string(res.Payload) != `{"v":1}` {
			t.Fatalf("invoke %d: unexpected result %+v", i, res)
		}
	}

	if got := calls.Load(); got != 1 {
		t.Fatalf("delegate called %d times for same attempt, want 1", got)
	}

	// A genuine retry (new attempt) must re-invoke.
	req.Attempt = 1
	if _, invokeErr := cache.Invoke(ctx, req); invokeErr != nil {
		t.Fatalf("retry invoke: %v", invokeErr)
	}

	if got := calls.Load(); got != 2 {
		t.Fatalf("delegate called %d times after retry, want 2", got)
	}
}

// TestCacheKeyedSeparatesKeyspaces encodes the two-layer contract behind
// packtrail's async result caching: the engine-side dispatch cache stores
// StatusPending for an async node under (execution, node, attempt), and the
// worker-side execution cache stores the real result of the *same* triple in
// the same bucket. With distinct key prefixes each layer sees only its own
// entry; sharing a key would freeze the node at Pending (the worker would read
// the dispatcher's entry and never invoke) or clobber the dispatch dedup.
func TestCacheKeyedSeparatesKeyspaces(t *testing.T) {
	ctx := context.Background()
	srv := natstest.Start(t)

	kv, err := srv.JS.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "test-cache-keyed"})
	if err != nil {
		t.Fatalf("kv: %v", err)
	}

	var dispatches, execs atomic.Int32

	dispatch := invoker.NewCache(kv, invoker.Func(func(context.Context, invoker.Request) (invoker.Result, error) {
		dispatches.Add(1)
		return invoker.Result{Status: invoker.StatusPending}, nil
	}))

	work := invoker.NewCacheKeyed(kv, invoker.Func(func(context.Context, invoker.Request) (invoker.Result, error) {
		execs.Add(1)
		return invoker.Result{Status: invoker.StatusOK, Payload: json.RawMessage(`{"done":true}`)}, nil
	}), "w.")

	req := invoker.Request{ExecutionID: "exec-1", NodeID: "agent", Attempt: 0}

	// Dispatch caches Pending under the unprefixed key.
	if res, invokeErr := dispatch.Invoke(ctx, req); invokeErr != nil || res.Status != invoker.StatusPending {
		t.Fatalf("dispatch: res=%+v err=%v, want pending", res, invokeErr)
	}

	// The worker must not see the cached Pending: it invokes and gets OK.
	res, invokeErr := work.Invoke(ctx, req)
	if invokeErr != nil || res.Status != invoker.StatusOK {
		t.Fatalf("worker: res=%+v err=%v, want ok (collided with dispatch entry?)", res, invokeErr)
	}

	// A redelivered job serves the worker's cached result without re-invoking.
	if res, invokeErr = work.Invoke(ctx, req); invokeErr != nil || res.Status != invoker.StatusOK {
		t.Fatalf("worker redelivery: res=%+v err=%v, want cached ok", res, invokeErr)
	}

	// And the dispatch layer still sees its own Pending, untouched by the worker.
	if res, invokeErr = dispatch.Invoke(ctx, req); invokeErr != nil || res.Status != invoker.StatusPending {
		t.Fatalf("dispatch redelivery: res=%+v err=%v, want cached pending", res, invokeErr)
	}

	if d, e := dispatches.Load(), execs.Load(); d != 1 || e != 1 {
		t.Fatalf("dispatches=%d execs=%d, want 1/1 (each layer invoked once, then cached)", d, e)
	}
}

// TestCacheDoesNotCacheTransportError ensures a transport failure is not cached,
// so a redelivery retries the call rather than replaying the error.
func TestCacheDoesNotCacheTransportError(t *testing.T) {
	ctx := context.Background()
	srv := natstest.Start(t)

	kv, err := srv.JS.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "test-cache-err"})
	if err != nil {
		t.Fatalf("kv: %v", err)
	}

	var calls atomic.Int32

	delegate := invoker.Func(func(_ context.Context, _ invoker.Request) (invoker.Result, error) {
		n := calls.Add(1)
		if n == 1 {
			return invoker.Result{}, context.DeadlineExceeded // transient
		}

		return invoker.Result{Status: invoker.StatusOK}, nil
	})
	cache := invoker.NewCache(kv, delegate)
	req := invoker.Request{ExecutionID: "exec-2", NodeID: "n", Attempt: 0}

	if _, invokeErr := cache.Invoke(ctx, req); invokeErr == nil {
		t.Fatal("expected transport error on first call")
	}

	res, err := cache.Invoke(ctx, req)
	if err != nil {
		t.Fatalf("second call: %v", err)
	}

	if res.Status != invoker.StatusOK {
		t.Fatalf("second call status = %q, want ok", res.Status)
	}

	if got := calls.Load(); got != 2 {
		t.Fatalf("delegate called %d times, want 2 (error not cached)", got)
	}
}

// TestCacheKVGetError verifies a KV lookup failure that is not "key not found"
// is surfaced to the caller and the delegate is never reached.
func TestCacheKVGetError(t *testing.T) {
	srv := natstest.Start(t)

	kv, err := srv.JS.CreateKeyValue(context.Background(), jetstream.KeyValueConfig{Bucket: "test-cache-get-err"})
	if err != nil {
		t.Fatalf("kv: %v", err)
	}

	var calls atomic.Int32

	delegate := invoker.Func(func(context.Context, invoker.Request) (invoker.Result, error) {
		calls.Add(1)
		return invoker.Result{Status: invoker.StatusOK}, nil
	})
	cache := invoker.NewCache(kv, delegate)

	// A cancelled context makes the KV Get fail with something other than
	// ErrKeyNotFound, exercising the error-propagation branch.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, invokeErr := cache.Invoke(ctx, invoker.Request{ExecutionID: "e", NodeID: "n", Attempt: 0}); invokeErr == nil {
		t.Fatal("expected error from KV Get with cancelled context")
	}

	if got := calls.Load(); got != 0 {
		t.Fatalf("delegate called %d times, want 0 (KV error short-circuits)", got)
	}
}
