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

package invoker

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"

	"github.com/nats-io/nats.go/jetstream"
)

// Cache is an Invoker decorator that makes invocations idempotent under packtrail's
// at-least-once delivery. Every step is driven by a durable work message; if an
// engine crashes after invoking a node but before persisting the advance/ack,
// the work item is redelivered and the node would otherwise run twice — double
// side effects (a re-billed LLM call, a duplicate write, a second e-mail).
//
// Cache keys a stored Result by (execution, node, attempt). A redelivery of the
// same attempt is served from the cache and never reaches the delegate, while a
// genuine retry (a new attempt number) gets a fresh key and does re-run, exactly
// as the node's retry policy intends. Transport errors are never cached, so a
// failed call is always retried.
//
// Cache solves the engine-side double-dispatch window. It cannot make a
// non-deterministic task deterministic; an Invoker with external side effects
// it cannot see should still carry its own idempotency key where it can.
type Cache struct {
	kv       jetstream.KeyValue
	delegate Invoker
}

// NewCache wraps delegate so its results are deduplicated through kv.
func NewCache(kv jetstream.KeyValue, delegate Invoker) *Cache {
	return &Cache{kv: kv, delegate: delegate}
}

func cacheKey(req Request) string {
	// KV keys allow [-/_=.a-zA-Z0-9]; execution/node ids are token-safe.
	return req.ExecutionID + "." + req.NodeID + "." + strconv.Itoa(req.Attempt)
}

// Invoke returns a cached Result for this (execution, node, attempt) if present;
// otherwise it calls the delegate and caches a non-error result before
// returning it.
func (c *Cache) Invoke(ctx context.Context, req Request) (Result, error) {
	key := cacheKey(req)
	if entry, err := c.kv.Get(ctx, key); err == nil {
		var r Result
		if json.Unmarshal(entry.Value(), &r) == nil {
			return r, nil
		}
	} else if !errors.Is(err, jetstream.ErrKeyNotFound) {
		return Result{}, err
	}

	res, err := c.delegate.Invoke(ctx, req)
	if err != nil {
		// Transient transport failure: do not cache, so a redelivery re-invokes.
		return res, err
	}

	// Cache all non-error results including StatusPending. Caching Pending is
	// intentional: a work item redelivered after a crash would otherwise
	// re-invoke the node and dispatch a second async activity. The cached
	// Pending causes re-parking instead, and the outstanding CompleteActivity
	// settles it. Consequence: if the async worker is permanently lost and
	// CompleteActivity is never called, the execution parks indefinitely —
	// async workers should carry their own timeout/failure mechanism.
	if data, mErr := json.Marshal(res); mErr == nil {
		_, _ = c.kv.Put(ctx, key, data)
	}

	return res, nil
}
