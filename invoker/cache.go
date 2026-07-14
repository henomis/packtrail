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
	"fmt"
	"log/slog"
	"strconv"
	"time"

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
	prefix   string
}

const (
	cacheEntryResult = "result"
	cacheEntryClaim  = "claim"
	cachePollDelay   = 25 * time.Millisecond
	cacheClaimTTL    = 5 * time.Minute
	cacheClaimGrace  = 5 * time.Second
)

var errCacheClaimExpired = errors.New("invoker cache: claim expired")

type cacheEntry struct {
	State      string    `json:"state"`
	Result     Result    `json:"result,omitempty"`
	ClaimUntil time.Time `json:"claim_until,omitzero"`
}

// NewCache wraps delegate so its results are deduplicated through kv.
func NewCache(kv jetstream.KeyValue, delegate Invoker) *Cache {
	return &Cache{kv: kv, delegate: delegate}
}

// NewCacheKeyed wraps delegate like NewCache but namespaces every key with
// prefix. Two Cache layers sharing one bucket must not collide on the same
// (execution, node, attempt): packtrail's engine-side dispatch cache stores
// StatusPending for an async node under that triple, while the async worker's
// execution cache stores the real result of the same triple — a shared key
// would either freeze the node at Pending or clobber the dispatch dedup.
func NewCacheKeyed(kv jetstream.KeyValue, delegate Invoker, prefix string) *Cache {
	return &Cache{kv: kv, delegate: delegate, prefix: prefix}
}

func (c *Cache) key(req Request) string {
	// KV keys allow [-/_=.a-zA-Z0-9]; execution/node ids are token-safe.
	return c.prefix + req.ExecutionID + "." + req.NodeID + "." + strconv.Itoa(req.Attempt)
}

// Invoke returns a cached Result for this (execution, node, attempt) if present;
// otherwise it calls the delegate and caches a non-error result before
// returning it.
func (c *Cache) Invoke(ctx context.Context, req Request) (Result, error) {
	key := c.key(req)

	claimRev, res, hit, err := c.getOrClaim(ctx, key, req)
	if err != nil {
		return Result{}, err
	}

	if hit {
		return res, nil
	}

	return c.invokeAndStore(ctx, key, claimRev, req)
}

func (c *Cache) getOrClaim(
	ctx context.Context, key string, req Request,
) (claimRev uint64, res Result, hit bool, err error) {
	entry, err := c.kv.Get(ctx, key)
	if err != nil {
		if !errors.Is(err, jetstream.ErrKeyNotFound) {
			return 0, Result{}, false, err
		}

		return c.claimMissing(ctx, key, req)
	}

	return c.resolveEntry(ctx, key, req, entry)
}

func (c *Cache) claimMissing(ctx context.Context, key string, req Request) (uint64, Result, bool, error) {
	rev, err := c.createClaim(ctx, key, req)
	if err == nil {
		return rev, Result{}, false, nil
	}

	if errors.Is(err, jetstream.ErrKeyExists) {
		return c.getOrClaim(ctx, key, req)
	}

	return 0, Result{}, false, err
}

func (c *Cache) resolveEntry(
	ctx context.Context, key string, req Request, entry jetstream.KeyValueEntry,
) (uint64, Result, bool, error) {
	decoded, err := decodeCacheEntry(entry.Value())
	if err != nil {
		return 0, Result{}, false, fmt.Errorf("%w: key %s", err, key)
	}

	switch decoded.State {
	case cacheEntryResult:
		return 0, decoded.Result, true, nil
	case cacheEntryClaim:
		return c.resolveClaim(ctx, key, req, entry.Revision(), decoded.ClaimUntil)
	default:
		return 0, Result{}, false, fmt.Errorf("invoker cache: unknown entry state %q for key %s", decoded.State, key)
	}
}

func (c *Cache) resolveClaim(
	ctx context.Context, key string, req Request, revision uint64, until time.Time,
) (uint64, Result, bool, error) {
	if time.Now().After(until) {
		return c.claimExpired(ctx, key, req, revision)
	}

	waited, err := c.waitForClaim(ctx, key, until)
	if err == nil {
		return 0, waited, true, nil
	}

	if errors.Is(err, errCacheClaimExpired) {
		return c.getOrClaim(ctx, key, req)
	}

	return 0, Result{}, false, err
}

func (c *Cache) claimExpired(
	ctx context.Context, key string, req Request, revision uint64,
) (uint64, Result, bool, error) {
	rev, err := c.stealExpiredClaim(ctx, key, revision, req)
	if err == nil {
		return rev, Result{}, false, nil
	}

	if errors.Is(err, jetstream.ErrKeyExists) {
		return c.getOrClaim(ctx, key, req)
	}

	return 0, Result{}, false, err
}

func (c *Cache) createClaim(ctx context.Context, key string, req Request) (uint64, error) {
	data, err := json.Marshal(cacheEntry{State: cacheEntryClaim, ClaimUntil: claimUntil(ctx, req)})
	if err != nil {
		return 0, err
	}

	return c.kv.Create(ctx, key, data)
}

func (c *Cache) stealExpiredClaim(ctx context.Context, key string, revision uint64, req Request) (uint64, error) {
	data, err := json.Marshal(cacheEntry{State: cacheEntryClaim, ClaimUntil: claimUntil(ctx, req)})
	if err != nil {
		return 0, err
	}

	return c.kv.Update(ctx, key, data, revision)
}

func claimUntil(ctx context.Context, req Request) time.Time {
	deadline := req.Deadline
	if deadline.IsZero() {
		if ctxDeadline, ok := ctx.Deadline(); ok {
			deadline = ctxDeadline
		}
	}

	if deadline.IsZero() {
		return time.Now().Add(cacheClaimTTL).UTC()
	}

	return deadline.Add(cacheClaimGrace).UTC()
}

func (c *Cache) waitForClaim(ctx context.Context, key string, claimUntil time.Time) (Result, error) {
	for {
		delay := cachePollDelay
		if remaining := time.Until(claimUntil); remaining <= 0 {
			return Result{}, errCacheClaimExpired
		} else if remaining < delay {
			delay = remaining
		}

		select {
		case <-ctx.Done():
			return Result{}, ctx.Err()
		case <-time.After(delay):
		}

		entry, err := c.kv.Get(ctx, key)
		if err != nil {
			if errors.Is(err, jetstream.ErrKeyNotFound) {
				return Result{}, errCacheClaimExpired
			}

			return Result{}, err
		}

		decoded, err := decodeCacheEntry(entry.Value())
		if err != nil {
			return Result{}, fmt.Errorf("%w: key %s", err, key)
		}

		switch decoded.State {
		case cacheEntryResult:
			return decoded.Result, nil
		case cacheEntryClaim:
			claimUntil = decoded.ClaimUntil
		default:
			return Result{}, fmt.Errorf("invoker cache: unknown entry state %q for key %s", decoded.State, key)
		}
	}
}

func (c *Cache) invokeAndStore(ctx context.Context, key string, claimRev uint64, req Request) (Result, error) {
	res, err := c.delegate.Invoke(ctx, req)
	if err != nil {
		// Transient transport failure: do not publish a result, so a redelivery can
		// steal the expired claim and re-invoke.
		c.expireClaim(ctx, key, claimRev)

		return res, err
	}

	// Cache all non-error results including StatusPending. Caching Pending is
	// intentional: a work item redelivered after a crash would otherwise
	// re-invoke the node and dispatch a second async activity. The cached
	// Pending causes re-parking instead, and the outstanding CompleteActivity
	// settles it. Consequence: if the async worker is permanently lost and
	// CompleteActivity is never called, the execution parks indefinitely —
	// async workers should carry their own timeout/failure mechanism.
	//
	// A failed Update is logged, not surfaced: the result is still returned, only
	// the dedup guarantee for a later redelivery of this attempt is lost. A
	// persistently failing cache visibly reopens the double-fire window.
	data, mErr := json.Marshal(cacheEntry{State: cacheEntryResult, Result: res})
	if mErr != nil {
		return Result{}, mErr
	}

	if _, putErr := c.kv.Update(ctx, key, data, claimRev); putErr != nil {
		slog.Debug("invoker cache: store result", "key", key, "err", putErr)
	}

	return res, nil
}

func (c *Cache) expireClaim(ctx context.Context, key string, claimRev uint64) {
	data, err := json.Marshal(cacheEntry{State: cacheEntryClaim, ClaimUntil: time.Now().UTC()})
	if err != nil {
		return
	}

	if _, err = c.kv.Update(ctx, key, data, claimRev); err != nil {
		slog.Debug("invoker cache: expire claim", "key", key, "err", err)
	}
}

func decodeCacheEntry(data []byte) (cacheEntry, error) {
	var entry cacheEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return cacheEntry{}, fmt.Errorf("invoker cache: decode entry: %w", err)
	}

	if entry.State != "" {
		return entry, nil
	}

	var legacy Result
	if err := json.Unmarshal(data, &legacy); err != nil {
		return cacheEntry{}, fmt.Errorf("invoker cache: decode legacy result: %w", err)
	}

	if legacy.Status == "" {
		return cacheEntry{}, errors.New("invoker cache: entry is neither a result nor a claim")
	}

	return cacheEntry{State: cacheEntryResult, Result: legacy}, nil
}
