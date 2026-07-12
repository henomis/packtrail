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

package asyncqueue

import (
	"context"
	"time"
)

// DeadLetterSink records a job a Worker gave up on (Term'd). key is "<exec>/<node>",
// reason is the completion error, deliveries is the message's delivery count.
type DeadLetterSink func(ctx context.Context, key, reason string, deliveries uint64)

const (
	defaultConcurrency     = 64
	defaultActivityTimeout = 5 * time.Minute
	defaultAckWait         = 30 * time.Second
	defaultMaxDeliver      = 10
	defaultDrainTimeout    = 30 * time.Second
	// defaultDedupWindow must exceed the maximum expected lag between a
	// Dispatcher publishing a job and a Worker consuming it, so a redelivered
	// dispatch of the same attempt is collapsed.
	defaultDedupWindow = 2 * time.Minute
)

// config holds the tunables shared by EnsureStream (dedup window) and Worker
// (concurrency, timeouts). Zero values fall back to defaults.
type config struct {
	concurrency     int
	activityTimeout time.Duration
	ackWait         time.Duration
	dedupWindow     time.Duration
	maxDeliver      int
	drainTimeout    time.Duration
	deadLetterSink  DeadLetterSink
}

func newConfig(opts []Option) config {
	c := config{
		concurrency:     defaultConcurrency,
		activityTimeout: defaultActivityTimeout,
		ackWait:         defaultAckWait,
		dedupWindow:     defaultDedupWindow,
		maxDeliver:      defaultMaxDeliver,
		drainTimeout:    defaultDrainTimeout,
	}

	for _, o := range opts {
		o(&c)
	}

	return c
}

// Option configures a Worker and/or the work-queue stream. The same options are
// accepted by EnsureStream, NewWorker and packtrail's WithAsyncInvoker.
type Option func(*config)

// WithConcurrency caps how many jobs a Worker runs at once (default 64).
func WithConcurrency(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.concurrency = n
		}
	}
}

// WithActivityTimeout sets the ceiling/backstop for each invocation the Worker
// runs (default 5m). A node's own per-call timeout tightens this when shorter;
// the effective bound is min(node timeout, activityTimeout). A node timeout
// longer than this ceiling is capped at it, so raise this when nodes legitimately
// need longer calls. The ack window is extended by heartbeats for the whole
// duration.
func WithActivityTimeout(d time.Duration) Option {
	return func(c *config) {
		if d > 0 {
			c.activityTimeout = d
		}
	}
}

// WithAckWait sets the job ack window, extended by heartbeats while a job runs
// (default 30s). A worker that dies mid-job has its job redelivered after this.
func WithAckWait(d time.Duration) Option {
	return func(c *config) {
		if d > 0 {
			c.ackWait = d
		}
	}
}

// WithDedupWindow sets the JetStream dedup window on the work-queue stream
// (default 2m). It only affects EnsureStream.
func WithDedupWindow(d time.Duration) Option {
	return func(c *config) {
		if d > 0 {
			c.dedupWindow = d
		}
	}
}

// WithDrainTimeout sets how long a graceful Worker shutdown waits for in-flight
// jobs to settle before aborting the stragglers (default 30s). Within the window
// a clean shutdown lets running invocations finish and settle via
// CompleteActivity instead of abandoning them to redelivery.
func WithDrainTimeout(d time.Duration) Option {
	return func(c *config) {
		if d > 0 {
			c.drainTimeout = d
		}
	}
}

// WithDeadLetterSink registers a callback invoked just before a Worker Terms a job
// it gave up on (a terminal completion error or an exhausted delivery cap), so the
// caller can record a durable trace of the dropped job. packtrail wires this to the
// dead-letter stream automatically.
func WithDeadLetterSink(sink DeadLetterSink) Option {
	return func(c *config) {
		c.deadLetterSink = sink
	}
}

// WithMaxDeliver caps how many times a job is delivered before the Worker
// dead-letters it (Term, no redelivery) rather than Nak-looping on a persistent
// completion failure (default 10). A terminal completion error (e.g. the engine
// no longer knows the flow) is dead-lettered immediately, regardless of this cap.
// A non-positive value disables the cap (only terminal errors dead-letter).
func WithMaxDeliver(n int) Option {
	return func(c *config) {
		c.maxDeliver = n
	}
}
