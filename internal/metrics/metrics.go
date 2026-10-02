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

// Package metrics counts what the engine does, without any metrics library:
// the embedding application exports the snapshot however it likes.
package metrics

import (
	"sync/atomic"
	"time"
)

// M holds the counters of one engine.
type M struct {
	Commands       atomic.Int64
	Conflicts      atomic.Int64
	EventsAppended atomic.Int64
	DeadLetters    atomic.Int64
	JobsDispatched atomic.Int64
	CacheHits      atomic.Int64
	Timers         atomic.Int64
	Archived       atomic.Int64
	// LatencyNanos accumulates command handling time (decide + append).
	LatencyNanos atomic.Int64
}

// Snapshot is a point-in-time copy of the counters.
type Snapshot struct {
	Commands       int64 `json:"commands"`
	Conflicts      int64 `json:"conflicts"`
	EventsAppended int64 `json:"events_appended"`
	DeadLetters    int64 `json:"dead_letters"`
	JobsDispatched int64 `json:"jobs_dispatched"`
	CacheHits      int64 `json:"cache_hits"`
	Timers         int64 `json:"timers"`
	Archived       int64 `json:"archived"`
	LatencyNanos   int64 `json:"latency_nanos"`
	// ProjectionLag is the number of events not yet processed by the
	// dispatcher (filled by the engine on request).
	ProjectionLag uint64 `json:"projection_lag"`
	// DispatchStall is the longest time any dispatcher partition with pending
	// events has gone without acknowledging one; DispatchStalls has it per
	// partition. The dispatcher retries transient failures forever, so this
	// is the signal to alert on (e.g. above one minute).
	DispatchStall  time.Duration         `json:"dispatch_stall"`
	DispatchStalls map[int]time.Duration `json:"dispatch_stalls,omitempty"`
}

// Snapshot returns the current values.
func (m *M) Snapshot() Snapshot {
	return Snapshot{
		Commands: m.Commands.Load(), Conflicts: m.Conflicts.Load(), EventsAppended: m.EventsAppended.Load(),
		DeadLetters: m.DeadLetters.Load(), JobsDispatched: m.JobsDispatched.Load(), CacheHits: m.CacheHits.Load(),
		Timers: m.Timers.Load(), Archived: m.Archived.Load(), LatencyNanos: m.LatencyNanos.Load(),
	}
}
