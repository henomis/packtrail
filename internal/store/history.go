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

package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	historyReadWait       = 500 * time.Millisecond
	defaultHistoryReadCap = 1000
)

// EnableHistory creates (or attaches to) the per-execution history stream with
// the given retention and turns on history emission: from here on every emitted
// domain event is also appended, best-effort, to the execution's history
// subject. Until it runs, EmitEvent skips history and History returns nothing.
// The trace is observability, not operational truth — the execution document
// and the events stream stay authoritative.
func (s *Store) EnableHistory(ctx context.Context, retention time.Duration) error {
	if _, err := s.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      s.names.StreamHistory,
		Subjects:  []string{s.names.SubjHistoryPrefix + ">"},
		MaxAge:    retention,
		Storage:   jetstream.FileStorage,
		Retention: jetstream.LimitsPolicy,
	}); err != nil {
		return fmt.Errorf("history stream: %w", err)
	}

	s.historyEnabled.Store(true)

	return nil
}

// History returns execID's transition records, oldest first, up to limit
// (non-positive = a generous default). Records live for the retention passed to
// EnableHistory; with history disabled it returns nothing.
func (s *Store) History(ctx context.Context, execID string, limit int) ([]Event, error) {
	if !s.historyEnabled.Load() {
		return nil, nil
	}

	if limit <= 0 {
		limit = defaultHistoryReadCap
	}

	stream, err := s.js.Stream(ctx, s.names.StreamHistory)
	if err != nil {
		return nil, err
	}

	cons, err := stream.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{
		DeliverPolicy:  jetstream.DeliverAllPolicy,
		FilterSubjects: []string{s.names.SubjHistoryPrefix + execID},
	})
	if err != nil {
		return nil, err
	}

	batch, err := cons.Fetch(limit, jetstream.FetchMaxWait(historyReadWait))
	if err != nil {
		return nil, err
	}

	var out []Event

	for msg := range batch.Messages() {
		var ev Event
		if json.Unmarshal(msg.Data(), &ev) == nil {
			out = append(out, ev)
		}
	}

	return out, batch.Error()
}
