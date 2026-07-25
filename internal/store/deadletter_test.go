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
	"strconv"
	"testing"
)

// A dead-letter record is durably appended and observable via both the durable
// stream depth (DeadLetterCount) and the in-process counter (DeadLetters), and
// the recent-records tail reads back what was emitted, oldest-first.
func TestEmitAndReadDeadLetters(t *testing.T) {
	ctx := context.Background()
	s := open(t)

	if n, err := s.DeadLetterCount(ctx); err != nil || n != 0 {
		t.Fatalf("initial DeadLetterCount = %d, %v; want 0", n, err)
	}

	emitted := []DeadLetter{
		{Kind: DeadLetterWork, Key: "exec-1", Reason: "unknown node", Deliveries: 1},
		{Kind: DeadLetterSchedule, Key: "start.gone", Reason: "unknown flow", Deliveries: 1},
		{Kind: DeadLetterAsync, Key: "exec-2/node-a", Reason: "unknown flow", Deliveries: 4},
	}
	for _, dl := range emitted {
		if err := s.EmitDeadLetter(ctx, dl); err != nil {
			t.Fatalf("emit %+v: %v", dl, err)
		}
	}

	if got := s.DeadLetters(); got != uint64(len(emitted)) {
		t.Fatalf("DeadLetters() = %d, want %d", got, len(emitted))
	}

	count, err := s.DeadLetterCount(ctx)
	if err != nil || count != uint64(len(emitted)) {
		t.Fatalf("DeadLetterCount = %d, %v; want %d", count, err, len(emitted))
	}

	recent, err := s.RecentDeadLetters(ctx, 10)
	if err != nil {
		t.Fatalf("recent: %v", err)
	}

	if len(recent) != len(emitted) {
		t.Fatalf("recent has %d records, want %d", len(recent), len(emitted))
	}

	// Oldest-first: the order matches emission order.
	for i, want := range emitted {
		if recent[i].Kind != want.Kind || recent[i].Key != want.Key || recent[i].Reason != want.Reason {
			t.Fatalf("recent[%d] = %+v, want kind/key/reason of %+v", i, recent[i], want)
		}

		if recent[i].Time.IsZero() {
			t.Fatalf("recent[%d] has zero time; EmitDeadLetter should stamp it", i)
		}
	}
}

// TestEmitDeadLetterDedupesSameKindAndKey is a regression test: a caller that
// records a dead-letter trace and then fails to msg.Term() the poisoned
// message sees it redelivered and re-dead-letters the same kind+key. Without
// msg-id dedup on the publish, that produced two records for one poisoned
// message instead of one.
func TestEmitDeadLetterDedupesSameKindAndKey(t *testing.T) {
	ctx := context.Background()
	s := open(t)

	dl := DeadLetter{Kind: DeadLetterWork, Key: "exec-1", Reason: "first attempt"}
	if err := s.EmitDeadLetter(ctx, dl); err != nil {
		t.Fatalf("emit 1: %v", err)
	}

	// Simulate the Term() failure retry: same kind+key, shortly after.
	dl.Reason = "retry after failed Term"
	if err := s.EmitDeadLetter(ctx, dl); err != nil {
		t.Fatalf("emit 2: %v", err)
	}

	count, err := s.DeadLetterCount(ctx)
	if err != nil {
		t.Fatalf("count: %v", err)
	}

	if count != 1 {
		t.Fatalf("DeadLetterCount = %d, want 1 (second emit for the same kind+key should have been deduped)", count)
	}

	// A different key must not be deduped against it.
	if emitErr := s.EmitDeadLetter(ctx, DeadLetter{Kind: DeadLetterWork, Key: "exec-2", Reason: "unrelated"}); emitErr != nil {
		t.Fatalf("emit 3: %v", emitErr)
	}

	if count, err = s.DeadLetterCount(ctx); err != nil || count != 2 {
		t.Fatalf("DeadLetterCount = %d, %v; want 2", count, err)
	}
}

// The recent-records tail is bounded by the limit, returning the most recent N.
func TestRecentDeadLettersLimit(t *testing.T) {
	ctx := context.Background()
	s := open(t)

	const total = 20
	for i := range total {
		// Distinct keys: EmitDeadLetter dedups same kind+key within
		// deadLetterDedupWindow (see TestEmitDeadLetterDedupesSameKindAndKey).
		key := "exec-" + strconv.Itoa(i)
		if err := s.EmitDeadLetter(ctx, DeadLetter{Kind: DeadLetterWork, Key: key, Reason: "boom"}); err != nil {
			t.Fatalf("emit %d: %v", i, err)
		}
	}

	recent, err := s.RecentDeadLetters(ctx, 5)
	if err != nil {
		t.Fatalf("recent: %v", err)
	}

	if len(recent) != 5 {
		t.Fatalf("recent capped wrong: got %d, want 5", len(recent))
	}
}
