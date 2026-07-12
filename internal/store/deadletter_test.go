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

// The recent-records tail is bounded by the limit, returning the most recent N.
func TestRecentDeadLettersLimit(t *testing.T) {
	ctx := context.Background()
	s := open(t)

	const total = 20
	for i := range total {
		if err := s.EmitDeadLetter(ctx, DeadLetter{Kind: DeadLetterWork, Key: "exec", Reason: "boom"}); err != nil {
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
