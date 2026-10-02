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

package sched

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail/internal/natstest"
)

var cronCases = []struct {
	expr string
	ok   bool
}{
	{"0 * * * * *", true},
	{"*/5 * * * * *", true},
	{"0 0 9 * * mon-fri", true},
	{"0 0 0 1 jan *", true},
	{"0 0 0 1 1 ?", true},
	{"5/10 * * * * *", true},
	{"1,,2 * * * * *", true},
	{"*-5 * * * * *", true},
	{"@daily", true},
	{"@hourly", true},
	{"@every 1s", true},
	{"@every 1h30m", true},
	{"@at 2030-01-02T03:04:05Z", true},
	{"", false},
	{"* * * * *", false},
	{"60 * * * * *", false},
	{"* * 24 * * *", false},
	{"* * * 0 * *", false},
	{"* * * * 13 *", false},
	{"* * * * * 7", false},
	{"5-1 * * * * *", false},
	{"*/0 * * * * *", false},
	{"1/2/3 * * * * *", false},
	{"1-2-3 * * * * *", false},
	{"foo * * * * *", false},
	{", * * * * *", false},
	{"@sometimes", false},
	{"@every 500ms", false},
	{"@every foo", false},
	{"@every ", false},
	{"@at tomorrow", false},
	{"@at 2030-01-02 03:04:05", false},
}

func TestValidateCron(t *testing.T) {
	for _, c := range cronCases {
		err := ValidateCron(c.expr)
		if (err == nil) != c.ok {
			t.Errorf("ValidateCron(%q) = %v, want ok=%v", c.expr, err, c.ok)
		}
	}
}

// TestValidateCronAgreesWithServer publishes every case as a real schedule to
// an embedded nats-server: the validator must accept exactly what the server
// accepts (I-22). Being stricter blocks working configurations, being looser
// moves the failure into the engine.
func TestValidateCronAgreesWithServer(t *testing.T) {
	s := natstest.Start(t)
	ctx := context.Background()

	if _, err := s.JS.CreateStream(ctx, jetstream.StreamConfig{
		Name: "S", Subjects: []string{"sched.*", "fire"}, AllowMsgSchedules: true,
	}); err != nil {
		t.Fatal(err)
	}

	for i, c := range cronCases {
		if strings.TrimSpace(c.expr) == "" {
			continue // the server treats an empty pattern as "no schedule".
		}

		m := nats.NewMsg("sched.c" + string(rune('a'+i)))
		m.Header.Set(HeaderSchedule, c.expr)
		m.Header.Set(HeaderScheduleTarget, "fire")

		_, err := s.JS.PublishMsg(ctx, m)
		if (err == nil) != c.ok {
			t.Errorf("server accepted %q = %v, validator wants ok=%v", c.expr, err, c.ok)
		}
	}
}

func TestAtFormat(t *testing.T) {
	ts := time.Date(2030, 1, 2, 3, 4, 5, 999, time.FixedZone("x", 3600))
	// UTC, with the fraction kept (a truncated time fires early).
	if got := At(ts); got != "@at 2030-01-02T02:04:05.000000999Z" {
		t.Fatalf("At = %q", got)
	}

	if err := ValidateCron(At(ts)); err != nil {
		t.Fatal(err)
	}
}

// TestAtKeepsSubSecondPrecision: a truncated @at fires up to a second early;
// the pattern keeps the fraction and still validates (G5-02).
func TestAtKeepsSubSecondPrecision(t *testing.T) {
	at := time.Date(2030, 1, 2, 3, 4, 5, 951_000_000, time.UTC)

	p := At(at)
	if p != "@at 2030-01-02T03:04:05.951Z" {
		t.Fatalf("At = %q", p)
	}

	if err := ValidateCron(p); err != nil {
		t.Fatal(err)
	}
}
