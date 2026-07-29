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

package scheduler

import (
	"slices"
	"strings"
	"testing"
)

// TestValidateCronAccepts covers the whole grammar the server's parser accepts.
// A false rejection here is worse than the late failure this replaces — it
// blocks a schedule that would have worked — so the accepted set is checked at
// least as carefully as the rejected one.
func TestValidateCronAccepts(t *testing.T) {
	valid := []string{
		"0 */5 * * * *",     // every five minutes
		"0 0 * * * *",       // hourly
		"0 0 0 1 1 *",       // new year
		"* * * * * *",       // every second
		"0 30 9 * * 1-5",    // weekday mornings
		"0 0 12 * * mon",    // day name
		"0 0 0 1 jan,jul *", // month names in a list
		"15,45 0 0 * * *",   // list of values
		"0 0 0 ? * *",       // ? is a synonym for * in the day fields
		"0 0 0 1-15/2 * *",  // range with a step
		"0 0 0 5/3 * *",     // "from 5 onwards, every 3"
		"@every 1h30m",
		"@at 2026-01-01T00:00:00Z",
		"  0 0 * * * *  ", // surrounding whitespace
	}

	// Every predefined schedule the server rewrites, taken from the list itself
	// so one added there without a test cannot slip through.
	valid = slices.Concat(valid, predefinedSchedules)

	for _, expr := range valid {
		if err := ValidateCron(expr); err != nil {
			t.Errorf("ValidateCron(%q) = %v, want nil", expr, err)
		}
	}
}

func TestValidateCronRejects(t *testing.T) {
	cases := []struct {
		expr, why string
	}{
		{"", "empty"},
		{"* * * * *", "five fields: the standard-cron shape, which this parser does not accept"},
		{"* * * * * * *", "seven fields"},
		{"0 0 25 * * *", "hour 25"},
		{"60 0 0 * * *", "second 60"},
		{"0 0 0 0 * *", "day-of-month 0"},
		{"0 0 0 * 13 *", "month 13"},
		{"0 0 0 * * 7", "day-of-week 7 — some dialects allow it as Sunday, this one does not"},
		{"0 0 0 * * -1", "negative"},
		{"0 0/0 * * * *", "zero step"},
		{"0 0/x * * * *", "non-numeric step"},
		{"0 30-10 * * * *", "reversed range"},
		{"0 0 0 * * funday", "unknown day name"},
		{"0 0 0 * hamburger *", "unknown month name"},
		{"@nightly", "unknown predefined schedule"},
		{"@every", "@every with no duration"},
		{"0 1-2-3 * * * *", "two hyphens"},
		{"0 */2/3 * * * *", "two slashes"},
	}

	for _, c := range cases {
		if err := ValidateCron(c.expr); err == nil {
			t.Errorf("ValidateCron(%q) = nil, want an error (%s)", c.expr, c.why)
		}
	}
}

// TestValidateCronErrorNamesTheField: an operator fixing a cron needs to know
// which of six positional fields is wrong.
func TestValidateCronErrorNamesTheField(t *testing.T) {
	err := ValidateCron("0 0 99 * * *")
	if err == nil {
		t.Fatal("ValidateCron accepted hour 99")
	}

	const hourField = 2

	for _, want := range []string{cronBounds[hourField].label, "99", "0-23"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}
