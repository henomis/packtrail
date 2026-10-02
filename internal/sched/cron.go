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

// Package sched holds packtrail's use of JetStream message scheduling: the
// validator for cron expressions (so a typo fails at registration instead of
// inside the engine) and the header set for durable one-shot timers and cron
// schedules.
package sched

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
)

// NATS message-scheduling headers.
const (
	HeaderSchedule       = "Nats-Schedule"
	HeaderScheduleTarget = "Nats-Schedule-Target"
	HeaderScheduleTZ     = "Nats-Schedule-Time-Zone"
	HeaderScheduler      = "Nats-Scheduler"
	HeaderScheduleNext   = "Nats-Schedule-Next"
)

// At returns the schedule pattern firing once at t. The server parses @at
// with time.RFC3339, which accepts fractional seconds, and fires a past time
// immediately. The fraction is kept: a time truncated to the second fires up
// to a second early — a 1 s attempt timeout could fire almost at once (found
// with G5-02).
func At(t time.Time) string { return "@at " + t.UTC().Format(time.RFC3339Nano) }

// ValidateCron reports whether expr is a schedule the NATS server will accept,
// without publishing anything.
//
// The grammar mirrors the server's parser (server/cron.go): the predefined `@`
// schedules, `@every <duration>` (at least one second), `@at <RFC3339 time>`,
// or six space-separated fields
//
//	second minute hour day-of-month month day-of-week
//
// each a comma-separated list of terms, where a term is `*`, `?`, a number or a
// name, optionally a `-`-range, optionally followed by `/step`. Like the
// server, empty comma terms are skipped and anything after `*`/`?` in a range
// is ignored. `@every`/`@at` arguments are fully parsed, not accepted on
// shape alone.
func ValidateCron(expr string) error {
	trimmed := strings.TrimSpace(expr)
	if trimmed == "" {
		return fmt.Errorf("cron expression is empty")
	}

	if strings.HasPrefix(trimmed, "@") {
		return validatePredefined(expr)
	}

	fields := strings.Fields(trimmed)
	if len(fields) != cronFieldCount {
		return fmt.Errorf(
			"cron %q must have %d fields (second minute hour day-of-month month day-of-week), got %d",
			expr, cronFieldCount, len(fields))
	}

	for i, f := range fields {
		if err := validateCronField(f, cronBounds[i]); err != nil {
			return fmt.Errorf("cron %q: %s field: %w", expr, cronBounds[i].label, err)
		}
	}

	return nil
}

const cronFieldCount = 6

type cronBound struct {
	label    string
	min, max uint
	names    map[string]uint
	hint     string
}

//nolint:mnd // A cron bounds table is field ranges; naming each is noise.
var (
	monthNames = map[string]uint{
		"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
		"jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
	}
	dayNames = map[string]uint{
		"sun": 0, "mon": 1, "tue": 2, "wed": 3, "thu": 4, "fri": 5, "sat": 6,
	}

	cronBounds = [cronFieldCount]cronBound{
		{label: "second", min: 0, max: 59},
		{label: "minute", min: 0, max: 59},
		{label: "hour", min: 0, max: 23},
		{label: "day-of-month", min: 1, max: 31},
		{label: "month", min: 1, max: 12, names: monthNames, hint: " or month name (jan-dec)"},
		{label: "day-of-week", min: 0, max: 6, names: dayNames, hint: " or day name (sun-sat)"},
	}
)

//nolint:goconst // the server's own spelling of each schedule.
var predefinedSchedules = []string{
	"@yearly", "@annually", "@monthly", "@weekly", "@daily", "@midnight", "@hourly",
}

const (
	everyPrefix = "@every "
	atPrefix    = "@at "
)

// validatePredefined checks the @-forms exactly as the server parses them: the
// server matches the raw pattern (no trimming), parses @every with
// time.ParseDuration requiring at least one second, and @at with RFC3339.
func validatePredefined(expr string) error {
	if slices.Contains(predefinedSchedules, expr) {
		return nil
	}

	if arg, ok := strings.CutPrefix(expr, everyPrefix); ok {
		d, err := time.ParseDuration(arg)
		if err != nil {
			return fmt.Errorf("cron %q: invalid @every duration: %w", expr, err)
		}

		if d < time.Second {
			return fmt.Errorf("cron %q: @every interval must be at least 1s", expr)
		}

		return nil
	}

	if arg, ok := strings.CutPrefix(expr, atPrefix); ok {
		if _, err := time.Parse(time.RFC3339, arg); err != nil {
			return fmt.Errorf("cron %q: @at needs an RFC3339 time: %w", expr, err)
		}

		return nil
	}

	return fmt.Errorf(
		"cron %q: unknown predefined schedule (want one of %s, @every <duration> or @at <RFC3339>)",
		expr, strings.Join(predefinedSchedules, ", "))
}

// validateCronField checks one comma-separated list of terms. Empty terms are
// skipped like the server does, but a field with no term at all never fires
// and the server rejects it at publish, so it is rejected here too.
func validateCronField(field string, b cronBound) error {
	terms := 0

	for term := range strings.SplitSeq(field, ",") {
		if term == "" {
			continue
		}

		terms++

		if err := validateCronTerm(term, b); err != nil {
			return err
		}
	}

	if terms == 0 {
		return fmt.Errorf("%q has no terms", field)
	}

	return nil
}

// validateCronTerm checks one term: (`*` | `?` | value [ `-` value ]) [ `/` step ].
func validateCronTerm(term string, b cronBound) error {
	parts := strings.Split(term, "/")
	if len(parts) > 2 { //nolint:mnd // range and step.
		return fmt.Errorf("%q has more than one /", term)
	}

	if len(parts) == 2 { //nolint:mnd // range and step.
		step, err := strconv.Atoi(parts[1])
		if err != nil || step <= 0 {
			return fmt.Errorf("%q: step must be a positive number", term)
		}
	}

	bounds := strings.Split(parts[0], "-")
	if bounds[0] == "*" || bounds[0] == "?" {
		// The server ignores anything after a wildcard start.
		return nil
	}

	if len(bounds) > 2 { //nolint:mnd // low and high.
		return fmt.Errorf("%q has more than one -", term)
	}

	low, err := cronValue(bounds[0], b)
	if err != nil {
		return fmt.Errorf("%q: %w", term, err)
	}

	if len(bounds) == 1 {
		return nil
	}

	high, err := cronValue(bounds[1], b)
	if err != nil {
		return fmt.Errorf("%q: %w", term, err)
	}

	if low > high {
		return fmt.Errorf("%q: range starts (%d) after it ends (%d)", term, low, high)
	}

	return nil
}

func cronValue(s string, b cronBound) (uint, error) {
	if b.names != nil {
		if v, ok := b.names[strings.ToLower(s)]; ok {
			return v, nil
		}
	}

	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("%q is not a number%s", s, b.hint)
	}

	if n < 0 || uint(n) < b.min || uint(n) > b.max {
		return 0, fmt.Errorf("%d is outside %d-%d", n, b.min, b.max)
	}

	return uint(n), nil
}
