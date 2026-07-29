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
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// ValidateCron reports whether expr is a schedule the NATS server will accept,
// without publishing anything.
//
// The server validates a schedule only on the first publish that installs it —
// which, for a schedule installed by the engine, happens inside the engine
// goroutine after startup has succeeded. A typo therefore passed construction,
// let the whole deployment come up, and then killed the engine with an opaque
// failure some time later. This turns that into an error at New.
//
// The grammar mirrors the server's parser (server/cron.go, derived from
// robfig/cron): the five predefined `@` schedules, `@every <duration>`,
// `@at <time>`, or six space-separated fields
//
//	second minute hour day-of-month month day-of-week
//
// each a comma-separated list of terms, where a term is `*`, `?`, a number, or a
// name, optionally a `-`-range, optionally followed by `/step`.
//
// It is deliberately no stricter than the server: rejecting something the server
// would have accepted is worse than the late error this replaces, because it
// blocks a working configuration. The two forms whose argument the server parses
// with its own time/duration helpers (`@every`, `@at`) are accepted on shape
// alone for that reason.
func ValidateCron(expr string) error {
	trimmed := strings.TrimSpace(expr)
	if trimmed == "" {
		return fmt.Errorf("cron expression is empty")
	}

	if strings.HasPrefix(trimmed, "@") {
		return validatePredefined(trimmed)
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

// cronBound is one field's accepted range and names, mirroring the server's
// bounds table. hint is appended to a "not a number" error so the message names
// the alternative spelling that field accepts.
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
		// 7 is NOT a second Sunday here, unlike some cron dialects.
		{label: "day-of-week", min: 0, max: 6, names: dayNames, hint: " or day name (sun-sat)"},
	}
)

// predefinedSchedules are the fixed schedules the server rewrites into cron
// patterns. Kept as a slice so the accepted set and the error listing it cannot
// disagree.
var predefinedSchedules = []string{
	"@yearly", "@annually", "@monthly", "@weekly", "@daily", "@midnight", "@hourly",
}

// argumentSchedules take an argument the server parses with its own time and
// duration helpers. Only the presence of that argument is checked here:
// re-implementing those parsers risks rejecting something the server accepts,
// which is worse than the late failure this validator replaces.
var argumentSchedules = []string{"@every ", "@at "}

func validatePredefined(expr string) error {
	if slices.Contains(predefinedSchedules, expr) {
		return nil
	}

	for _, prefix := range argumentSchedules {
		if strings.HasPrefix(expr, prefix) && strings.TrimSpace(strings.TrimPrefix(expr, prefix)) != "" {
			return nil
		}
	}

	return fmt.Errorf(
		"cron %q: unknown predefined schedule (want one of %s, or %s<argument>)",
		expr, strings.Join(predefinedSchedules, ", "), strings.Join(argumentSchedules, "/ "))
}

// validateCronField checks one comma-separated list of terms.
func validateCronField(field string, b cronBound) error {
	for term := range strings.SplitSeq(field, ",") {
		if err := validateCronTerm(term, b); err != nil {
			return err
		}
	}

	return nil
}

// validateCronTerm checks one term: (`*` | `?` | value [ `-` value ]) [ `/` step ].
func validateCronTerm(term string, b cronBound) error {
	rangeExpr, stepExpr, hasStep := strings.Cut(term, "/")

	if hasStep {
		if strings.Contains(stepExpr, "/") {
			return fmt.Errorf("%q has more than one /", term)
		}

		step, err := strconv.Atoi(stepExpr)
		if err != nil || step <= 0 {
			return fmt.Errorf("%q: step must be a positive number", term)
		}
	}

	if rangeExpr == "*" || rangeExpr == "?" {
		return nil
	}

	lo, hi, isRange := strings.Cut(rangeExpr, "-")

	low, err := cronValue(lo, b)
	if err != nil {
		return fmt.Errorf("%q: %w", term, err)
	}

	if !isRange {
		return nil
	}

	if strings.Contains(hi, "-") {
		return fmt.Errorf("%q has more than one -", term)
	}

	high, err := cronValue(hi, b)
	if err != nil {
		return fmt.Errorf("%q: %w", term, err)
	}

	if low > high {
		return fmt.Errorf("%q: range starts (%d) after it ends (%d)", term, low, high)
	}

	return nil
}

// cronValue resolves one number or name and bounds-checks it.
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
