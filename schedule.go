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

package packtrail

import (
	"github.com/henomis/packtrail/internal/dsl"
	"github.com/henomis/packtrail/internal/scheduler"
)

// ValidateCron reports whether expr is a schedule the NATS server will accept,
// without publishing anything. It accepts the six-field form
//
//	second minute hour day-of-month month day-of-week
//
// the predefined `@yearly` / `@annually` / `@monthly` / `@weekly` / `@daily` /
// `@midnight` / `@hourly`, and the `@every <duration>` / `@at <time>` forms.
//
// [New] applies it to [WithReconcileActive] and [WithReconcileFull], and
// [Server.ScheduleFlow] applies it to its argument, so most callers never need
// it directly. It is exported for a layer that accepts cron expressions from its
// own configuration and wants to reject them where the author can see the error,
// rather than shipping a second, drifting copy of this grammar.
func ValidateCron(expr string) error {
	return scheduler.ValidateCron(expr)
}

// FlowSchemaVersion is the flow-definition schema version this build accepts —
// the value a [FlowDef] (or a YAML flow document) must carry in its Version
// field.
//
// It is exported so a layer that compiles its own surface syntax into FlowDef
// values can stamp the version it is actually building against, instead of
// pinning a literal that silently disagrees after an upgrade.
const FlowSchemaVersion = dsl.SupportedVersion
