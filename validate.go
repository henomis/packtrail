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
	"fmt"

	"github.com/henomis/packtrail/internal/names"
	"github.com/henomis/packtrail/internal/sched"
)

// ValidateNamespace checks a namespace prefix (WithNamespace,
// WithClientNamespace, worker.WithNamespace): it must match
// [A-Za-z0-9_-]{1,64}. The error wraps ErrInvalidArgument.
func ValidateNamespace(ns string) error {
	if !names.ValidPrefix(ns) {
		return fmt.Errorf("%w: namespace %q must match [A-Za-z0-9_-]{1,64}", ErrInvalidArgument, ns)
	}

	return nil
}

// ValidateName checks an identifier that becomes a single NATS subject token
// or KV key segment — flow names, node ids, signal names, worker kinds,
// schedule names, execution ids: it must match [A-Za-z0-9_-]{1,128}. what
// names the identifier in the message ("flow name", …). The error wraps
// ErrInvalidArgument.
func ValidateName(what, s string) error {
	if err := names.CheckToken(what, s); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidArgument, err)
	}

	return nil
}

// ValidateCron checks a cron expression as schedules accept it: six fields
// (second minute hour day-of-month month day-of-week) or a predefined
// "@..." form. The error wraps ErrInvalidArgument.
func ValidateCron(expr string) error {
	if err := sched.ValidateCron(expr); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidArgument, err)
	}

	return nil
}

// ValidateOptions runs every check New does on opts without a connection, so
// a configuration (flows, schedules, namespace, tuning) can be validated
// offline. It does no network I/O; WithFlowsDir still reads its directory.
func ValidateOptions(opts ...Option) error {
	_, err := buildConfig(opts)

	return err
}
