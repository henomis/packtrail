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
	"errors"

	"github.com/henomis/packtrail/internal/apierr"
	"github.com/henomis/packtrail/internal/fold"
)

// Errors returned by the public API. They are sentinel values: test with
// errors.Is.
var (
	// ErrNotFound means the execution does not exist (it was never archived,
	// or it was deleted), or a Store key has no value.
	ErrNotFound = apierr.ErrNotFound
	// ErrArchived means the execution finished and was archived: it can be
	// read, but not driven any more (I-17).
	ErrArchived = errors.New("packtrail: execution is archived")
	// ErrInvalidArgument wraps every validation error of caller input
	// (malformed ids, names, payloads).
	ErrInvalidArgument = apierr.ErrInvalidArgument
	// ErrUnknownFlow means the flow (or version) is not registered.
	ErrUnknownFlow = errors.New("packtrail: unknown flow")
	// ErrExecutionConflict means a Start named the id of an execution of
	// another flow: nothing was started and that execution is unchanged.
	ErrExecutionConflict = errors.New("packtrail: execution id belongs to another flow")
	// ErrExecutionExists means a Start with WithJoinExisting(false) found
	// its execution already there (of the same flow).
	ErrExecutionExists = errors.New("packtrail: execution already exists")
	// ErrTerminal means the execution already reached a final status.
	ErrTerminal = errors.New("packtrail: execution already finished")
	// ErrNoValue means a value State decodes (a result, a channel, a signal,
	// the output) is absent.
	ErrNoValue = fold.ErrNoValue
	// ErrNotProvisioned means the namespace does not exist yet: no engine
	// has run Init (or Run) on it. A Client or worker started before the
	// engine gets it and may retry. It says nothing about whether an engine
	// is running now: once provisioned, commands are accepted and wait for
	// one.
	ErrNotProvisioned = apierr.ErrNotProvisioned
)
