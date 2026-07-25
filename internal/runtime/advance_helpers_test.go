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

package runtime

import (
	"context"

	"github.com/henomis/packtrail/internal/store"
)

// advanceTo and advanceToAttempt are test-only conveniences over
// advanceToGenerationAttempt: every real transition (choice.go, fanout.go,
// task.go) calls advanceToGenerationAttempt directly with a real generation,
// so these two "don't care about generation/attempt" shortcuts have no
// production caller. They live in this _test.go file (excluded from the
// production binary) rather than engine.go, but exercise the exact same guard
// logic (advanceGuardMatches, activity-stash clearing) production code paths
// depend on — see TestAdvanceClearsActivityStash and TestStaleAdvanceNoRewind.

// advanceTo moves the execution from fromNode to nextNode (or completes it if
// nextNode == "") via a CAS write that also commits the next step's work item
// (transactional outbox), then flushes the outbox. mutate may apply additional
// changes (e.g. merge payload) within the same CAS write.
func (e *Engine) advanceTo(
	ctx context.Context, execID, fromNode, nextNode string, mutate func(*store.Execution),
) error {
	return e.advanceToAttempt(ctx, execID, fromNode, -1, nextNode, mutate)
}

func (e *Engine) advanceToAttempt(
	ctx context.Context, execID, fromNode string, expectedAttempt int, nextNode string, mutate func(*store.Execution),
) error {
	return e.advanceToGenerationAttempt(ctx, execID, fromNode, 0, expectedAttempt, nextNode, mutate)
}
