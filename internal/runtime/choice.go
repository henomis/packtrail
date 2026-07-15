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
	"fmt"

	"github.com/henomis/packtrail/internal/dsl"
	"github.com/henomis/packtrail/internal/store"
)

// stepChoice evaluates a choice node's rules in order against the assembled
// invocation context ({input, results, signals}) and advances to the first
// matching rule's target, or the default. A rule whose expression errors (e.g.
// a missing field) is treated as no-match so the default still applies — unless
// the node sets on_error: fail, which fails the execution on any eval error
// instead of silently routing to default (for safety-relevant routing).
func (e *Engine) stepChoice(ctx context.Context, _ *dsl.Flow, node *dsl.Node, exec *store.Execution) error {
	contextDoc, err := e.assembleContext(ctx, exec)
	if err != nil {
		return err
	}

	defaultTo := ""

	for _, r := range node.Rules {
		if r.Default {
			defaultTo = r.To
			continue
		}

		prog, ok := e.programs[r.When]
		if !ok {
			// Deterministic: re-delivery re-evaluates the same fixed context and
			// hits the same missing program, so retrying can never succeed.
			// Dead-letter immediately rather than Nak-loop to MaxDeliver.
			// (Unreachable in practice — programs are compiled at startup.)
			return terminal("choice %q: expression not compiled: %q", node.ID, r.When)
		}

		match, matchErr := prog.MatchContext(ctx, contextDoc)
		if matchErr != nil {
			if node.OnError == dsl.OnErrorFail {
				return e.failNode(ctx, exec.ID, node.ID,
					fmt.Sprintf("choice %s: rule %q evaluation error: %v", node.ID, r.When, matchErr))
			}

			e.log.Warn("choice rule eval", "node", node.ID, "when", r.When, "err", matchErr)

			continue
		}

		if match {
			return e.advanceToGenerationAttempt(ctx, exec.ID, node.ID, exec.NodeGeneration, exec.Attempt, r.To, nil)
		}
	}

	if defaultTo == "" {
		// Deterministic given the assembled context, so retrying is futile:
		// dead-letter immediately instead of Nak-looping to MaxDeliver.
		// (Unreachable in practice — validation requires a default rule.)
		return terminal("choice %q: no rule matched and no default", node.ID)
	}

	return e.advanceToGenerationAttempt(ctx, exec.ID, node.ID, exec.NodeGeneration, exec.Attempt, defaultTo, nil)
}
