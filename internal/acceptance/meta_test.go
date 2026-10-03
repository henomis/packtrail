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

package acceptance

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/worker"
)

func metaFlow(prompt string) string {
	return fmt.Sprintf(`
name: cfg
nodes:
  - {id: wait, type: await, signal: go, timeout: 1h, next: step}
  - {id: step, type: task, kind: meta-echo, meta: {agent: writer, prompt: %q}, next: bare}
  - {id: bare, type: task, kind: meta-echo}
`, prompt)
}

// metaEcho returns the job's meta prompt, or "none".
func metaEcho(_ context.Context, j *worker.Job) (*worker.Result, error) {
	var m struct{ Prompt string }
	if err := j.DecodeMeta(&m); err != nil {
		m.Prompt = "none"
	}

	return &worker.Result{Output: map[string]any{"prompt": m.Prompt}}, nil
}

func prompt(t *testing.T, st *packtrail.State, node string) string {
	t.Helper()

	var out struct{ Prompt string }
	if err := json.Unmarshal(st.Results[node], &out); err != nil {
		t.Fatalf("%s result %s: %v", node, st.Results[node], err)
	}

	return out.Prompt
}

// TestNodeMetaFollowsTheFlowVersion: a job carries the meta of the flow
// version its execution runs: registering a new version changes it for new
// executions only, not for one already running nor for a rerun of an old one.
// Client.Flow returns the meta of each version.
func TestNodeMetaFollowsTheFlowVersion(t *testing.T) {
	e := NewEnv(t, []string{metaFlow("v1")})
	e.Worker("meta-echo", metaEcho)

	old := e.Start("cfg", nil)
	e.WaitStatus(old, packtrail.StatusWaiting)

	v2 := e.Register(metaFlow("v2"))
	fresh := e.Start("cfg", nil)
	e.WaitStatus(fresh, packtrail.StatusWaiting)

	for _, id := range []string{old, fresh} {
		if err := e.Client.Signal(e.Ctx, id, "go", nil); err != nil {
			t.Fatal(err)
		}
	}

	ost, nst := e.Completed(old), e.Completed(fresh)

	if got := prompt(t, ost, "step"); got != "v1" {
		t.Fatalf("running execution got meta %q, want v1", got)
	}

	if got := prompt(t, nst, "step"); got != "v2" || nst.FlowHash != v2 {
		t.Fatalf("new execution got meta %q on %s, want v2 on %s", got, nst.FlowHash, v2)
	}

	if got := prompt(t, ost, "bare"); got != "none" {
		t.Fatalf("node without meta got %q", got)
	}

	rerun, err := e.Client.Rerun(e.Ctx, old, "step")
	if err != nil {
		t.Fatal(err)
	}

	if got := prompt(t, e.Completed(rerun), "step"); got != "v1" {
		t.Fatalf("rerun of a v1 execution got meta %q", got)
	}

	for hash, want := range map[string]string{ost.FlowHash: "v1", v2: "v2"} {
		def, ferr := e.Client.Flow(e.Ctx, "cfg", hash)
		if ferr != nil {
			t.Fatal(ferr)
		}

		if got := def.Node("step").Meta["prompt"]; got != want {
			t.Fatalf("Flow(cfg, %s) meta prompt %v, want %s", hash, got, want)
		}
	}
}
