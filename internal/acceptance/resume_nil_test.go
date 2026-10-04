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
	"testing"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/worker"
)

// Client.Resume with a nil value (the dashboard's resume with an
// empty value) re-runs the node; that run must see what it asked.
func TestResumeNilValueSeesInterruptPayload(t *testing.T) {
	e := NewEnv(t, []string{`
name: ask
nodes:
  - {id: draft, type: task, kind: asker}
`})

	e.Worker("asker", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		var asked struct{ Question string }

		interrupted, err := j.Interrupted(&asked)
		if err != nil {
			return nil, worker.Permanent(err)
		}

		if j.Generation == 1 {
			return nil, worker.Interrupt(map[string]any{"question": "ok?"})
		}

		return &worker.Result{Output: map[string]any{"interrupted": interrupted, "asked": asked.Question}}, nil
	})

	id := e.Start("ask", nil)
	e.WaitStatus(id, packtrail.StatusWaiting)

	if err := e.Client.Resume(e.Ctx, id, "draft", nil); err != nil {
		t.Fatal(err)
	}

	st := e.Completed(id)
	if got := string(st.Results["draft"]); got != `{"asked":"ok?","interrupted":true}` {
		t.Fatalf("resumed run saw %s", got)
	}
}
