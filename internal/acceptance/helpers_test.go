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
	"encoding/json"
	"testing"

	"github.com/nats-io/nats.go"

	"github.com/henomis/packtrail/internal/names"
	"github.com/henomis/packtrail/worker"
)

func cmdSubject(execID string) string {
	return names.New("").CmdSubject(names.Partition(execID, testPartitions), execID)
}

// publishRawCommand publishes body to the command subject of execID, as any
// (possibly broken) client could.
func publishRawCommand(t *testing.T, e *Env, execID, body string) {
	t.Helper()

	js := e.S.JS
	if _, err := js.Publish(e.Ctx, cmdSubject(execID), []byte(body)); err != nil {
		t.Fatal(err)
	}
}

// publishDuplicateComplete sends a completion for j under a different command
// id, bypassing stream deduplication: only the fold can reject it.
func publishDuplicateComplete(t *testing.T, nc *nats.Conn, j *worker.Job) {
	t.Helper()

	body, _ := json.Marshal(map[string]any{
		"id": "dup-" + j.ExecID + "-" + j.Key, "type": "complete", "exec_id": j.ExecID,
		"data": map[string]any{
			"key": j.Key, "generation": j.Generation, "attempt": j.Attempt,
			"output": map[string]any{"dup": true},
		},
	})

	if err := nc.Publish(cmdSubject(j.ExecID), body); err != nil {
		t.Fatal(err)
	}

	_ = nc.Flush()
}
