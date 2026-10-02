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
	"time"

	"github.com/nats-io/nats.go"
)

// TestReviewF401LateBlobDoesNotDeadLetterCommand: a claim-checked command
// whose body cannot be read yet (an object-store blip, a lagging replica) is
// retried, not dead-lettered (F4-01).
func TestReviewF401LateBlobDoesNotDeadLetterCommand(t *testing.T) {
	e := NewEnv(t, []string{m1Linear})
	e.Worker("echo", Echo)

	const id = "late-blob"

	body, err := json.Marshal(map[string]any{
		"id": "start." + id, "type": "start", "exec_id": id,
		"data": map[string]any{"flow": "linear", "input": map[string]any{"big": true}},
	})
	if err != nil {
		t.Fatal(err)
	}

	m := nats.NewMsg(cmdSubject(id))
	m.Header.Set("Nats-Msg-Id", "start."+id)
	m.Header.Set("Pt-Blob", id+"/late")

	if _, err = e.S.JS.PublishMsg(e.Ctx, m); err != nil {
		t.Fatal(err)
	}

	time.Sleep(1500 * time.Millisecond) // the body is not readable yet

	obs, err := e.S.JS.ObjectStore(e.Ctx, "packtrail-blobs")
	if err != nil {
		t.Fatal(err)
	}

	if _, err = obs.PutBytes(e.Ctx, id+"/late", body); err != nil {
		t.Fatal(err)
	}

	if st := e.Completed(id); string(st.Input) != `{"big":true}` {
		t.Fatalf("input %s", st.Input)
	}

	if dl, _ := e.Client.DeadLetters(e.Ctx, 10); len(dl) != 0 {
		t.Fatalf("dead letters: %+v", dl)
	}
}
