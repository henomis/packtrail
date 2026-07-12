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
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/henomis/packtrail/internal/store"
	"github.com/henomis/packtrail/pkg/protocol"
)

func fanFlow(joinPolicy string) string {
	return `
name: fan
nodes:
  - {id: start, type: task, subject: "tasks.start.{execution_id}"}
  - {id: fo, type: fanout, branches: [x, y, z]}
  - {id: x, type: task, subject: "tasks.x.{execution_id}"}
  - {id: y, type: task, subject: "tasks.y.{execution_id}"}
  - {id: z, type: task, subject: "tasks.z.{execution_id}"}
  - {id: join, type: fanin, wait_for: [x, y, z], join_policy: "` + joinPolicy + `"}
  - {id: done, type: task, subject: "tasks.done.{execution_id}"}
edges:
  - {from: start, to: fo}
  - {from: fo, to: join}
  - {from: join, to: done}
`
}

// okBranch replies ok with a small result identifying the branch.
func okBranch(name string) protocol.Handler {
	return func(_ context.Context, _ protocol.TaskRequest) (protocol.TaskResponse, error) {
		return protocol.TaskResponse{Status: protocol.StatusOK, Payload: json.RawMessage(`{"branch":"` + name + `"}`)}, nil
	}
}

func passthrough(_ context.Context, req protocol.TaskRequest) (protocol.TaskResponse, error) {
	return protocol.TaskResponse{Status: protocol.StatusOK, Payload: req.Payload}, nil
}

func TestFanoutFaninAll(t *testing.T) {
	h := newHarness(t, fanFlow("all"), Config{})
	h.serve(t, "tasks.start.*", passthrough)
	h.serve(t, "tasks.x.*", okBranch("x"))
	h.serve(t, "tasks.y.*", okBranch("y"))
	h.serve(t, "tasks.z.*", okBranch("z"))

	var doneCalled atomic.Bool

	h.serve(t, "tasks.done.*", func(_ context.Context, req protocol.TaskRequest) (protocol.TaskResponse, error) {
		doneCalled.Store(true)
		return protocol.TaskResponse{Status: protocol.StatusOK, Payload: req.Payload}, nil
	})

	id, err := h.engine.Start(context.Background(), "fan", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	ex := h.waitStatus(t, id, store.StatusCompleted, 5*time.Second)

	for _, b := range []string{"x", "y", "z"} {
		if ex.Branches[b].Status != store.BranchCompleted {
			t.Errorf("branch %s status = %q", b, ex.Branches[b].Status)
		}
	}

	if !doneCalled.Load() {
		t.Error("done task was not reached")
	}

	// Branch outputs live in the data plane under results.<branch>.
	doc, err := h.engine.Results(context.Background(), id)
	if err != nil {
		t.Fatalf("results: %v", err)
	}

	got := parseCtx(t, doc)
	for _, b := range []string{"x", "y", "z"} {
		if len(got.Results[b]) == 0 {
			t.Errorf("results.%s missing from the assembled context: %s", b, doc)
		}
	}
}

// TestFanoutQuorum: with quorum:2, two ok branches and one failing branch should
// still satisfy the join and reach done.
func TestFanoutQuorum(t *testing.T) {
	h := newHarness(t, fanFlow("quorum:2"), Config{RetryBaseDelay: 20 * time.Millisecond})
	h.serve(t, "tasks.start.*", passthrough)
	h.serve(t, "tasks.x.*", okBranch("x"))
	h.serve(t, "tasks.y.*", okBranch("y"))
	h.serve(t, "tasks.z.*", func(_ context.Context, _ protocol.TaskRequest) (protocol.TaskResponse, error) {
		return protocol.TaskResponse{Status: protocol.StatusError, Error: "z down"}, nil
	})

	var doneCalled atomic.Bool

	h.serve(t, "tasks.done.*", func(_ context.Context, req protocol.TaskRequest) (protocol.TaskResponse, error) {
		doneCalled.Store(true)
		return protocol.TaskResponse{Status: protocol.StatusOK, Payload: req.Payload}, nil
	})

	id, err := h.engine.Start(context.Background(), "fan", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	ex := h.waitStatus(t, id, store.StatusCompleted, 5*time.Second)
	if ex.Branches["z"].Status != store.BranchFailed {
		t.Errorf("z branch status = %q, want failed", ex.Branches["z"].Status)
	}

	if !doneCalled.Load() {
		t.Error("done not reached despite quorum met")
	}
}

// TestFaninAllFailsWhenBranchFails: join_policy all must fail the execution if a
// branch fails.
func TestFaninAllFailsWhenBranchFails(t *testing.T) {
	h := newHarness(t, fanFlow("all"), Config{})
	h.serve(t, "tasks.start.*", passthrough)
	h.serve(t, "tasks.x.*", okBranch("x"))
	h.serve(t, "tasks.y.*", okBranch("y"))
	h.serve(t, "tasks.z.*", func(_ context.Context, _ protocol.TaskRequest) (protocol.TaskResponse, error) {
		return protocol.TaskResponse{Status: protocol.StatusError, Error: "z down"}, nil
	})
	h.serve(t, "tasks.done.*", passthrough)

	id, err := h.engine.Start(context.Background(), "fan", nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	h.waitStatus(t, id, store.StatusFailed, 5*time.Second)
}
