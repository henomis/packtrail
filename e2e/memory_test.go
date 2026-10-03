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

package e2e_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/worker"
)

// assistantFlow recalls what it knows about a user, answers, and learns the
// new fact: memory that outlives each execution.
const assistantFlow = `
name: assistant
nodes:
  - {id: recall, type: task, kind: recall, next: answer}
  - {id: answer, type: task, kind: answer, next: learn}
  - {id: learn, type: task, kind: learn}
`

type chat struct {
	User string `json:"user"`
	Fact string `json:"fact"`
}

// memoryGetter is the read side shared by packtrail.Store (operators) and
// worker.Store (jobs).
type memoryGetter interface {
	Get(ctx context.Context, ns, key string) (json.RawMessage, error)
}

// recallFacts reads a user's facts; none yet is an empty memory.
func recallFacts(ctx context.Context, store memoryGetter, user string) ([]string, error) {
	raw, err := store.Get(ctx, "memory", user)
	if errors.Is(err, packtrail.ErrNotFound) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	var facts []string

	return facts, json.Unmarshal(raw, &facts)
}

// TestMemoryAcrossExecutions: conversations per user read and extend a
// long-term memory the workers keep in the store through Job.Store. Each run
// sees what the previous ones learned — across a NATS restart — users do not
// see each other's, and an operator can edit or wipe a memory between runs
// through Client.Store.
func TestMemoryAcrossExecutions(t *testing.T) {
	cl := newCluster(t, []string{assistantFlow})

	cl.worker("recall", func(ctx context.Context, j *worker.Job) (*worker.Result, error) {
		var c chat
		if err := j.Input(&c); err != nil {
			return nil, worker.Permanent(err)
		}

		facts, err := recallFacts(ctx, j.Store(), c.User)
		if err != nil {
			return nil, err
		}

		return &worker.Result{Output: map[string]any{"facts": facts}}, nil
	})

	cl.worker("answer", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		var r struct{ Facts []string }
		if err := j.Result("recall", &r); err != nil {
			return nil, worker.Permanent(err)
		}

		return &worker.Result{Output: map[string]any{"known": len(r.Facts)}}, nil
	})

	cl.worker("learn", func(ctx context.Context, j *worker.Job) (*worker.Result, error) {
		var (
			c chat
			r struct{ Facts []string }
		)

		if err := j.Input(&c); err != nil {
			return nil, worker.Permanent(err)
		}

		if err := j.Result("recall", &r); err != nil {
			return nil, worker.Permanent(err)
		}

		// Learn from what this execution recalled: a redelivered job writes
		// the same value again.
		if err := j.Store().Put(ctx, "memory", c.User, append(r.Facts, c.Fact)); err != nil {
			return nil, err
		}

		return &worker.Result{Output: map[string]any{"learned": c.Fact}}, nil
	})

	// recalled waits for one conversation and returns what it knew beforehand.
	recalled := func(id string) []string {
		st := cl.wait(id)
		cl.check(id)

		var r struct{ Facts []string }
		if st.Status != packtrail.StatusCompleted || st.Result("recall", &r) != nil {
			t.Fatalf("%s: %s %s", id, st.Status, st.Error)
		}

		return r.Facts
	}

	talk := func(user, fact string) []string {
		return recalled(cl.start("assistant", chat{User: user, Fact: fact}))
	}

	facts := func(n int, user string) []string {
		out := make([]string, n)
		for i := range out {
			out[i] = fmt.Sprintf("%s-fact-%d", user, i)
		}

		return out
	}

	// Two users talk at the same time, round after round; NATS restarts
	// between two rounds.
	const rounds = 4

	users := map[string][]string{"ana": facts(rounds, "ana"), "bo": facts(rounds, "bo")}

	for i := range rounds {
		if i == rounds/2 {
			cl.s.Restart(t)
		}

		// Both users' conversations run at once.
		ids := map[string]string{}
		for user, want := range users {
			ids[user] = cl.start("assistant", chat{User: user, Fact: want[i]})
		}

		for user, want := range users {
			if got := recalled(ids[user]); !slices.Equal(got, want[:i]) {
				t.Fatalf("%s round %d recalled %v, want %v", user, i, got, want[:i])
			}
		}
	}

	if keys, kerr := cl.c.Store().Keys(cl.ctx, "memory"); kerr != nil || !slices.Equal(keys, []string{"ana", "bo"}) {
		t.Fatalf("memory keys %v %v", keys, kerr)
	}

	// An operator corrects one memory and wipes the other.
	if err := cl.c.Store().Put(cl.ctx, "memory", "ana", []string{"prefers email"}); err != nil {
		t.Fatal(err)
	}

	if err := cl.c.Store().Delete(cl.ctx, "memory", "bo"); err != nil {
		t.Fatal(err)
	}

	if got := talk("ana", "moved to Rome"); !slices.Equal(got, []string{"prefers email"}) {
		t.Fatalf("ana recalled %v after the edit", got)
	}

	if got := talk("bo", "new start"); len(got) != 0 {
		t.Fatalf("bo recalled %v after the wipe", got)
	}

	if got, _ := recallFacts(cl.ctx, cl.c.Store(), "ana"); !slices.Equal(got, []string{"prefers email", "moved to Rome"}) {
		t.Fatalf("ana's memory %v", got)
	}
}
