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
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/worker"
)

// TestJobStore: a job reads and writes the long-term store of its namespace
// through Job.Store, without a client of its own. The client sees what the
// job wrote and the job sees what the client wrote; errors are the
// packtrail sentinels.
func TestJobStore(t *testing.T) {
	e := NewEnv(t, []string{wtOne})

	if err := e.Client.Store().Put(e.Ctx, "users", "ana", map[string]int{"visits": 1}); err != nil {
		t.Fatal(err)
	}

	e.Worker("echo", func(ctx context.Context, j *worker.Job) (*worker.Result, error) {
		s := j.Store()

		_, err := s.Get(ctx, "users", "nobody")
		if !errors.Is(err, packtrail.ErrNotFound) || !errors.Is(err, worker.ErrNotFound) {
			return nil, worker.Permanent(fmt.Errorf("missing key: %v", err))
		}

		if err = s.Put(ctx, "users", "bad key", 1); !errors.Is(err, packtrail.ErrInvalidArgument) {
			return nil, worker.Permanent(fmt.Errorf("invalid key: %v", err))
		}

		raw, err := s.Get(ctx, "users", "ana")
		if err != nil {
			return nil, err
		}

		var v struct{ Visits int }
		if err = json.Unmarshal(raw, &v); err != nil {
			return nil, worker.Permanent(err)
		}

		// Idempotent under redelivery: the value derives from the execution.
		if err = s.Put(ctx, "users", "bo", map[string]any{"by": j.ExecID}); err != nil {
			return nil, err
		}

		keys, err := s.Keys(ctx, "users")
		if err != nil {
			return nil, err
		}

		if err = s.Delete(ctx, "users", "ana"); err != nil {
			return nil, err
		}

		return &worker.Result{Output: map[string]any{"visits": v.Visits, "keys": keys}}, nil
	})

	id := e.Start("one", nil)
	st := e.Completed(id)

	var out struct {
		Visits int
		Keys   []string
	}
	if err := st.Result("a", &out); err != nil {
		t.Fatal(err)
	}

	if out.Visits != 1 || !slices.Equal(out.Keys, []string{"ana", "bo"}) {
		t.Fatalf("job saw %+v", out)
	}

	raw, err := e.Client.Store().Get(e.Ctx, "users", "bo")
	if err != nil {
		t.Fatal(err)
	}

	var by struct{ By string }
	if err = json.Unmarshal(raw, &by); err != nil || by.By != id {
		t.Fatalf("client read %s (%v)", raw, err)
	}

	if _, err = e.Client.Store().Get(e.Ctx, "users", "ana"); !errors.Is(err, packtrail.ErrNotFound) {
		t.Fatalf("deleted by the job: %v", err)
	}
}
