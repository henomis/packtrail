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
	"math"
	"strings"
	"testing"
	"time"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/internal/blob"
	"github.com/henomis/packtrail/internal/dispatch"
	"github.com/henomis/packtrail/worker"
)

// TestReviewR601UnencodableResultFailsTheTask: a result the worker cannot
// encode (NaN usage, invalid raw JSON) fails the task permanently instead of
// publishing an empty command and leaving the execution running (R6-01).
func TestReviewR601UnencodableResultFailsTheTask(t *testing.T) {
	for name, res := range map[string]*worker.Result{
		"nan usage":   {Usage: map[string]float64{"cost": math.NaN()}},
		"raw output":  {Output: []byte("not json")},
		"empty write": {Writes: map[string]any{"c": []byte("{")}},
	} {
		t.Run(name, func(t *testing.T) {
			e := NewEnv(t, []string{`
name: bad
nodes:
  - {id: a, type: task, kind: bad, retry: {max_attempts: 3}}
`})

			e.Worker("bad", func(context.Context, *worker.Job) (*worker.Result, error) { return res, nil })

			id := e.Start("bad", nil)

			wctx, cancel := context.WithTimeout(e.Ctx, 10*time.Second)
			defer cancel()

			st, err := e.Client.Wait(wctx, id)
			if err != nil {
				t.Fatalf("execution never ended: %v", err)
			}

			if st.Status != packtrail.StatusFailed || !strings.Contains(st.Error, "encode") {
				t.Fatalf("status %s error %q", st.Status, st.Error)
			}
		})
	}
}

// TestReviewR603QuarantinedTerminalWithLateBlob: the terminal decision of a
// quarantined execution is claim-checked and its body is not readable yet (a
// lagging replica). It is retried, not acked away, so the closing effects
// still run once the body is readable (R6-03).
func TestReviewR603QuarantinedTerminalWithLateBlob(t *testing.T) {
	e := NewEnv(t, []string{`
name: big
nodes:
  - {id: a, type: task, kind: big}
`})

	release := make(chan struct{})

	e.Worker("big", func(ctx context.Context, _ *worker.Job) (*worker.Result, error) {
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}

		return &worker.Result{Output: map[string]any{"big": strings.Repeat("x", 2<<20)}}, nil
	})

	id := e.Start("big", nil)
	st := e.WaitStatus(id, packtrail.StatusRunning)

	in := attach(t, e)

	if err := dispatch.Quarantine(e.Ctx, in, id, st.LastSeq+1, "test quarantine"); err != nil {
		t.Fatal(err)
	}

	e.KillEngines()
	e.StartEngine(packtrail.WithoutDispatcher())

	close(release)

	events, err := e.S.JS.Stream(e.Ctx, "packtrail-events")
	if err != nil {
		t.Fatal(err)
	}

	var name string

	e.Eventually(func() bool {
		m, lerr := events.GetLastMsgForSubject(e.Ctx, in.EventSubject(id))
		if lerr == nil {
			name = m.Header.Get(blob.Header)
		}

		return name != ""
	}, func() string { return "the terminal decision was not claim-checked" })

	obs, err := e.S.JS.ObjectStore(e.Ctx, "packtrail-blobs")
	if err != nil {
		t.Fatal(err)
	}

	body, err := obs.GetBytes(e.Ctx, name)
	if err != nil {
		t.Fatal(err)
	}

	if err = obs.Delete(e.Ctx, name); err != nil {
		t.Fatal(err)
	}

	e.KillEngines()
	e.StartEngine()

	time.Sleep(1500 * time.Millisecond) // at least one failed delivery

	if _, err = obs.PutBytes(e.Ctx, name, body); err != nil {
		t.Fatal(err)
	}

	e.Eventually(func() bool {
		l, _ := e.Client.List(e.Ctx, packtrail.ListFilter{Status: packtrail.StatusCompleted, Flow: "big"})

		return len(l) == 1 && l[0].Quarantined
	}, func() string { return "the quarantined execution's terminal decision was dropped" })
}
