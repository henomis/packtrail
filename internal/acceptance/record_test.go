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
	"strings"
	"testing"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/event"
	"github.com/henomis/packtrail/worker"
)

const recordFlow = `
name: recorded
nodes:
  - id: a
    type: task
    kind: rec
    retry: {max_attempts: 2, backoff: fixed, delay: 10ms}
    next: b
  - {id: b, type: task, kind: rec, next: c}
  - {id: c, type: task, kind: plain, next: d}
  - {id: d, type: task, kind: rec, cache: {ttl: 1h}}
`

const recordPanicFlow = `
name: recpanic
nodes:
  - {id: p, type: task, kind: recpanic}
`

// recWorker records {node, attempt, resumed} on every attempt: node a fails
// its first attempt, node b interrupts (with usage) until resumed.
func recWorker(_ context.Context, j *worker.Job) (*worker.Result, error) {
	resumed, _ := j.Resumed(nil)

	if err := j.SetRecord(map[string]any{"node": j.Node, "attempt": j.Attempt, "resumed": resumed}); err != nil {
		return nil, err
	}

	switch {
	case j.Node == "a" && j.Attempt == 1:
		return nil, errors.New("first attempt fails")
	case j.Node == "b" && !resumed:
		return nil, worker.WithUsage(worker.Interrupt("ask"), map[string]float64{"tokens": 1})
	}

	return &worker.Result{Output: map[string]any{"node": j.Node}}, nil
}

func recordJSON(node string, attempt int, resumed bool) string {
	return fmt.Sprintf(`{"attempt":%d,"node":%q,"resumed":%t}`, attempt, node, resumed)
}

// TestRecordPerAttempt: what a worker records rides with each attempt's
// outcome — a retried failure, a completion, an interrupt (wrapped by
// WithUsage), the resumed run — and RecordHistory returns them oldest first
// with the attempt they belong to. An attempt that records nothing, or only
// an unencodable value, stores no field; a cached completion stores none.
func TestRecordPerAttempt(t *testing.T) {
	e := NewEnv(t, []string{recordFlow})
	e.Worker("rec", recWorker)

	unencodable := make(chan error, 2)

	e.Worker("plain", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		unencodable <- j.SetRecord(func() {})

		return &worker.Result{Output: map[string]any{}}, nil
	})

	id := e.Start("recorded", nil)
	e.WaitStatus(id, packtrail.StatusWaiting)

	if err := e.Client.Resume(e.Ctx, id, "b", "go"); err != nil {
		t.Fatal(err)
	}

	e.Completed(id)

	if err := <-unencodable; err == nil {
		t.Fatal("SetRecord accepted an unencodable value")
	}

	type want struct {
		outcome string
		attempt int
		record  string
	}

	check := func(execID, node string, wants []want) {
		t.Helper()

		recs, err := e.Client.RecordHistory(e.Ctx, execID, node)
		if err != nil {
			t.Fatal(err)
		}

		if len(recs) != len(wants) {
			t.Fatalf("%s: %d records, want %d: %+v", node, len(recs), len(wants), recs)
		}

		for i, w := range wants {
			r := recs[i]
			if r.Outcome != w.outcome || r.Attempt != w.attempt || string(r.Record) != w.record || r.Key != node ||
				r.Generation == 0 || r.Seq == 0 || r.Time.IsZero() {
				t.Fatalf("%s record %d = %+v (%s), want %+v", node, i, r, r.Record, w)
			}
		}
	}

	check(id, "a", []want{
		{packtrail.OutcomeFailed, 1, recordJSON("a", 1, false)},
		{packtrail.OutcomeCompleted, 2, recordJSON("a", 2, false)},
	})
	check(id, "b", []want{
		{packtrail.OutcomeInterrupted, 1, recordJSON("b", 1, false)},
		{packtrail.OutcomeCompleted, 1, recordJSON("b", 1, true)},
	})
	check(id, "c", nil)
	check(id, "d", []want{{packtrail.OutcomeCompleted, 1, recordJSON("d", 1, false)}})

	// The events carry the same values; c carries none.
	evs, err := e.Client.History(e.Ctx, id)
	if err != nil {
		t.Fatal(err)
	}

	for _, ev := range evs {
		if d, ok := ev.Data.(*event.NodeDone); ok && d.Node == "c" && d.Record != nil {
			t.Fatalf("c recorded %s", d.Record)
		}
	}

	// A second run of d is served from the cache: no worker ran, no record.
	id2 := e.Start("recorded", nil)
	e.WaitStatus(id2, packtrail.StatusWaiting)

	if err = e.Client.Resume(e.Ctx, id2, "b", "go"); err != nil {
		t.Fatal(err)
	}

	e.Completed(id2)
	check(id2, "d", nil)

	evs, err = e.Client.History(e.Ctx, id2)
	if err != nil {
		t.Fatal(err)
	}

	cached := false

	for _, ev := range evs {
		if d, ok := ev.Data.(*event.NodeDone); ok && d.Node == "d" {
			cached = d.Cached
		}
	}

	if !cached {
		t.Fatal("d was not served from the cache")
	}
}

// TestRecordKeptOnPanic: a handler that records and then panics fails its
// attempt with the record it set.
func TestRecordKeptOnPanic(t *testing.T) {
	e := NewEnv(t, []string{recordPanicFlow})
	e.Worker("recpanic", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		_ = j.SetRecord(map[string]int{"attempt": j.Attempt})

		panic("boom")
	})

	id := e.Start("recpanic", nil)
	e.WaitStatus(id, packtrail.StatusFailed)

	recs, err := e.Client.RecordHistory(e.Ctx, id, "p")
	if err != nil || len(recs) == 0 {
		t.Fatalf("records %+v, %v", recs, err)
	}

	for i, r := range recs {
		if r.Outcome != packtrail.OutcomeFailed || string(r.Record) != fmt.Sprintf(`{"attempt":%d}`, i+1) {
			t.Fatalf("record %d = %+v (%s)", i, r, r.Record)
		}
	}
}

// TestRecordTruncatedAtCap: a record over WithMaxRecordBytes is stored as a
// truncation marker within the cap and the attempt completes.
func TestRecordTruncatedAtCap(t *testing.T) {
	e := NewEnv(t, []string{wtOne})
	e.Worker("echo", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		if err := j.SetRecord(map[string]string{"prompt": strings.Repeat("p", 10000)}); err != nil {
			return nil, err
		}

		return &worker.Result{Output: map[string]any{}}, nil
	}, worker.WithMaxRecordBytes(1000))

	id := e.Start("one", nil)
	e.Completed(id)

	recs, err := e.Client.RecordHistory(e.Ctx, id, "a")
	if err != nil || len(recs) != 1 {
		t.Fatalf("records %+v, %v", recs, err)
	}

	var tr worker.RecordTruncated
	if err = json.Unmarshal(recs[0].Record, &tr); err != nil || !tr.Truncated || tr.Size <= 10000 ||
		len(recs[0].Record) > 1000 || !strings.HasPrefix(tr.Head, `{"prompt":"ppp`) {
		t.Fatalf("record %d bytes: %.200s (%v)", len(recs[0].Record), recs[0].Record, err)
	}
}

// TestRecordLargeViaBlob: a record above the claim-check threshold goes
// command → blob → event and reads back intact; deleting the execution
// removes its blobs (I-91).
func TestRecordLargeViaBlob(t *testing.T) {
	clock := &skewed{}
	e := NewEnv(t, []string{expiryDelete}, packtrail.WithClock(clock.now))

	// Larger than the server's max payload: the worker's command and the
	// engine's event are both claim-checked.
	big := strings.Repeat("r", 3*1024*1024)

	e.Worker("echo", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		if err := j.SetRecord(map[string]string{"transcript": big}); err != nil {
			return nil, err
		}

		return &worker.Result{Output: map[string]any{}}, nil
	})

	id := e.Start("gone", map[string]any{"customer": "kim"}, packtrail.WithExecutionID("rec-big"))
	e.Completed(id)

	recs, err := e.Client.RecordHistory(e.Ctx, id, "a")
	if err != nil || len(recs) != 1 {
		t.Fatalf("records %d, %v", len(recs), err)
	}

	var got struct{ Transcript string }
	if err = json.Unmarshal(recs[0].Record, &got); err != nil || got.Transcript != big {
		t.Fatalf("record of %d bytes did not round-trip (%v)", len(recs[0].Record), err)
	}

	obs, err := e.S.JS.ObjectStore(e.Ctx, "packtrail-blobs")
	if err != nil {
		t.Fatal(err)
	}

	list, err := obs.List(e.Ctx)
	if err != nil && !errors.Is(err, jetstream.ErrNoObjectsFound) {
		t.Fatal(err)
	}

	blobs := 0

	for _, o := range list {
		if strings.HasPrefix(o.Name, id+"/") {
			blobs++
		}
	}

	if blobs == 0 {
		t.Fatal("the record was not claim-checked")
	}

	clock.pastDedupWindow()
	gone(t, e, id, "gone")
}
