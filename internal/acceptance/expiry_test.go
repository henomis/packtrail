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
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail"
)

const expiryDelete = `
name: gone
retention: 3s
on_expire: delete
search_attributes: {customer: input.customer}
nodes:
  - {id: a, type: task, kind: echo}
`

const expiryArchiveThenDelete = `
name: kept
retention: 1s
archive_retention: 3s
search_attributes: {customer: input.customer}
nodes:
  - {id: a, type: task, kind: echo}
`

const expiryLoop = `
name: longgone
retention: 3s
on_expire: delete
start: step
nodes:
  - {id: step, type: task, kind: echo, next: again}
  - {id: again, type: choice, rules: [{when: "visits.step < 30", to: step}, {default: true, to: $end}]}
`

// skewed is an engine clock a test can move forward. An execution is only
// deleted once the deduplication window (ten minutes) has passed since it
// finished: the tests jump past it instead of waiting.
type skewed struct{ offset atomic.Int64 }

func (c *skewed) now() time.Time { return time.Now().Add(time.Duration(c.offset.Load())) }

// pastDedupWindow makes every execution finished so far old enough to be
// deleted.
func (c *skewed) pastDedupWindow() { c.offset.Add(int64(11 * time.Minute)) }

// leftovers lists what NATS still holds about an execution: messages on its
// event and timer subjects, keys of the index and snapshot buckets, and
// objects of the archive and blob stores, delete markers included.
func leftovers(t *testing.T, e *Env, id string) []string {
	t.Helper()

	var out []string

	for _, name := range []string{
		"packtrail-events", "packtrail-cmd", "KV_packtrail-index", "KV_packtrail-snapshots",
	} {
		s, err := e.S.JS.Stream(e.Ctx, name)
		if err != nil {
			t.Fatal(err)
		}

		info, err := s.Info(e.Ctx, jetstream.WithSubjectFilter(">"))
		if err != nil {
			t.Fatal(err)
		}

		for subject, n := range info.State.Subjects {
			for token := range strings.SplitSeq(subject, ".") {
				if token == id {
					out = append(out, fmt.Sprintf("%s %s (%d)", name, subject, n))
				}
			}
		}
	}

	for _, bucket := range []string{"packtrail-archive", "packtrail-blobs"} {
		obs, err := e.S.JS.ObjectStore(e.Ctx, bucket)
		if err != nil {
			t.Fatal(err)
		}

		list, err := obs.List(e.Ctx, jetstream.ListObjectsShowDeleted())
		if err != nil && !errors.Is(err, jetstream.ErrNoObjectsFound) {
			t.Fatal(err)
		}

		for _, o := range list {
			if o.Name == id || strings.HasPrefix(o.Name, id+".") || strings.HasPrefix(o.Name, id+"/") {
				out = append(out, bucket+" "+o.Name)
			}
		}
	}

	return out
}

// gone waits until the execution reads as never having existed, and checks
// that nothing of it is left in NATS.
func gone(t *testing.T, e *Env, id, flowName string) {
	t.Helper()

	e.Eventually(func() bool {
		_, err := e.Client.Get(e.Ctx, id)

		return errors.Is(err, packtrail.ErrNotFound)
	}, func() string { return id + " never deleted" })

	if _, err := e.Client.History(e.Ctx, id); !errors.Is(err, packtrail.ErrNotFound) {
		t.Fatalf("history of deleted %s = %v, want ErrNotFound", id, err)
	}

	if err := e.Client.Cancel(e.Ctx, id, ""); !errors.Is(err, packtrail.ErrNotFound) {
		t.Fatalf("cancel of deleted %s = %v, want ErrNotFound", id, err)
	}

	e.Eventually(func() bool {
		l, err := e.Client.List(e.Ctx, packtrail.ListFilter{Flow: flowName})
		if err != nil {
			return false
		}

		for _, s := range l {
			if s.ExecID == id {
				return false
			}
		}

		return true
	}, func() string { return id + " still listed" })

	e.Eventually(func() bool { return len(leftovers(t, e, id)) == 0 }, func() string {
		return "left behind: " + strings.Join(leftovers(t, e, id), ", ")
	})
}

// TestExpiryDelete: with on_expire: delete a finished execution is removed
// after its retention instead of being archived. Nothing of it is left —
// events, index entries, snapshot, blobs, archive, not even delete markers —
// and its id starts a new execution (I-91).
func TestExpiryDelete(t *testing.T) {
	clock := &skewed{}
	e := NewEnv(t, []string{expiryDelete}, packtrail.WithBlobThreshold(1024), packtrail.WithClock(clock.now))
	e.Worker("echo", Echo)

	// Above the blob threshold: the execution has blobs to delete too.
	big := strings.Repeat("x", 4096)

	id := e.Start("gone", map[string]any{"customer": "kim", "note": big}, packtrail.WithExecutionID("gone-1"))
	e.Completed(id)
	clock.pastDedupWindow()

	gone(t, e, id, "gone")

	if n := e.Engine.Metrics(e.Ctx).Deleted; n != 1 {
		t.Fatalf("deleted metric = %d, want 1", n)
	}

	if n := e.Engine.Metrics(e.Ctx).Archived; n != 0 {
		t.Fatalf("archived metric = %d: a deleted execution must not be archived", n)
	}

	// The id is free: it starts a new execution, with its own input, not a
	// duplicate of the old one. (It cannot run here: the test only pretended
	// that the deduplication window had passed.)
	e.Start("gone", map[string]any{"customer": "ana"}, packtrail.WithExecutionID("gone-1"))

	st, err := e.Client.Get(e.Ctx, id)
	if err != nil || string(st.Input) != `{"customer":"ana"}` {
		t.Fatalf("restarted execution: input %s, %v", st.Input, err)
	}
}

// TestExpiryDeletePostponed: an execution is not deleted while its id is
// still inside the deduplication window, whatever its retention: until then
// it stays as it is (not archived), its id still joins it, and its deletion
// is scheduled for the end of the window (I-91).
func TestExpiryDeletePostponed(t *testing.T) {
	e := NewEnv(t, []string{expiryDelete})
	e.Worker("echo", Echo)

	id := e.Start("gone", map[string]any{"customer": "kim"}, packtrail.WithExecutionID("gone-1"))
	done := e.Completed(id)

	cmds, err := e.S.JS.Stream(e.Ctx, "packtrail-cmd")
	if err != nil {
		t.Fatal(err)
	}

	timer := "packtrail.timer." + id + ".delete"

	e.Eventually(func() bool {
		info, ierr := cmds.Info(e.Ctx, jetstream.WithSubjectFilter(timer))

		return ierr == nil && info.State.Subjects[timer] == 1
	}, func() string { return "the deletion was never rescheduled" })

	msg, err := cmds.GetLastMsgForSubject(e.Ctx, timer)
	if err != nil {
		t.Fatal(err)
	}

	want := "@at " + done.Updated.Add(10*time.Minute).UTC().Format(time.RFC3339Nano)
	if got := msg.Header.Get("Nats-Schedule"); got != want {
		t.Fatalf("deletion scheduled %q, want %q", got, want)
	}

	st, err := e.Client.Get(e.Ctx, id)
	if err != nil || st.Archived || st.Status != packtrail.StatusCompleted {
		t.Fatalf("postponed execution: %+v %v", st, err)
	}

	// Its id is still taken: a start joins it.
	e.Start("gone", map[string]any{"customer": "ana"}, packtrail.WithExecutionID("gone-1"))

	if st, _ = e.Client.Get(e.Ctx, id); string(st.Input) != `{"customer":"kim"}` {
		t.Fatalf("postponed id was restarted: input %s", st.Input)
	}
}

// TestArchiveRetention: with archive_retention an execution is archived
// after its retention, stays readable, and is deleted that long after
// (I-92).
func TestArchiveRetention(t *testing.T) {
	clock := &skewed{}
	e := NewEnv(t, []string{expiryArchiveThenDelete}, packtrail.WithClock(clock.now))
	e.Worker("echo", Echo)

	id := e.Start("kept", map[string]any{"customer": "kim"}, packtrail.WithExecutionID("kept-1"))
	e.Completed(id)

	e.Eventually(func() bool {
		st, err := e.Client.Get(e.Ctx, id)

		return err == nil && st.Archived
	}, func() string { return "execution never archived" })

	// Only now: the deletion is scheduled on the engine's clock when the
	// execution is archived, and must still fire in this test's lifetime.
	clock.pastDedupWindow()

	if evs, err := e.Client.History(e.Ctx, id); err != nil || len(evs) == 0 {
		t.Fatalf("archived history: %d events, %v", len(evs), err)
	}

	gone(t, e, id, "kept")
}

// TestExpiryDeleteContinued: an execution whose log was continued as new
// leaves no history segments behind when it is deleted (I-91).
func TestExpiryDeleteContinued(t *testing.T) {
	clock := &skewed{}
	e := NewEnv(t, []string{expiryLoop}, packtrail.WithHistoryLimit(20), packtrail.WithClock(clock.now))
	e.Worker("echo", Echo)

	id := e.Start("longgone", nil, packtrail.WithExecutionID("long-1"))

	st := e.Completed(id)
	if len(st.Segments) == 0 {
		t.Fatal("the execution was never continued as new: the test needs segments")
	}

	clock.pastDedupWindow()
	gone(t, e, id, "longgone")
}

// TestDeleteByHand: Engine.Delete removes a finished execution, archived or
// not, refuses one still running or just finished, and reports a missing
// one (I-93).
func TestDeleteByHand(t *testing.T) {
	clock := &skewed{}
	e := NewEnv(t, []string{m1Linear, `
name: parked
nodes:
  - {id: gate, type: await, signal: go, timeout: 1h}
`}, packtrail.WithClock(clock.now))
	e.Worker("echo", Echo)

	done := e.Start("linear", map[string]any{"k": "v"}, packtrail.WithExecutionID("hand-1"))
	e.Completed(done)

	archived := e.Start("linear", nil, packtrail.WithExecutionID("hand-2"))
	e.Completed(archived)

	if err := e.Engine.Archive(e.Ctx, archived); err != nil {
		t.Fatal(err)
	}

	open := e.Start("parked", nil, packtrail.WithExecutionID("hand-3"))
	e.WaitStatus(open, packtrail.StatusWaiting)

	if err := e.Engine.Delete(e.Ctx, open); !errors.Is(err, packtrail.ErrInvalidArgument) {
		t.Fatalf("delete of a running execution = %v, want ErrInvalidArgument", err)
	}

	if err := e.Engine.Delete(e.Ctx, "nobody"); !errors.Is(err, packtrail.ErrNotFound) {
		t.Fatalf("delete of a missing execution = %v, want ErrNotFound", err)
	}

	// Just finished: its id is still inside the deduplication window.
	if err := e.Engine.Delete(e.Ctx, done); !errors.Is(err, packtrail.ErrInvalidArgument) {
		t.Fatalf("delete of a just finished execution = %v, want ErrInvalidArgument", err)
	}

	clock.pastDedupWindow()

	for _, id := range []string{done, archived} {
		if err := e.Engine.Delete(e.Ctx, id); err != nil {
			t.Fatalf("delete %s: %v", id, err)
		}

		gone(t, e, id, "linear")

		// Deleted twice: it does not exist any more.
		if err := e.Engine.Delete(e.Ctx, id); !errors.Is(err, packtrail.ErrNotFound) {
			t.Fatalf("second delete of %s = %v, want ErrNotFound", id, err)
		}
	}

	if st, err := e.Client.Get(e.Ctx, open); err != nil || st.Status != packtrail.StatusWaiting {
		t.Fatalf("running execution after the deletes: %+v %v", st, err)
	}
}
