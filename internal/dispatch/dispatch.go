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

// Package dispatch is the projection that turns events into side effects. It
// consumes each partition of the events stream in order, keeps the folded
// state of every execution it sees, and for each event:
//
//	NodeScheduled    publishes a job to <ns>.work.<kind> (or a cached result)
//	TimerScheduled   installs a JetStream schedule that fires a timer command
//	ChildStarted     starts the child execution
//	terminal events  notifies the parent, cancels children, purges timers and
//	                 schedules the archive
//	NodeCompleted    stores cacheable results
//
// and updates the visibility index. Effects are never written together with
// the events (they live in other streams): every publish is idempotent instead
// (deduplication ids, rollup subjects, stale checks in the fold), so
// redelivering an event after a crash is harmless.
package dispatch

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail/event"
	"github.com/henomis/packtrail/flow"
	"github.com/henomis/packtrail/internal/blob"
	"github.com/henomis/packtrail/internal/cmd"
	"github.com/henomis/packtrail/internal/consume"
	"github.com/henomis/packtrail/internal/fold"
	"github.com/henomis/packtrail/internal/infra"
	"github.com/henomis/packtrail/internal/loader"
	"github.com/henomis/packtrail/internal/metrics"
	"github.com/henomis/packtrail/internal/projection"
	"github.com/henomis/packtrail/internal/sched"
	"github.com/henomis/packtrail/internal/statecache"
	"github.com/henomis/packtrail/internal/wire"
)

// Dispatcher projects events into effects.
type Dispatcher struct {
	In      *infra.Infra
	Loader  *loader.Loader
	Cache   *statecache.Cache
	Metrics *metrics.M
	Drain   time.Duration

	qmu         sync.Mutex
	quarantined map[string]bool
	hmu         sync.Mutex
	heads       map[string]head
	ctlOnce     sync.Once
}

const (
	// retryFirst and retryCap bound the backoff of a transient failure: the
	// partition waits for the infrastructure instead of giving up on an
	// execution (F2-03).
	retryFirst = time.Second
	retryCap   = 30 * time.Second
)

// Run consumes partition p until ctx is done. ready is called once it pulls.
func (d *Dispatcher) Run(ctx context.Context, p int, ready func()) error {
	d.ctlOnce.Do(func() { d.watchControl(ctx) })
	d.loadQuarantined(ctx)

	return consume.Run(ctx, d.In.JS, consume.Config{
		Stream: d.In.Names.StreamEvents,
		// No MaxDeliver: an event is never dropped. Deterministic failures
		// quarantine their execution; transient ones are retried.
		Consumer: jetstream.ConsumerConfig{
			Durable: d.In.Names.DurDispatch(p), FilterSubject: d.In.Names.EventPartitionFilter(p),
			AckPolicy: jetstream.AckExplicitPolicy, MaxAckPending: 1, DeliverPolicy: jetstream.DeliverAllPolicy,
			AckWait: d.In.AckWait,
		},
		// No pinned group: MaxAckPending 1 already serialises the partition
		// across any number of dispatchers, and pinning combined with a single
		// in-flight message can stall a consumer after a hand-over (observed
		// with nats-server 2.14; see docs/bench.md).
		Drain:      d.Drain,
		PullExpiry: d.In.PullExpiry,
		Logger:     d.In.Logger,
		Pulling:    ready,
		Handler:    d.handle,
	})
}

func (d *Dispatcher) handle(ctx context.Context, msg jetstream.Msg) {
	execID := execOf(msg.Subject())

	skip, err := d.skip(ctx, execID)
	if err != nil {
		d.In.Logger.Warn("packtrail: quarantine check failed, will retry", "exec", execID, "err", err)

		_ = msg.NakWithDelay(retryFirst)

		return
	}

	if skip {
		d.handleQuarantined(ctx, msg, execID)

		return
	}

	err = d.process(ctx, msg)
	if err == nil {
		_ = msg.Ack()

		return
	}

	// Another dispatcher may have quarantined it meanwhile.
	if q, qerr := projection.IsQuarantined(ctx, d.In, execID); qerr == nil && q {
		d.markQuarantined(execID)
		d.handleQuarantined(ctx, msg, execID)

		return
	}

	var delivered uint64
	if md, merr := msg.Metadata(); merr == nil {
		delivered = md.NumDelivered
	}

	if quarantinable(err, delivered) {
		if qerr := d.quarantine(ctx, msg, execID, err); qerr == nil {
			_ = msg.Ack()

			return
		} else { //nolint:revive // keep the quarantine failure next to its cause.
			d.In.Logger.Error("packtrail: cannot quarantine, will retry", "exec", execID, "cause", err,
				"err", qerr)
		}
	}

	d.In.Logger.Warn("packtrail: dispatch failed, will retry", "subject", msg.Subject(), "deliveries", delivered,
		"err", err)

	_ = msg.NakWithDelay(backoff(delivered))
}

func backoff(delivered uint64) time.Duration {
	if delivered < 1 {
		delivered = 1
	}

	shift := min(delivered-1, 5) //nolint:mnd // 1s .. 32s, capped below.

	return min(retryFirst<<shift, retryCap)
}

func execOf(subject string) string { return subject[strings.LastIndexByte(subject, '.')+1:] }

func (d *Dispatcher) process(ctx context.Context, msg jetstream.Msg) error {
	evs, err := d.Loader.Log.Decode(ctx, msg)
	if err != nil {
		return err
	}

	return d.applyDecision(ctx, d.Cache, execOf(msg.Subject()), evs)
}

// applyDecision folds the events of one decision into the execution's state,
// runs their effects in order and updates the index. The state is loaded once,
// before the first event: the events of a decision share a stream sequence, so
// a reload in the middle would land before the whole decision. A failure
// retries the whole decision; effects are idempotent. States are folded in
// place, so cache must belong to the calling goroutine's path: the partition
// runner uses d.Cache, a replay its own.
func (d *Dispatcher) applyDecision(ctx context.Context, cache *statecache.Cache, execID string,
	evs []event.Event,
) error {
	e, err := d.stateBefore(ctx, cache, execID, evs[0])
	if err != nil {
		cache.Drop(execID)

		return err
	}

	if e.State == nil {
		return nil // archived meanwhile: nothing left to do
	}

	prev := e.State.Status
	created := !e.State.Exists()
	stop := wire.Stop{ExecID: execID}

	for _, ev := range evs {
		stopsOf(e.State, ev, &stop)

		if err = e.State.Apply(e.Def, ev); err != nil {
			cache.Drop(execID)

			return err
		}

		e.State.LastSeq = ev.Seq

		if err = d.effects(ctx, e, ev); err != nil {
			cache.Drop(execID)

			return err
		}
	}

	cache.Put(execID, e)
	d.publishStop(stop)

	return projection.IndexFrom(ctx, d.In, e.State, created, prev)
}

// stopsOf adds to stop what event ev ends while a worker may still be running
// it, judged on the state before ev: every running job when the execution
// ends, the attempt of a cancelled task, an attempt that timed out (G5-02).
func stopsOf(st *fold.State, ev event.Event, stop *wire.Stop) {
	switch d := ev.Data.(type) {
	case *event.Completed, *event.Failed, *event.Cancelled:
		for _, t := range st.Tasks {
			if t.Status == fold.TaskScheduled {
				stop.All = true
			}
		}
	case *event.NodeCancel:
		if t := st.Tasks[d.Key]; t != nil && t.Status == fold.TaskScheduled {
			stop.Tasks = append(stop.Tasks, wire.StopTask{Key: t.Key, Generation: t.Generation, Attempt: t.Attempt})
		}
	case *event.NodeFail:
		if d.Reason == event.ReasonTimeout {
			stop.Tasks = append(stop.Tasks, wire.StopTask{Key: d.Key, Generation: d.Generation, Attempt: d.Attempt})
		}
	}
}

// publishStop tells workers about attempts no longer wanted. Best effort:
// core NATS, errors only logged (the worker also checks the log head, and a
// late result is stale anyway).
func (d *Dispatcher) publishStop(stop wire.Stop) {
	if !stop.All && len(stop.Tasks) == 0 {
		return
	}

	b, err := json.Marshal(stop)
	if err == nil {
		err = d.In.NC.Publish(d.In.Names.StopSubject(stop.ExecID), b)
	}

	if err != nil {
		d.In.Logger.Warn("packtrail: stop notice not sent", "exec", stop.ExecID, "err", err)
	}
}

// stateBefore returns the state of execID just before ev, the first event of
// a decision.
func (d *Dispatcher) stateBefore(ctx context.Context, cache *statecache.Cache, execID string,
	ev event.Event,
) (statecache.Entry, error) {
	// The cache is only trusted when it is exactly one decision behind:
	// another dispatcher may have handled events of this execution meanwhile.
	if e, ok := cache.Get(execID); ok && e.State.Events == ev.Index-1 && e.State.LastSeq < ev.Seq {
		return e, nil
	}

	// A continuation replaces the state with the one it carries: what came
	// before (archived, possibly purged) is not needed.
	if ev.Type == event.ExecutionContinued {
		def, err := d.Loader.DefOf(ctx, ev)
		if err != nil {
			return statecache.Entry{}, err
		}

		return statecache.Entry{State: fold.New(execID), Def: def}, nil
	}

	var (
		st  = fold.New(execID)
		def *flow.Flow
		err error
	)

	// The first event starts from nothing (and Load(…, 0) would mean "all").
	if ev.Index != 1 {
		if st, def, err = d.Loader.Load(ctx, execID, ev.Seq-1); err != nil {
			return statecache.Entry{}, err
		}
	}

	if def == nil {
		if ev.Type != event.ExecutionStarted && ev.Type != event.ExecutionForked {
			return statecache.Entry{}, nil
		}

		if def, err = d.Loader.DefOf(ctx, ev); err != nil {
			return statecache.Entry{}, err
		}
	}

	return statecache.Entry{State: st, Def: def}, nil
}

func (d *Dispatcher) effects(ctx context.Context, e statecache.Entry, ev event.Event) error {
	switch data := ev.Data.(type) {
	case *event.Scheduled:
		return d.job(ctx, e, ev, data)
	case *event.Timer:
		return d.timer(ctx, e.State.ExecID, data)
	case *event.Child:
		return d.startChild(ctx, e.State, ev, data)
	case *event.NodeDone:
		return d.storeCache(ctx, e.Def, data)
	case *event.Completed, *event.Failed, *event.Cancelled:
		return d.terminal(ctx, e, ev)
	default:
		return nil
	}
}

// ---------------------------------------------------------------------------
// Jobs

func (d *Dispatcher) job(ctx context.Context, e statecache.Entry, ev event.Event, s *event.Scheduled) error {
	st := e.State

	t := st.Tasks[s.Key]
	if t == nil || t.Generation != s.Generation || t.Attempt != s.Attempt {
		return nil // superseded within the same decision
	}

	// The dispatcher may be behind: never start work the head of the log has
	// already ended (cancelled, failed, settled, superseded) (F-06).
	if over, err := d.endedAtHead(ctx, st.ExecID, ev.Seq, s); err != nil || over {
		return err
	}

	n := e.Def.Node(s.Node)

	view := st.ContextView(t)

	ctxJSON, err := json.Marshal(view)
	if err != nil {
		return err
	}

	job := wire.Job{
		ExecID: st.ExecID, Flow: st.Flow, FlowHash: st.FlowHash, Node: s.Node, Kind: s.Kind, Key: s.Key,
		Index: s.Index, Generation: s.Generation, Attempt: s.Attempt, Context: ctxJSON, Meta: n.MetaJSON(),
		Reply: d.In.CmdSubject(st.ExecID), Traceparent: ev.Trace,
	}

	if n.Cache != nil {
		job.CacheKey = CacheKey(st.FlowHash, s.Node, view)

		hit, lerr := d.cachedResult(ctx, job)
		if lerr != nil || hit {
			return lerr
		}
	}

	if p := n.ConcurrencyProgram(); p != nil {
		v, lerr := p.Eval(ctx, st.Env(t))
		if lerr == nil && v != nil {
			job.Concurrency = &wire.Concurrency{Key: s.Node + "." + tokenHash(fmt.Sprint(v)), Max: n.Concurrency.Max}
		}
	}

	return d.publishJob(ctx, job)
}

// endedAtHead reports whether the attempt s, scheduled at seq, has already
// ended at the head of the log (completed, failed, interrupted, cancelled,
// superseded, or the execution finished). At the head this costs one
// last-sequence lookup; while catching up, the head state is loaded once per
// execution and reused for every event up to it (F-06, F2-05).
func (d *Dispatcher) endedAtHead(ctx context.Context, execID string, seq uint64, s *event.Scheduled) (bool, error) {
	if hv, ok := d.headView(execID, seq); ok {
		return !hv.live(s), nil
	}

	last, err := d.Loader.Log.LastSeq(ctx, execID)
	if err != nil || last <= seq {
		d.dropHead(execID)

		return false, err
	}

	st, _, err := d.Loader.Load(ctx, execID, 0)
	if err != nil {
		return false, err
	}

	hv := head{seq: st.LastSeq, st: st}
	d.putHead(execID, hv)

	return !hv.live(s), nil
}

// head is the state at the head of an execution's log, kept while the
// dispatcher catches up to it.
type head struct {
	seq uint64
	st  *fold.State
}

func (h head) live(s *event.Scheduled) bool {
	t := h.st.Tasks[s.Key]

	return !h.st.Status.Terminal() && t != nil && t.Generation == s.Generation && t.Attempt == s.Attempt &&
		t.Status == fold.TaskScheduled
}

const maxHeads = 1024

func (d *Dispatcher) headView(execID string, seq uint64) (head, bool) {
	d.hmu.Lock()
	defer d.hmu.Unlock()

	h, ok := d.heads[execID]
	if !ok || seq > h.seq {
		delete(d.heads, execID)

		return head{}, false
	}

	return h, true
}

func (d *Dispatcher) putHead(execID string, h head) {
	d.hmu.Lock()
	defer d.hmu.Unlock()

	if d.heads == nil || len(d.heads) >= maxHeads {
		d.heads = map[string]head{}
	}

	d.heads[execID] = h
}

func (d *Dispatcher) dropHead(execID string) {
	d.hmu.Lock()
	defer d.hmu.Unlock()

	delete(d.heads, execID)
}

func (d *Dispatcher) publishJob(ctx context.Context, job wire.Job) error {
	b, err := json.Marshal(job)
	if err != nil {
		return err
	}

	m := nats.NewMsg(d.In.Names.WorkSubject(job.Kind))
	m.Header.Set(wire.HeaderMsgID, wire.JobMsgID(job.ExecID, job.Key, job.Generation, job.Attempt))
	m.Header.Set(wire.HeaderProtocol, infra.ProtocolVersion)

	if job.Traceparent != "" {
		m.Header.Set(wire.HeaderTraceparent, job.Traceparent)
	}

	if m.Data, err = blob.Offload(ctx, d.In, job.ExecID, b, m.Header); err != nil {
		return err
	}

	if _, err = d.In.JS.PublishMsg(ctx, m); err != nil {
		return fmt.Errorf("dispatch: publish job: %w", err)
	}

	d.Metrics.JobsDispatched.Add(1)

	return nil
}

func tokenHash(s string) string {
	sum := sha256.Sum256([]byte(s))

	return hex.EncodeToString(sum[:8])
}

// CacheKey derives the result-cache key of a task: the flow version, the node
// and everything the worker can observe of the state (not visit counters,
// which change on every loop). Keys are hashes, so they are always valid KV
// keys and never collide across prefixes.
func CacheKey(flowHash, node string, v fold.Context) string {
	b, _ := json.Marshal(struct { //nolint:errchkjson // raw JSON fields were validated when stored
		F, N   string
		I      json.RawMessage
		C      map[string]json.RawMessage
		R      map[string]json.RawMessage
		S      map[string]json.RawMessage
		It, Re json.RawMessage
		Idx    *int
	}{flowHash, node, v.Input, v.Channels, v.Results, v.Signals, v.Item, v.Resume, v.Index})

	sum := sha256.Sum256(b)

	return hex.EncodeToString(sum[:16])
}

func (d *Dispatcher) cachedResult(ctx context.Context, job wire.Job) (bool, error) {
	kv, err := d.In.KV(ctx, d.In.Names.BucketCache)
	if err != nil {
		return false, err
	}

	entry, err := kv.Get(ctx, job.CacheKey)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return false, nil
		}

		d.In.Logger.Warn("packtrail: result cache read failed", "err", err)

		return false, nil
	}

	var ce wire.CacheEntry
	if err = json.Unmarshal(entry.Value(), &ce); err != nil || time.Now().After(ce.Expires) {
		return false, nil //nolint:nilerr // a corrupt or expired entry is a miss.
	}

	c, err := cmd.New(wire.CmdID(job.ExecID, job.Key, job.Generation, job.Attempt), cmd.Complete, job.ExecID,
		cmd.CompleteData{
			TaskRef: cmd.TaskRef{Key: job.Key, Generation: job.Generation, Attempt: job.Attempt},
			Output:  ce.Output, Writes: ce.Writes, Next: ce.Next, Usage: ce.Usage, Cached: true,
			CacheKey: job.CacheKey,
		})
	if err != nil {
		return false, err
	}

	if err = wire.PublishCmd(ctx, d.In, c, job.Traceparent); err != nil {
		return false, err
	}

	d.Metrics.CacheHits.Add(1)

	return true, nil
}

func (d *Dispatcher) storeCache(ctx context.Context, def *flow.Flow, done *event.NodeDone) error {
	if done.CacheKey == "" || done.Cached {
		return nil
	}

	n := def.Node(done.Node)
	if n == nil || n.Cache == nil {
		return nil
	}

	b, err := json.Marshal(wire.CacheEntry{
		Output: done.Output, Writes: done.Writes, Next: done.Next, Usage: done.Usage,
		Expires: time.Now().Add(n.Cache.TTL.D()),
	})
	if err != nil {
		return err
	}

	kv, err := d.In.KV(ctx, d.In.Names.BucketCache)
	if err != nil {
		return err
	}

	if _, err = kv.Put(ctx, done.CacheKey, b); err != nil {
		// I-05: a failed put only costs a future miss, but is logged.
		d.In.Logger.Warn("packtrail: result cache write failed", "key", done.CacheKey, "err", err)
	}

	return nil
}

// ---------------------------------------------------------------------------
// Timers

func (d *Dispatcher) timer(ctx context.Context, execID string, t *event.Timer) error {
	c, err := cmd.New("timer."+execID+"."+t.ID, cmd.Timer, execID, cmd.TimerData{TimerID: t.ID})
	if err != nil {
		return err
	}

	if err = d.schedule(ctx, execID, t.ID, t.At, c); err != nil {
		return err
	}

	d.Metrics.Timers.Add(1)

	return nil
}

// schedule installs a one-shot JetStream schedule on the timer subject that
// publishes command c to the execution's command subject at time at. The
// schedule subject is a rollup, so re-installing it is idempotent.
func (d *Dispatcher) schedule(ctx context.Context, execID, timerID string, at time.Time, c cmd.Command) error {
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	m := nats.NewMsg(d.In.Names.TimerSubject(execID, timerID))
	m.Header.Set(sched.HeaderSchedule, sched.At(at))
	m.Header.Set(sched.HeaderScheduleTarget, d.In.CmdSubject(execID))
	m.Header.Set(cmd.HeaderType, string(c.Type))
	m.Data = b

	if _, err = d.In.JS.PublishMsg(ctx, m); err != nil {
		return fmt.Errorf("dispatch: schedule timer: %w", err)
	}

	return nil
}

// ---------------------------------------------------------------------------
// Children and termination

func (d *Dispatcher) startChild(ctx context.Context, st *fold.State, ev event.Event, c *event.Child) error {
	start, err := cmd.New("start."+c.ChildID, cmd.Start, c.ChildID, cmd.StartData{
		Flow: c.Flow, Input: c.Input,
		Parent: &event.ParentRef{ExecID: st.ExecID, Node: c.Node, ChildN: c.ChildN},
	})
	if err != nil {
		return err
	}

	return wire.PublishCmd(ctx, d.In, start, ev.Trace)
}

func (d *Dispatcher) terminal(ctx context.Context, e statecache.Entry, ev event.Event) error {
	st := e.State

	var children []string

	switch data := ev.Data.(type) {
	case *event.Failed:
		children = data.CancelChildren
	case *event.Cancelled:
		children = data.CancelChildren
	}

	for _, child := range children {
		c, err := cmd.New("parent-closed."+child, cmd.Cancel, child, cmd.CancelData{Reason: "parent closed"})
		if err != nil {
			return err
		}

		if err = wire.PublishCmd(ctx, d.In, c, ev.Trace); err != nil {
			return err
		}
	}

	if st.Parent != nil {
		c, err := cmd.New("child-done."+st.ExecID, cmd.ChildDone, st.Parent.ExecID, cmd.ChildDoneData{
			ChildID: st.ExecID, Status: string(st.Status), Output: st.Output, Error: st.Error, Counters: st.Counters,
		})
		if err != nil {
			return err
		}

		if err = wire.PublishCmd(ctx, d.In, c, ev.Trace); err != nil {
			return err
		}
	}

	d.purgeTimers(ctx, st.ExecID)

	if e.Def == nil {
		return nil
	}

	if r := e.Def.Retention.D(); r > 0 {
		c, err := cmd.New("archive."+st.ExecID, cmd.Archive, st.ExecID, nil)
		if err != nil {
			return err
		}

		return d.schedule(ctx, st.ExecID, "archive", ev.Time.Add(r), c)
	}

	return nil
}

// purgeTimers drops the pending schedules of a finished execution: they could
// only fire stale timer commands.
func (d *Dispatcher) purgeTimers(ctx context.Context, execID string) {
	s, err := d.In.JS.Stream(ctx, d.In.Names.StreamCmd)
	if err == nil {
		err = s.Purge(ctx, jetstream.WithPurgeSubject(d.In.Names.TimerExecFilter(execID)))
	}

	if err != nil {
		d.In.Logger.Debug("packtrail: purge timers", "exec", execID, "err", err)
	}
}
