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

// Package runtime is the packtrail execution engine: it walks flow graphs, invokes
// nodes through a pluggable Invoker, and drives fanout/fanin, choice and signal
// nodes. All progress is durable — every transition is a CAS write to the
// executions KV and each step is triggered by a durable work message, so a
// crashed instance's work is picked up by another that acquires the ownership
// lease.
package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"

	"github.com/henomis/packtrail/internal/dsl"
	"github.com/henomis/packtrail/internal/names"
	"github.com/henomis/packtrail/internal/rules"
	"github.com/henomis/packtrail/internal/scheduler"
	"github.com/henomis/packtrail/internal/signal"
	"github.com/henomis/packtrail/internal/store"
	"github.com/henomis/packtrail/invoker"
)

// workItem is the JSON body of a message on packtrail.work.<execID>. The subject
// also carries the execID; the body adds the kind of work to perform.
type workItem struct {
	ExecID    string    `json:"exec_id"`
	Kind      string    `json:"kind"`
	Node      string    `json:"node,omitempty"`      // node/branch this item is allowed to drive
	Attempt   int       `json:"attempt,omitempty"`   // expected task/branch attempt
	NotBefore time.Time `json:"not_before,omitzero"` // retry deadline guard
	Signal    string    `json:"signal,omitempty"`    // signal name (wait timeout)
}

// Work kinds.
const (
	kindAdvance     = "advance"      // run the step for exec.CurrentNode
	kindFaninEval   = "fanin_eval"   // re-evaluate a fanin join
	kindBranchRetry = "branch_retry" // re-dispatch one async fanout branch attempt
	kindWaitTimeout = "wait_timeout" // a signal node's timeout fired
)

// Reconcile schedule keys. Each firing triggers the matching visibility
// reconciliation callback (see OnReconcileActive / OnReconcileFull). They are
// independent durable schedules so the cheap active-set pass and the
// authoritative full scan can run on separate cadences.
const (
	reconcileActiveKey = "reconcile-active"
	reconcileFullKey   = "reconcile-full"
)

const (
	defaultLeaseTTL      = 30 * time.Second
	defaultAckWait       = 60 * time.Second
	defaultRetryMaxDelay = 60 * time.Second
	defaultTimeout       = 30 * time.Second
	defaultConcurrency   = 64
	defaultMaxDeliver    = 10
	defaultDrainTimeout  = 30 * time.Second
	nakDelayShort        = 2 * time.Second
	nakDelayLong         = 5 * time.Second
	heartbeatDivisor     = 3
	maxAckPendingMult    = 2
)

// terminalError marks a process error as non-retryable: the work item can never
// succeed (e.g. its flow or node was removed, or its kind is unknown), so
// redelivering it would Nak-loop forever and burn a concurrency slot each cycle.
// handle dead-letters such items — failing the execution with a descriptive
// reason and Term-ing the message — instead of retrying.
type terminalError struct{ reason string }

func (e *terminalError) Error() string { return e.reason }

// Terminal reports that this error is non-retryable. The fired-schedule, signal
// and async-worker consumers detect it structurally (via an
// interface{ Terminal() bool } check) and dead-letter instead of Nak-looping,
// without importing this package.
func (e *terminalError) Terminal() bool { return true }

// terminal builds a non-retryable process error.
func terminal(format string, args ...any) error {
	return &terminalError{reason: fmt.Sprintf(format, args...)}
}

// Config tunes engine behaviour. Zero values fall back to sensible defaults.
type Config struct {
	OwnerID        string        // unique per instance; defaults to a random id
	LeaseTTL       time.Duration // ownership lease TTL (default 30s)
	AckWait        time.Duration // work consumer ack wait (default 60s)
	RetryBaseDelay time.Duration // base backoff for task retries (default 1s)
	RetryMaxDelay  time.Duration // cap on backoff (default 60s)
	MaxConcurrency int           // max work items processed at once (default 64)
	DefaultTimeout time.Duration // task timeout when a node omits one (default 30s)
	MaxDeliver     int           // max deliveries of a work item before dead-lettering (default 10)
	DrainTimeout   time.Duration // max time a graceful shutdown waits for in-flight work (default 30s)
}

// Engine processes executions for a set of flows.
type Engine struct {
	invoker  invoker.Invoker
	js       jetstream.JetStream
	store    *store.Store
	sched    *scheduler.Scheduler
	signals  *signal.Signals
	names    names.Names
	flows    map[string]*dsl.Flow
	programs map[string]*rules.Program // choice expression -> compiled program
	cfg      Config
	log      *slog.Logger

	onReconcileActive func(context.Context) error // optional cheap active-set reconcile hook
	onReconcileFull   func(context.Context) error // optional authoritative full reconcile hook

	sem chan struct{}

	// leaseRefs reference-counts the ownership lease per execution across this
	// instance's concurrent work items (e.g. an advance racing a fanin_eval for
	// the same execution). Without it, the first item to finish would release
	// the KV lease out from under the one still processing, opening the door
	// for another instance to acquire mid-flight. The KV lease is acquired on
	// 0→1 and released on 1→0 only. leaseMu guards only the map itself; each
	// entry carries its own mutex so the final KV release round-trip serializes
	// work items of that one execution, never unrelated ones.
	leaseMu   sync.Mutex
	leaseRefs map[string]*leaseRef
}

// leaseRef is one execution's in-process lease bookkeeping. Its mutex
// serializes retain/track/release — including the last holder's KV delete —
// for that execution only. removed marks an entry that has been unlinked from
// the map: a goroutine that fetched it before the unlink must re-fetch rather
// than mutate a refcount nobody can see anymore.
type leaseRef struct {
	mu      sync.Mutex
	refs    int
	removed bool
}

// New builds an engine and precompiles every choice expression in flows. flows
// maps flow name -> definition. inv executes task/branch nodes; it is typically
// an *invoker.Registry (optionally wrapped in an *invoker.Cache for idempotency).
// New performs no NATS I/O: sched and signals must already have their streams
// ensured by the caller (scheduler.EnsureStream / signal.Signals.EnsureStream),
// which keeps all stream creation in the caller's single-threaded, context-
// carrying setup phase.
func New(
	inv invoker.Invoker, st *store.Store, sched *scheduler.Scheduler, signals *signal.Signals,
	flows map[string]*dsl.Flow, cfg Config,
) (*Engine, error) {
	if cfg.OwnerID == "" {
		cfg.OwnerID = "engine-" + nuid.Next()
	}

	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = defaultLeaseTTL
	}

	if cfg.AckWait == 0 {
		cfg.AckWait = defaultAckWait
	}

	if cfg.RetryBaseDelay == 0 {
		cfg.RetryBaseDelay = time.Second
	}

	if cfg.RetryMaxDelay == 0 {
		cfg.RetryMaxDelay = defaultRetryMaxDelay
	}

	if cfg.MaxConcurrency <= 0 {
		cfg.MaxConcurrency = defaultConcurrency
	}

	if cfg.DefaultTimeout == 0 {
		cfg.DefaultTimeout = defaultTimeout
	}

	// Non-positive clamps to the default rather than disabling the cap: the
	// dead-letter discipline promises no message can loop forever, and a
	// negative value would otherwise disable it silently (uint64 conversion on
	// the work path, the >0 guards on the fired/signal paths).
	if cfg.MaxDeliver <= 0 {
		cfg.MaxDeliver = defaultMaxDeliver
	}

	if cfg.DrainTimeout == 0 {
		cfg.DrainTimeout = defaultDrainTimeout
	}

	programs, err := compilePrograms(flows)
	if err != nil {
		return nil, err
	}

	return &Engine{
		invoker:   inv,
		js:        st.JS(),
		store:     st,
		sched:     sched,
		signals:   signals,
		names:     st.Names(),
		flows:     flows,
		programs:  programs,
		cfg:       cfg,
		log:       slog.Default().With("owner", cfg.OwnerID),
		sem:       make(chan struct{}, cfg.MaxConcurrency),
		leaseRefs: map[string]*leaseRef{},
	}, nil
}

// getLeaseRef returns execID's live refcount entry, creating it if absent. The
// map mutex is held only for the lookup/insert — never across NATS I/O. The
// returned entry may since have been unlinked by a concurrent release; callers
// detect that via removed (under the entry's own mutex) and re-fetch.
func (e *Engine) getLeaseRef(execID string) *leaseRef {
	e.leaseMu.Lock()
	defer e.leaseMu.Unlock()

	r := e.leaseRefs[execID]
	if r == nil {
		r = &leaseRef{}
		e.leaseRefs[execID] = r
	}

	return r
}

// retainLease increments the in-process lease refcount if this instance already
// holds the lease (refcount > 0), reporting whether it did. The fast path skips
// the KV round-trip entirely for concurrent work items of one execution.
func (e *Engine) retainLease(execID string) bool {
	for {
		r := e.getLeaseRef(execID)
		r.mu.Lock()

		if r.removed {
			r.mu.Unlock()
			continue // unlinked by a concurrent release: re-fetch the live entry
		}

		held := r.refs > 0
		if held {
			r.refs++
		}

		r.mu.Unlock()

		return held
	}
}

// trackLease registers a freshly KV-acquired lease in the refcount.
func (e *Engine) trackLease(execID string) {
	for {
		r := e.getLeaseRef(execID)
		r.mu.Lock()

		if r.removed {
			r.mu.Unlock()
			continue
		}

		r.refs++
		r.mu.Unlock()

		return
	}
}

// releaseLease decrements the refcount and, on the last holder, deletes the KV
// lease. The KV delete happens under the entry's mutex so a concurrent
// retain/acquire for the same execution cannot interleave between the decision
// and the delete — but the mutex is per-execution, so the network round-trip
// never serializes work items of unrelated executions.
func (e *Engine) releaseLease(ctx context.Context, execID string) {
	for {
		r := e.getLeaseRef(execID)
		r.mu.Lock()

		if r.removed {
			r.mu.Unlock()
			continue
		}

		if r.refs > 1 {
			r.refs--
			r.mu.Unlock()

			return
		}

		// Last holder: release the KV lease, then unlink the entry. Unlinking
		// happens under both mutexes and only after removed is set, so the map
		// invariantly points at a live entry (or nothing) and any goroutine
		// holding this stale pointer re-fetches instead of mutating it.
		_ = e.store.ReleaseLease(ctx, execID, e.cfg.OwnerID)

		r.removed = true

		e.leaseMu.Lock()
		delete(e.leaseRefs, execID)
		e.leaseMu.Unlock()

		r.mu.Unlock()

		return
	}
}

// compilePrograms compiles every non-default choice rule expression up front so
// invalid expressions are caught at startup rather than at runtime.
//
//nolint:gocognit,funlen
func compilePrograms(flows map[string]*dsl.Flow) (map[string]*rules.Program, error) {
	programs := map[string]*rules.Program{}

	for _, flow := range flows {
		for i := range flow.Nodes {
			n := &flow.Nodes[i]
			if n.Type != dsl.NodeChoice {
				continue
			}

			for _, r := range n.Rules {
				if r.Default || r.When == "" {
					continue
				}

				if _, ok := programs[r.When]; ok {
					continue
				}

				prog, err := rules.Compile(r.When)
				if err != nil {
					return nil, err
				}

				programs[r.When] = prog
			}
		}
	}

	return programs, nil
}

// Start creates a new execution of flowName with the given initial payload,
// minting a fresh execution id, and enqueues the first step. It returns the id.
// The payload must be a JSON object (or empty, defaulted to {}); it becomes the
// `input` field of every invocation context and choice expression. nil is
// treated as an empty object.
func (e *Engine) Start(ctx context.Context, flowName string, payload json.RawMessage) (string, error) {
	return e.start(ctx, "exec-"+nuid.Next(), flowName, payload)
}

// StartWithID is an idempotent Start keyed by a caller-supplied execution id (an
// idempotency key). The first call creates and enqueues the execution; any later
// call with the same id and the same arguments is a no-op that returns the id
// unchanged — so a timed-out-then-retried Start produces exactly one execution.
// First-write wins, and reuse is checked: the id is bound to the first call's
// flow and byte-identical payload; a repeat naming a different flow or carrying
// a different payload returns an error rather than silently reporting the
// existing execution as its own. (The arguments are also validated before the
// existence check, so a retry that supplies an unknown flow or a non-object
// payload returns that validation error — retries must replay the same
// arguments.) The id must match [A-Za-z0-9_-]{1,128} (it becomes a NATS subject
// token and KV key); supply a stable key such as your domain id (e.g.
// "order-12345"). Like Start, the payload must be a JSON object (or empty).
func (e *Engine) StartWithID(ctx context.Context, execID, flowName string, payload json.RawMessage) (string, error) {
	if !validExecID(execID) {
		return "", fmt.Errorf("invalid execution id %q: must match [A-Za-z0-9_-]{1,128}", execID)
	}

	return e.start(ctx, execID, flowName, payload)
}

// execIDPattern bounds caller-supplied execution ids to characters safe as a
// single NATS subject token and KV key.
var execIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

func validExecID(id string) bool { return execIDPattern.MatchString(id) }

func advanceWorkItem(execID, node string, attempt int, notBefore time.Time) (json.RawMessage, error) {
	return json.Marshal(workItem{
		ExecID:    execID,
		Kind:      kindAdvance,
		Node:      node,
		Attempt:   attempt,
		NotBefore: notBefore.UTC(),
	})
}

func faninEvalWorkItem(execID, fanin string) (json.RawMessage, error) {
	return json.Marshal(workItem{ExecID: execID, Kind: kindFaninEval, Node: fanin})
}

func branchRetryWorkItem(execID, branchID string, attempt int, notBefore time.Time) (json.RawMessage, error) {
	return json.Marshal(workItem{
		ExecID:    execID,
		Kind:      kindBranchRetry,
		Node:      branchID,
		Attempt:   attempt,
		NotBefore: notBefore.UTC(),
	})
}

// requireObjectPayload rejects a start input that is not a JSON object. The
// input is addressed field-wise from choice expressions (`input.tier`) and
// documented as an object throughout; node outputs, by contrast, are free-form
// (each lives under its own results.<node> key). An empty payload is defaulted
// to {} before this check, so it is always a valid object.
func requireObjectPayload(payload json.RawMessage) error {
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return fmt.Errorf("initial payload must be a JSON object")
	}

	if !json.Valid(trimmed) {
		return fmt.Errorf("initial payload is not valid JSON")
	}

	return nil
}

// start creates the execution under id and enqueues its first step. A repeated id
// (store.ErrAlreadyExists) is treated as idempotent success: the existing
// execution's id is returned without re-creating or re-enqueueing it.
func (e *Engine) start(ctx context.Context, id, flowName string, payload json.RawMessage) (string, error) {
	flow, ok := e.flows[flowName]
	if !ok {
		return "", fmt.Errorf("unknown flow %q", flowName)
	}

	if len(payload) == 0 {
		payload = json.RawMessage("{}")
	}

	if err := requireObjectPayload(payload); err != nil {
		return "", err
	}

	// Data before control: the input is written to the data plane first, so
	// the execution can never exist without its input being readable. The
	// write is create-if-absent — first write wins — so an idempotent Start
	// retry carrying a different payload cannot overwrite the input the
	// original execution runs on. A crash before Create leaves a tiny orphaned
	// entry the retry reuses. A pre-existing input that differs from this
	// call's payload is a contract violation (the id was reused with different
	// arguments) and errors out here: without the check, two concurrent
	// StartWithID calls racing the two planes could bind one caller's document
	// to the other caller's payload — with both callers told success.
	existing, err := e.store.CreatePayload(ctx, store.InputKey(id), payload)
	if err != nil {
		return "", err
	}

	if existing != nil && !bytes.Equal(existing, payload) {
		return "", fmt.Errorf(
			"execution id %q is already bound to a different input payload: "+
				"an idempotency key must be retried with byte-identical arguments", id)
	}

	exec := &store.Execution{
		ID:          id,
		FlowName:    flowName,
		CurrentNode: flow.StartNode(),
		Status:      store.StatusRunning,
	}

	// The first work item is committed in the same write that creates the
	// execution (transactional outbox): the document and its driving message
	// can never disagree, whatever crashes.
	item, err := advanceWorkItem(id, exec.CurrentNode, exec.Attempt, time.Time{})
	if err != nil {
		return "", err
	}

	exec.AppendWork(item)

	if _, err = e.store.Create(ctx, exec); err != nil {
		if errors.Is(err, store.ErrAlreadyExists) {
			// Idempotent: this id already started. Re-flush anything the
			// original commit left unpublished before returning it.
			if repairErr := e.repairStart(ctx, id, flowName); repairErr != nil {
				return "", repairErr
			}

			return id, nil
		}

		return "", err
	}

	e.emitEvent(ctx, exec)

	if err = e.flushOutbox(ctx, exec); err != nil {
		return "", fmt.Errorf(
			"execution %s created and its first work item committed, but publishing is failing "+
				"(retry StartWithID with the same id, or the stall watchdog re-publishes it): %w",
			id, err)
	}

	return id, nil
}

// repairStart heals the crash window between the create and the first outbox
// flush: a retried Start/StartWithID re-flushes whatever the original commit
// left unpublished. The flush is msg-id-deduped, so nudging an id whose first
// item is merely still in the queue publishes nothing new. A retry naming a
// different flow than the one the id is bound to is a contract violation and
// errors instead of masquerading as idempotent success.
func (e *Engine) repairStart(ctx context.Context, id, flowName string) error {
	ex, err := e.store.Get(ctx, id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil // archived or pruned: nothing to repair
		}

		return err
	}

	if ex.FlowName != flowName {
		return fmt.Errorf("execution id %q is bound to flow %q, not %q: "+
			"an idempotency key must be retried with the same arguments", id, ex.FlowName, flowName)
	}

	return e.flushOutbox(ctx, ex)
}

// flushOutbox publishes the execution's committed follow-on messages and clears
// the ones that made it out. It is the second half of the transactional-outbox
// pattern: the transition and its outbox items commit in one CAS write, then
// this flush moves them onto the work/schedule streams. A crash or fault at any
// point is safe — unpublished items stay durably on the document and are
// re-flushed by the next touch (work delivery, completion, retried Start, or
// the stall watchdog); re-published items are dropped by the streams' msg-id
// dedup window, and beyond it a duplicate is state-safe against the guarded
// transitions.
func (e *Engine) flushOutbox(ctx context.Context, exec *store.Execution) error {
	if len(exec.Outbox) == 0 {
		return nil
	}

	flushed := make(map[uint64]bool, len(exec.Outbox))

	var pubErr error

	for _, it := range exec.Outbox {
		msgID := exec.ID + "." + strconv.FormatUint(it.Seq, 10)

		switch it.Kind {
		case store.OutboxWork:
			pubErr = e.publishWork(ctx, exec.ID, it.Item, msgID)
		case store.OutboxSched:
			pubErr = e.sched.AtID(ctx, exec.ID, msgID, it.At, it.Item)
		default:
			// Unknown kind (a newer writer in a mixed-version fleet): it can never
			// publish, so clear it rather than poisoning the outbox forever. Emit a
			// durable dead-letter trace so the dropped item is observable, not just
			// logged — the upgrade rule is engines first, kind-producing features after.
			e.log.Error("dropping outbox item of unknown kind", "exec", exec.ID, "kind", it.Kind, "seq", it.Seq)
			e.emitDeadLetter(ctx, store.DeadLetterWork, exec.ID,
				fmt.Sprintf("dropped outbox item of unknown kind %q (seq %d)", it.Kind, it.Seq), 0)

			flushed[it.Seq] = true

			continue
		}

		if pubErr != nil {
			break // publish the prefix; the remainder re-flushes on the next touch
		}

		flushed[it.Seq] = true
	}

	if len(flushed) > 0 {
		if _, err := e.store.Mutate(ctx, exec.ID, func(ex *store.Execution) error {
			if !ex.DropOutbox(flushed) {
				return errSkip // another flusher already cleared them
			}

			return nil
		}); err != nil && !errors.Is(err, errSkip) && !errors.Is(err, store.ErrNotFound) {
			return err // items published but not cleared: a re-flush dedups
		}
	}

	return pubErr
}

// stallRedriveDefaultMult sets the default stall threshold as a multiple of
// AckWait: anything legitimately in flight refreshes its work item within one
// AckWait (heartbeat InProgress), so several multiples of quiet is a strong
// signal the driving work item was lost in a crash window.
const stallRedriveDefaultMult = 5

// RedriveStalled re-drives one execution if it looks stranded: still active,
// quiet for longer than olderThan (non-positive means the default of
// 5×AckWait), not inside a scheduled retry backoff, and with no live ownership
// lease (a held lease means an instance is processing it right now). A stalled
// running execution gets an advance; a stalled fanin wait gets a join
// re-evaluation. Signal waits are excluded (their timeout owns them) and so
// are async task waits (CompleteActivity owns them, and it may legitimately
// take arbitrarily long).
//
// Every transition is guarded, so a false-positive re-drive is state-safe —
// at worst it duplicates an invocation within the documented at-least-once
// contract. It returns whether a work item was enqueued.
//
// This is the operational backstop for the transactional outbox: an execution
// whose committed follow-on messages were never flushed (crash between the CAS
// write and the publish) self-heals within one watchdog pass instead of
// waiting for a manual Resume; the blind re-drives below additionally cover
// anything with an empty outbox that still looks stranded.
func (e *Engine) RedriveStalled(ctx context.Context, execID string, olderThan time.Duration) (bool, error) {
	if olderThan <= 0 {
		olderThan = stallRedriveDefaultMult * e.cfg.AckWait
	}

	ex, err := e.store.Get(ctx, execID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return false, nil
		}

		return false, err
	}

	if !ex.Active() {
		return false, nil
	}

	// Measure quiet time from the later of the last write and the scheduled
	// retry fire time, so a long backoff is never mistaken for a stall.
	quietSince := ex.UpdatedAt
	if ex.RetryAt.After(quietSince) {
		quietSince = ex.RetryAt
	}

	if time.Since(quietSince) < olderThan {
		return false, nil
	}

	held, err := e.store.LeaseHeld(ctx, execID)
	if err != nil || held {
		return false, err // in-flight work heartbeats its lease: not stalled
	}

	// A non-empty outbox is the precise diagnosis: a transition committed its
	// follow-on work but the flush never landed. Re-flush exactly that instead
	// of guessing a blind re-drive.
	if len(ex.Outbox) > 0 {
		return true, e.flushOutbox(ctx, ex)
	}

	switch {
	case ex.Status == store.StatusRunning:
		return true, e.enqueue(ctx, execID, workItem{
			Kind: kindAdvance, Node: ex.CurrentNode, Attempt: ex.Attempt, NotBefore: ex.RetryAt,
		})
	case ex.Status == store.StatusWaiting && ex.WaitSignal == "":
		flow, ok := e.flows[ex.FlowName]
		if !ok {
			return false, nil
		}

		if node := flow.Node(ex.CurrentNode); node == nil || node.Type != dsl.NodeFanin {
			return false, nil // async task wait: CompleteActivity owns it
		}

		return true, e.enqueue(ctx, execID, workItem{Kind: kindFaninEval, Node: ex.CurrentNode})
	default:
		return false, nil // signal wait: the wait timeout owns it
	}
}

// ScheduleFlow installs a recurring schedule that starts a new execution of
// flowName on the given 6-field cron expression ("sec min hour dom mon dow").
// name uniquely identifies the schedule; reusing it replaces the schedule.
func (e *Engine) ScheduleFlow(ctx context.Context, name, flowName, cronExpr string, payload json.RawMessage) error {
	if !validExecID(name) {
		return fmt.Errorf("invalid schedule name %q: must match [A-Za-z0-9_-]{1,128} (it becomes a NATS subject token)", name)
	}

	if _, ok := e.flows[flowName]; !ok {
		return fmt.Errorf("unknown flow %q", flowName)
	}

	if len(payload) == 0 {
		payload = json.RawMessage("{}")
	}

	return e.sched.Cron(ctx, name, "start."+flowName, cronExpr, payload)
}

// ReclaimFiredSchedules purges already-processed fired-schedule messages from
// the schedule stream (see scheduler.ReclaimFired), bounding the growth of
// consumed fire.* messages. It is safe to run on the full-reconcile cadence.
// Returns how many messages were purged.
func (e *Engine) ReclaimFiredSchedules(ctx context.Context) (uint64, error) {
	return e.sched.ReclaimFired(ctx, e.names.DurFired)
}

// OnReconcileActive registers the callback fired by the active-set reconcile
// schedule (the cheap, frequent pass over in-flight executions). Optional; if
// unset, fired active schedules are ignored.
func (e *Engine) OnReconcileActive(fn func(context.Context) error) { e.onReconcileActive = fn }

// OnReconcileFull registers the callback fired by the full reconcile schedule
// (the authoritative deep scan). Optional; if unset, fired full schedules are
// ignored.
func (e *Engine) OnReconcileFull(fn func(context.Context) error) { e.onReconcileFull = fn }

// ScheduleReconcileActive installs the recurring active-set reconcile schedule
// on the given 6-field cron expression ("sec min hour dom mon dow"), e.g.
// "0 */5 * * * *". Pair it with OnReconcileActive.
func (e *Engine) ScheduleReconcileActive(ctx context.Context, cronExpr string) error {
	return e.sched.Cron(ctx, reconcileActiveKey, reconcileActiveKey, cronExpr, nil)
}

// ScheduleReconcileFull installs the recurring full reconcile schedule on the
// given 6-field cron expression, e.g. "0 0 * * * *" for hourly. Run it less
// often than the active schedule; pair it with OnReconcileFull.
func (e *Engine) ScheduleReconcileFull(ctx context.Context, cronExpr string) error {
	return e.sched.Cron(ctx, reconcileFullKey, reconcileFullKey, cronExpr, nil)
}

// assembleContext builds the invocation context an Invoker (or a choice rule)
// sees — {"input": …, "results": {node: output, …}, "signals": {name: payload,
// …}, "branches": {branch: output, …}, "last_node": "…"} — by reading the
// execution's data-plane entries: the start input, every settled node output
// (Outputs), and every received signal (LastSeq keys). A missing entry
// (archived/pruned) is skipped rather than failing the assembly.
//
// Two navigation aids come with the raw maps, because results alone is
// unordered: last_node is the id of the most recently settled output (chain
// flows read "the previous step's result" as results[last_node]), and branches
// is the subset of results produced by the current fan's branches (a node
// after a join reads its inputs there; whether the join just happened is
// last_node ∈ branches).
func (e *Engine) assembleContext(ctx context.Context, ex *store.Execution) (json.RawMessage, error) {
	in, err := e.store.GetPayload(ctx, store.InputKey(ex.ID))
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return nil, err
	}

	results := make(map[string]json.RawMessage, len(ex.Outputs))

	for _, node := range ex.Outputs {
		out, getErr := e.store.GetPayload(ctx, store.OutputKey(ex.ID, node))
		if getErr != nil {
			if errors.Is(getErr, store.ErrNotFound) {
				continue
			}

			return nil, getErr
		}

		results[node] = out
	}

	signals := make(map[string]json.RawMessage, len(ex.LastSeq))

	for name, seq := range ex.LastSeq {
		p, getErr := e.store.GetPayload(ctx, store.SignalKey(ex.ID, name, seq))
		if getErr != nil {
			if errors.Is(getErr, store.ErrNotFound) {
				continue
			}

			return nil, getErr
		}

		signals[name] = p
	}

	if len(in) == 0 {
		in = json.RawMessage("{}")
	}

	branches := map[string]json.RawMessage{}

	for b := range ex.Branches {
		if out, ok := results[b]; ok {
			branches[b] = out
		}
	}

	lastNode := ""
	if len(ex.Outputs) > 0 {
		lastNode = ex.Outputs[len(ex.Outputs)-1]
	}

	return json.Marshal(struct {
		Input    json.RawMessage            `json:"input"`
		Results  map[string]json.RawMessage `json:"results"`
		Signals  map[string]json.RawMessage `json:"signals"`
		Branches map[string]json.RawMessage `json:"branches"`
		LastNode string                     `json:"last_node"`
	}{Input: in, Results: results, Signals: signals, Branches: branches, LastNode: lastNode})
}

// Results assembles the execution's data-plane view — the same context
// document invokers and choice rules see. ErrNotFound if the execution does
// not exist.
func (e *Engine) Results(ctx context.Context, execID string) (json.RawMessage, error) {
	if !validExecID(execID) {
		return nil, fmt.Errorf("invalid execution id %q: must match [A-Za-z0-9_-]{1,128}", execID)
	}

	ex, err := e.store.Get(ctx, execID)
	if err != nil {
		return nil, err
	}

	return e.assembleContext(ctx, ex)
}

// enqueue publishes a work item to the execution's work subject.
func (e *Engine) enqueue(ctx context.Context, execID string, wi workItem) error {
	wi.ExecID = execID

	data, err := json.Marshal(wi)
	if err != nil {
		return err
	}

	return e.publishWork(ctx, execID, data, "")
}

// publishWork publishes raw work-item bytes to the execution's work subject.
// A non-empty msgID rides as Nats-Msg-Id, so a re-publish of the same outbox
// item inside the stream's dedup window is dropped.
func (e *Engine) publishWork(ctx context.Context, execID string, data []byte, msgID string) error {
	var opts []jetstream.PublishOpt
	if msgID != "" {
		opts = append(opts, jetstream.WithMsgID(msgID))
	}

	_, err := e.js.Publish(ctx, e.names.SubjWorkPrefix+execID, data, opts...)

	return err
}

// Run subscribes to the work stream and processes items until ctx is cancelled.
func (e *Engine) Run(ctx context.Context) error {
	cons, err := e.js.CreateOrUpdateConsumer(ctx, e.names.StreamWork, jetstream.ConsumerConfig{
		Durable:       e.names.DurEngine,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       e.cfg.AckWait,
		MaxAckPending: e.cfg.MaxConcurrency * maxAckPendingMult,
		FilterSubject: e.names.SubjWorkPrefix + ">",
	})
	if err != nil {
		return fmt.Errorf("work consumer: %w", err)
	}

	// In-flight handlers run under procParent, which is detached from ctx's
	// cancellation (it keeps ctx's values but is not cancelled when ctx is). That
	// lets a graceful shutdown drain work already in flight within DrainTimeout
	// instead of aborting it mid-invocation: cancelling ctx alone would otherwise
	// cancel each handler's derived context and turn the invocation into a transient
	// failure to be redelivered. A hard crash still relies on redelivery
	// (at-least-once); a clean restart no longer double-fires in-flight work for
	// naturally non-idempotent targets. stopProc force-aborts any stragglers still
	// running past the drain deadline.
	procParent, stopProc := context.WithCancel(context.WithoutCancel(ctx))
	defer stopProc()

	// draining (under drainMu) fences handler registration against the drain:
	// cc.Stop does not wait for a callback already executing, so without the
	// fence such a callback could inflight.Add(1) concurrently with drain's
	// inflight.Wait() on a zero counter — a WaitGroup misuse the race detector
	// flags — and its handler could outlive Run un-drained.
	var (
		inflight sync.WaitGroup
		drainMu  sync.Mutex
		draining bool
	)

	cc, err := cons.Consume(func(msg jetstream.Msg) {
		// Don't pick up new work once shutdown has begun; let it redeliver.
		if ctx.Err() != nil {
			_ = msg.NakWithDelay(nakDelayShort)
			return
		}

		e.sem <- struct{}{}

		drainMu.Lock()
		if draining {
			drainMu.Unlock()
			<-e.sem

			_ = msg.NakWithDelay(nakDelayShort)

			return
		}

		inflight.Add(1)
		drainMu.Unlock()

		go func() {
			defer inflight.Done()
			defer func() { <-e.sem }()

			e.handle(procParent, msg)
		}()
	})
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	// On shutdown: stop delivery, then wait for in-flight handlers to settle and
	// ack, bounded by DrainTimeout. If the drain window elapses, stopProc cancels
	// the stragglers so they unwind (their work redelivers) and Run can return.
	defer func() {
		cc.Stop()

		drainMu.Lock()
		draining = true
		drainMu.Unlock()

		e.drain(&inflight, stopProc)
	}()

	// Forward fired schedules (retry backoff, wait timeouts) back onto the work
	// stream. The fired payload is the original work item; its subject key is
	// the execution id.
	firedDeadLetter := func(key, reason string, n uint64) {
		e.emitDeadLetter(ctx, store.DeadLetterSchedule, key, reason, n)
	}

	fc, err := e.sched.ConsumeFired(ctx, e.names.DurFired, e.cfg.MaxDeliver, firedDeadLetter, e.handleFired(ctx))
	if err != nil {
		return fmt.Errorf("fired consumer: %w", err)
	}
	defer fc.Stop()

	// Apply external signals idempotently (CAS before ack). The signals stream is
	// ensured once at construction (see New), not here, so concurrent engines do
	// not issue racing stream updates.
	signalDeadLetter := func(execID, name, reason string, n uint64) {
		e.emitDeadLetter(ctx, store.DeadLetterSignal, execID+"/"+name, reason, n)
	}

	sc, err := e.signals.Consume(ctx, e.names.DurSignals, e.cfg.MaxDeliver, signalDeadLetter, e.applySignal)
	if err != nil {
		return fmt.Errorf("signal consumer: %w", err)
	}
	defer sc.Stop()

	<-ctx.Done()

	return nil
}

// handleFired returns the fired-schedule handler: it forwards reconcile ticks to
// the matching callback, starts a new execution for a "start.<flow>" cron key (a
// removed flow is terminal, so the tick dead-letters rather than Nak-looping), and
// otherwise re-injects the fired payload as a work item on its execution subject.
//
// firedID is a stable per-firing idempotency key (the fired message's stream
// sequence). A cron tick can be redelivered — the handler Naks on a transient
// Start fault, or the ack itself is lost after the Start — so the execution is
// created via StartWithID keyed on firedID: every redelivery of the same tick
// resolves to the same id and dedups on the KV create, producing exactly one
// execution per firing instead of one per delivery.
func (e *Engine) handleFired(ctx context.Context) func(key string, payload []byte, firedID string) error {
	return func(key string, payload []byte, firedID string) error {
		switch key {
		case reconcileActiveKey:
			if e.onReconcileActive != nil {
				return e.onReconcileActive(ctx)
			}

			return nil
		case reconcileFullKey:
			if e.onReconcileFull != nil {
				return e.onReconcileFull(ctx)
			}

			return nil
		}

		if flowName, ok := strings.CutPrefix(key, "start."); ok {
			if _, known := e.flows[flowName]; !known {
				// The scheduled flow was removed; every cron tick would Nak forever.
				// Terminal: the fired consumer dead-letters this tick.
				return terminal("cron start: unknown flow %q", flowName)
			}

			// StartWithID keyed on the firing makes a redelivered tick idempotent.
			// firedID is empty only when metadata is unavailable; fall back to a
			// random id then (a rare best-effort at-least-once tick).
			if firedID != "" {
				_, startErr := e.StartWithID(ctx, "cron-"+firedID, flowName, payload)

				return startErr
			}

			_, startErr := e.Start(ctx, flowName, payload)

			return startErr
		}

		_, pubErr := e.js.Publish(ctx, e.names.SubjWorkPrefix+key, payload)

		return pubErr
	}
}

// drain waits for in-flight handlers to finish, bounded by DrainTimeout. If they
// all settle within the window it returns cleanly; otherwise it cancels the
// stragglers (via stopProc) so they unwind — their work redelivers later — and
// waits for them to return so no handler goroutine outlives Run.
func (e *Engine) drain(inflight *sync.WaitGroup, stopProc context.CancelFunc) {
	done := make(chan struct{})

	go func() {
		inflight.Wait()
		close(done)
	}()

	select {
	case <-done:
		return
	case <-time.After(e.cfg.DrainTimeout):
		e.log.Warn("drain timeout: aborting in-flight work", "timeout", e.cfg.DrainTimeout)
		stopProc()
		inflight.Wait()
	}
}

// handle processes one work item under the ownership lease.
func (e *Engine) handle(ctx context.Context, msg jetstream.Msg) {
	var wi workItem
	if err := json.Unmarshal(msg.Data(), &wi); err != nil {
		e.log.Error("bad work item", "err", err)

		_ = msg.Term() // poison message: do not redeliver

		return
	}

	// Acquire ownership. If another instance owns this execution, back off and
	// let it (or a later redelivery) make progress. When another work item for
	// the same execution is already running on this instance, retain its lease
	// in-process instead of a redundant KV acquire.
	if !e.retainLease(wi.ExecID) {
		ok, err := e.store.AcquireLease(ctx, wi.ExecID, e.cfg.OwnerID, e.cfg.LeaseTTL)
		if err != nil {
			e.log.Error("lease acquire", "exec", wi.ExecID, "err", err)

			_ = msg.NakWithDelay(nakDelayShort)

			return
		}

		if !ok {
			_ = msg.NakWithDelay(nakDelayShort)
			return
		}

		e.trackLease(wi.ExecID)
	}

	// Heartbeat: renew the lease and extend the ack window while we work. It runs
	// against procCtx and, if it detects the lease was lost (another instance took
	// over after a pause longer than LeaseTTL), cancels procCtx so processing
	// aborts early — narrowing, though not eliminating, the window in which both
	// instances run the node. ReleaseLease below uses the outer ctx so it still
	// runs after a cancel.
	procCtx, cancelProc := context.WithCancel(ctx)
	go e.heartbeat(procCtx, cancelProc, msg, wi.ExecID)

	defer func() {
		cancelProc()

		e.releaseLease(ctx, wi.ExecID)
	}()

	err := e.processGuarded(procCtx, wi)
	if err != nil {
		e.log.Error("process", "exec", wi.ExecID, "kind", wi.Kind, "err", err)

		// Terminal error: the item can never succeed (removed flow/node, unknown
		// kind). Dead-letter it instead of Nak-looping forever.
		var termErr *terminalError
		if errors.As(err, &termErr) {
			e.deadLetter(ctx, msg, wi.ExecID, termErr.reason)
			return
		}

		// Transient error (store/NATS fault): retry, but cap the number of
		// deliveries so a persistently failing item is eventually dead-lettered
		// rather than burning a slot every nakDelayLong forever.
		if e.deliveriesExhausted(msg) {
			e.deadLetter(ctx, msg, wi.ExecID,
				fmt.Sprintf("work item %q exhausted %d deliveries: %v", wi.Kind, e.cfg.MaxDeliver, err))

			return
		}

		_ = msg.NakWithDelay(nakDelayLong)

		return
	}

	_ = msg.Ack()
}

// deliveriesExhausted reports whether this message has been delivered at least
// MaxDeliver times. A metadata read failure is treated as not-exhausted so a
// transient fault keeps retrying rather than prematurely dead-lettering.
func (e *Engine) deliveriesExhausted(msg jetstream.Msg) bool {
	meta, err := msg.Metadata()
	if err != nil {
		return false
	}

	return meta.NumDelivered >= uint64(e.cfg.MaxDeliver) //nolint:gosec // MaxDeliver is a small positive config value
}

// deadLetterHardCapMult bounds how long deadLetter keeps Nak-ing when it cannot
// record the failure on the execution document. Past MaxDeliver × this multiple
// the item is Term'd anyway — but only once the durable dead-letter trace is
// emitted, so the drop is always observable. Until the cap it keeps retrying, so
// a transient store/NATS blip never loses work.
const deadLetterHardCapMult = 3

// deadLetter settles a poisoned work item: it fails the execution with reason
// (best-effort — a missing or already-terminal execution is fine), records a
// durable dead-letter trace, and Terms the message so it is never redelivered.
//
// If the failure cannot be recorded on the document, the message is normally
// Naked so a later delivery retries the dead-lettering rather than silently
// dropping the work. A *persistently* broken recording path would Nak forever
// (livelock); past a hard cap the item is dropped anyway, but only if the durable
// dead-letter trace lands first — so the drop is never silent. If even the trace
// cannot be written (total NATS outage) it keeps Nak-ing, which is correct: the
// loop clears once NATS recovers.
func (e *Engine) deadLetter(ctx context.Context, msg jetstream.Msg, execID, reason string) {
	failErr := e.fail(ctx, execID, reason)
	if failErr == nil || errors.Is(failErr, store.ErrNotFound) {
		e.emitDeadLetter(ctx, store.DeadLetterWork, execID, reason, deliveries(msg))
		e.log.Warn("dead-lettered work item", "exec", execID, "reason", reason)

		_ = msg.Term()

		return
	}

	//nolint:gosec // MaxDeliver is a small positive config value (clamped in New)
	if deliveries(msg) >= uint64(e.cfg.MaxDeliver)*deadLetterHardCapMult {
		dl := store.DeadLetter{
			Kind: store.DeadLetterWork, Key: execID, Deliveries: deliveries(msg),
			Reason: "failure could not be recorded on the execution (persistent): " + reason,
		}
		if emitErr := e.store.EmitDeadLetter(ctx, dl); emitErr == nil {
			e.log.Error("dropping work item after persistent record failure",
				"exec", execID, "err", failErr, "reason", reason)

			_ = msg.Term()

			return
		}
	}

	e.log.Error("dead-letter: record failure", "exec", execID, "err", failErr)

	_ = msg.NakWithDelay(nakDelayLong)
}

// emitDeadLetter records a durable dead-letter trace (best-effort: a failure to
// record only loses observability, never correctness). It is also the sink the
// fired-schedule and signal consumers call when they Term poisoned work.
func (e *Engine) emitDeadLetter(ctx context.Context, kind, key, reason string, deliveries uint64) {
	dl := store.DeadLetter{Kind: kind, Key: key, Reason: reason, Deliveries: deliveries}
	if err := e.store.EmitDeadLetter(ctx, dl); err != nil {
		e.log.Debug("emit dead-letter", "kind", kind, "key", key, "err", err)
	}
}

// deliveries reads a message's delivery count, or 0 when metadata is unavailable.
func deliveries(msg jetstream.Msg) uint64 {
	if msg == nil {
		return 0
	}

	if meta, err := msg.Metadata(); err == nil {
		return meta.NumDelivered
	}

	return 0
}

// emitEvent publishes a domain event, logging at debug level on failure. Event
// emission is best-effort: the visibility indexes are eventually reconciled, so
// a failure here never blocks execution progress.
func (e *Engine) emitEvent(ctx context.Context, ex *store.Execution) {
	if err := e.store.EmitEvent(ctx, ex); err != nil {
		e.log.Debug("emit event", "exec", ex.ID, "err", err)
	}
}

// heartbeat renews the lease and extends the ack window while a work item runs.
// If a renewal reports the lease is no longer held by this instance — another
// instance took over after this one paused past LeaseTTL — it calls onLost to
// cancel processing. This narrows but does not eliminate double-firing: an
// invocation already in flight may still complete on both instances, so node
// invocation is at-least-once (the execution-doc CAS fences *state*, not external
// side-effects). Use invoker.Cache / WithResultCache for non-idempotent targets.
func (e *Engine) heartbeat(ctx context.Context, onLost context.CancelFunc, msg jetstream.Msg, execID string) {
	t := time.NewTicker(e.cfg.LeaseTTL / heartbeatDivisor)
	defer t.Stop()

	renewalErrs := 0

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if !e.heartbeatTick(ctx, onLost, msg, execID, &renewalErrs) {
				return
			}
		}
	}
}

// heartbeatTick performs one heartbeat cycle: it extends the ack window
// (InProgress, under a panic guard) and renews the ownership lease. It returns
// false — after calling onLost where appropriate — when processing should abort:
// the InProgress panic-guard fired (onLost cancels ctx), ctx was cancelled,
// renewals have failed heartbeatDivisor times in a row, or the lease is no longer
// held. renewalErrs accumulates transient renewal failures across ticks and is
// reset on a successful renewal.
func (e *Engine) heartbeatTick(
	ctx context.Context, onLost context.CancelFunc, msg jetstream.Msg, execID string, renewalErrs *int,
) bool {
	func() {
		defer func() {
			if r := recover(); r != nil {
				e.log.Error("heartbeat panic",
					"exec", execID,
					"panic", r, "stack", string(debug.Stack()))
				onLost()
			}
		}()

		_ = msg.InProgress()
	}()

	if ctx.Err() != nil {
		return false
	}

	held, err := e.store.AcquireLease(ctx, execID, e.cfg.OwnerID, e.cfg.LeaseTTL)
	if err != nil {
		// A single failed renewal is a blip; a full TTL of them means the lease
		// may have expired and been taken over while this instance runs blind —
		// exactly the double-fire window the heartbeat exists to narrow. Assume
		// lost and abort: the CAS still fences state and the work redelivers.
		*renewalErrs++
		if *renewalErrs >= heartbeatDivisor {
			e.log.Warn("lease renewal failing; assuming lost and aborting",
				"exec", execID, "err", err)
			onLost()

			return false
		}

		return true // transient blip: keep heartbeating without resetting the count
	}

	*renewalErrs = 0

	if !held {
		e.log.Warn("lease lost during processing; aborting", "exec", execID)
		onLost()

		return false
	}

	return true
}

// processGuarded runs process under a top-level panic recovery. process runs on
// the work-consumer goroutine, which has no recovery above it, so a panic
// anywhere it reaches — choice-expression evaluation (prog.Match), JSON
// (un)marshaling, or any engine-code bug — would otherwise crash the whole
// engine process, taking every other in-flight execution with it. Recovering
// here converts the panic into a terminal error so only the offending execution
// is dead-lettered: a redelivery would likely re-panic, so retrying is pointless
// (the same reasoning as invoke's Invoker-panic guard, one layer out).
func (e *Engine) processGuarded(ctx context.Context, wi workItem) (err error) {
	defer func() {
		if r := recover(); r != nil {
			e.log.Error("process panic",
				"exec", wi.ExecID, "kind", wi.Kind,
				"panic", r, "stack", string(debug.Stack()))

			err = terminal("process panic: %v", r)
		}
	}()

	return e.process(ctx, wi)
}

// process dispatches a work item to the right handler.
func (e *Engine) process(ctx context.Context, wi workItem) error {
	exec, err := e.store.Get(ctx, wi.ExecID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil // nothing to do
		}

		return err
	}

	if !exec.Active() {
		return nil // terminal; drop the item
	}

	// Re-publish anything a previous transition committed but failed to flush
	// (crash between the CAS write and the publish): any delivery for the
	// execution heals its outbox immediately, without waiting for the watchdog.
	// Re-publishing this very item is possible and harmless (msg-id dedup, then
	// the guarded transitions).
	if err = e.flushOutbox(ctx, exec); err != nil {
		return err // transient: Nak and retry
	}

	flow, ok := e.flows[exec.FlowName]
	if !ok {
		return terminal("unknown flow %q", exec.FlowName)
	}

	switch wi.Kind {
	case kindAdvance:
		return e.processAdvance(ctx, flow, exec, wi)
	case kindFaninEval:
		return e.processFaninEval(ctx, flow, exec, wi)
	case kindBranchRetry:
		return e.processBranchRetry(ctx, flow, exec, wi)
	case kindWaitTimeout:
		return e.onWaitTimeout(ctx, flow, exec, wi)
	default:
		return terminal("unknown work kind %q", wi.Kind)
	}
}

func (e *Engine) processAdvance(ctx context.Context, flow *dsl.Flow, exec *store.Execution, wi workItem) error {
	if exec.Status != store.StatusRunning {
		return nil
	}

	if !advanceApplies(exec, wi) {
		return nil
	}

	if due := advanceNotBefore(exec, wi); !due.IsZero() && time.Now().Before(due) {
		return e.deferWorkUntil(ctx, exec, wi, due)
	}

	node := flow.Node(exec.CurrentNode)
	if node == nil {
		return terminal("unknown node %q in flow %q", exec.CurrentNode, exec.FlowName)
	}

	return e.stepNode(ctx, flow, node, exec)
}

func advanceApplies(exec *store.Execution, wi workItem) bool {
	if wi.Node == "" {
		return true // legacy item: still fenced by StatusRunning and RetryAt
	}

	return exec.CurrentNode == wi.Node && exec.Attempt == wi.Attempt
}

func advanceNotBefore(exec *store.Execution, wi workItem) time.Time {
	due := wi.NotBefore
	if exec.RetryAt.After(due) {
		due = exec.RetryAt
	}

	return due
}

func (e *Engine) processFaninEval(ctx context.Context, flow *dsl.Flow, exec *store.Execution, wi workItem) error {
	if exec.Status != store.StatusWaiting {
		return nil
	}

	if wi.Node != "" && exec.CurrentNode != wi.Node {
		return nil
	}

	node := flow.Node(exec.CurrentNode)
	if node == nil {
		return terminal("unknown node %q in flow %q", exec.CurrentNode, exec.FlowName)
	}

	if node.Type != dsl.NodeFanin {
		return nil
	}

	return e.evalFanin(ctx, flow, exec)
}

func (e *Engine) processBranchRetry(ctx context.Context, flow *dsl.Flow, exec *store.Execution, wi workItem) error {
	if wi.Node == "" {
		return terminal("branch retry work item missing branch node")
	}

	if !branchWorkApplies(flow, exec, wi.Node) {
		return nil
	}

	bs, ok := exec.Branches[wi.Node]
	if !ok || bs.Status != store.BranchPending || bs.Attempt != wi.Attempt {
		return nil
	}

	if !wi.NotBefore.IsZero() && time.Now().Before(wi.NotBefore) {
		return e.deferWorkUntil(ctx, exec, wi, wi.NotBefore)
	}

	node := flow.Node(wi.Node)
	if node == nil || node.Type != dsl.NodeTask {
		return terminal("branch retry node %q is not a task in flow %q", wi.Node, exec.FlowName)
	}

	contextDoc, err := e.assembleContext(ctx, exec)
	if err != nil {
		return err
	}

	res, callErr := e.invoke(ctx, node, exec.ID, contextDoc, wi.Attempt)
	if callErr == nil && res.Status == invoker.StatusPending {
		return nil
	}

	return e.completeBranch(ctx, flow, exec, wi.Node, wi.Attempt, settleResult(res, callErr))
}

func branchWorkApplies(flow *dsl.Flow, exec *store.Execution, branchID string) bool {
	node := flow.Node(exec.CurrentNode)
	if node == nil {
		return false
	}

	switch node.Type {
	case dsl.NodeFanout:
		return containsNode(node.Branches, branchID)
	case dsl.NodeFanin:
		return faninOwnsBranch(flow, node.ID, branchID)
	default:
		return false
	}
}

func containsNode(nodes []string, node string) bool {
	for _, n := range nodes {
		if n == node {
			return true
		}
	}

	return false
}

func (e *Engine) deferWorkUntil(ctx context.Context, exec *store.Execution, wi workItem, due time.Time) error {
	wi.ExecID = exec.ID
	wi.NotBefore = due.UTC()

	if wi.Kind == kindAdvance && wi.Node == "" {
		wi.Node = exec.CurrentNode
		wi.Attempt = exec.Attempt
	}

	if wi.Kind == kindFaninEval && wi.Node == "" {
		wi.Node = exec.CurrentNode
	}

	item, err := json.Marshal(wi)
	if err != nil {
		return err
	}

	updated, err := e.store.Mutate(ctx, exec.ID, func(ex *store.Execution) error {
		if !deferredWorkApplies(ex, wi) {
			return errSkip
		}

		ex.AppendSched(item, due)

		return nil
	})
	if err != nil {
		if errors.Is(err, errSkip) {
			return nil
		}

		return err
	}

	return e.flushOutbox(ctx, updated)
}

func deferredWorkApplies(ex *store.Execution, wi workItem) bool {
	switch wi.Kind {
	case kindAdvance:
		return ex.Status == store.StatusRunning && advanceApplies(ex, wi)
	case kindBranchRetry:
		bs, ok := ex.Branches[wi.Node]

		return ok && bs.Status == store.BranchPending && bs.Attempt == wi.Attempt
	case kindFaninEval:
		return ex.Status == store.StatusWaiting && (wi.Node == "" || ex.CurrentNode == wi.Node)
	default:
		return false
	}
}

// stepNode runs the step for the execution's current node.
func (e *Engine) stepNode(ctx context.Context, flow *dsl.Flow, node *dsl.Node, exec *store.Execution) error {
	switch node.Type {
	case dsl.NodeTask:
		return e.stepTask(ctx, flow, node, exec)
	case dsl.NodeFanout:
		return e.stepFanout(ctx, flow, node, exec)
	case dsl.NodeFanin:
		return e.evalFanin(ctx, flow, exec)
	case dsl.NodeChoice:
		return e.stepChoice(ctx, flow, node, exec)
	case dsl.NodeSignal:
		return e.stepSignal(ctx, flow, node, exec)
	default:
		return terminal("unsupported node type %q", node.Type)
	}
}

// advanceTo moves the execution from fromNode to nextNode (or completes it if
// nextNode == "") via a CAS write that also commits the next step's work item
// (transactional outbox), then flushes the outbox. mutate may apply additional
// changes (e.g. merge payload) within the same CAS write.
//
// The write is guarded: it applies only while the execution is still active and
// still at fromNode. A stale caller — a duplicate delivery, or an instance that
// lost its lease mid-invocation and settles late — must not rewind CurrentNode
// or resurrect a terminal (notably cancelled) execution; its advance is a no-op.
func (e *Engine) advanceTo(
	ctx context.Context, execID, fromNode, nextNode string, mutate func(*store.Execution),
) error {
	var item json.RawMessage

	if nextNode != "" {
		data, err := advanceWorkItem(execID, nextNode, 0, time.Time{})
		if err != nil {
			return err
		}

		item = data
	}

	updated, err := e.store.Mutate(ctx, execID, func(ex *store.Execution) error {
		if !ex.Active() || ex.CurrentNode != fromNode {
			return errSkip // cancelled, completed, or already moved on: stale advance
		}

		if mutate != nil {
			mutate(ex)
		}

		ex.Attempt = 0
		// Drop any unconsumed early-completion stash: it belongs to the node
		// being left, and — because attempts reset to 0 and task cycles are
		// legal — a stale stash could otherwise satisfy a later revisit of the
		// same node with the old result instead of invoking it. RetryAt is
		// likewise per-node state and must not survive the move.
		ex.Activity = nil
		ex.RetryAt = time.Time{}

		if nextNode == "" {
			ex.Status = store.StatusCompleted
			ex.CurrentNode = ""
		} else {
			ex.Status = store.StatusRunning
			ex.CurrentNode = nextNode
			ex.AppendWork(item)
		}

		return nil
	})
	if err != nil {
		if errors.Is(err, errSkip) {
			return nil // guard failed: nothing advanced, nothing to enqueue
		}
		// An over-limit payload or control document can never be persisted:
		// retrying produces the same oversized write. Fail the execution with the
		// actionable reason instead. failNode keeps the prior (within-limit)
		// document, so its write succeeds.
		if errors.Is(err, store.ErrPayloadTooLarge) || errors.Is(err, store.ErrDocumentTooLarge) {
			return e.failNode(ctx, execID, fromNode, err.Error())
		}

		return err
	}

	e.emitEvent(ctx, updated)

	return e.flushOutbox(ctx, updated)
}

// CompleteActivity settles an asynchronous activity that an Invoker previously
// reported as StatusPending. node and attempt identify the dispatched work; res
// is its outcome (StatusOK to advance, StatusError to fail, StatusRetry/transient
// to retry per the node policy). It is idempotent and stale-safe: a duplicate or
// out-of-date completion (the execution already moved on, or a different attempt)
// is a no-op. It settles either the execution's current task node or a pending
// fanout branch.
func (e *Engine) CompleteActivity(
	ctx context.Context, execID, node string, attempt int, res invoker.Result,
) (err error) {
	// Top-level panic guard, parity with processGuarded and the invoke guards. The
	// settle path (settleTask/completeBranch, JSON, store mutates) runs on the
	// async worker's goroutine or a direct external caller's goroutine — neither
	// has recovery above it — so an unrecovered panic would crash the hosting
	// process (and, on the claim path, could strand the execution mid-flip). Recover
	// into a returned error instead: the worker Naks and retries (bounded by its
	// delivery cap), and a synchronous caller gets an error rather than a panic.
	defer func() {
		if r := recover(); r != nil {
			e.log.Error("complete activity panic",
				"exec", execID, "node", node, "attempt", attempt,
				"panic", r, "stack", string(debug.Stack()))

			err = fmt.Errorf("complete activity panic: %v", r)
		}
	}()

	return e.completeActivity(ctx, execID, node, attempt, res)
}

// completeActivity is CompleteActivity's body, run under its panic guard.
func (e *Engine) completeActivity(ctx context.Context, execID, node string, attempt int, res invoker.Result) error {
	if !validExecID(execID) {
		return fmt.Errorf("invalid execution id %q: must match [A-Za-z0-9_-]{1,128}", execID)
	}

	exec, err := e.store.Get(ctx, execID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil
		}

		return err
	}

	if !exec.Active() {
		return nil // terminal: drop
	}

	// Re-flush anything a previous transition committed but failed to publish
	// (e.g. a redelivered completion whose original settle lost its fanin
	// eval): the outbox on the document is the source of truth for follow-on
	// work, so a duplicate delivery heals it here instead of being dropped.
	if err = e.flushOutbox(ctx, exec); err != nil {
		return err
	}

	flow, ok := e.flows[exec.FlowName]
	if !ok {
		// This engine does not know the flow (e.g. it was renamed/removed across a
		// rolling deploy). Re-dispatching the async job can never succeed, so this
		// is terminal: the async worker dead-letters it instead of Nak-looping.
		return terminal("unknown flow %q", exec.FlowName)
	}

	// Branch path: node is a pending fanout branch. A completion for an
	// already-settled branch is a duplicate: its follow-on eval was committed
	// with the settle (transactional outbox) and re-flushed above, so it drops
	// through to the stale no-op below.
	if bs, found := exec.Branches[node]; found && bs.Status == store.BranchPending {
		return e.completeBranch(ctx, flow, exec, node, attempt, res)
	}

	// Task-await path. If the task is parked (waiting) at this node/attempt, claim
	// it (flip to running so a duplicate no-ops) and settle. If it is still
	// dispatching (running at this node/attempt — the completion beat the park),
	// stash the result; the parking stepTask consumes it. Anything else is stale.
	settle := false

	claimed, err := e.store.Mutate(ctx, execID, func(ex *store.Execution) error {
		if ex.CurrentNode != node || ex.Attempt != attempt {
			return errSkip
		}

		switch ex.Status {
		case store.StatusWaiting:
			ex.Status = store.StatusRunning
			settle = true

			return nil
		case store.StatusRunning:
			ex.Activity = &store.ActivityResult{
				Node: node, Attempt: attempt,
				Status: string(res.Status), Payload: res.Payload, Error: res.Error,
			}

			return nil
		default:
			return errSkip
		}
	})
	if err != nil {
		if errors.Is(err, errSkip) {
			return nil
		}

		return err
	}

	if !settle {
		return nil // stashed for stepTask to consume
	}

	return e.settleClaimedTask(ctx, flow, claimed, node, attempt, res)
}

// settleClaimedTask settles a task-await node CompleteActivity just claimed
// (waiting→running). On a transient settle failure it reverts the claim so a
// redelivered completion re-settles cleanly instead of stranding in running.
func (e *Engine) settleClaimedTask(
	ctx context.Context, flow *dsl.Flow, claimed *store.Execution, node string, attempt int, res invoker.Result,
) error {
	n := flow.Node(node)
	if n == nil || n.Type != dsl.NodeTask {
		return nil
	}

	r, callErr := activityResult(store.ActivityResult{Status: string(res.Status), Payload: res.Payload, Error: res.Error})

	settleErr := e.settleTask(ctx, flow, n, claimed, r, callErr)
	if settleErr != nil {
		// The claim above flipped waiting→running. A transient settle failure would
		// otherwise leave the execution stuck running with no follow-on work: a
		// redelivered completion would only re-stash it, so recovery would hinge on
		// the stall watchdog (and be impossible if it is disabled). Revert the claim
		// to waiting so the worker's redelivered completion re-claims and settles it
		// directly. Guarded, so a settle that actually advanced (or a Cancel) is
		// never rewound.
		e.revertClaim(ctx, claimed.ID, node, attempt)
	}

	return settleErr
}

// revertClaim undoes CompleteActivity's waiting→running claim after a transient
// settle failure, flipping the execution back to waiting so a redelivered
// completion can re-claim and settle it (instead of stranding in running until
// the watchdog). It is guarded — it reverts only an execution still running at
// the claimed node/attempt, so a settleTask that advanced the execution, or a
// Cancel that landed meanwhile, is left untouched — and best-effort: if the
// revert itself fails, the stall watchdog remains the backstop.
func (e *Engine) revertClaim(ctx context.Context, execID, node string, attempt int) {
	_, _ = e.store.Mutate(ctx, execID, func(ex *store.Execution) error {
		if ex.Status != store.StatusRunning || ex.CurrentNode != node || ex.Attempt != attempt {
			return errSkip
		}

		ex.Status = store.StatusWaiting

		return nil
	})
}

// activityResult converts a stored ActivityResult into an invoker.Result and a
// transport error: a retry status with a message becomes a non-nil error so
// settleTask treats it as a transient failure.
func activityResult(a store.ActivityResult) (invoker.Result, error) {
	r := invoker.Result{Status: invoker.Status(a.Status), Payload: a.Payload, Error: a.Error}
	if r.Status == invoker.StatusRetry && a.Error != "" {
		return r, errors.New(a.Error)
	}

	return r, nil
}

// completeBranch settles a pending fanout branch and, when it reaches a terminal
// state, triggers a fanin re-evaluation. A retry with attempts remaining
// re-dispatches the branch (a new attempt) instead.
const (
	branchSettleSkip       = ""
	branchSettleSettled    = "settled"
	branchSettleRedispatch = "redispatch"
)

func (e *Engine) completeBranch(
	ctx context.Context, flow *dsl.Flow, exec *store.Execution,
	branchID string, attempt int, res invoker.Result,
) error {
	node := flow.Node(branchID)
	if node == nil || node.Type != dsl.NodeTask {
		return nil
	}

	faninID := faninForBranch(flow, exec, branchID)
	if faninID == "" {
		return nil
	}

	evalItem, marshalErr := faninEvalWorkItem(exec.ID, faninID)
	if marshalErr != nil {
		return marshalErr
	}

	nextAttempt := attempt + 1
	retryAt := time.Now().Add(backoff(node, nextAttempt, e.cfg.RetryBaseDelay, e.cfg.RetryMaxDelay)).UTC()

	retryItem, marshalErr := branchRetryWorkItem(exec.ID, branchID, nextAttempt, retryAt)
	if marshalErr != nil {
		return marshalErr
	}

	// Data before control: a completed branch's output is readable before the
	// settle commits, so the flow can never join on a result that is missing.
	// A stale completion that loses the guarded settle below leaves a harmless
	// overwritable entry.
	if res.Status == invoker.StatusOK && len(res.Payload) > 0 {
		if err := e.store.PutPayload(ctx, store.OutputKey(exec.ID, branchID), res.Payload); err != nil {
			return err
		}
	}

	claimed, err := e.store.Mutate(ctx, exec.ID, func(ex *store.Execution) error {
		var mutErr error

		_, mutErr = settleBranchAttempt(ex, node, branchID, attempt, res, evalItem, retryItem, retryAt)

		return mutErr
	})
	if err != nil {
		if errors.Is(err, errSkip) {
			return nil
		}

		return err
	}

	return e.flushOutbox(ctx, claimed)
}

func faninForBranch(flow *dsl.Flow, exec *store.Execution, branchID string) string {
	node := flow.Node(exec.CurrentNode)
	if node == nil {
		return ""
	}

	switch node.Type {
	case dsl.NodeFanout:
		if !containsNode(node.Branches, branchID) {
			return ""
		}

		return flow.Successor(node.ID)
	case dsl.NodeFanin:
		if faninOwnsBranch(flow, node.ID, branchID) {
			return node.ID
		}
	}

	return ""
}

func faninOwnsBranch(flow *dsl.Flow, faninID, branchID string) bool {
	for i := range flow.Nodes {
		node := &flow.Nodes[i]
		if node.Type == dsl.NodeFanout && flow.Successor(node.ID) == faninID {
			return containsNode(node.Branches, branchID)
		}
	}

	return false
}

// settleBranchAttempt is completeBranch's Mutate callback: on the fresh
// document, it decides whether the dispatched attempt still applies and, if
// so, records the branch's next state (settled or bumped for redispatch),
// committing the fanin re-evaluation when it settles or the scheduled
// redispatch when it retries in the same write.
func settleBranchAttempt(
	ex *store.Execution, node *dsl.Node, branchID string, attempt int, res invoker.Result,
	evalItem, retryItem json.RawMessage, retryAt time.Time,
) (action string, err error) {
	// Decide on the fresh document: a Cancel (or completion) that landed
	// after the caller's snapshot must not have a branch settled onto it.
	if !ex.Active() {
		return branchSettleSkip, errSkip
	}

	bs, ok := ex.Branches[branchID]
	if !ok || bs.Status != store.BranchPending || bs.Attempt != attempt {
		return branchSettleSkip, errSkip
	}

	next, action := nextBranchState(node, branchID, bs, attempt, res)
	ex.Branches[branchID] = next

	if action == branchSettleRedispatch {
		ex.AppendSched(retryItem, retryAt)

		return action, nil
	}

	if action != branchSettleSettled {
		// Unrecognized res.Status: leave the branch as computed without appending
		// the fanin eval.
		return action, nil
	}

	if next.Status == store.BranchCompleted && len(res.Payload) > 0 {
		ex.AddOutput(branchID)
	}

	// The join re-evaluation is committed with the settle (transactional
	// outbox): a crash between this write and the flush can never lose it.
	ex.AppendWork(evalItem)

	return branchSettleSettled, nil
}

// nextBranchState computes a pending branch's next state given its dispatch
// result: completed or failed (branchSettleSettled), bumped to the next
// pending attempt (branchSettleRedispatch) when a transient failure still has
// attempts remaining, or left untouched (branchSettleSkip) for a res.Status
// this engine version does not recognize — never guess at an unknown status.
func nextBranchState(
	node *dsl.Node, branchID string, bs store.BranchState, attempt int, res invoker.Result,
) (next store.BranchState, action string) {
	switch res.Status {
	case invoker.StatusOK:
		return store.BranchState{NodeID: branchID, Status: store.BranchCompleted, Attempt: attempt}, branchSettleSettled
	case invoker.StatusError:
		return store.BranchState{
			NodeID: branchID, Status: store.BranchFailed, Error: res.Error, Attempt: attempt,
		}, branchSettleSettled
	case invoker.StatusRetry, invoker.StatusPending: // transient/retry
		if attempt+1 >= maxAttempts(node) {
			return store.BranchState{
				NodeID: branchID, Status: store.BranchFailed, Error: retryReason(res, nil), Attempt: attempt,
			}, branchSettleSettled
		}

		bs.Attempt = attempt + 1

		return bs, branchSettleRedispatch // stays pending at the new attempt
	}

	return bs, branchSettleSkip
}

// settleResult normalises a raw invoke outcome (result + transport error) into a
// Result whose Status drives branch settling.
func settleResult(res invoker.Result, callErr error) invoker.Result {
	if callErr != nil {
		return invoker.Result{Status: invoker.StatusRetry, Error: callErr.Error()}
	}

	return res
}

// Resume revives a failed execution, re-running its current node with a fresh
// retry budget. The durable payload is preserved, so the flow continues from the
// node that failed (useful when the failure was transient). Only failed
// executions can be resumed; anything else returns an error. It enqueues durable
// work, so the engine need not be the same instance — any running engine picks
// it up (and if none is running yet, it runs when one starts).
func (e *Engine) Resume(ctx context.Context, execID string) error {
	if !validExecID(execID) {
		return fmt.Errorf("invalid execution id %q: must match [A-Za-z0-9_-]{1,128}", execID)
	}

	ex, err := e.store.Get(ctx, execID)
	if err != nil {
		return err // includes store.ErrNotFound
	}

	if ex.Status != store.StatusFailed {
		return fmt.Errorf("execution %s is %s; only failed executions can be resumed", execID, ex.Status)
	}

	if ex.CurrentNode == "" {
		return fmt.Errorf("execution %s has no current node to resume", execID)
	}

	item, err := advanceWorkItem(execID, ex.CurrentNode, 0, time.Time{})
	if err != nil {
		return err
	}

	updated, err := e.store.Mutate(ctx, execID, func(ex *store.Execution) error {
		if ex.Status != store.StatusFailed {
			return errSkip // raced with another transition
		}

		ex.Status = store.StatusRunning
		ex.Attempt = 0
		ex.Error = ""
		ex.Activity = nil
		ex.RetryAt = time.Time{}
		ex.AppendWork(item) // committed with the revival (transactional outbox)

		return nil
	})
	if err != nil {
		if errors.Is(err, errSkip) {
			return nil
		}

		return err
	}

	e.emitEvent(ctx, updated)

	return e.flushOutbox(ctx, updated)
}

// Cancel transitions a running or waiting execution to the terminal cancelled
// state with the given reason. It is idempotent and stale-safe: cancelling an
// already-terminal execution (completed/failed/cancelled) — or one that no longer
// exists — is a no-op. In-flight work is abandoned rather than interrupted: any
// later work item or async CompleteActivity for this execution finds it
// non-active and no-ops (process and CompleteActivity both drop non-active
// executions), so pending retries, fanin evaluations and signal timeouts settle
// harmlessly. Unlike Resume, a cancelled execution is terminal and cannot be
// revived.
func (e *Engine) Cancel(ctx context.Context, execID, reason string) error {
	if !validExecID(execID) {
		return fmt.Errorf("invalid execution id %q: must match [A-Za-z0-9_-]{1,128}", execID)
	}

	updated, err := e.store.Mutate(ctx, execID, func(ex *store.Execution) error {
		if !ex.Active() {
			return errSkip // already terminal: no-op
		}

		ex.Status = store.StatusCancelled
		ex.Error = reason

		return nil
	})
	if err != nil {
		if errors.Is(err, errSkip) || errors.Is(err, store.ErrNotFound) {
			return nil
		}

		return err
	}

	e.emitEvent(ctx, updated)

	return nil
}

// fail marks an execution failed via CAS and emits an event. It applies only
// while the execution is still active: a terminal execution — notably cancelled —
// is never overwritten to failed (which would make it resumable again). Callers
// that know which node produced the failure should prefer failNode.
func (e *Engine) fail(ctx context.Context, execID, reason string) error {
	return e.failNode(ctx, execID, "", reason)
}

// maxFailReasonBytes caps the failure reason stored on the execution document.
// The reason is control metadata folded into the (size-guarded) document; an
// unbounded one — e.g. a giant error string from an Invoker, or a message that
// itself embeds an over-limit payload — could push the document past the size
// guard and dead-end the very write that records the failure. Truncating keeps
// the failure path always able to persist; the full detail remains in logs.
const maxFailReasonBytes = 4096

// truncateReason bounds a failure reason to maxFailReasonBytes so recording a
// failure can never itself exceed the document-size guard.
func truncateReason(reason string) string {
	if len(reason) <= maxFailReasonBytes {
		return reason
	}

	return reason[:maxFailReasonBytes] + "… (truncated)"
}

// failNode is fail additionally guarded on the failing node: when nodeID is
// non-empty the write applies only while the execution is still at nodeID, so a
// stale failure — from a duplicate delivery or an instance that lost its lease
// mid-invocation — cannot fail an execution that has already moved on.
func (e *Engine) failNode(ctx context.Context, execID, nodeID, reason string) error {
	reason = truncateReason(reason)

	updated, err := e.store.Mutate(ctx, execID, func(ex *store.Execution) error {
		if !ex.Active() || (nodeID != "" && ex.CurrentNode != nodeID) {
			return errSkip // terminal or moved on: stale failure, leave unchanged
		}

		ex.Status = store.StatusFailed
		ex.Error = reason

		return nil
	})
	if err != nil {
		if errors.Is(err, errSkip) {
			return nil
		}

		return err
	}

	e.emitEvent(ctx, updated)

	return nil
}
