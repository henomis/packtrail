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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
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
	ExecID string `json:"exec_id"`
	Kind   string `json:"kind"`
	Node   string `json:"node,omitempty"`   // node that scheduled this item (timeout staleness guard)
	Signal string `json:"signal,omitempty"` // signal name (wait timeout)
}

// Work kinds.
const (
	kindAdvance     = "advance"      // run the step for exec.CurrentNode
	kindFaninEval   = "fanin_eval"   // re-evaluate a fanin join
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
	nakDelayShort        = 2 * time.Second
	nakDelayLong         = 5 * time.Second
	heartbeatDivisor     = 3
	maxAckPendingMult    = 2
)

// Config tunes engine behaviour. Zero values fall back to sensible defaults.
type Config struct {
	OwnerID        string        // unique per instance; defaults to a random id
	LeaseTTL       time.Duration // ownership lease TTL (default 30s)
	AckWait        time.Duration // work consumer ack wait (default 60s)
	RetryBaseDelay time.Duration // base backoff for task retries (default 1s)
	RetryMaxDelay  time.Duration // cap on backoff (default 60s)
	MaxConcurrency int           // max work items processed at once (default 64)
	DefaultTimeout time.Duration // task timeout when a node omits one (default 30s)
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
}

// New builds an engine and precompiles every choice expression in flows. flows
// maps flow name -> definition. inv executes task/branch nodes; it is typically
// an *invoker.Registry (optionally wrapped in an *invoker.Cache for idempotency).
func New(
	inv invoker.Invoker, st *store.Store, sched *scheduler.Scheduler,
	flows map[string]*dsl.Flow, cfg Config,
) (*Engine, error) {
	if cfg.OwnerID == "" {
		cfg.OwnerID = "engine-" + nuid.Next()
	}

	if cfg.LeaseTTL == 0 {
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

	if cfg.MaxConcurrency == 0 {
		cfg.MaxConcurrency = defaultConcurrency
	}

	if cfg.DefaultTimeout == 0 {
		cfg.DefaultTimeout = defaultTimeout
	}

	programs, err := compilePrograms(flows)
	if err != nil {
		return nil, err
	}

	// Ensure the signals stream during construction. This is the only piece of
	// JetStream setup the engine owns; doing it here (rather than in Run) keeps
	// all stream creation in the single-threaded setup phase. Two engines whose
	// Run methods race would otherwise issue concurrent CreateOrUpdateStream
	// calls, which nats-server does not serialise across an account.
	signals := signal.New(st.JS(), st.Names())
	if err = signals.EnsureStream(context.Background()); err != nil {
		return nil, err
	}

	return &Engine{
		invoker:  inv,
		js:       st.JS(),
		store:    st,
		sched:    sched,
		signals:  signals,
		names:    st.Names(),
		flows:    flows,
		programs: programs,
		cfg:      cfg,
		log:      slog.Default().With("owner", cfg.OwnerID),
		sem:      make(chan struct{}, cfg.MaxConcurrency),
	}, nil
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

// Start creates a new execution of flowName with the given initial payload and
// enqueues the first step. It returns the new execution id.
func (e *Engine) Start(ctx context.Context, flowName string, payload json.RawMessage) (string, error) {
	flow, ok := e.flows[flowName]
	if !ok {
		return "", fmt.Errorf("unknown flow %q", flowName)
	}

	if len(payload) == 0 {
		payload = json.RawMessage("{}")
	}

	id := "exec-" + nuid.Next()

	exec := &store.Execution{
		ID:          id,
		FlowName:    flowName,
		CurrentNode: flow.StartNode(),
		Status:      store.StatusRunning,
		Payload:     payload,
	}
	if _, err := e.store.Create(ctx, exec); err != nil {
		return "", err
	}

	e.emitEvent(ctx, exec)

	if err := e.enqueue(ctx, id, workItem{ExecID: id, Kind: kindAdvance}); err != nil {
		return "", err
	}

	return id, nil
}

// ScheduleFlow installs a recurring schedule that starts a new execution of
// flowName on the given 6-field cron expression ("sec min hour dom mon dow").
// name uniquely identifies the schedule; reusing it replaces the schedule.
func (e *Engine) ScheduleFlow(ctx context.Context, name, flowName, cronExpr string, payload json.RawMessage) error {
	if _, ok := e.flows[flowName]; !ok {
		return fmt.Errorf("unknown flow %q", flowName)
	}

	if len(payload) == 0 {
		payload = json.RawMessage("{}")
	}

	return e.sched.Cron(ctx, name, "start."+flowName, cronExpr, payload)
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

// enqueue publishes a work item to the execution's work subject.
func (e *Engine) enqueue(ctx context.Context, execID string, wi workItem) error {
	wi.ExecID = execID

	data, err := json.Marshal(wi)
	if err != nil {
		return err
	}

	_, err = e.js.Publish(ctx, e.names.SubjWorkPrefix+execID, data)

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

	cc, err := cons.Consume(func(msg jetstream.Msg) {
		e.sem <- struct{}{}

		go func() {
			defer func() { <-e.sem }()

			e.handle(ctx, msg)
		}()
	})
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}
	defer cc.Stop()

	// Forward fired schedules (retry backoff, wait timeouts) back onto the work
	// stream. The fired payload is the original work item; its subject key is
	// the execution id.
	fc, err := e.sched.ConsumeFired(ctx, e.names.DurFired, func(key string, payload []byte) error {
		// A reconcile key triggers the matching visibility reconciliation.
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
		// A "start.<flow>" key triggers a new execution (recurring cron starts);
		// any other key is an execution id whose work item we re-inject.
		if flowName, ok := strings.CutPrefix(key, "start."); ok {
			_, startErr := e.Start(ctx, flowName, payload)
			return startErr
		}

		_, pubErr := e.js.Publish(ctx, e.names.SubjWorkPrefix+key, payload)

		return pubErr
	})
	if err != nil {
		return fmt.Errorf("fired consumer: %w", err)
	}
	defer fc.Stop()

	// Apply external signals idempotently (CAS before ack). The signals stream is
	// ensured once at construction (see New), not here, so concurrent engines do
	// not issue racing stream updates.
	sc, err := e.signals.Consume(ctx, e.names.DurSignals, e.applySignal)
	if err != nil {
		return fmt.Errorf("signal consumer: %w", err)
	}
	defer sc.Stop()

	<-ctx.Done()

	return nil
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
	// let it (or a later redelivery) make progress.
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

	// Heartbeat: renew the lease and extend the ack window while we work.
	hb, cancelHB := context.WithCancel(ctx)
	go e.heartbeat(hb, msg, wi.ExecID)

	defer func() {
		cancelHB()

		_ = e.store.ReleaseLease(ctx, wi.ExecID, e.cfg.OwnerID)
	}()

	err = e.process(ctx, wi)
	if err != nil {
		e.log.Error("process", "exec", wi.ExecID, "kind", wi.Kind, "err", err)

		_ = msg.NakWithDelay(nakDelayLong)

		return
	}

	_ = msg.Ack()
}

// emitEvent publishes a domain event, logging at debug level on failure. Event
// emission is best-effort: the visibility indexes are eventually reconciled, so
// a failure here never blocks execution progress.
func (e *Engine) emitEvent(ctx context.Context, ex *store.Execution) {
	if err := e.store.EmitEvent(ctx, ex); err != nil {
		e.log.Debug("emit event", "exec", ex.ID, "err", err)
	}
}

func (e *Engine) heartbeat(ctx context.Context, msg jetstream.Msg, execID string) {
	t := time.NewTicker(e.cfg.LeaseTTL / heartbeatDivisor)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			_ = msg.InProgress()
			_, _ = e.store.AcquireLease(ctx, execID, e.cfg.OwnerID, e.cfg.LeaseTTL)
		}
	}
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

	flow, ok := e.flows[exec.FlowName]
	if !ok {
		return fmt.Errorf("unknown flow %q", exec.FlowName)
	}

	node := flow.Node(exec.CurrentNode)
	if node == nil {
		return fmt.Errorf("unknown node %q in flow %q", exec.CurrentNode, exec.FlowName)
	}

	switch wi.Kind {
	case kindAdvance:
		return e.stepNode(ctx, flow, node, exec)
	case kindFaninEval:
		return e.evalFanin(ctx, flow, exec)
	case kindWaitTimeout:
		return e.onWaitTimeout(ctx, flow, exec, wi)
	default:
		return fmt.Errorf("unknown work kind %q", wi.Kind)
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
		return fmt.Errorf("unsupported node type %q", node.Type)
	}
}

// advanceTo moves the execution to nextNode (or completes it if nextNode == "")
// via a CAS write, then enqueues the next step. mutate may apply additional
// changes (e.g. merge payload) within the same CAS write.
func (e *Engine) advanceTo(ctx context.Context, execID, nextNode string, mutate func(*store.Execution)) error {
	updated, err := e.store.Mutate(ctx, execID, func(ex *store.Execution) error {
		if mutate != nil {
			mutate(ex)
		}

		ex.Attempt = 0
		if nextNode == "" {
			ex.Status = store.StatusCompleted
			ex.CurrentNode = ""
		} else {
			ex.Status = store.StatusRunning
			ex.CurrentNode = nextNode
		}

		return nil
	})
	if err != nil {
		return err
	}

	e.emitEvent(ctx, updated)

	if nextNode == "" {
		return nil
	}

	return e.enqueue(ctx, execID, workItem{Kind: kindAdvance})
}

// CompleteActivity settles an asynchronous activity that an Invoker previously
// reported as StatusPending. node and attempt identify the dispatched work; res
// is its outcome (StatusOK to advance, StatusError to fail, StatusRetry/transient
// to retry per the node policy). It is idempotent and stale-safe: a duplicate or
// out-of-date completion (the execution already moved on, or a different attempt)
// is a no-op. It settles either the execution's current task node or a pending
// fanout branch.
func (e *Engine) CompleteActivity(ctx context.Context, execID, node string, attempt int, res invoker.Result) error {
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

	flow, ok := e.flows[exec.FlowName]
	if !ok {
		return fmt.Errorf("unknown flow %q", exec.FlowName)
	}

	// Branch path: node is a pending fanout branch.
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

	n := flow.Node(node)
	if n == nil || n.Type != dsl.NodeTask {
		return nil
	}

	r, callErr := activityResult(store.ActivityResult{Status: string(res.Status), Payload: res.Payload, Error: res.Error})

	return e.settleTask(ctx, flow, n, claimed, r, callErr)
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
func (e *Engine) completeBranch(
	ctx context.Context, flow *dsl.Flow, exec *store.Execution,
	branchID string, attempt int, res invoker.Result,
) error {
	node := flow.Node(branchID)
	if node == nil || node.Type != dsl.NodeTask {
		return nil
	}

	const (
		actionSkip       = ""
		actionSettled    = "settled"
		actionRedispatch = "redispatch"
	)

	action := actionSkip

	claimed, err := e.store.Mutate(ctx, exec.ID, func(ex *store.Execution) error {
		bs, ok := ex.Branches[branchID]
		if !ok || bs.Status != store.BranchPending || bs.Attempt != attempt {
			return errSkip
		}

		switch res.Status {
		case invoker.StatusOK:
			ex.Branches[branchID] = store.BranchState{
				NodeID: branchID, Status: store.BranchCompleted,
				Result: res.Payload, Attempt: attempt,
			}
			action = actionSettled
		case invoker.StatusError:
			ex.Branches[branchID] = store.BranchState{
				NodeID: branchID, Status: store.BranchFailed,
				Error: res.Error, Attempt: attempt,
			}
			action = actionSettled
		case invoker.StatusRetry, invoker.StatusPending: // transient/retry
			if attempt+1 >= maxAttempts(node) {
				ex.Branches[branchID] = store.BranchState{
					NodeID: branchID, Status: store.BranchFailed,
					Error: retryReason(res, nil), Attempt: attempt,
				}
				action = actionSettled
			} else {
				bs.Attempt = attempt + 1
				ex.Branches[branchID] = bs // stays pending at the new attempt
				action = actionRedispatch
			}
		}

		return nil
	})
	if err != nil {
		if errors.Is(err, errSkip) {
			return nil
		}

		return err
	}

	if action == actionRedispatch {
		res2, callErr := e.invoke(ctx, node, exec.ID, claimed.Payload, attempt+1)
		if callErr == nil && res2.Status == invoker.StatusPending {
			return nil // re-dispatched; await the next CompleteActivity
		}
		// A synchronous branch invoker settled immediately.
		return e.completeBranch(ctx, flow, claimed, branchID, attempt+1, settleResult(res2, callErr))
	}
	// Terminal branch state: re-evaluate the fanin join.
	return e.enqueue(ctx, exec.ID, workItem{Kind: kindFaninEval})
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

	updated, err := e.store.Mutate(ctx, execID, func(ex *store.Execution) error {
		if ex.Status != store.StatusFailed {
			return errSkip // raced with another transition
		}

		ex.Status = store.StatusRunning
		ex.Attempt = 0
		ex.Error = ""
		ex.Activity = nil

		return nil
	})
	if err != nil {
		if errors.Is(err, errSkip) {
			return nil
		}

		return err
	}

	e.emitEvent(ctx, updated)

	return e.enqueue(ctx, execID, workItem{Kind: kindAdvance})
}

// fail marks an execution failed via CAS and emits an event.
func (e *Engine) fail(ctx context.Context, execID, reason string) error {
	updated, err := e.store.Mutate(ctx, execID, func(ex *store.Execution) error {
		ex.Status = store.StatusFailed
		ex.Error = reason

		return nil
	})
	if err != nil {
		return err
	}

	e.emitEvent(ctx, updated)

	return nil
}
