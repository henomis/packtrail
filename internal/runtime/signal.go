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
	"errors"
	"fmt"
	"time"

	"github.com/henomis/packtrail/internal/dsl"
	"github.com/henomis/packtrail/internal/signal"
	"github.com/henomis/packtrail/internal/store"
)

// errSkip is returned from a store.Mutate callback to abort the write without an
// error: the read-modify-write cycle ends as a no-op.
var errSkip = errors.New("skip write")

// Signal publishes an external signal to an execution.
func (e *Engine) Signal(ctx context.Context, execID, name string, payload json.RawMessage) error {
	return e.signals.Publish(ctx, execID, name, payload)
}

// stepSignal makes the execution wait for an external signal. If the signal has
// already arrived (early delivery), it is consumed immediately; otherwise the
// execution enters the waiting state and a timeout is scheduled.
//
// The already-arrived check runs on the fresh document INSIDE the park
// mutation, not on this work item's snapshot: a signal applied between the
// snapshot read and the park write is acked by its consumer (which saw the
// execution still running and left consumption to us), so parking on the stale
// snapshot would strand the execution waiting for a signal it already holds.
func (e *Engine) stepSignal(ctx context.Context, flow *dsl.Flow, node *dsl.Node, exec *store.Execution) error {
	name := node.SignalName
	next := flow.Successor(node.ID)

	// Both possible follow-ons are marshaled up front so the park/consume CAS
	// can commit whichever it needs atomically (transactional outbox).
	var advanceItem, timeoutItem json.RawMessage

	if next != "" {
		data, err := json.Marshal(workItem{ExecID: exec.ID, Kind: kindAdvance})
		if err != nil {
			return err
		}

		advanceItem = data
	}

	if node.Timeout.D() > 0 {
		data, err := json.Marshal(workItem{ExecID: exec.ID, Kind: kindWaitTimeout, Node: node.ID, Signal: name})
		if err != nil {
			return err
		}

		timeoutItem = data
	}

	updated, err := e.store.Mutate(ctx, exec.ID, func(ex *store.Execution) error {
		// Park only while still active at this node: a Cancel (or a competing
		// transition) racing this step must not be overwritten back to waiting.
		if !ex.Active() || ex.CurrentNode != node.ID {
			return errSkip
		}

		// Early delivery: the signal already arrived (marker on the document,
		// payload in the data plane) — consume it in this same CAS write
		// instead of parking.
		if ex.Signals[name] {
			consumeSignal(ex, name, next)

			if next != "" {
				ex.AppendWork(advanceItem)
			}

			return nil
		}

		// A redelivered advance for an already-parked wait is a no-op: the park
		// below committed its timeout in the same write, and process() re-flushes
		// any outbox a faulted flush left behind before stepping.
		if ex.Status == store.StatusWaiting && ex.WaitSignal == name {
			return errSkip
		}

		ex.Status = store.StatusWaiting
		ex.WaitSignal = name

		if timeoutItem != nil {
			ex.AppendSched(timeoutItem, time.Now().Add(node.Timeout.D()))
		}

		return nil
	})
	if err != nil {
		if errors.Is(err, errSkip) {
			return nil // cancelled, moved on, or already parked
		}

		return err
	}

	e.emitEvent(ctx, updated)

	return e.flushOutbox(ctx, updated)
}

// applySignal is the signal-consumer callback. It stores the signal payload in
// the data plane, records the delivery idempotently (by stream sequence) and,
// if the execution is waiting on it, advances. State is always persisted via
// CAS before the message is acked.
func (e *Engine) applySignal(ctx context.Context, d signal.Delivery) error {
	// Data before control: the payload entry (versioned by the delivery's
	// stream sequence) is readable before the marker commits, so a consumer
	// can never observe a committed sequence whose payload is missing. An
	// oversized payload errors here — the delivery Naks and eventually
	// dead-letters, observable via RecentDeadLetters.
	if err := e.store.PutPayload(ctx, store.SignalKey(d.ExecID, d.Name, d.Seq), d.Payload); err != nil {
		return err
	}

	updated, err := e.store.Mutate(ctx, d.ExecID, func(ex *store.Execution) error {
		if !ex.Active() {
			return errSkip // terminal: never mutate; ack and drop the signal
		}

		if ex.LastSeq != nil && ex.LastSeq[d.Name] >= d.Seq {
			return errSkip // duplicate: already applied at >= this sequence
		}

		if ex.LastSeq == nil {
			ex.LastSeq = map[string]uint64{}
		}

		if ex.Signals == nil {
			ex.Signals = map[string]bool{}
		}

		ex.LastSeq[d.Name] = d.Seq
		ex.Signals[d.Name] = true // received, not yet consumed

		return nil
	})
	if err != nil {
		if errors.Is(err, errSkip) {
			// Duplicate or terminal. A duplicate redelivery only exists because
			// the original handling faulted after its apply — and one such fault
			// is a committed transition whose outbox flush failed. Re-flush
			// whatever it left behind (a no-op when the outbox is empty).
			if ex, getErr := e.store.Get(ctx, d.ExecID); getErr == nil {
				return e.flushOutbox(ctx, ex)
			}

			return nil // gone (archived/pruned): nothing to heal
		}

		if errors.Is(err, store.ErrNotFound) {
			// The execution does not exist (yet). Most often this is a signal
			// published just before its StartWithID landed: Nak for redelivery so
			// the slightly-early signal finds its execution on a later attempt. A
			// genuinely orphaned signal (typo'd id, or an execution already expired
			// out of the archive) exhausts the delivery cap and lands in the
			// dead-letter stream — observable via RecentDeadLetters — instead of
			// vanishing silently.
			return fmt.Errorf("signal %q for unknown execution %q: %w", d.Name, d.ExecID, err)
		}

		return err
	}

	// If the execution is waiting on this signal at a signal node, advance.
	if updated.Status == store.StatusWaiting && updated.WaitSignal == d.Name {
		flow, ok := e.flows[updated.FlowName]
		if !ok {
			return nil
		}

		node := flow.Node(updated.CurrentNode)
		if node != nil && node.Type == dsl.NodeSignal && node.SignalName == d.Name {
			return e.transitionFromSignal(ctx, flow, d.ExecID, node.ID, d.Name)
		}
	}

	return nil
}

// onWaitTimeout fires when a signal node's timeout elapses. It routes to the
// node's on_timeout target (or fails) only if the execution is still waiting on
// that signal — a stale timeout for an already-signalled node is a no-op.
// (A crash after guardedAdvance's commit no longer strands the execution: the
// advance item is committed in the same CAS write — transactional outbox —
// and re-flushed by the next touch or the stall watchdog.)
func (e *Engine) onWaitTimeout(ctx context.Context, flow *dsl.Flow, exec *store.Execution, wi workItem) error {
	if exec.Status != store.StatusWaiting || exec.CurrentNode != wi.Node || exec.WaitSignal != wi.Signal {
		return nil // signal already consumed, or moved on
	}

	node := flow.Node(wi.Node)
	if node == nil || node.Type != dsl.NodeSignal {
		return nil
	}

	if node.OnTimeout == "" {
		return e.failNode(ctx, exec.ID, wi.Node, "signal "+node.SignalName+" timed out")
	}

	return e.guardedAdvance(ctx, exec.ID, wi.Node, wi.Signal, node.OnTimeout)
}

// transitionFromSignal advances a waiting execution to the signal node's
// successor. The signal payload is already in the data plane and appears in
// downstream contexts under signals.<name>.
func (e *Engine) transitionFromSignal(ctx context.Context, flow *dsl.Flow, execID, signalNodeID, name string) error {
	return e.guardedAdvance(ctx, execID, signalNodeID, name, flow.Successor(signalNodeID))
}

// guardedAdvance atomically advances an execution out of a signal wait, but only
// if it is still waiting on (signalNodeID, name). This makes signal arrival and
// timeout mutually exclusive: whichever applies first wins, the other no-ops.
func (e *Engine) guardedAdvance(ctx context.Context, execID, signalNodeID, name, nextNode string) error {
	var item json.RawMessage

	if nextNode != "" {
		data, err := json.Marshal(workItem{ExecID: execID, Kind: kindAdvance})
		if err != nil {
			return err
		}

		item = data
	}

	updated, err := e.store.Mutate(ctx, execID, func(ex *store.Execution) error {
		if ex.Status != store.StatusWaiting || ex.CurrentNode != signalNodeID || ex.WaitSignal != name {
			return errSkip // guard failed: leave unchanged
		}

		consumeSignal(ex, name, nextNode)

		if nextNode != "" {
			ex.AppendWork(item) // committed with the transition (transactional outbox)
		}

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

// consumeSignal applies a stored signal to the execution within a Mutate
// callback: it clears the wait state and the received-marker and advances to
// nextNode (or completes the execution when nextNode is empty). The payload
// itself lives in the data plane and stays readable (signals.<name> in the
// assembled context). Shared by guardedAdvance (signal arrival / timeout) and
// stepSignal's early-delivery consumption.
func consumeSignal(ex *store.Execution, name, nextNode string) {
	ex.WaitSignal = ""
	ex.Attempt = 0
	ex.Activity = nil        // see advanceTo: a stale stash must not survive the move
	ex.RetryAt = time.Time{} // per-node state, like Activity

	delete(ex.Signals, name) // consumed

	if nextNode == "" {
		ex.Status = store.StatusCompleted
		ex.CurrentNode = ""
	} else {
		ex.Status = store.StatusRunning
		ex.CurrentNode = nextNode
	}
}
