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
func (e *Engine) stepSignal(ctx context.Context, flow *dsl.Flow, node *dsl.Node, exec *store.Execution) error {
	name := node.SignalName
	if _, ok := exec.Signals[name]; ok {
		return e.transitionFromSignal(ctx, flow, exec.ID, node.ID, name)
	}

	updated, err := e.store.Mutate(ctx, exec.ID, func(ex *store.Execution) error {
		ex.Status = store.StatusWaiting
		ex.WaitSignal = name

		return nil
	})
	if err != nil {
		return err
	}

	e.emitEvent(ctx, updated)

	if node.Timeout.D() > 0 {
		item, marshalErr := json.Marshal(workItem{ExecID: exec.ID, Kind: kindWaitTimeout, Node: node.ID, Signal: name})
		if marshalErr != nil {
			return marshalErr
		}

		return e.sched.After(ctx, exec.ID, node.Timeout.D(), item)
	}

	return nil
}

// applySignal is the signal-consumer callback. It records the signal
// idempotently (by stream sequence) and, if the execution is waiting on it,
// advances. State is always persisted via CAS before the message is acked.
func (e *Engine) applySignal(ctx context.Context, d signal.Delivery) error {
	updated, err := e.store.Mutate(ctx, d.ExecID, func(ex *store.Execution) error {
		if ex.LastSeq != nil && ex.LastSeq[d.Name] >= d.Seq {
			return errSkip // duplicate: already applied at >= this sequence
		}

		if ex.LastSeq == nil {
			ex.LastSeq = map[string]uint64{}
		}

		if ex.Signals == nil {
			ex.Signals = map[string]json.RawMessage{}
		}

		ex.LastSeq[d.Name] = d.Seq
		ex.Signals[d.Name] = append(json.RawMessage(nil), d.Payload...)

		return nil
	})
	if err != nil {
		if errors.Is(err, errSkip) || errors.Is(err, store.ErrNotFound) {
			return nil // duplicate or unknown execution: ack and move on
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
func (e *Engine) onWaitTimeout(ctx context.Context, flow *dsl.Flow, exec *store.Execution, wi workItem) error {
	if exec.Status != store.StatusWaiting || exec.CurrentNode != wi.Node || exec.WaitSignal != wi.Signal {
		return nil // signal already consumed, or moved on
	}

	node := flow.Node(wi.Node)
	if node == nil || node.Type != dsl.NodeSignal {
		return nil
	}

	if node.OnTimeout == "" {
		return e.fail(ctx, exec.ID, "signal "+node.SignalName+" timed out")
	}

	return e.guardedAdvance(ctx, exec.ID, wi.Node, wi.Signal, node.OnTimeout, false)
}

// transitionFromSignal advances a waiting execution to the signal node's
// successor, merging the received signal payload into the shared context.
func (e *Engine) transitionFromSignal(ctx context.Context, flow *dsl.Flow, execID, signalNodeID, name string) error {
	next := flow.Successor(signalNodeID)
	return e.guardedAdvance(ctx, execID, signalNodeID, name, next, true)
}

// guardedAdvance atomically advances an execution out of a signal wait, but only
// if it is still waiting on (signalNodeID, name). This makes signal arrival and
// timeout mutually exclusive: whichever applies first wins, the other no-ops.
// When mergeSignal is true the received signal payload is merged into the
// payload under its signal name.
func (e *Engine) guardedAdvance(
	ctx context.Context, execID, signalNodeID, name, nextNode string, mergeSignal bool,
) error {
	updated, err := e.store.Mutate(ctx, execID, func(ex *store.Execution) error {
		if ex.Status != store.StatusWaiting || ex.CurrentNode != signalNodeID || ex.WaitSignal != name {
			return errSkip // guard failed: leave unchanged
		}

		ex.WaitSignal = ""

		ex.Attempt = 0
		if mergeSignal {
			if p, ok := ex.Signals[name]; ok {
				mergeSignalPayload(ex, name, p)
			}
		}

		delete(ex.Signals, name) // consumed

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
		if errors.Is(err, errSkip) {
			return nil
		}

		return err
	}

	e.emitEvent(ctx, updated)

	if nextNode == "" {
		return nil
	}

	return e.enqueue(ctx, execID, workItem{Kind: kindAdvance})
}

// mergeSignalPayload stores the signal payload into the shared payload under key
// name.
func mergeSignalPayload(ex *store.Execution, name string, payload json.RawMessage) {
	root := map[string]json.RawMessage{}
	_ = json.Unmarshal(ex.Payload, &root)

	root[name] = payload
	if data, err := json.Marshal(root); err == nil {
		ex.Payload = data
	}
}
