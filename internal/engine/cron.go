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

package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"

	"github.com/henomis/packtrail/flow"
	"github.com/henomis/packtrail/internal/cmd"
	"github.com/henomis/packtrail/internal/consume"
	"github.com/henomis/packtrail/internal/names"
	"github.com/henomis/packtrail/internal/wire"
)

// ScheduleSpec is the body of a cron schedule message: what to start on each
// firing.
type ScheduleSpec struct {
	Flow    string          `json:"flow"`
	Version string          `json:"version,omitempty"`
	Input   json.RawMessage `json:"input,omitempty"`
}

// RunCron turns fired cron messages into start commands. The execution id is
// "<schedule>-<stream sequence of the firing>": unique per firing and stable
// across redelivery, so a firing starts exactly one execution. ready is
// called once it pulls.
func (e *Engine) RunCron(ctx context.Context, ready func()) error {
	return consume.Run(ctx, e.In.JS, consume.Config{
		Stream: e.In.Names.StreamCmd,
		Consumer: jetstream.ConsumerConfig{
			Durable: e.In.Names.DurCron(), FilterSubject: e.In.Names.CronFilter(),
			AckPolicy: jetstream.AckExplicitPolicy, MaxDeliver: maxDeliver + 1,
		},
		Group:      "cron",
		PullExpiry: e.In.PullExpiry,
		Drain:      e.Drain,
		Logger:     e.In.Logger,
		Pulling:    ready,
		Handler:    e.handleCron,
	})
}

func (e *Engine) handleCron(ctx context.Context, msg jetstream.Msg) {
	name := msg.Subject()[strings.LastIndexByte(msg.Subject(), '.')+1:]

	md, err := msg.Metadata()
	if err != nil {
		_ = msg.NakWithDelay(nakDelay)

		return
	}

	var spec ScheduleSpec
	if err = json.Unmarshal(msg.Data(), &spec); err != nil {
		e.deadLetter(ctx, msg, name, "invalid schedule spec: "+err.Error())

		return
	}

	execID := name + "-" + strconv.FormatUint(md.Sequence.Stream, 10)
	if !names.ValidToken(execID) {
		execID = "cron-" + strconv.FormatUint(md.Sequence.Stream, 10)
	}

	c, err := cmd.New("cron."+execID, cmd.Start, execID, cmd.StartData{
		Flow: spec.Flow, Version: spec.Version, Input: spec.Input,
	})
	if err == nil {
		err = wire.PublishCmd(ctx, e.In, c, "")
	}

	if err != nil {
		_ = msg.NakWithDelay(nakDelay)

		return
	}

	_ = msg.Ack()
}

const flushTimeout = time.Second

// RunTrigger starts def whenever a message arrives on one of its triggers.
// The execution id is "<flow>-<Nats-Msg-Id>" (see triggerExecID), so a
// duplicate message starts nothing twice and every flow triggered by one
// message gets its own execution. With a stream the trigger is a durable
// consumer (at-least-once; without a usable Msg-Id the id is
// "<flow>-<stream>-<sequence>", stable across redelivery): a message that
// cannot be started after the redelivery budget is dead-lettered. Otherwise
// it is a core queue subscription (at-most-once). ready is called once it
// receives.
func (e *Engine) RunTrigger(ctx context.Context, def *flow.Flow, i int, tr flow.Trigger, ready func()) error {
	if tr.Stream == "" {
		return e.runCoreTrigger(ctx, def, tr, ready)
	}

	return consume.Run(ctx, e.In.JS, consume.Config{
		Stream: tr.Stream,
		Consumer: jetstream.ConsumerConfig{
			Durable:       e.In.Names.Prefix + "-trigger-" + def.Name + "-" + strconv.Itoa(i),
			FilterSubject: tr.Subject, AckPolicy: jetstream.AckExplicitPolicy, MaxDeliver: maxDeliver + 1,
			AckWait: e.In.AckWait,
		},
		Drain:      e.Drain,
		PullExpiry: e.In.PullExpiry,
		Logger:     e.In.Logger,
		Pulling:    ready,
		Handler: func(ctx context.Context, msg jetstream.Msg) {
			md, err := msg.Metadata()
			if err != nil {
				_ = msg.NakWithDelay(nakDelay)

				return
			}

			execID, ok := triggerExecID(def, msg.Headers().Get(wire.HeaderMsgID))
			if !ok {
				execID = def.Name + "-" + tr.Stream + "-" + strconv.FormatUint(md.Sequence.Stream, 10)
			}

			if err = e.startFromTrigger(ctx, def, execID, msg.Data()); err != nil {
				if md.NumDelivered >= e.triggerDeliveries() {
					// One record per flow: flows triggered by the same
					// message must not dedupe each other's dead letter.
					e.deadLetterAs(ctx, msg, wire.DLQTrigger, execID, "delivery attempts exhausted: "+err.Error(),
						"trigger."+def.Name+"."+tr.Stream+"."+strconv.FormatUint(md.Sequence.Stream, 10))

					return
				}

				e.In.Logger.Warn("packtrail: trigger start failed, will retry", "flow", def.Name, "exec", execID,
					"deliveries", md.NumDelivered, "err", err)

				_ = msg.NakWithDelay(retryDelay(md.NumDelivered))

				return
			}

			_ = msg.Ack()
		},
	})
}

// triggerExecID is the execution a trigger message with Nats-Msg-Id msgID
// starts: "<flow>-<msgID>", so flows triggered by the same message do not
// share an id (nor a start command id). ok is false without a Msg-Id or when
// the result is not a valid token.
func triggerExecID(def *flow.Flow, msgID string) (string, bool) {
	if msgID == "" {
		return "", false
	}

	id := def.Name + "-" + msgID

	return id, names.ValidToken(id)
}

// triggerDeliveries is how many deliveries a stream trigger message gets
// before it is dead-lettered.
func (e *Engine) triggerDeliveries() uint64 {
	if e.triggerMaxDeliver > 0 {
		return e.triggerMaxDeliver
	}

	return maxDeliver
}

// triggerDataKey wraps a trigger message that is not a JSON object.
const triggerDataKey = "data"

// runCoreTrigger serves a trigger without a stream: a core queue
// subscription.
func (e *Engine) runCoreTrigger(ctx context.Context, def *flow.Flow, tr flow.Trigger, ready func()) error {
	sub, err := e.In.NC.QueueSubscribe(tr.Subject, e.In.Names.Prefix+"-trigger-"+def.Name, func(m *nats.Msg) {
		execID, ok := triggerExecID(def, m.Header.Get(wire.HeaderMsgID))
		if !ok {
			execID = def.Name + "-" + nuid.Next()
		}

		if err := e.startFromTrigger(ctx, def, execID, m.Data); err != nil {
			e.In.Logger.Warn("packtrail: trigger start failed", "flow", def.Name, "err", err)
		}
	})
	if err != nil {
		return fmt.Errorf("engine: trigger %s: %w", tr.Subject, err)
	}

	go e.readyWhenFlushed(ctx, ready)

	<-ctx.Done()

	return sub.Unsubscribe()
}

// readyWhenFlushed calls ready once a round trip to the server completes:
// the server then has every subscription sent before (while disconnected,
// they are sent on reconnect).
func (e *Engine) readyWhenFlushed(ctx context.Context, ready func()) {
	for ctx.Err() == nil {
		if e.In.NC.FlushTimeout(flushTimeout) == nil {
			ready()

			return
		}

		select {
		case <-ctx.Done():
		case <-time.After(flushTimeout):
		}
	}
}

func (e *Engine) startFromTrigger(ctx context.Context, def *flow.Flow, execID string, data []byte) error {
	input := json.RawMessage(data)
	if len(data) == 0 || !json.Valid(data) {
		b, _ := json.Marshal(map[string]string{triggerDataKey: string(data)}) //nolint:errchkjson // strings always encode
		input = b
	} else if data[0] != '{' {
		b, _ := json.Marshal(map[string]json.RawMessage{triggerDataKey: data}) //nolint:errchkjson // data is valid JSON
		input = b
	}

	c, err := cmd.New("trigger."+execID, cmd.Start, execID, cmd.StartData{Flow: def.Name, Input: input})
	if err != nil {
		return err
	}

	return wire.PublishCmd(ctx, e.In, c, "")
}
