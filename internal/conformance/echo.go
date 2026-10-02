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

// Package conformance is the worker-protocol conformance suite. It drives a
// worker of kind "echo" through every protocol path (output, channel writes,
// dynamic next, usage, retryable and permanent errors, interrupt/resume,
// claim-checked payloads, long jobs kept alive by heartbeats).
//
// The worker under test implements the echo contract below. By default the Go
// reference implementation (Echo) runs in-process; set PT_CONFORMANCE_WORKER
// to a command to test another SDK instead: it is started with NATS_URL and
// PACKTRAIL_NAMESPACE set and must serve kind "echo" until killed.
//
// Echo contract — the job input (an object) drives the behaviour:
//
//	sleep_ms      sleep this long first (exercises heartbeats)
//	fail_until    {node: n}: that node fails retryably while attempt < n
//	permanent     fail permanently (no retry)
//	interrupt     node id: that node interrupts with {"ask": <node>} unless resumed
//	writes        return as channel writes
//	next          {node: target}: the dynamic next node of that node
//	usage         return as usage counters
//
// and the output is {"node", "attempt", "input", "resume"?, "item"?}.
package conformance

import (
	"context"
	"errors"
	"time"

	"github.com/henomis/packtrail/worker"
)

// Kind is the worker kind the suite uses.
const Kind = "echo"

type echoInput struct {
	SleepMS   int                `json:"sleep_ms"`
	FailUntil map[string]int     `json:"fail_until"`
	Permanent bool               `json:"permanent"`
	Interrupt string             `json:"interrupt"`
	Writes    map[string]any     `json:"writes"`
	Next      map[string]string  `json:"next"`
	Usage     map[string]float64 `json:"usage"`
}

// Echo is the Go reference implementation of the echo contract.
func Echo(ctx context.Context, j *worker.Job) (*worker.Result, error) {
	var in echoInput
	if err := j.Input(&in); err != nil {
		return nil, worker.Permanent(err)
	}

	if in.SleepMS > 0 {
		select {
		case <-time.After(time.Duration(in.SleepMS) * time.Millisecond):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	if j.Attempt < in.FailUntil[j.Node] {
		return nil, errors.New("transient failure requested")
	}

	if in.Permanent {
		return nil, worker.Permanent(errors.New("permanent failure requested"))
	}

	resumed, _ := j.Resumed(nil)
	if in.Interrupt == j.Node && !resumed {
		return nil, worker.Interrupt(map[string]string{"ask": j.Node})
	}

	out := map[string]any{"node": j.Node, "attempt": j.Attempt, "input": j.Context.Input}

	if resumed {
		out["resume"] = j.Context.Resume
	}

	if len(j.Context.Item) > 0 {
		out["item"] = j.Context.Item
	}

	return &worker.Result{Output: out, Writes: in.Writes, Next: in.Next[j.Node], Usage: in.Usage}, nil
}
