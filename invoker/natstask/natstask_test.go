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

package natstask_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/henomis/packtrail/internal/natstest"
	"github.com/henomis/packtrail/invoker"
	"github.com/henomis/packtrail/invoker/natstask"
	"github.com/henomis/packtrail/pkg/protocol"
)

// TestInvokeMapsResponse verifies the invoker prepends the namespace to the
// target subject, forwards the request fields and maps a protocol response back
// to an invoker.Result.
func TestInvokeMapsResponse(t *testing.T) {
	srv := natstest.Start(t)

	var gotReq protocol.TaskRequest

	// Worker subscribes under the namespaced subject the invoker will use.
	sub, err := protocol.ServeNamespaced(context.Background(), srv.NC, "packtrail", "tasks.echo.*", func(_ context.Context, req protocol.TaskRequest) (protocol.TaskResponse, error) {
		gotReq = req
		return protocol.TaskResponse{Status: protocol.StatusOK, Payload: json.RawMessage(`{"ok":true}`)}, nil
	})
	if err != nil {
		t.Fatalf("serve: %v", err)
	}

	t.Cleanup(func() { _ = sub.Unsubscribe() })

	inv := natstask.New(srv.NC, "packtrail")

	res, err := inv.Invoke(context.Background(), invoker.Request{
		Target:      "tasks.echo.x",
		ExecutionID: "exec-1",
		NodeID:      "node-1",
		Payload:     json.RawMessage(`{"in":1}`),
		Attempt:     3,
	})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}

	if res.Status != invoker.StatusOK {
		t.Fatalf("status = %q, want ok", res.Status)
	}

	if string(res.Payload) != `{"ok":true}` {
		t.Fatalf("payload = %s, want {\"ok\":true}", res.Payload)
	}

	if gotReq.ExecutionID != "exec-1" || gotReq.NodeID != "node-1" || gotReq.Attempt != 3 {
		t.Fatalf("worker saw %+v, want exec-1/node-1/attempt 3", gotReq)
	}
}

// TestInvokeMapsError verifies a protocol error status (and message) is mapped
// onto the invoker result.
func TestInvokeMapsError(t *testing.T) {
	srv := natstest.Start(t)

	sub, err := protocol.ServeNamespaced(context.Background(), srv.NC, "packtrail", "tasks.fail.*", func(_ context.Context, _ protocol.TaskRequest) (protocol.TaskResponse, error) {
		return protocol.TaskResponse{Status: protocol.StatusError, Error: "permanent"}, nil
	})
	if err != nil {
		t.Fatalf("serve: %v", err)
	}

	t.Cleanup(func() { _ = sub.Unsubscribe() })

	inv := natstask.New(srv.NC, "packtrail")

	res, err := inv.Invoke(context.Background(), invoker.Request{Target: "tasks.fail.x", ExecutionID: "exec-2"})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}

	if res.Status != invoker.StatusError {
		t.Fatalf("status = %q, want error", res.Status)
	}

	if res.Error != "permanent" {
		t.Fatalf("error = %q, want permanent", res.Error)
	}
}

// TestInvokeRejectsPending verifies an out-of-contract "pending" reply is
// converted to a permanent error instead of parking the execution in a wait
// no request/reply worker can ever settle.
func TestInvokeRejectsPending(t *testing.T) {
	srv := natstest.Start(t)

	sub, err := srv.NC.Subscribe("packtrail.tasks.rogue.*", func(msg *nats.Msg) {
		data, _ := json.Marshal(protocol.TaskResponse{Status: "pending"})
		_ = msg.Respond(data)
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	if err = srv.NC.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	t.Cleanup(func() { _ = sub.Unsubscribe() })

	inv := natstask.New(srv.NC, "packtrail")

	res, err := inv.Invoke(context.Background(), invoker.Request{Target: "tasks.rogue.x", ExecutionID: "exec-5"})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}

	if res.Status != invoker.StatusError {
		t.Fatalf("status = %q, want error (pending must not park a request/reply node)", res.Status)
	}

	if res.Error == "" {
		t.Fatal("expected an actionable error message for the pending reply")
	}

	if !strings.Contains(res.Error, "pending") {
		t.Fatalf("error = %q, want it to mention pending", res.Error)
	}
}

func TestInvokeUsesRequestDeadlineWithoutCallerDeadline(t *testing.T) {
	srv := natstest.Start(t)

	sub, err := srv.NC.Subscribe("packtrail.tasks.silent.*", func(_ *nats.Msg) {})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	if err = srv.NC.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	t.Cleanup(func() { _ = sub.Unsubscribe() })

	inv := natstask.New(srv.NC, "packtrail")
	start := time.Now()

	_, err = inv.Invoke(context.Background(), invoker.Request{
		Target:      "tasks.silent.x",
		ExecutionID: "exec-deadline",
		Deadline:    time.Now().Add(100 * time.Millisecond),
	})
	if err == nil {
		t.Fatal("invoke succeeded, want request-deadline error from silent responder")
	}

	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("invoke took %v, want it bounded by request deadline", elapsed)
	}
}

// TestInvokeNoWorkerReturnsError verifies a request with no responder surfaces a
// transport error (the engine treats this as a transient failure).
func TestInvokeNoWorkerReturnsError(t *testing.T) {
	srv := natstest.Start(t)
	inv := natstask.New(srv.NC, "packtrail")

	_, err := inv.Invoke(context.Background(), invoker.Request{Target: "tasks.missing.x", ExecutionID: "exec-3"})
	if err == nil {
		t.Fatal("expected a transport error when no worker is listening")
	}
}

// TestNewDefaultsPrefix verifies an empty prefix falls back to "packtrail".
func TestNewDefaultsPrefix(t *testing.T) {
	srv := natstest.Start(t)

	sub, err := protocol.ServeNamespaced(context.Background(), srv.NC, "packtrail", "tasks.echo.*", func(_ context.Context, _ protocol.TaskRequest) (protocol.TaskResponse, error) {
		return protocol.TaskResponse{Status: protocol.StatusOK}, nil
	})
	if err != nil {
		t.Fatalf("serve: %v", err)
	}

	t.Cleanup(func() { _ = sub.Unsubscribe() })

	inv := natstask.New(srv.NC, "") // empty prefix -> "packtrail"

	res, err := inv.Invoke(context.Background(), invoker.Request{Target: "tasks.echo.x", ExecutionID: "exec-4"})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}

	if res.Status != invoker.StatusOK {
		t.Fatalf("status = %q, want ok", res.Status)
	}
}
