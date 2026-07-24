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

package protocol_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/henomis/packtrail/internal/natstest"
	"github.com/henomis/packtrail/pkg/protocol"
)

const requestTimeout = 3 * time.Second

// request marshals req, performs a NATS request to subject and decodes the
// reply.
func request(t *testing.T, srv *natstest.Server, subject string, req protocol.TaskRequest) protocol.TaskResponse {
	t.Helper()

	data, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	msg, err := srv.NC.Request(subject, data, requestTimeout)
	if err != nil {
		t.Fatalf("request: %v", err)
	}

	var resp protocol.TaskResponse
	if unmarshalErr := json.Unmarshal(msg.Data, &resp); unmarshalErr != nil {
		t.Fatalf("unmarshal response: %v", unmarshalErr)
	}

	return resp
}

// TestServeRoundTrip verifies a handler's response is delivered to the caller
// and that the decoded request carries the fields the caller sent.
func TestServeRoundTrip(t *testing.T) {
	srv := natstest.Start(t)

	var gotReq protocol.TaskRequest

	sub, err := protocol.Serve(context.Background(), srv.NC, "tasks.echo.*", func(_ context.Context, req protocol.TaskRequest) (protocol.TaskResponse, error) {
		gotReq = req
		return protocol.TaskResponse{Status: protocol.StatusOK, Payload: req.Payload}, nil
	})
	if err != nil {
		t.Fatalf("serve: %v", err)
	}

	t.Cleanup(func() { _ = sub.Unsubscribe() })

	resp := request(t, srv, "tasks.echo.x1", protocol.TaskRequest{
		ExecutionID: "exec-1",
		NodeID:      "node-1",
		Payload:     json.RawMessage(`{"k":"v"}`),
		Attempt:     2,
	})

	if resp.Status != protocol.StatusOK {
		t.Fatalf("status = %q, want ok", resp.Status)
	}

	if string(resp.Payload) != `{"k":"v"}` {
		t.Fatalf("payload = %s, want {\"k\":\"v\"}", resp.Payload)
	}

	if gotReq.ExecutionID != "exec-1" || gotReq.NodeID != "node-1" || gotReq.Attempt != 2 {
		t.Fatalf("handler saw %+v, want exec-1/node-1/attempt 2", gotReq)
	}
}

// TestServeHandlerErrorIsRetry verifies a handler error is reported to the
// caller as StatusRetry (a transient failure).
func TestServeHandlerErrorIsRetry(t *testing.T) {
	srv := natstest.Start(t)

	sub, err := protocol.Serve(context.Background(), srv.NC, "tasks.fail.*", func(_ context.Context, _ protocol.TaskRequest) (protocol.TaskResponse, error) {
		return protocol.TaskResponse{}, errors.New("transient boom")
	})
	if err != nil {
		t.Fatalf("serve: %v", err)
	}

	t.Cleanup(func() { _ = sub.Unsubscribe() })

	resp := request(t, srv, "tasks.fail.x", protocol.TaskRequest{ExecutionID: "exec-2"})
	if resp.Status != protocol.StatusRetry {
		t.Fatalf("status = %q, want retry", resp.Status)
	}

	if resp.Error != "transient boom" {
		t.Fatalf("error = %q, want transient boom", resp.Error)
	}
}

func TestServeRejectsInvalidStatus(t *testing.T) {
	srv := natstest.Start(t)

	sub, err := protocol.Serve(context.Background(), srv.NC, "tasks.invalid.*", func(_ context.Context, _ protocol.TaskRequest) (protocol.TaskResponse, error) {
		return protocol.TaskResponse{Status: "pending"}, nil
	})
	if err != nil {
		t.Fatalf("serve: %v", err)
	}

	t.Cleanup(func() { _ = sub.Unsubscribe() })

	resp := request(t, srv, "tasks.invalid.x", protocol.TaskRequest{ExecutionID: "exec-invalid"})
	if resp.Status != protocol.StatusError {
		t.Fatalf("status = %q, want error", resp.Status)
	}

	if !strings.Contains(resp.Error, "invalid task status") {
		t.Fatalf("error = %q, want invalid-status explanation", resp.Error)
	}
}

// TestServeBadRequest verifies malformed JSON is answered with StatusError
// rather than dropped.
func TestServeBadRequest(t *testing.T) {
	srv := natstest.Start(t)

	sub, err := protocol.Serve(context.Background(), srv.NC, "tasks.bad.*", func(_ context.Context, _ protocol.TaskRequest) (protocol.TaskResponse, error) {
		t.Fatal("handler should not be called for a malformed request")
		return protocol.TaskResponse{}, nil
	})
	if err != nil {
		t.Fatalf("serve: %v", err)
	}

	t.Cleanup(func() { _ = sub.Unsubscribe() })

	msg, err := srv.NC.Request("tasks.bad.x", []byte("not json"), requestTimeout)
	if err != nil {
		t.Fatalf("request: %v", err)
	}

	var resp protocol.TaskResponse
	if unmarshalErr := json.Unmarshal(msg.Data, &resp); unmarshalErr != nil {
		t.Fatalf("unmarshal response: %v", unmarshalErr)
	}

	if resp.Status != protocol.StatusError {
		t.Fatalf("status = %q, want error", resp.Status)
	}
}

// TestServeRecoversHandlerPanic is a regression test: a panicking Handler used
// to escape onto NATS's shared per-connection dispatcher goroutine, crashing
// the whole worker process (every subscription on that connection, not just
// this one). Serve must recover it and reply with StatusError instead.
func TestServeRecoversHandlerPanic(t *testing.T) {
	srv := natstest.Start(t)

	sub, err := protocol.Serve(context.Background(), srv.NC, "tasks.panic.*", func(_ context.Context, _ protocol.TaskRequest) (protocol.TaskResponse, error) {
		panic("boom")
	})
	if err != nil {
		t.Fatalf("serve: %v", err)
	}

	t.Cleanup(func() { _ = sub.Unsubscribe() })

	resp := request(t, srv, "tasks.panic.x", protocol.TaskRequest{ExecutionID: "exec-panic"})
	if resp.Status != protocol.StatusError {
		t.Fatalf("status = %q, want error", resp.Status)
	}

	if !strings.Contains(resp.Error, "panic") {
		t.Fatalf("error = %q, want panic explanation", resp.Error)
	}

	// The connection (and every other subscription on it) must still be alive.
	resp2 := request(t, srv, "tasks.panic.x", protocol.TaskRequest{ExecutionID: "exec-panic-2"})
	if resp2.Status != protocol.StatusError {
		t.Fatalf("second request status = %q, want error (connection should have survived the panic)", resp2.Status)
	}
}

// TestServeHandlesConcurrentRequestsWithoutHeadOfLineBlocking is a regression
// test: Serve used to invoke handlers inline on NATS's single shared
// per-connection dispatcher goroutine, so one slow handler for one subject
// blocked every other subject sharing the same *nats.Conn. Here a slow
// handler and a fast handler share one connection; the fast one must not wait
// for the slow one to finish.
func TestServeHandlesConcurrentRequestsWithoutHeadOfLineBlocking(t *testing.T) {
	srv := natstest.Start(t)

	release := make(chan struct{})
	slowStarted := make(chan struct{})

	slowSub, err := protocol.Serve(context.Background(), srv.NC, "tasks.slow.*", func(_ context.Context, _ protocol.TaskRequest) (protocol.TaskResponse, error) {
		close(slowStarted)
		<-release

		return protocol.TaskResponse{Status: protocol.StatusOK}, nil
	})
	if err != nil {
		t.Fatalf("serve slow: %v", err)
	}

	t.Cleanup(func() { _ = slowSub.Unsubscribe() })

	fastSub, err := protocol.Serve(context.Background(), srv.NC, "tasks.fast.*", func(_ context.Context, _ protocol.TaskRequest) (protocol.TaskResponse, error) {
		return protocol.TaskResponse{Status: protocol.StatusOK}, nil
	})
	if err != nil {
		t.Fatalf("serve fast: %v", err)
	}

	t.Cleanup(func() { _ = fastSub.Unsubscribe() })

	// Fire the slow request asynchronously (it blocks until we release it) on
	// the SAME connection the fast request below uses.
	slowDone := make(chan protocol.TaskResponse, 1)

	go func() {
		slowDone <- request(t, srv, "tasks.slow.x", protocol.TaskRequest{ExecutionID: "exec-slow"})
	}()

	select {
	case <-slowStarted:
	case <-time.After(requestTimeout):
		t.Fatal("slow handler never started")
	}

	// The fast request must complete promptly even though the slow handler on
	// the same connection is still blocked.
	fastResp := request(t, srv, "tasks.fast.x", protocol.TaskRequest{ExecutionID: "exec-fast"})
	if fastResp.Status != protocol.StatusOK {
		t.Fatalf("fast status = %q, want ok (blocked behind the slow handler?)", fastResp.Status)
	}

	close(release)

	select {
	case slowResp := <-slowDone:
		if slowResp.Status != protocol.StatusOK {
			t.Fatalf("slow status = %q, want ok", slowResp.Status)
		}
	case <-time.After(requestTimeout):
		t.Fatal("slow handler never completed")
	}
}

// TestServeNamespaced verifies the namespace is prepended to the subscription
// subject.
func TestServeNamespaced(t *testing.T) {
	srv := natstest.Start(t)

	sub, err := protocol.ServeNamespaced(context.Background(), srv.NC, "acme", "tasks.echo.*", func(_ context.Context, _ protocol.TaskRequest) (protocol.TaskResponse, error) {
		return protocol.TaskResponse{Status: protocol.StatusOK}, nil
	})
	if err != nil {
		t.Fatalf("serve namespaced: %v", err)
	}

	t.Cleanup(func() { _ = sub.Unsubscribe() })

	resp := request(t, srv, "acme.tasks.echo.x", protocol.TaskRequest{ExecutionID: "exec-3"})
	if resp.Status != protocol.StatusOK {
		t.Fatalf("status = %q, want ok", resp.Status)
	}
}
