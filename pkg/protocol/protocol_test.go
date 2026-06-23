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

	sub, err := protocol.Serve(srv.NC, "tasks.echo.*", func(_ context.Context, req protocol.TaskRequest) (protocol.TaskResponse, error) {
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

	sub, err := protocol.Serve(srv.NC, "tasks.fail.*", func(_ context.Context, _ protocol.TaskRequest) (protocol.TaskResponse, error) {
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

// TestServeBadRequest verifies malformed JSON is answered with StatusError
// rather than dropped.
func TestServeBadRequest(t *testing.T) {
	srv := natstest.Start(t)

	sub, err := protocol.Serve(srv.NC, "tasks.bad.*", func(_ context.Context, _ protocol.TaskRequest) (protocol.TaskResponse, error) {
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

// TestServeNamespaced verifies the namespace is prepended to the subscription
// subject.
func TestServeNamespaced(t *testing.T) {
	srv := natstest.Start(t)

	sub, err := protocol.ServeNamespaced(srv.NC, "acme", "tasks.echo.*", func(_ context.Context, _ protocol.TaskRequest) (protocol.TaskResponse, error) {
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
