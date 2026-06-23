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
	"testing"
	"time"

	"github.com/henomis/packtrail/internal/natstest"
	"github.com/henomis/packtrail/pkg/protocol"
)

// TestServeAppliesDeadline verifies a non-zero request deadline is applied to the
// handler's context.
func TestServeAppliesDeadline(t *testing.T) {
	srv := natstest.Start(t)

	var hasDeadline bool

	sub, err := protocol.Serve(srv.NC, "tasks.deadline.*", func(ctx context.Context, _ protocol.TaskRequest) (protocol.TaskResponse, error) {
		_, hasDeadline = ctx.Deadline()
		return protocol.TaskResponse{Status: protocol.StatusOK}, nil
	})
	if err != nil {
		t.Fatalf("serve: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	resp := request(t, srv, "tasks.deadline.x", protocol.TaskRequest{
		ExecutionID: "exec-d",
		Deadline:    time.Now().Add(time.Minute),
	})
	if resp.Status != protocol.StatusOK {
		t.Fatalf("status = %q, want ok", resp.Status)
	}
	if !hasDeadline {
		t.Fatal("handler context had no deadline despite a non-zero request deadline")
	}
}

// TestReplyMarshalFallback verifies that when a handler returns an unmarshalable
// response (invalid raw-JSON payload), reply falls back to a StatusError envelope
// instead of replying with nothing.
func TestReplyMarshalFallback(t *testing.T) {
	srv := natstest.Start(t)

	sub, err := protocol.Serve(srv.NC, "tasks.badresp.*", func(_ context.Context, _ protocol.TaskRequest) (protocol.TaskResponse, error) {
		// An invalid json.RawMessage makes json.Marshal of the response fail.
		return protocol.TaskResponse{Status: protocol.StatusOK, Payload: json.RawMessage("{not json")}, nil
	})
	if err != nil {
		t.Fatalf("serve: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	resp := request(t, srv, "tasks.badresp.x", protocol.TaskRequest{ExecutionID: "exec-b"})
	if resp.Status != protocol.StatusError {
		t.Fatalf("status = %q, want error (marshal fallback)", resp.Status)
	}
	if resp.Error == "" {
		t.Fatal("fallback response carried no error message")
	}
}
