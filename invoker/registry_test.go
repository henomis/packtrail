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

package invoker_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/henomis/packtrail/invoker"
)

func TestRegistryHasAndInvoke(t *testing.T) {
	ctx := context.Background()
	r := invoker.NewRegistry()

	if r.Has("agent") {
		t.Fatal("empty registry reports kind present")
	}

	r.Register("agent", invoker.Func(func(_ context.Context, req invoker.Request) (invoker.Result, error) {
		return invoker.Result{Status: invoker.StatusOK, Payload: req.Payload}, nil
	}))

	if !r.Has("agent") {
		t.Fatal("registered kind not reported by Has")
	}

	res, err := r.Invoke(ctx, invoker.Request{Invoker: "agent", Payload: json.RawMessage(`{"k":1}`)})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if res.Status != invoker.StatusOK || string(res.Payload) != `{"k":1}` {
		t.Fatalf("unexpected result: %+v", res)
	}
}

func TestRegistryUnknownKind(t *testing.T) {
	r := invoker.NewRegistry()

	_, err := r.Invoke(context.Background(), invoker.Request{Invoker: "missing"})
	if err == nil {
		t.Fatal("Invoke on unregistered kind succeeded, want error")
	}
	if !strings.Contains(err.Error(), "missing") {
		t.Errorf("error %q does not name the missing kind", err)
	}
}

func TestRegistryRegisterReplaces(t *testing.T) {
	ctx := context.Background()
	r := invoker.NewRegistry()

	r.Register("k", invoker.Func(func(context.Context, invoker.Request) (invoker.Result, error) {
		return invoker.Result{Status: invoker.StatusError, Error: "first"}, nil
	}))
	// Re-registering the same kind replaces the previous binding.
	r.Register("k", invoker.Func(func(context.Context, invoker.Request) (invoker.Result, error) {
		return invoker.Result{Status: invoker.StatusOK}, nil
	}))

	res, err := r.Invoke(ctx, invoker.Request{Invoker: "k"})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if res.Status != invoker.StatusOK {
		t.Fatalf("Register did not replace: got %+v", res)
	}
}
