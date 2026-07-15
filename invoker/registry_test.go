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

	if err := r.Register("agent", invoker.Func(func(_ context.Context, req invoker.Request) (invoker.Result, error) {
		return invoker.Result{Status: invoker.StatusOK, Payload: req.Payload}, nil
	})); err != nil {
		t.Fatalf("register: %v", err)
	}

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

func TestRegistryRegisterRejectsDuplicate(t *testing.T) {
	ctx := context.Background()
	r := invoker.NewRegistry()

	if err := r.Register("k", invoker.Func(func(context.Context, invoker.Request) (invoker.Result, error) {
		return invoker.Result{Status: invoker.StatusError, Error: "first"}, nil
	})); err != nil {
		t.Fatalf("register first: %v", err)
	}

	if err := r.Register("k", invoker.Func(func(context.Context, invoker.Request) (invoker.Result, error) {
		return invoker.Result{Status: invoker.StatusOK}, nil
	})); err == nil || !strings.Contains(err.Error(), "already registered") {
		t.Fatalf("register duplicate err = %v, want already registered", err)
	}

	res, err := r.Invoke(ctx, invoker.Request{Invoker: "k"})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}

	if res.Status != invoker.StatusError || res.Error != "first" {
		t.Fatalf("duplicate Register replaced first binding: got %+v", res)
	}
}

func TestRegistryReplace(t *testing.T) {
	ctx := context.Background()
	r := invoker.NewRegistry()

	if err := r.Register("k", invoker.Func(func(context.Context, invoker.Request) (invoker.Result, error) {
		return invoker.Result{Status: invoker.StatusError, Error: "first"}, nil
	})); err != nil {
		t.Fatalf("register first: %v", err)
	}

	if err := r.Replace("k", invoker.Func(func(context.Context, invoker.Request) (invoker.Result, error) {
		return invoker.Result{Status: invoker.StatusOK}, nil
	})); err != nil {
		t.Fatalf("replace: %v", err)
	}

	res, err := r.Invoke(ctx, invoker.Request{Invoker: "k"})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}

	if res.Status != invoker.StatusOK {
		t.Fatalf("Replace did not update binding: got %+v", res)
	}
}

func TestRegistryRejectsNil(t *testing.T) {
	r := invoker.NewRegistry()

	if err := r.Register("nil", nil); err == nil || !strings.Contains(err.Error(), "nil Invoker") {
		t.Fatalf("Register nil err = %v, want nil Invoker", err)
	}

	var typedNil invoker.Func
	if err := r.Register("typed-nil", typedNil); err == nil || !strings.Contains(err.Error(), "nil Invoker") {
		t.Fatalf("Register typed nil err = %v, want nil Invoker", err)
	}

	if err := r.Replace("nil", nil); err == nil || !strings.Contains(err.Error(), "nil Invoker") {
		t.Fatalf("Replace nil err = %v, want nil Invoker", err)
	}
}
