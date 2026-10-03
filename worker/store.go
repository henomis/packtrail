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

package worker

import (
	"context"
	"encoding/json"

	"github.com/henomis/packtrail/internal/apierr"
	"github.com/henomis/packtrail/internal/infra"
	"github.com/henomis/packtrail/internal/store"
)

// Store errors. They are the same values as packtrail.ErrNotFound and
// packtrail.ErrInvalidArgument: test with errors.Is against either.
var (
	// ErrNotFound means a store key has no value.
	ErrNotFound = apierr.ErrNotFound
	// ErrInvalidArgument wraps an invalid namespace, key or value.
	ErrInvalidArgument = apierr.ErrInvalidArgument
)

// Store is the long-term key-value store of the worker's namespace: the one
// packtrail.Client.Store reads and writes, for application data that
// outlives executions (per-user memory, learned facts). Namespaces and keys
// are tokens.
//
// Store writes are not part of the execution log: a redelivered or retried
// job runs its writes again, so make them idempotent (write a value derived
// from the job's context rather than appending blindly).
type Store struct{ in *infra.Infra }

// Store returns the long-term store. It works only on a job handed to a
// Handler by Worker.Run.
func (j *Job) Store() *Store { return j.store }

// Put stores value (any JSON-encodable value) under ns/key.
func (s *Store) Put(ctx context.Context, ns, key string, value any) error {
	return store.Put(ctx, s.in, ns, key, value)
}

// Get returns the value under ns/key, ErrNotFound when there is none.
func (s *Store) Get(ctx context.Context, ns, key string) (json.RawMessage, error) {
	return store.Get(ctx, s.in, ns, key)
}

// Delete removes ns/key.
func (s *Store) Delete(ctx context.Context, ns, key string) error {
	return store.Delete(ctx, s.in, ns, key)
}

// Keys lists the keys of a namespace, sorted.
func (s *Store) Keys(ctx context.Context, ns string) ([]string, error) {
	return store.Keys(ctx, s.in, ns)
}
