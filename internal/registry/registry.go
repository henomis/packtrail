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

// Package registry stores flow definitions immutably in KV: key
// "<name>.<hash>" holds the definition, "<name>.latest" the hash of the most
// recently registered version. Every execution records the hash it started
// with, so deploying a new version never changes executions in flight.
package registry

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail/flow"
	"github.com/henomis/packtrail/internal/infra"
)

const latest = "latest"

// ErrUnknownFlow is returned for a flow (or version) that is not registered.
var ErrUnknownFlow = errors.New("registry: unknown flow")

// Registry reads and writes flow definitions, caching immutable versions.
type Registry struct {
	in *infra.Infra

	mu    sync.Mutex
	cache map[string]*flow.Flow
}

// New returns a Registry.
func New(in *infra.Infra) *Registry { return &Registry{in: in, cache: map[string]*flow.Flow{}} }

// Register stores def (validated) and makes it the latest version. It is
// idempotent; it returns the version hash.
func (r *Registry) Register(ctx context.Context, def *flow.Flow) (string, error) {
	if err := def.Validate(); err != nil {
		return "", err
	}

	b, err := def.JSON()
	if err != nil {
		return "", err
	}

	hash, err := def.Hash()
	if err != nil {
		return "", err
	}

	kv, err := r.in.KV(ctx, r.in.Names.BucketFlows)
	if err != nil {
		return "", err
	}

	key := def.Name + "." + hash

	if _, err = kv.Create(ctx, key, b); err != nil && !errors.Is(err, jetstream.ErrKeyExists) {
		return "", fmt.Errorf("registry: store %s: %w", key, err)
	} else if err != nil {
		e, gerr := kv.Get(ctx, key)
		if gerr == nil && !bytes.Equal(e.Value(), b) {
			return "", fmt.Errorf("registry: %s exists with different content (hash collision)", key)
		}
	}

	if _, err = kv.PutString(ctx, def.Name+"."+latest, hash); err != nil {
		return "", fmt.Errorf("registry: set latest %s: %w", def.Name, err)
	}

	return hash, nil
}

// Get returns version hash of flow name ("" = latest).
func (r *Registry) Get(ctx context.Context, name, hash string) (*flow.Flow, error) {
	if hash == "" {
		h, err := r.Latest(ctx, name)
		if err != nil {
			return nil, err
		}

		hash = h
	}

	key := name + "." + hash

	r.mu.Lock()
	def, ok := r.cache[key]
	r.mu.Unlock()

	if ok {
		return def, nil
	}

	kv, err := r.in.KV(ctx, r.in.Names.BucketFlows)
	if err != nil {
		return nil, err
	}

	e, err := kv.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) || errors.Is(err, jetstream.ErrInvalidKey) {
			return nil, fmt.Errorf("%w: %s@%s", ErrUnknownFlow, name, hash)
		}

		return nil, err
	}

	def, err = flow.ParseJSON(e.Value())
	if err != nil {
		return nil, fmt.Errorf("registry: %s: %w", key, err)
	}

	r.mu.Lock()
	r.cache[key] = def
	r.mu.Unlock()

	return def, nil
}

// Latest returns the latest version hash of name.
func (r *Registry) Latest(ctx context.Context, name string) (string, error) {
	kv, err := r.in.KV(ctx, r.in.Names.BucketFlows)
	if err != nil {
		return "", err
	}

	e, err := kv.Get(ctx, name+"."+latest)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) || errors.Is(err, jetstream.ErrInvalidKey) {
			return "", fmt.Errorf("%w: %s", ErrUnknownFlow, name)
		}

		return "", err
	}

	return string(e.Value()), nil
}

// Version describes one registered version.
type Version struct {
	Name   string `json:"name"`
	Hash   string `json:"hash"`
	Latest bool   `json:"latest"`
}

// List returns every registered version, sorted.
func (r *Registry) List(ctx context.Context) ([]Version, error) {
	kv, err := r.in.KV(ctx, r.in.Names.BucketFlows)
	if err != nil {
		return nil, err
	}

	keys, err := kv.ListKeys(ctx)
	if err != nil {
		return nil, err
	}

	latestOf := map[string]string{}

	var all []Version

	for k := range keys.Keys() {
		name, rest, _ := strings.Cut(k, ".")
		if rest == latest {
			if e, ierr := kv.Get(ctx, k); ierr == nil {
				latestOf[name] = string(e.Value())
			}

			continue
		}

		all = append(all, Version{Name: name, Hash: rest})
	}

	for i := range all {
		all[i].Latest = latestOf[all[i].Name] == all[i].Hash
	}

	sort.Slice(all, func(i, j int) bool {
		if all[i].Name != all[j].Name {
			return all[i].Name < all[j].Name
		}

		return all[i].Hash < all[j].Hash
	})

	return all, nil
}
