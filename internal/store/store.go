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

// Package store is the long-term key-value store for application data that
// outlives executions, shared by clients and worker jobs. Values live in the
// <ns>-store bucket under <namespace>.<key>; namespaces and keys are tokens.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail/internal/apierr"
	"github.com/henomis/packtrail/internal/infra"
	"github.com/henomis/packtrail/internal/names"
)

func bucket(ctx context.Context, in *infra.Infra, ns, key string) (jetstream.KeyValue, string, error) {
	if err := names.CheckToken("store namespace", ns); err != nil {
		return nil, "", fmt.Errorf("%w: %w", apierr.ErrInvalidArgument, err)
	}

	if key != "" {
		if err := names.CheckToken("store key", key); err != nil {
			return nil, "", fmt.Errorf("%w: %w", apierr.ErrInvalidArgument, err)
		}
	}

	kv, err := in.KV(ctx, in.Names.BucketStore)

	return kv, ns + "." + key, err
}

// encode encodes v as JSON (nil = empty value).
func encode(v any) ([]byte, error) {
	switch x := v.(type) {
	case nil:
		return nil, nil
	case json.RawMessage:
		if !json.Valid(x) {
			return nil, fmt.Errorf("%w: invalid JSON", apierr.ErrInvalidArgument)
		}

		return x, nil
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", apierr.ErrInvalidArgument, err)
		}

		return b, nil
	}
}

// Put stores value (any JSON-encodable value) under ns/key.
func Put(ctx context.Context, in *infra.Infra, ns, key string, value any) error {
	kv, k, err := bucket(ctx, in, ns, key)
	if err != nil {
		return err
	}

	b, err := encode(value)
	if err != nil {
		return err
	}

	_, err = kv.Put(ctx, k, b)

	return err
}

// Get returns the value under ns/key, apierr.ErrNotFound when there is none.
func Get(ctx context.Context, in *infra.Infra, ns, key string) (json.RawMessage, error) {
	kv, k, err := bucket(ctx, in, ns, key)
	if err != nil {
		return nil, err
	}

	e, err := kv.Get(ctx, k)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, fmt.Errorf("%w: %s/%s", apierr.ErrNotFound, ns, key)
	}

	if err != nil {
		return nil, err
	}

	return e.Value(), nil
}

// Delete removes ns/key.
func Delete(ctx context.Context, in *infra.Infra, ns, key string) error {
	kv, k, err := bucket(ctx, in, ns, key)
	if err != nil {
		return err
	}

	return kv.Delete(ctx, k)
}

// Keys lists the keys of a namespace, sorted.
func Keys(ctx context.Context, in *infra.Infra, ns string) ([]string, error) {
	kv, _, err := bucket(ctx, in, ns, "")
	if err != nil {
		return nil, err
	}

	l, err := kv.ListKeysFiltered(ctx, ns+".*")
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return nil, nil
		}

		return nil, err
	}

	var out []string
	for k := range l.Keys() {
		out = append(out, strings.TrimPrefix(k, ns+"."))
	}

	slices.Sort(out)

	return out, nil
}
