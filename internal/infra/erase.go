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

package infra

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
)

// A KV delete or purge, like an object delete, leaves a marker message
// behind. That is right for values that come and go, but an execution that
// is deleted must leave nothing: with one marker per key and per object,
// storage would still grow with every execution that ever ran. EraseKey and
// EraseObject purge the subject itself in the stream that backs the bucket,
// markers included.

// EraseKey removes a key of a KV bucket without leaving a delete marker.
// Erasing a missing key is a no-op.
func (i *Infra) EraseKey(ctx context.Context, bucket, key string) error {
	s, err := i.JS.Stream(ctx, "KV_"+bucket)
	if err != nil {
		return fmt.Errorf("infra: bucket %s: %w", bucket, err)
	}

	if err = s.Purge(ctx, jetstream.WithPurgeSubject("$KV."+bucket+"."+key)); err != nil {
		return fmt.Errorf("infra: erase %s in bucket %s: %w", key, bucket, err)
	}

	return nil
}

// EraseObject removes an object of an object store without leaving a delete
// marker. Erasing a missing object is a no-op.
func (i *Infra) EraseObject(ctx context.Context, bucket, name string) error {
	obs, err := i.Object(ctx, bucket)
	if err != nil {
		return err
	}

	// Delete drops the chunks and rewrites the object's info as a marker...
	if err = obs.Delete(ctx, name); err != nil && !errors.Is(err, jetstream.ErrObjectNotFound) {
		return fmt.Errorf("infra: delete object %s in %s: %w", name, bucket, err)
	}

	s, err := i.JS.Stream(ctx, "OBJ_"+bucket)
	if err != nil {
		return fmt.Errorf("infra: object store %s: %w", bucket, err)
	}

	// ... which is then purged too.
	meta := "$O." + bucket + ".M." + base64.URLEncoding.EncodeToString([]byte(name))
	if err = s.Purge(ctx, jetstream.WithPurgeSubject(meta)); err != nil {
		return fmt.Errorf("infra: erase object %s in %s: %w", name, bucket, err)
	}

	return nil
}
