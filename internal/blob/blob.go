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

// Package blob implements the claim-check: a message body larger than the
// threshold is stored in the blobs object store and the message carries only
// its name in the Pt-Blob header. It applies uniformly to events, commands and
// jobs, so every reader resolves bodies the same way.
package blob

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"

	"github.com/henomis/packtrail/internal/infra"
)

// Header names the object holding the message body.
const Header = "Pt-Blob"

// Offload returns the body to publish: body itself when it fits under the
// threshold, otherwise an empty body after storing body under
// "<execID>/<unique>" and setting the header on h.
func Offload(ctx context.Context, in *infra.Infra, execID string, body []byte, h nats.Header) ([]byte, error) {
	if len(body) <= in.BlobThreshold {
		return body, nil
	}

	obs, err := in.Object(ctx, in.Names.ObjectBlobs)
	if err != nil {
		return nil, err
	}

	name := execID + "/" + nuid.Next()

	ctx, cancel := transferContext(ctx, in.BlobTimeout)
	defer cancel()

	if _, err = obs.PutBytes(ctx, name, body); err != nil {
		return nil, fmt.Errorf("blob: put %s: %w", name, err)
	}

	h.Set(Header, name)

	return []byte{}, nil
}

// Resolve returns the real body of a message: body itself, or the object named
// by the header.
func Resolve(ctx context.Context, in *infra.Infra, body []byte, h nats.Header) ([]byte, error) {
	name := h.Get(Header)
	if name == "" {
		return body, nil
	}

	obs, err := in.Object(ctx, in.Names.ObjectBlobs)
	if err != nil {
		return nil, err
	}

	ctx, cancel := transferContext(ctx, in.BlobTimeout)
	defer cancel()

	b, err := obs.GetBytes(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("blob: get %s: %w", name, err)
	}

	return b, nil
}

// DeleteExec removes every blob of an execution (archive garbage collection).
func DeleteExec(ctx context.Context, in *infra.Infra, execID string) error {
	obs, err := in.Object(ctx, in.Names.ObjectBlobs)
	if err != nil {
		return err
	}

	list, err := obs.List(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoObjectsFound) {
			return nil
		}

		return fmt.Errorf("blob: list: %w", err)
	}

	var errs []error

	for _, o := range list {
		if strings.HasPrefix(o.Name, execID+"/") {
			if err = obs.Delete(ctx, o.Name); err != nil {
				errs = append(errs, err)
			}
		}
	}

	return errors.Join(errs...)
}

// EraseExec removes every blob of an execution that is being deleted,
// leaving no delete markers behind.
func EraseExec(ctx context.Context, in *infra.Infra, execID string) error {
	obs, err := in.Object(ctx, in.Names.ObjectBlobs)
	if err != nil {
		return err
	}

	list, err := obs.List(ctx, jetstream.ListObjectsShowDeleted())
	if err != nil {
		if errors.Is(err, jetstream.ErrNoObjectsFound) {
			return nil
		}

		return fmt.Errorf("blob: list: %w", err)
	}

	var errs []error

	for _, o := range list {
		if strings.HasPrefix(o.Name, execID+"/") {
			errs = append(errs, in.EraseObject(ctx, in.Names.ObjectBlobs, o.Name))
		}
	}

	return errors.Join(errs...)
}

// transferContext bounds one blob transfer by timeout unless the caller
// already set a deadline. Blobs can be large, so the default is generous.
func transferContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}

	if timeout <= 0 {
		timeout = infra.DefaultBlobTimeout
	}

	return context.WithTimeout(ctx, timeout)
}
