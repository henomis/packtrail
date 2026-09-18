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

package packtrail

import (
	"context"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail/internal/names"
)

// Exists reports whether a packtrail deployment has ever been provisioned for
// namespace, without provisioning anything itself.
//
// It answers the question a read-only client actually has, which [Server]
// cannot: provisioning is lazy and implicit, so the first operation on a Server
// — including a read like [Server.List] — creates the whole resource set for
// whatever namespace it was handed. A client command against a mistyped
// namespace therefore answered "no executions", indistinguishable from a healthy
// but idle deployment, and left a full set of durable buckets, streams and
// consumers behind for the typo. Scripted, that consumes a shared JetStream
// account's stream limits with garbage nobody knows to delete.
//
// Call it before a read-only operation on a namespace that came from a human:
//
//	ok, err := packtrail.Exists(ctx, nc, ns)
//	if err != nil {
//		return err
//	}
//	if !ok {
//		return fmt.Errorf("no packtrail deployment for namespace %q", ns)
//	}
//
// False means "nothing has ever run here", not "the deployment is down": a
// running engine that has executed nothing still reports true, because it
// provisions at startup. An empty namespace means the default namespace, exactly
// as in [WithNamespace]. An invalid namespace is an error rather than a false —
// it is a caller mistake, not an answer about the cluster.
func Exists(ctx context.Context, nc *nats.Conn, namespace string) (bool, error) {
	if err := ValidateNamespace(namespace); err != nil {
		return false, err
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return false, fmt.Errorf("packtrail: jetstream: %w", err)
	}

	// The executions bucket is the cheapest proof: it is created by Init before
	// anything can run, and it is the one resource no deployment can lack.
	// Looking it up neither creates nor updates it.
	if _, err = js.KeyValue(ctx, names.New(namespace).BucketExecutions); err != nil {
		if errors.Is(err, jetstream.ErrBucketNotFound) {
			return false, nil
		}

		return false, fmt.Errorf("packtrail: looking up namespace %q: %w", namespace, err)
	}

	return true, nil
}

// ValidateNamespace reports whether namespace is usable as a [WithNamespace]
// prefix: it must match [A-Za-z0-9_-]{1,64}, since it becomes a segment of every
// bucket, stream, consumer and subject name this package derives. An empty
// namespace is valid and means the default namespace, exactly as in
// [WithNamespace]. The returned error wraps [ErrInvalidArgument].
//
// [New] and [Exists] apply it themselves, so a caller that only builds a Server
// never needs it. It is exported for a layer that takes a namespace from its own
// configuration — where three things make an early check worth having: the rule
// is enforced in more than one place here, a violation that slips past
// construction reaches internal name derivation which *panics* rather than
// returns, and such a layer usually wants to reject the value while it can still
// name the setting the author wrote.
//
// A caller that composes a namespace from several parts (a deployment name and a
// session, say) should validate the composed string, not the parts: two
// individually-legal values can exceed the length bound once joined.
func ValidateNamespace(namespace string) error {
	if namespace != "" && !resourceTokenPattern.MatchString(namespace) {
		return fmt.Errorf("%w: invalid namespace %q: must match [A-Za-z0-9_-]{1,64}",
			ErrInvalidArgument, namespace)
	}

	return nil
}
