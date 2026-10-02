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

package dispatch

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail/internal/fold"
	"github.com/henomis/packtrail/internal/infra"
	"github.com/henomis/packtrail/internal/metrics"
	"github.com/henomis/packtrail/internal/names"
	"github.com/henomis/packtrail/internal/natstest"
	"github.com/henomis/packtrail/internal/projection"
	"github.com/henomis/packtrail/internal/registry"
	"github.com/henomis/packtrail/internal/statecache"
)

func newDispatcher(t *testing.T) (*Dispatcher, context.Context) {
	t.Helper()

	s := natstest.Start(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	in, err := infra.New(s.NC, names.New(""), nil)
	if err != nil {
		t.Fatal(err)
	}

	if err = in.Provision(ctx, 1); err != nil {
		t.Fatal(err)
	}

	return &Dispatcher{In: in, Cache: statecache.New(0), Metrics: &metrics.M{}}, ctx
}

// TestMissedReleaseDoesNotKeepSkipping: the in-memory quarantine set is only
// a cache. A dispatcher that missed the release broadcast (core NATS has no
// delivery guarantee) must notice from the index and stop skipping (F3-01).
func TestMissedReleaseDoesNotKeepSkipping(t *testing.T) {
	d, ctx := newDispatcher(t)

	d.markQuarantined("released") // the release message never arrived

	if skip, err := d.skip(ctx, "released"); err != nil || skip {
		t.Fatalf("skip = %v, %v: a released execution is still skipped", skip, err)
	}

	if d.isQuarantined("released") {
		t.Fatal("stale in-memory mark kept")
	}

	if err := projection.Quarantine(ctx, d.In, "poisoned", 7, "bad"); err != nil {
		t.Fatal(err)
	}

	d.markQuarantined("poisoned")

	if skip, err := d.skip(ctx, "poisoned"); err != nil || !skip {
		t.Fatalf("skip = %v, %v: a quarantined execution is processed", skip, err)
	}

	if skip, _ := d.skip(ctx, "healthy"); skip {
		t.Fatal("a healthy execution is skipped")
	}
}

// TestNotFoundIsDeterministicOnlyWhenPersistent: on a replicated deployment a
// replica can serve a read before it caught up, so "not found" quarantines
// only after it persisted over a few deliveries (R3 note).
func TestNotFoundIsDeterministicOnlyWhenPersistent(t *testing.T) {
	for _, err := range []error{
		fmt.Errorf("x: %w", jetstream.ErrObjectNotFound), fmt.Errorf("x: %w", registry.ErrUnknownFlow),
	} {
		if quarantinable(err, 1) || quarantinable(err, notFoundGrace-1) {
			t.Errorf("%v quarantined on an early delivery", err)
		}

		if !quarantinable(err, notFoundGrace) {
			t.Errorf("%v never quarantined", err)
		}
	}

	if quarantinable(errors.New("plain"), 99) || !quarantinable(fmt.Errorf("x: %w", errPoisonTest), 1) {
		t.Fatal("quarantinable must follow Deterministic for other errors")
	}
}

var errPoisonTest = fmt.Errorf("apply: %w", fold.ErrApply)
