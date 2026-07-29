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

package packtrail_test

import (
	"context"
	"errors"
	"testing"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/internal/natstest"
)

// TestExistsDoesNotProvision is the whole point of Exists: probing a namespace
// that was never deployed must leave the cluster exactly as it found it.
//
// Asking the same question through a Server does not — provisioning is lazy and
// implicit, so even a read creates the full resource set for whatever namespace
// it was handed, and a typo'd namespace both answered "idle" and littered the
// account with durable resources nobody would know to delete.
func TestExistsDoesNotProvision(t *testing.T) {
	srv := natstest.Start(t)
	ctx := context.Background()

	ok, err := packtrail.Exists(ctx, srv.NC, "never-deployed")
	if err != nil {
		t.Fatalf("exists: %v", err)
	}

	if ok {
		t.Fatal("Exists reported a namespace that was never deployed")
	}

	js, err := jetstream.New(srv.NC)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}

	for name := range js.KeyValueStoreNames(ctx).Name() {
		t.Errorf("probing created KV bucket %q; Exists must not provision", name)
	}
}

// TestExistsAfterInit: a deployment that has been provisioned reports true, even
// with no executions — "nothing has run here" and "the fleet is idle" are
// different answers and only the first is what Exists reports.
func TestExistsAfterInit(t *testing.T) {
	srv := natstest.Start(t)
	ctx := context.Background()

	s, err := packtrail.New(srv.NC,
		packtrail.WithNamespace("deployed"),
		packtrail.WithFlow([]byte(natsTaskFlow)),
	)
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	if initErr := s.Init(ctx); initErr != nil {
		t.Fatalf("init: %v", initErr)
	}

	ok, err := packtrail.Exists(ctx, srv.NC, "deployed")
	if err != nil {
		t.Fatalf("exists: %v", err)
	}

	if !ok {
		t.Fatal("Exists reported false for a provisioned namespace")
	}

	// A neighbouring namespace on the same cluster is still absent: the probe
	// must not be satisfied by any deployment, only by this one.
	if ok, err = packtrail.Exists(ctx, srv.NC, "deployed-2"); err != nil || ok {
		t.Fatalf("Exists(deployed-2) = (%v, %v), want (false, nil)", ok, err)
	}
}

// TestExistsRejectsInvalidNamespace: a namespace that could never have been
// deployed is a caller mistake, and answering "false" would present it as a fact
// about the cluster. It also keeps Exists from panicking in names.New.
func TestExistsRejectsInvalidNamespace(t *testing.T) {
	srv := natstest.Start(t)

	_, err := packtrail.Exists(context.Background(), srv.NC, "not a namespace")
	if !errors.Is(err, packtrail.ErrInvalidArgument) {
		t.Fatalf("Exists(invalid) error = %v, want ErrInvalidArgument", err)
	}
}

// TestFlowSchemaVersionMatchesParser guards the constant a flow builder stamps
// onto its FlowDefs against the version the parser actually accepts.
func TestFlowSchemaVersionMatchesParser(t *testing.T) {
	def := packtrail.FlowDef{
		Version: packtrail.FlowSchemaVersion,
		Name:    "v",
		Nodes:   []packtrail.NodeDef{{ID: "a", Type: "task", Subject: "tasks.a"}},
	}

	if err := packtrail.ValidateFlowDef(def); err != nil {
		t.Fatalf("FlowSchemaVersion %q is not what the parser accepts: %v", packtrail.FlowSchemaVersion, err)
	}
}
