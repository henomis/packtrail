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

package acceptance

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/internal/natstest"
)

// TestAttachDoesNotRaceWait: goroutine A Waits on a client whose namespace
// is not provisioned yet (ErrNotProvisioned); later, goroutine B's first call
// after Init attaches the client. Under -race, Wait's look at the attached
// infra must not race attach's write.
func TestAttachDoesNotRaceWait(t *testing.T) {
	s := natstest.Start(t)

	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
	defer cancel()

	c, err := packtrail.NewClient(s.Connect(t))
	if err != nil {
		t.Fatal(err)
	}

	var (
		a, b    sync.WaitGroup
		release = make(chan struct{})
	)

	// A: a Wait made during start-up, before any engine provisioned. A stays
	// alive afterwards (it is a long-lived goroutine of the application).
	a.Go(func() {
		if _, werr := c.Wait(ctx, "x"); !errors.Is(werr, packtrail.ErrNotProvisioned) {
			t.Errorf("wait before provisioning: %v", werr)
		}

		<-release
	})

	time.Sleep(300 * time.Millisecond) // A's Wait has returned; no synchronisation with it

	eng, err := packtrail.New(s.Connect(t), packtrail.WithFlowYAML([]byte(wtOne)), packtrail.WithPartitions(1))
	if err != nil {
		t.Fatal(err)
	}

	if err = eng.Init(ctx); err != nil {
		t.Fatal(err)
	}

	// B: the next call attaches.
	b.Go(func() {
		if _, ferr := c.Flows(ctx); ferr != nil {
			t.Errorf("flows: %v", ferr)
		}
	})

	b.Wait()
	close(release)
	a.Wait()
}
