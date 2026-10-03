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
	"sync"
	"testing"
	"time"

	"github.com/henomis/packtrail"
)

// TestWaitStartedDuringServerRestart: Waits begun while the server goes down
// — their log watch created just as it stops — still return once the
// executions end. A watch created without a deadline used to wait forever
// for a reply the old server never sent.
func TestWaitStartedDuringServerRestart(t *testing.T) {
	e := NewEnv(t, []string{wtAwait})

	ids := make([]string, 40)
	for i := range ids {
		ids[i] = e.Start("hold", nil)
	}

	for _, id := range ids {
		e.WaitStatus(id, packtrail.StatusWaiting)
	}

	// Waits start every 25ms over a second; the restart lands among them.
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		hung []string
	)

	for i, id := range ids {
		wg.Go(func() {
			time.Sleep(time.Duration(i) * 25 * time.Millisecond)

			ctx, cancel := context.WithTimeout(e.Ctx, 20*time.Second)
			defer cancel()

			if _, err := e.Client.Wait(ctx, id); err != nil {
				mu.Lock()

				hung = append(hung, id)
				mu.Unlock()
			}
		})
	}

	time.Sleep(400 * time.Millisecond)
	e.S.Restart(t)

	for _, id := range ids {
		if err := e.Client.Signal(e.Ctx, id, "go", nil); err != nil {
			t.Fatal(err)
		}
	}

	wg.Wait()

	if len(hung) > 0 {
		t.Fatalf("%d of %d Waits never returned: %v", len(hung), len(ids), hung)
	}
}
