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
	"testing"
	"time"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/internal/natstest"
)

// TestShutdownWithMissingTriggerStream: a clean shutdown while a
// trigger's stream does not exist yet (I-99) should not make Run fail.
func TestShutdownWithMissingTriggerStream(t *testing.T) {
	s := natstest.Start(t)

	eng, err := packtrail.New(s.Connect(t), packtrail.WithFlowYAML([]byte(trOrder)), packtrail.WithPartitions(1))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() { done <- eng.Run(ctx) }()

	time.Sleep(1500 * time.Millisecond)
	cancel()

	if err = <-done; err != nil {
		t.Fatalf("Run after a clean shutdown: %v", err)
	}
}
