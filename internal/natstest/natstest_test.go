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

package natstest

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func TestRestartKeepsJetStreamState(t *testing.T) {
	s := Start(t)
	ctx := context.Background()

	if _, err := s.JS.CreateStream(ctx, jetstream.StreamConfig{Name: "S", Subjects: []string{"s.>"}}); err != nil {
		t.Fatal(err)
	}

	if _, err := s.JS.Publish(ctx, "s.a", []byte("x")); err != nil {
		t.Fatal(err)
	}

	s.Restart(t)

	deadline := time.Now().Add(5 * time.Second)

	for {
		st, err := s.JS.Stream(ctx, "S")
		if err == nil {
			info, lerr := st.Info(ctx)
			if lerr == nil && info.State.Msgs == 1 {
				return
			}
		}

		if time.Now().After(deadline) {
			t.Fatalf("stream not recovered after restart: %v", err)
		}

		time.Sleep(50 * time.Millisecond)
	}
}
