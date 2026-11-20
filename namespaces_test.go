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
	"slices"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/packtrailtest"
)

func TestNamespaces(t *testing.T) {
	s := packtrailtest.Start(t)
	nc := s.Connect(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	got, err := packtrail.Namespaces(ctx, nc)
	if err != nil || got == nil || len(got) != 0 {
		t.Fatalf("no engine: got %#v, %v; want empty non-nil list", got, err)
	}

	for _, ns := range []string{"beta", "alpha"} {
		eng, nerr := packtrail.New(s.Connect(t), packtrail.WithNamespace(ns))
		if nerr != nil {
			t.Fatal(nerr)
		}

		if nerr = eng.Init(ctx); nerr != nil {
			t.Fatal(nerr)
		}
	}

	// A stray events stream without its cmd stream is not a namespace.
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}

	if _, err = js.CreateStream(ctx, jetstream.StreamConfig{Name: "gamma-events", Subjects: []string{"gamma.ev.>"}}); err != nil {
		t.Fatal(err)
	}

	got, err = packtrail.Namespaces(ctx, nc)
	if want := []string{"alpha", "beta"}; err != nil || !slices.Equal(got, want) {
		t.Fatalf("got %v, %v; want %v", got, err, want)
	}
}
