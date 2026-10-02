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

package snapshot

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/henomis/packtrail/internal/fold"
	"github.com/henomis/packtrail/internal/infra"
	"github.com/henomis/packtrail/internal/names"
	"github.com/henomis/packtrail/internal/natstest"
)

func TestLargeSnapshotGoesToObjectStore(t *testing.T) {
	s := natstest.Start(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	in, err := infra.New(s.NC, names.New(""), nil)
	if err != nil {
		t.Fatal(err)
	}

	if err = in.Provision(ctx, 1); err != nil {
		t.Fatal(err)
	}

	in.BlobThreshold = 1024

	store := New(in, 0)

	for _, size := range []int{10, 100_000} {
		st := fold.New("e1")
		st.Input = json.RawMessage(`"` + strings.Repeat("x", size) + `"`)
		st.LastSeq = 42

		if err = store.Save(ctx, st); err != nil {
			t.Fatal(err)
		}

		got, lerr := store.Load(ctx, "e1")
		if lerr != nil || got == nil || got.LastSeq != 42 || len(got.Input) != size+2 {
			t.Fatalf("size %d: %+v %v", size, got, lerr)
		}
	}
}
