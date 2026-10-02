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
	"testing"
	"time"

	"github.com/henomis/packtrail"
	"github.com/henomis/packtrail/internal/natstest"
	"github.com/henomis/packtrail/worker"
)

const linearFlow = `
name: lin
nodes:
  - {id: a, type: task, kind: upper, next: b}
  - {id: b, type: task, kind: upper}
`

func TestEndToEndLinear(t *testing.T) {
	s := natstest.Start(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	eng, err := packtrail.New(s.NC, packtrail.WithFlowYAML([]byte(linearFlow)), packtrail.WithPartitions(2))
	if err != nil {
		t.Fatal(err)
	}

	if err = eng.Init(ctx); err != nil {
		t.Fatal(err)
	}

	runCtx, stop := context.WithCancel(ctx)
	defer stop()

	go func() { _ = eng.Run(runCtx) }()

	w, err := worker.New(s.Connect(t), "upper", func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		return &worker.Result{Output: map[string]any{"node": j.Node}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	go func() { _ = w.Run(runCtx) }()

	id, err := eng.Client().Start(ctx, "lin", map[string]any{"x": 1})
	if err != nil {
		t.Fatal(err)
	}

	st, err := eng.Client().Wait(ctx, id)
	if err != nil {
		t.Fatal(err)
	}

	if st.Status != packtrail.StatusCompleted || string(st.Output) != `{"node":"b"}` {
		t.Fatalf("state = %s %s %s", st.Status, st.Output, st.Error)
	}
}
