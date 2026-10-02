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

package worker_test

import (
	"context"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go"

	"github.com/henomis/packtrail/worker"
)

func ExampleNew() {
	var nc *nats.Conn // a connected NATS client

	handler := func(_ context.Context, j *worker.Job) (*worker.Result, error) {
		var in struct{ Text string }
		if err := j.Input(&in); err != nil {
			return nil, worker.Permanent(err) // bad input: do not retry
		}

		if in.Text == "" {
			return nil, errors.New("empty text") // retried by the node's policy
		}

		return &worker.Result{
			Output: map[string]any{"length": len(in.Text)},
			Writes: map[string]any{"seen": 1},
		}, nil
	}

	_, err := worker.New(nc, "measure", handler, worker.WithConcurrency(8))
	fmt.Println(err)
	// Output: worker: nil connection or handler
}
