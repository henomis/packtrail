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
	"testing"

	"github.com/henomis/packtrail/worker"
)

func TestJobLoggerWithoutWorker(t *testing.T) {
	// A Job built by hand (a handler unit test) still has a logger.
	j := &worker.Job{ExecID: "e", Node: "n"}

	l := j.Logger()
	if l == nil || j.Logger() != l {
		t.Fatal("Logger must be non-nil and stable")
	}
}
