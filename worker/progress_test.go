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

package worker

import (
	"errors"
	"testing"
)

// TestProgressNeedsARunningJob: a Job built outside the worker (e.g. in a
// handler's unit test) refuses progress instead of panicking (G5-04).
func TestProgressNeedsARunningJob(t *testing.T) {
	if err := (&Job{}).Progress(1); !errors.Is(err, ErrNoProgress) {
		t.Fatalf("Progress on a bare Job: %v", err)
	}
}
