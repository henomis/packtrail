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

package signal

import (
	"context"
	"testing"

	"github.com/henomis/packtrail/internal/names"
)

// TestPublishValidatesTokens: the execution id and signal name become subject
// tokens; unsafe values are rejected before anything reaches NATS (so a nil
// JetStream context is fine here — validation short-circuits).
func TestPublishValidatesTokens(t *testing.T) {
	s := New(nil, names.New(""))
	ctx := context.Background()

	for _, tc := range []struct{ execID, name string }{
		{"has space", "go"},
		{"dotted.id", "go"},
		{"exec-1", "bad name"},
		{"exec-1", "wild*"},
		{"exec-1", "dotted.name"},
		{"", "go"},
		{"exec-1", ""},
	} {
		if err := s.Publish(ctx, tc.execID, tc.name, nil); err == nil {
			t.Errorf("Publish(%q, %q) = nil error, want rejection", tc.execID, tc.name)
		}
	}
}
