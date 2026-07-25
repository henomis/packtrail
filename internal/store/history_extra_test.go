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

package store

import (
	"context"
	"testing"
	"time"
)

// TestEnableHistoryNormalizesNegativeRetention is a regression test:
// EnableHistory used to pass a negative retention straight through to the
// stream's MaxAge, unlike Signals.SetRetention which normalizes negative to 0
// ("no MaxAge"). A negative retention must disable the age limit, not produce
// a negative MaxAge.
func TestEnableHistoryNormalizesNegativeRetention(t *testing.T) {
	ctx := context.Background()
	s := open(t)

	if err := s.EnableHistory(ctx, -time.Hour); err != nil {
		t.Fatalf("enable history: %v", err)
	}

	info, err := s.js.Stream(ctx, s.names.StreamHistory)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}

	cfg, err := info.Info(ctx)
	if err != nil {
		t.Fatalf("info: %v", err)
	}

	if cfg.Config.MaxAge != 0 {
		t.Fatalf("MaxAge = %v, want 0 (no age limit) for a negative retention", cfg.Config.MaxAge)
	}
}
