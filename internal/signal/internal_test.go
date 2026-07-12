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
	"time"

	"github.com/henomis/packtrail/internal/names"
	"github.com/henomis/packtrail/internal/natstest"
)

func TestParseSubject(t *testing.T) {
	s := New(nil, names.New(""))
	prefix := s.prefix

	cases := []struct {
		name       string
		subject    string
		wantExec   string
		wantSignal string
		wantOK     bool
	}{
		{"valid", prefix + "exec-1.approval", "exec-1", "approval", true},
		{"name with dots", prefix + "exec-1.a.b", "exec-1", "a.b", true},
		{"missing prefix", "other.exec.name", "", "", false},
		{"no dot separator", prefix + "execonly", "", "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			exec, sig, ok := s.parseSubject(c.subject)
			if ok != c.wantOK || exec != c.wantExec || sig != c.wantSignal {
				t.Fatalf("parseSubject(%q) = (%q, %q, %v), want (%q, %q, %v)",
					c.subject, exec, sig, ok, c.wantExec, c.wantSignal, c.wantOK)
			}
		})
	}
}

// TestConsumeBadSubjectTermed verifies a message whose subject does not parse is
// terminated (never handled), while a well-formed one that follows is delivered.
func TestConsumeBadSubjectTermed(t *testing.T) {
	ctx := context.Background()
	srv := natstest.Start(t)

	s := New(srv.JS, names.New(""))
	if err := s.EnsureStream(ctx); err != nil {
		t.Fatalf("ensure stream: %v", err)
	}

	ch := make(chan Delivery, 4)

	cc, err := s.Consume(ctx, "bad-subj", 10, nil, func(_ context.Context, d Delivery) error {
		ch <- d
		return nil
	})
	if err != nil {
		t.Fatalf("consume: %v", err)
	}

	t.Cleanup(cc.Stop)

	// A subject under the filter prefix but without the "exec.name" structure
	// must be Term'd and never reach the handler.
	if _, err = s.js.Publish(ctx, s.prefix+"nodotsubject", []byte("junk")); err != nil {
		t.Fatalf("publish bad: %v", err)
	}

	if err = s.Publish(ctx, "exec-ok", "go", []byte("1")); err != nil {
		t.Fatalf("publish good: %v", err)
	}

	select {
	case d := <-ch:
		if d.ExecID != "exec-ok" || d.Name != "go" {
			t.Fatalf("delivered %+v, want exec-ok/go (bad subject leaked through)", d)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("good signal not delivered")
	}
}
