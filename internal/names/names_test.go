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

package names

import "testing"

func TestNewEmptyPrefixFallsBackToDefault(t *testing.T) {
	if New("") != New(Default) {
		t.Fatal("empty prefix must fall back to the default")
	}
}

func TestNewPanicsOnInvalidPrefix(t *testing.T) {
	for _, p := range []string{"a.b", "a*", "a>", "a b", string(make([]byte, 65))} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("New(%q) did not panic", p)
				}
			}()

			New(p)
		}()
	}
}

func TestValidToken(t *testing.T) {
	good := []string{"a", "A-b_9", "0123456789"}
	bad := []string{"", "a.b", "*", ">", "a b", "a\tb", "ä"}

	for _, s := range good {
		if !ValidToken(s) {
			t.Errorf("ValidToken(%q) = false", s)
		}
	}

	for _, s := range bad {
		if ValidToken(s) {
			t.Errorf("ValidToken(%q) = true", s)
		}

		if CheckToken("x", s) == nil {
			t.Errorf("CheckToken(%q) = nil", s)
		}
	}
}

func TestPartitionStableAndBounded(t *testing.T) {
	// Pinned values: SDKs in other languages must compute the same mapping.
	cases := map[string]int{"exec-1": Partition("exec-1", 16), "abc": Partition("abc", 16)}
	for id, want := range cases {
		for range 3 {
			if got := Partition(id, 16); got != want {
				t.Fatalf("Partition(%q) unstable", id)
			}
		}
	}

	if got := Partition("abc", 16); got != 0x1a47e90b%16 {
		t.Fatalf("Partition(abc,16) = %d, want FNV-1a based %d", got, 0x1a47e90b%16)
	}

	for i := range 1000 {
		p := Partition(string(rune('a'+i%26))+string(rune(i)), 7)
		if p < 0 || p >= 7 {
			t.Fatalf("partition %d out of range", p)
		}
	}

	if Partition("x", 1) != 0 || Partition("x", 0) != 0 {
		t.Fatal("single partition must map to 0")
	}
}

func TestSubjects(t *testing.T) {
	n := New("ns")

	checks := map[string]string{
		n.EventSubject(3, "e1"):      "ns.ev.3.e1",
		n.CmdSubject(3, "e1"):        "ns.cmd.3.e1",
		n.TimerSubject("e1", "t1"):   "ns.timer.e1.t1",
		n.WorkSubject("k"):           "ns.work.k",
		n.ScheduleSubject("nightly"): "ns.sched.nightly",
		n.CronSubject("nightly"):     "ns.cron.nightly",
		n.DLQSubject("cmd", "e1"):    "ns.dlq.cmd.e1",
		n.EventPartitionFilter(2):    "ns.ev.2.*",
		n.CmdPartitionFilter(2):      "ns.cmd.2.*",
		n.DurEngine(1):               "ns-engine-1",
		n.DurWorker("k"):             "ns-work-k",
		n.StreamEvents:               "ns-events",
		n.ObjectBlobs:                "ns-blobs",
	}
	for got, want := range checks {
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	}
}
