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

package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// diagnose describes why id may be stuck, through the public API: its open
// work, the end of its log, what the engines report about dispatching, and
// any dead letter or quarantine.
func (cl *cluster) diagnose(id string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var b strings.Builder

	fmt.Fprintf(&b, "\n=== diagnosis of %s\n", id)

	if st, err := cl.c.Get(ctx, id); err == nil {
		fmt.Fprintf(&b, "status %s steps %d last %s updated %s lastSeq %d\n", st.Status, st.Steps, st.LastNode,
			st.Updated.Format(time.RFC3339Nano), st.LastSeq)

		for name, open := range map[string]any{
			"tasks": st.Tasks, "timers": st.Timers, "awaits": st.Awaits,
			"fans": st.Fans, "maps": st.Maps, "children": st.Children,
		} {
			if j, _ := json.Marshal(open); string(j) != "null" && string(j) != "{}" { //nolint:errchkjson // diagnostics.
				fmt.Fprintf(&b, "open %s: %s\n", name, j)
			}
		}
	} else {
		fmt.Fprintf(&b, "get: %v\n", err)
	}

	if evs, err := cl.c.History(ctx, id); err == nil {
		from := max(0, len(evs)-12)
		for _, ev := range evs[from:] {
			d, _ := json.Marshal(ev.Data) //nolint:errchkjson // diagnostics.
			fmt.Fprintf(&b, "  #%d seq %d %s %s %.200s\n", ev.Index, ev.Seq, ev.Time.Format("15:04:05.000"), ev.Type, d)
		}
	}

	for i, eng := range cl.liveEngines() {
		m := eng.Metrics(ctx)
		fmt.Fprintf(&b, "engine %d: commands %d conflicts %d dead letters %d dispatched %d projection lag %d stall %s %v\n",
			i, m.Commands, m.Conflicts, m.DeadLetters, m.JobsDispatched, m.ProjectionLag, m.DispatchStall, m.DispatchStalls)
	}

	if dl, err := cl.c.DeadLetters(ctx, 20); err == nil {
		for _, d := range dl {
			fmt.Fprintf(&b, "dead letter: %+v\n", d)
		}
	}

	if q, err := cl.c.Quarantined(ctx); err == nil && len(q) > 0 {
		fmt.Fprintf(&b, "quarantined: %v\n", q)
	}

	return b.String()
}
