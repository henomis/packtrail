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

	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail/internal/names"
)

// diagnose describes why id may be stuck: the end of its log, its open
// work, and the streams and consumers its work goes through.
func (cl *cluster) diagnose(id string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var b strings.Builder

	n := names.New(names.Default)
	p := names.Partition(id, partitions)

	fmt.Fprintf(&b, "\n=== diagnosis of %s (partition %d)\n", id, p)

	if st, err := cl.c.Get(ctx, id); err == nil {
		fmt.Fprintf(&b, "status %s steps %d last %s updated %s lastSeq %d\n", st.Status, st.Steps, st.LastNode,
			st.Updated.Format(time.RFC3339Nano), st.LastSeq)

		for k, task := range st.Tasks {
			tj, _ := json.Marshal(task) //nolint:errchkjson // diagnostics.
			fmt.Fprintf(&b, "open task %s: %s\n", k, tj)
		}

		for k, tm := range st.Timers {
			tj, _ := json.Marshal(tm) //nolint:errchkjson // diagnostics.
			fmt.Fprintf(&b, "timer %s: %s\n", k, tj)
		}
	}

	if evs, err := cl.c.History(ctx, id); err == nil {
		from := max(0, len(evs)-12)
		for _, ev := range evs[from:] {
			d, _ := json.Marshal(ev.Data) //nolint:errchkjson // diagnostics.
			fmt.Fprintf(&b, "  #%d seq %d %s %s %.200s\n", ev.Index, ev.Seq, ev.Time.Format("15:04:05.000"), ev.Type, d)
		}
	}

	consumer := func(stream, durable string) {
		c, err := cl.s.JS.Consumer(ctx, stream, durable)
		if err != nil {
			fmt.Fprintf(&b, "consumer %s/%s: %v\n", stream, durable, err)

			return
		}

		i, err := c.Info(ctx)
		if err != nil {
			fmt.Fprintf(&b, "consumer %s/%s info: %v\n", stream, durable, err)

			return
		}

		fmt.Fprintf(&b, "consumer %s: pending %d ack-pending %d redelivered %d waiting %d delivered %d/%d ackfloor %d/%d\n",
			durable, i.NumPending, i.NumAckPending, i.NumRedelivered, i.NumWaiting,
			i.Delivered.Consumer, i.Delivered.Stream, i.AckFloor.Consumer, i.AckFloor.Stream)
	}

	consumer(n.StreamCmd, n.DurEngine(p))
	consumer(n.StreamEvents, n.DurDispatch(p))

	if ws, err := cl.s.JS.Stream(ctx, n.StreamWork); err == nil {
		lister := ws.ListConsumers(ctx)
		for ci := range lister.Info() {
			consumer(n.StreamWork, ci.Name)
		}

		if info, ierr := ws.Info(ctx, jetstream.WithSubjectFilter(n.Prefix+".work.>")); ierr == nil {
			fmt.Fprintf(&b, "work stream: msgs %d subjects %v\n", info.State.Msgs, info.State.Subjects)
		}
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
