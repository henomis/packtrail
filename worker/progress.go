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
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/henomis/packtrail/internal/wire"
)

// ErrNoProgress is returned by Job.Progress outside a running job.
var ErrNoProgress = errors.New("worker: progress needs a running job")

// progressSink publishes the progress messages of one job attempt.
type progressSink struct {
	nc      *nats.Conn
	subject string
	base    wire.Progress
	seq     atomic.Int64
}

// Progress publishes v (anything JSON-encodable) as an intermediate result of
// this job: partial output, a percentage, a log line. It is delivered to
// clients watching the execution's progress (Client.Progress) and never
// stored — the log only records the final result. Delivery is best effort,
// in order per job; a message larger than the server's max payload is
// refused.
func (j *Job) Progress(v any) error {
	if j.progress == nil {
		return ErrNoProgress
	}

	return j.progress.publish(v)
}

func (p *progressSink) publish(v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("worker: progress: %w", err)
	}

	msg := p.base
	msg.Seq = p.seq.Add(1)
	msg.Time = time.Now().UTC()
	msg.Data = data

	b, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("worker: progress: %w", err)
	}

	if err = p.nc.Publish(p.subject, b); err != nil {
		return fmt.Errorf("worker: progress: %w", err)
	}

	return nil
}

func (w *Worker) progressSink(wj wire.Job) *progressSink {
	return &progressSink{
		nc: w.nc, subject: w.in.Names.ProgressSubject(wj.ExecID, wj.Node),
		base: wire.Progress{
			ExecID: wj.ExecID, Node: wj.Node, Key: wj.Key, Generation: wj.Generation, Attempt: wj.Attempt,
		},
	}
}
