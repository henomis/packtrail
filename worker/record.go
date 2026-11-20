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
	"fmt"
	"sync"
	"unicode/utf8"
)

// minMaxRecord is the smallest record cap: room for the truncation marker.
const minMaxRecord = 128

// recordSink holds the record of one attempt.
type recordSink struct {
	mu  sync.Mutex
	max int
	raw json.RawMessage
}

// SetRecord records what this attempt did (the request it sent, the query it
// ran, the prompt and responses of a model call), stored with the attempt's
// outcome: its completion, its failure or its interrupt, also when the
// handler panics. v must encode as JSON; it is encoded now, and an
// encoding error is returned without failing the attempt. A later call
// replaces an earlier one. Nothing is recorded unless it is called.
//
// A record larger than WithMaxRecordBytes is replaced by a truncation
// marker (see WithMaxRecordBytes). Records are diagnostic: the engine never
// reads them, and they are kept in NATS with the execution's events, for its
// retention.
func (j *Job) SetRecord(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("worker: record: %w", err)
	}

	if limit := j.record.max; limit > 0 && len(b) > limit {
		j.Logger().Warn("worker: record truncated", "size", len(b), "max", limit)

		b = truncateRecord(b, limit)
	}

	j.record.mu.Lock()
	j.record.raw = b
	j.record.mu.Unlock()

	return nil
}

// recorded returns the record set on the job, nil when none.
func (j *Job) recorded() json.RawMessage {
	if j == nil {
		return nil
	}

	j.record.mu.Lock()
	defer j.record.mu.Unlock()

	return j.record.raw
}

// RecordTruncated is the shape of a record that exceeded the cap: the first
// bytes of its JSON encoding as a string, and its full encoded size.
type RecordTruncated struct {
	Truncated bool   `json:"truncated"`
	Size      int    `json:"size"`
	Head      string `json:"head"`
}

// truncateRecord replaces the encoded record b (longer than limit) with a
// RecordTruncated whose encoding fits in limit: the longest head that does
// (escaping can make a head longer than its bytes), cut on a rune boundary.
func truncateRecord(b []byte, limit int) json.RawMessage {
	marker := func(n int) json.RawMessage {
		head := b[:n]
		for len(head) > 0 && !utf8.Valid(head) {
			head = head[:len(head)-1]
		}

		tr := RecordTruncated{Truncated: true, Size: len(b), Head: string(head)}
		out, _ := json.Marshal(tr) //nolint:errchkjson // a bool, an int and a valid string always encode.

		return out
	}

	// The largest n in [0, limit] whose marker fits: markers grow with n.
	lo, hi := 0, min(limit, len(b))
	for lo < hi {
		if mid := (lo + hi + 1) / 2; len(marker(mid)) <= limit { //nolint:mnd // midpoint.
			lo = mid
		} else {
			hi = mid - 1
		}
	}

	return marker(lo)
}

// WithMaxRecordBytes caps the encoded size of a job's record (Job.SetRecord).
// A larger record is not refused and does not fail the attempt: it is stored
// as {"truncated": true, "size": <encoded size>, "head": "<the first bytes of
// its JSON encoding>"}, at most n bytes in all (a cap under 128 bytes is
// raised to 128, room for the marker). Zero, the default, sets no cap: a
// record is bounded like an output, and above the claim-check threshold it is
// stored as a blob.
func WithMaxRecordBytes(n int) Option {
	return func(w *Worker) error {
		if n < 0 {
			return fmt.Errorf("worker: max record bytes must not be negative: %d", n)
		}

		if n > 0 {
			n = max(n, minMaxRecord)
		}

		w.maxRecord = n

		return nil
	}
}
