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

// Package names centralises every NATS resource name Packtrail uses — KV buckets,
// streams, subject prefixes and durable consumer names — derived from a single
// namespace prefix. The default prefix is "packtrail"; embedding applications can
// pick their own so multiple independent Packtrail deployments can share a NATS
// cluster without colliding.
package names

import (
	"fmt"
	"regexp"
)

// Default is the namespace prefix used when none is supplied.
const Default = "packtrail"

// prefixPattern bounds a namespace prefix, which becomes a segment of every
// bucket, stream, subject and durable name New builds. It mirrors
// packtrail.go's resourceTokenPattern; duplicated rather than shared, since
// this package deliberately has no dependency on the top-level one.
var prefixPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// Names holds every concrete resource name for one namespace.
type Names struct {
	Prefix string

	// KV buckets
	BucketExecutions  string
	BucketExecArchive string
	BucketLeases      string
	BucketIdxStatus   string
	BucketIdxFlow     string
	BucketResultCache string
	BucketFlows       string
	BucketPayloads    string

	// streams
	StreamEvents     string
	StreamWork       string
	StreamSignals    string
	StreamSchedule   string
	StreamDeadLetter string
	StreamHistory    string

	// subject prefixes (each followed by an execution id or routing token)
	SubjEventsPrefix     string
	SubjWorkPrefix       string
	SubjSignalPrefix     string
	SubjSchedPrefix      string
	SubjSchedFirePrefix  string
	SubjDeadLetterPrefix string
	SubjHistoryPrefix    string

	// durable consumer names
	DurEngine  string
	DurFired   string
	DurSignals string
	DurIndexer string
}

// New builds the resource names for prefix. An empty prefix falls back to
// Default ("packtrail").
//
// New panics if prefix is non-empty and invalid. Every caller in this module
// is expected to have already validated a caller-supplied prefix (see
// packtrail.go's resourceTokenPattern check, which New's own pattern
// mirrors) — this is a last-resort defense against a future caller skipping
// that step, not a substitute for it, since an unsafe prefix would otherwise
// fail much later with an opaque NATS error instead of failing fast here.
func New(prefix string) Names {
	if prefix == "" {
		prefix = Default
	}

	if !prefixPattern.MatchString(prefix) {
		panic(fmt.Sprintf("names: invalid prefix %q: must match %s", prefix, prefixPattern))
	}

	return Names{
		Prefix: prefix,

		BucketExecutions:  prefix + "-executions",
		BucketExecArchive: prefix + "-executions-archive",
		BucketLeases:      prefix + "-leases",
		BucketIdxStatus:   prefix + "-idx-status",
		BucketIdxFlow:     prefix + "-idx-flow",
		BucketResultCache: prefix + "-result-cache",
		BucketFlows:       prefix + "-flows",
		BucketPayloads:    prefix + "-payloads",

		StreamEvents:     prefix + "-events",
		StreamWork:       prefix + "-work",
		StreamSignals:    prefix + "-signals",
		StreamSchedule:   prefix + "-schedule",
		StreamDeadLetter: prefix + "-deadletter",
		StreamHistory:    prefix + "-history",

		SubjEventsPrefix:     prefix + ".events.",
		SubjWorkPrefix:       prefix + ".work.",
		SubjSignalPrefix:     prefix + ".signal.",
		SubjSchedPrefix:      prefix + ".sched.",
		SubjSchedFirePrefix:  prefix + ".sched.fire.",
		SubjDeadLetterPrefix: prefix + ".deadletter.",
		SubjHistoryPrefix:    prefix + ".history.",

		DurEngine:  prefix + "-engine",
		DurFired:   prefix + "-engine-fired",
		DurSignals: prefix + "-engine-signals",
		DurIndexer: prefix + "-indexer",
	}
}
