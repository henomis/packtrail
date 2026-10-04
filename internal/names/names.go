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

// Package names centralises every NATS resource name packtrail uses — streams,
// KV buckets, object stores, subjects and durable consumer names — derived from
// a single namespace prefix, plus the validation rules for every identifier
// that ends up as a NATS subject token or KV key segment.
package names

import (
	"crypto/sha256"
	"encoding/base32"
	"fmt"
	"hash/fnv"
	"regexp"
	"strconv"
	"strings"
)

// Default is the namespace prefix used when none is supplied.
const Default = "packtrail"

var (
	// prefixPattern bounds a namespace prefix to the token shape every resource
	// name is built from.
	prefixPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

	// tokenPattern bounds identifiers that become a single NATS subject token or
	// KV key segment: execution ids, flow names, node ids, signal names, worker
	// kinds, schedule names. Wildcards ('*', '>'), dots and whitespace would
	// silently broaden a subject filter or split a key, so they are rejected at
	// the boundary instead of producing an opaque NATS error (or a data leak)
	// later.
	tokenPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
)

// maxToken is the longest valid token.
const maxToken = 128

// ValidPrefix reports whether p is a valid namespace prefix.
func ValidPrefix(p string) bool { return prefixPattern.MatchString(p) }

// ValidToken reports whether s can be used as a single NATS subject token and
// KV key segment.
func ValidToken(s string) bool { return tokenPattern.MatchString(s) }

// CheckToken returns a descriptive error when s is not a valid token. what
// names the identifier in the message ("execution id", "flow name", …).
func CheckToken(what, s string) error {
	if !ValidToken(s) {
		return fmt.Errorf("invalid %s %q: must match [A-Za-z0-9_-]{1,128}", what, s)
	}

	return nil
}

// Names holds every concrete resource name for one namespace.
type Names struct {
	Prefix string

	// Streams.
	StreamEvents string // source of truth: <p>.ev.<partition>.<exec>
	StreamCmd    string // command inbox + durable timers + cron schedules
	StreamWork   string // worker jobs: <p>.work.<kind>
	StreamDLQ    string // dead letters: <p>.dlq.<kind>.<key>

	// KV buckets.
	BucketSnapshots string
	BucketFlows     string
	BucketIndex     string
	BucketCache     string
	BucketSem       string
	BucketStore     string

	// Object stores.
	ObjectBlobs   string
	ObjectArchive string
}

// New builds the resource names for prefix. An empty prefix falls back to
// Default. New panics on an invalid prefix: it is an unrecoverable
// configuration error that would corrupt every derived name, and callers are
// expected to validate user input with ValidPrefix first.
func New(prefix string) Names {
	if prefix == "" {
		prefix = Default
	}

	if !ValidPrefix(prefix) {
		panic("names: invalid namespace prefix " + strconv.Quote(prefix) + "; must match [A-Za-z0-9_-]{1,64}")
	}

	return Names{
		Prefix: prefix,

		StreamEvents: prefix + "-events",
		StreamCmd:    prefix + "-cmd",
		StreamWork:   prefix + "-work",
		StreamDLQ:    prefix + "-dlq",

		BucketSnapshots: prefix + "-snapshots",
		BucketFlows:     prefix + "-flows",
		BucketIndex:     prefix + "-index",
		BucketCache:     prefix + "-cache",
		BucketSem:       prefix + "-sem",
		BucketStore:     prefix + "-store",

		ObjectBlobs:   prefix + "-blobs",
		ObjectArchive: prefix + "-archive",
	}
}

// Partition maps an execution id to its partition in [0, n). It is FNV-1a
// (32 bit) modulo n, chosen because it is trivial to reproduce in any language
// SDK: every command for an execution must land in the same partition.
func Partition(execID string, n int) int {
	if n <= 1 {
		return 0
	}

	h := fnv.New32a()
	_, _ = h.Write([]byte(execID))

	return int(h.Sum32() % uint32(n)) //nolint:gosec // n > 1 and bounded by configuration.
}

// EventsSubjects is the subject filter of the events stream.
func (n Names) EventsSubjects() string { return n.Prefix + ".ev.*.*" }

// EventSubject is the subject holding every event of one execution.
func (n Names) EventSubject(partition int, execID string) string {
	return n.Prefix + ".ev." + strconv.Itoa(partition) + "." + execID
}

// EventPartitionFilter matches every execution of one partition.
func (n Names) EventPartitionFilter(partition int) string {
	return n.Prefix + ".ev." + strconv.Itoa(partition) + ".*"
}

// CmdSubject is the command inbox subject of one execution.
func (n Names) CmdSubject(partition int, execID string) string {
	return n.Prefix + ".cmd." + strconv.Itoa(partition) + "." + execID
}

// CmdPartitionFilter matches every command of one partition.
func (n Names) CmdPartitionFilter(partition int) string {
	return n.Prefix + ".cmd." + strconv.Itoa(partition) + ".*"
}

// TimerSubject holds the schedule message of one durable timer.
func (n Names) TimerSubject(execID, timerID string) string {
	return n.Prefix + ".timer." + execID + "." + timerID
}

// TimerExecFilter matches every pending timer of one execution.
func (n Names) TimerExecFilter(execID string) string { return n.Prefix + ".timer." + execID + ".*" }

// ScheduleSubject holds the cron schedule message named name.
func (n Names) ScheduleSubject(name string) string { return n.Prefix + ".sched." + name }

// CronSubject receives the messages fired by the cron schedule named name.
func (n Names) CronSubject(name string) string { return n.Prefix + ".cron." + name }

// CronFilter matches every fired cron message.
func (n Names) CronFilter() string { return n.Prefix + ".cron.*" }

// CmdStreamSubjects lists the subjects of the command stream.
func (n Names) CmdStreamSubjects() []string {
	return []string{n.Prefix + ".cmd.*.*", n.Prefix + ".timer.*.*", n.Prefix + ".sched.*", n.CronFilter()}
}

// WorkSubject is the job queue of one worker kind.
func (n Names) WorkSubject(kind string) string { return n.Prefix + ".work." + kind }

// WorkSubjects is the subject filter of the work stream.
func (n Names) WorkSubjects() string { return n.Prefix + ".work.*" }

// WorkDelaySubject holds the schedule that hands a busy job back to its
// kind's queue later.
func (n Names) WorkDelaySubject(kind, id string) string {
	return n.Prefix + ".workdelay." + kind + "." + id
}

// WorkDelaySubjects is the subject filter of hand-back schedules.
func (n Names) WorkDelaySubjects() string { return n.Prefix + ".workdelay.*.*" }

// DLQSubject is the dead-letter subject for one kind of dead letter.
func (n Names) DLQSubject(kind, key string) string { return n.Prefix + ".dlq." + kind + "." + key }

// DLQSubjects is the subject filter of the dead-letter stream.
func (n Names) DLQSubjects() string { return n.Prefix + ".dlq.>" }

// CtlSubject carries core-NATS control broadcasts between engine processes
// (not persisted).
func (n Names) CtlSubject(kind string) string { return n.Prefix + ".ctl." + kind }

// StopSubject carries the core-NATS stop notices for jobs of execID: the work
// they do is no longer wanted (G5-02).
func (n Names) StopSubject(execID string) string { return n.Prefix + ".ctl.stop." + execID }

// StopSubjects matches every stop notice (one subscription per worker process).
func (n Names) StopSubjects() string { return n.Prefix + ".ctl.stop.*" }

// ProgressSubject carries the core-NATS progress messages a worker publishes
// while running node of execID: never stored (G5-04).
func (n Names) ProgressSubject(execID, node string) string {
	return n.Prefix + ".progress." + execID + "." + node
}

// ProgressFilter matches every progress message of execID.
func (n Names) ProgressFilter(execID string) string { return n.Prefix + ".progress." + execID + ".*" }

// DurEngine is the durable consumer that feeds one engine partition.
func (n Names) DurEngine(partition int) string {
	return n.Prefix + "-engine-" + strconv.Itoa(partition)
}

// DurDispatch is the durable consumer that feeds one dispatcher partition.
func (n Names) DurDispatch(partition int) string {
	return n.Prefix + "-dispatch-" + strconv.Itoa(partition)
}

// DurCron is the durable consumer of fired cron messages.
func (n Names) DurCron() string { return n.Prefix + "-cron" }

// DurIndexer is the durable consumer that maintains the visibility index.
func (n Names) DurIndexer() string { return n.Prefix + "-indexer" }

// DurWorker is the durable consumer shared by every worker of one kind.
func (n Names) DurWorker(kind string) string { return n.Prefix + "-work-" + kind }

// triggerHashLen is the length of the digest that ends a trigger's execution
// id: 26 base32 characters, 130 bits.
const triggerHashLen = 26

// TriggerMsgExecID is the execution a trigger of flow starts for a message
// with Nats-Msg-Id msgID. Flows triggered by the same message get different
// ids, and a redelivered or republished message the same one.
func TriggerMsgExecID(flow, msgID string) string { return triggerExecID(flow, "msg\x00"+msgID) }

// TriggerSeqExecID is the execution a stream trigger of flow starts for a
// message without Nats-Msg-Id: the message's stream and sequence identify it.
func TriggerSeqExecID(flow, stream string, seq uint64) string {
	return triggerExecID(flow, "seq\x00"+stream+"\x00"+strconv.FormatUint(seq, 10))
}

// triggerExecID is "<flow>-t<digest of flow and key>", the flow cut so the
// id stays a valid token. The digest has a fixed length, so two ids are equal
// only when flow and key are: a '-' in a flow name or a Msg-Id cannot make
// two triggers collide.
func triggerExecID(flow, key string) string {
	sum := sha256.Sum256([]byte(flow + "\x00" + key))
	digest := strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(sum[:]))

	if keep := maxToken - len("-t") - triggerHashLen; len(flow) > keep {
		flow = flow[:keep]
	}

	return flow + "-t" + digest[:triggerHashLen]
}
