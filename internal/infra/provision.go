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

package infra

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Minimum server version: message schedules, per message TTL and pinned
// priority groups all arrived by 2.12.
const (
	minMajor = 2
	minMinor = 12
)

// DedupWindow is how long the command and work streams remember a message
// id. Ids are derived from the execution id, so an execution id must not be
// used again before its last messages have left the window: a deleted
// execution is kept at least this long after it finished.
const DedupWindow = dedupWindow

const (
	dedupWindow    = 10 * time.Minute
	dlqMaxAge      = 30 * 24 * time.Hour
	cacheMaxAge    = 30 * 24 * time.Hour
	semMaxAge      = 24 * time.Hour
	storeHistory   = 5
	schemaVersions = 1
)

// CheckServer fails fast when the connected server is too old for packtrail
// (backlog: "require NATS >= 2.12 and fail at startup").
func (i *Infra) CheckServer() error {
	v := i.NC.ConnectedServerVersion()

	parts := strings.SplitN(strings.TrimPrefix(v, "v"), ".", 3) //nolint:mnd // major.minor.patch
	if len(parts) < 2 {                                         //nolint:mnd // major.minor
		return fmt.Errorf("packtrail: cannot parse server version %q", v)
	}

	major, err1 := strconv.Atoi(parts[0])
	minor, err2 := strconv.Atoi(parts[1])

	if err1 != nil || err2 != nil {
		return fmt.Errorf("packtrail: cannot parse server version %q", v)
	}

	if major < minMajor || (major == minMajor && minor < minMinor) {
		return fmt.Errorf("packtrail: nats-server %s is too old: >= %d.%d is required "+
			"(message schedules, pinned consumers)", v, minMajor, minMinor)
	}

	return nil
}

// Provision creates or updates every resource of the namespace. partitions
// is the desired partition count for a new deployment (0 = DefaultPartitions
// or the deployed value); changing it on an existing deployment is refused,
// because it would re-route in-flight executions.
func (i *Infra) Provision(ctx context.Context, partitions int) error {
	if err := i.CheckServer(); err != nil {
		return err
	}

	// A standalone server accepts any replica count and runs R1: refuse
	// instead of pretending the source of truth is replicated.
	if i.replicas() > 1 && i.NC.ConnectedClusterName() == "" {
		return fmt.Errorf("packtrail: %d replicas requested but the server is not clustered", i.replicas())
	}

	p, err := i.resolvePartitions(ctx, partitions)
	if err != nil {
		return err
	}

	i.Partitions = p

	if err = i.provisionStreams(ctx); err != nil {
		return err
	}

	for _, cfg := range i.kvConfigs() {
		if cfg.Replicas, err = i.replicasFor(ctx, "KV_"+cfg.Bucket); err != nil {
			return err
		}

		if i.kvUpToDate(ctx, cfg) {
			continue
		}

		if _, err = i.JS.CreateOrUpdateKeyValue(ctx, cfg); err != nil {
			return fmt.Errorf("packtrail: bucket %s: %w", cfg.Bucket, err)
		}
	}

	for _, bucket := range []string{i.Names.ObjectBlobs, i.Names.ObjectArchive} {
		r, rerr := i.replicasFor(ctx, "OBJ_"+bucket)
		if rerr != nil {
			return rerr
		}

		cfg := jetstream.ObjectStoreConfig{
			Bucket: bucket, Storage: jetstream.FileStorage, Compression: true, Replicas: r,
		}

		if i.objectUpToDate(ctx, cfg) {
			continue
		}

		if _, err = i.JS.CreateOrUpdateObjectStore(ctx, cfg); err != nil {
			return fmt.Errorf("packtrail: object store %s: %w", bucket, err)
		}
	}

	return nil
}

// replicas is the explicitly requested replication (0 = not requested).
func (i *Infra) replicas() int { return max(i.Replicas, 0) }

// replicasFor returns the replication to apply to the stream backing a
// resource: the requested value, or — when none was requested — the value it
// is deployed with (1 for a new resource). An engine started without
// WithReplicas therefore never scales a replicated deployment down (F2-01).
func (i *Infra) replicasFor(ctx context.Context, stream string) (int, error) {
	if i.Replicas > 0 {
		return i.Replicas, nil
	}

	s, err := i.JS.Stream(ctx, stream)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		return 1, nil
	}

	if err != nil {
		return 0, fmt.Errorf("packtrail: stream %s: %w", stream, err)
	}

	return max(s.CachedInfo().Config.Replicas, 1), nil
}

func (i *Infra) provisionStreams(ctx context.Context) error {
	for _, cfg := range i.streamConfigs() {
		var err error
		if cfg.Replicas, err = i.replicasFor(ctx, cfg.Name); err != nil {
			return err
		}

		s, err := i.ensureStream(ctx, cfg)
		if err != nil {
			return fmt.Errorf("packtrail: stream %s: %w", cfg.Name, err)
		}

		if err = verifyCapabilities(cfg, s.CachedInfo().Config); err != nil {
			return err
		}

		if got := s.CachedInfo().Config.Replicas; got != cfg.Replicas {
			return fmt.Errorf("packtrail: stream %s: requested %d replicas, server applied %d",
				cfg.Name, cfg.Replicas, got)
		}
	}

	return nil
}

// Provisioning runs at every engine start. Resources already configured as
// wanted are left alone: a no-op update still goes through the server's
// stream-update path, and concurrent with other engines creating consumers it
// has triggered a data race inside nats-server 2.14 (seen under -race in the
// load test), besides being needless load.

// ensureStream returns the stream, creating or updating it only when its
// configuration differs from cfg.
func (i *Infra) ensureStream(ctx context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error) {
	s, err := i.JS.Stream(ctx, cfg.Name)
	if err == nil && streamUpToDate(cfg, s.CachedInfo().Config) {
		return s, nil
	}

	if err != nil && !errors.Is(err, jetstream.ErrStreamNotFound) {
		return nil, err
	}

	return i.JS.CreateOrUpdateStream(ctx, cfg)
}

func streamUpToDate(want, have jetstream.StreamConfig) bool { return covers(have, want) }

// covers reports whether the deployed configuration have already contains
// everything set in want: want (zero values omitted by its JSON encoding) is
// overlaid onto have — keeping what the server filled in — and the result
// must equal have. Comparing whole structs, not a hand-picked field list,
// means any field we start setting reaches existing deployments (F4-02).
func covers(have, want any) bool {
	hm, err1 := toMap(have)
	wm, err2 := toMap(want)

	if err1 != nil || err2 != nil {
		return false
	}

	return reflect.DeepEqual(overlay(clone(hm), dropZeroNumbers(wm)), hm)
}

// dropZeroNumbers removes numeric zeros: in a NATS configuration a zero limit
// means "not set" (the server reports it as -1), so it asserts nothing.
func dropZeroNumbers(m map[string]any) map[string]any {
	for k, v := range m {
		switch x := v.(type) {
		case float64:
			if x == 0 {
				delete(m, k)
			}
		case map[string]any:
			m[k] = dropZeroNumbers(x)
		}
	}

	return m
}

func toMap(v any) (map[string]any, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}

	var m map[string]any

	return m, json.Unmarshal(b, &m)
}

func clone(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))

	for k, v := range m {
		if sub, ok := v.(map[string]any); ok {
			v = clone(sub)
		}

		out[k] = v
	}

	return out
}

// overlay writes every value of src into dst, recursing into objects.
func overlay(dst, src map[string]any) map[string]any {
	for k, v := range src {
		if sv, ok := v.(map[string]any); ok {
			if dv, isMap := dst[k].(map[string]any); isMap {
				dst[k] = overlay(dv, sv)

				continue
			}
		}

		dst[k] = v
	}

	return dst
}

func (i *Infra) kvUpToDate(ctx context.Context, cfg jetstream.KeyValueConfig) bool {
	kv, err := i.JS.KeyValue(ctx, cfg.Bucket)
	if err != nil {
		return false
	}

	st, err := kv.Status(ctx)
	if err != nil {
		return false
	}

	kvs, ok := st.(*jetstream.KeyValueBucketStatus)
	if !ok {
		return false
	}

	return covers(kvs.Config(), cfg)
}

func (i *Infra) objectUpToDate(ctx context.Context, cfg jetstream.ObjectStoreConfig) bool {
	obs, err := i.JS.ObjectStore(ctx, cfg.Bucket)
	if err != nil {
		return false
	}

	st, err := obs.Status(ctx)
	if err != nil {
		return false
	}

	have := jetstream.ObjectStoreConfig{
		Bucket: st.Bucket(), Description: st.Description(), TTL: st.TTL(), Storage: st.Storage(),
		Replicas: st.Replicas(), Compression: st.IsCompressed(), Metadata: st.Metadata(),
	}

	return covers(have, cfg)
}

func (i *Infra) resolvePartitions(ctx context.Context, want int) (int, error) {
	s, err := i.JS.Stream(ctx, i.Names.StreamCmd)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		if want <= 0 {
			want = DefaultPartitions
		}

		return want, nil
	}

	if err != nil {
		return 0, fmt.Errorf("packtrail: command stream: %w", err)
	}

	have, err := strconv.Atoi(s.CachedInfo().Config.Metadata[MetaPartitions])
	if err != nil || have <= 0 {
		return 0, fmt.Errorf("packtrail: command stream has no valid %s metadata", MetaPartitions)
	}

	if want > 0 && want != have {
		return 0, fmt.Errorf("packtrail: namespace is deployed with %d partitions, %d requested; "+
			"the partition count cannot change", have, want)
	}

	return have, nil
}

func (i *Infra) streamConfigs() []jetstream.StreamConfig {
	n := i.Names

	return []jetstream.StreamConfig{
		{
			Name: n.StreamEvents, Subjects: []string{n.EventsSubjects()}, Retention: jetstream.LimitsPolicy,
			Storage: jetstream.FileStorage, Compression: jetstream.S2Compression,
			AllowDirect: true, Discard: jetstream.DiscardNew,
			Description: "packtrail events: the source of truth, one subject per execution",
		},
		{
			Name: n.StreamCmd, Subjects: n.CmdStreamSubjects(), Retention: jetstream.WorkQueuePolicy,
			Storage: jetstream.FileStorage, AllowMsgSchedules: true, AllowRollup: true, Duplicates: dedupWindow,
			Metadata: map[string]string{
				MetaPartitions: strconv.Itoa(i.Partitions), MetaProtocol: ProtocolVersion,
			},
			Description: "packtrail commands, durable timers and cron schedules",
		},
		{
			Name: n.StreamWork, Subjects: []string{n.WorkSubjects(), n.WorkDelaySubjects()},
			Retention: jetstream.WorkQueuePolicy, Storage: jetstream.FileStorage, Duplicates: dedupWindow,
			AllowMsgSchedules: true, AllowRollup: true,
			Description: "packtrail worker jobs, one subject per worker kind",
		},
		{
			Name: n.StreamDLQ, Subjects: []string{n.DLQSubjects()}, Retention: jetstream.LimitsPolicy,
			Storage: jetstream.FileStorage, MaxAge: dlqMaxAge, Description: "packtrail dead letters",
		},
	}
}

func (i *Infra) kvConfigs() []jetstream.KeyValueConfig {
	n := i.Names

	return []jetstream.KeyValueConfig{
		{Bucket: n.BucketSnapshots, History: 1, Storage: jetstream.FileStorage, Compression: true},
		{Bucket: n.BucketFlows, History: schemaVersions, Storage: jetstream.FileStorage},
		{Bucket: n.BucketIndex, History: 1, Storage: jetstream.FileStorage},
		{Bucket: n.BucketCache, History: 1, Storage: jetstream.FileStorage, TTL: cacheMaxAge},
		// Active concurrency keys are rewritten every ack wait (lease
		// renewal); a key untouched for a day belongs to nothing (F3-03).
		{Bucket: n.BucketSem, History: 1, Storage: jetstream.FileStorage, TTL: semMaxAge},
		{Bucket: n.BucketStore, History: storeHistory, Storage: jetstream.FileStorage},
	}
}

// verifyCapabilities checks that the server kept the opt-in features: an old
// server silently drops unknown config fields instead of rejecting them.
func verifyCapabilities(want, got jetstream.StreamConfig) error {
	if want.AllowMsgSchedules && !got.AllowMsgSchedules {
		return fmt.Errorf("packtrail: stream %s: server does not support message schedules", want.Name)
	}

	return nil
}
