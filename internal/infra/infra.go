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

// Package infra holds the NATS handles shared by every packtrail component —
// connection, JetStream context, resource names, partition count — and the
// provisioning of the streams, buckets and object stores they use.
package infra

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/henomis/packtrail/internal/apierr"
	"github.com/henomis/packtrail/internal/names"
)

// MetaPartitions is the command-stream metadata key holding the partition
// count: every process (engine, client, SDK) reads it from there, so all agree
// on where an execution's commands go.
const MetaPartitions = "packtrail.partitions"

// MetaProtocol is the command-stream metadata key holding the wire protocol
// version.
const MetaProtocol = "packtrail.protocol"

// ProtocolVersion is the version of the wire protocol (docs/protocol.md).
const ProtocolVersion = "1"

// Default timeouts (all configurable on the engine, client and worker).
const (
	DefaultReadTimeout = 5 * time.Second
	DefaultBlobTimeout = 2 * time.Minute
)

// DefaultPartitions is the partition count of a new deployment. The count is
// permanent, so the default leaves room to add engines (G5-08).
const DefaultPartitions = 64

// payloadMargin is the headroom kept below the server max_payload for headers
// and subject, so a message right at the claim-check threshold still fits
// (I-20).
const payloadMargin = 16 * 1024

// defaultMaxPayload is the nats-server default max_payload.
const defaultMaxPayload = 1024 * 1024

// Infra bundles the shared NATS handles.
type Infra struct {
	NC         *nats.Conn
	JS         jetstream.JetStream
	Names      names.Names
	Partitions int
	Logger     *slog.Logger
	// BlobThreshold is the body size above which a message body is moved to
	// the blobs object store (claim-check).
	BlobThreshold int
	// AckWait and PullExpiry configure the engine and dispatcher consumers.
	AckWait    time.Duration
	PullExpiry time.Duration
	// ReadTimeout bounds one batched read of an event log.
	ReadTimeout time.Duration
	// BlobTimeout bounds one blob transfer when the caller sets no deadline.
	BlobTimeout time.Duration
	// Replicas is the replication factor of every stream, bucket and object
	// store (0 = 1). Use 3 in production: the events stream is the source of
	// truth.
	Replicas int

	mu  sync.Mutex
	kvs map[string]jetstream.KeyValue
	obs map[string]jetstream.ObjectStore
}

// New returns an Infra for an existing connection. Partitions and the blob
// threshold are resolved by Attach (from the deployed command stream) or
// Provision.
func New(nc *nats.Conn, ns names.Names, logger *slog.Logger) (*Infra, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("infra: jetstream: %w", err)
	}

	if logger == nil {
		logger = slog.Default()
	}

	return &Infra{
		NC: nc, JS: js, Names: ns, Logger: logger, Partitions: DefaultPartitions,
		BlobThreshold: DefaultBlobThreshold(nc), ReadTimeout: DefaultReadTimeout, BlobTimeout: DefaultBlobTimeout,
		kvs: map[string]jetstream.KeyValue{}, obs: map[string]jetstream.ObjectStore{},
	}, nil
}

// DefaultBlobThreshold derives the claim-check threshold from the server's
// max_payload minus a margin for headers and subject.
func DefaultBlobThreshold(nc *nats.Conn) int {
	mp := int(nc.MaxPayload())
	if mp <= 0 {
		mp = defaultMaxPayload
	}

	return max(mp-payloadMargin, mp/2) //nolint:mnd // never below half the payload limit.
}

// Partition returns the partition of an execution.
func (i *Infra) Partition(execID string) int { return names.Partition(execID, i.Partitions) }

// CmdSubject returns the command subject of an execution.
func (i *Infra) CmdSubject(execID string) string {
	return i.Names.CmdSubject(i.Partition(execID), execID)
}

// EventSubject returns the event subject of an execution.
func (i *Infra) EventSubject(execID string) string {
	return i.Names.EventSubject(i.Partition(execID), execID)
}

// ErrNotProvisioned is returned by Attach when the deployment does not exist.
var ErrNotProvisioned = apierr.ErrNotProvisioned

// Attach reads the partition count from a deployed command stream.
func (i *Infra) Attach(ctx context.Context) error {
	s, err := i.JS.Stream(ctx, i.Names.StreamCmd)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return ErrNotProvisioned
		}

		return fmt.Errorf("infra: command stream: %w", err)
	}

	p, err := strconv.Atoi(s.CachedInfo().Config.Metadata[MetaPartitions])
	if err != nil || p <= 0 {
		return fmt.Errorf("infra: command stream has no valid %s metadata", MetaPartitions)
	}

	i.Partitions = p

	return nil
}

// KV returns (and caches) a key-value bucket handle.
func (i *Infra) KV(ctx context.Context, bucket string) (jetstream.KeyValue, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	if kv, ok := i.kvs[bucket]; ok {
		return kv, nil
	}

	kv, err := i.JS.KeyValue(ctx, bucket)
	if err != nil {
		return nil, fmt.Errorf("infra: bucket %s: %w", bucket, err)
	}

	i.kvs[bucket] = kv

	return kv, nil
}

// Object returns (and caches) an object store handle.
func (i *Infra) Object(ctx context.Context, bucket string) (jetstream.ObjectStore, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	if o, ok := i.obs[bucket]; ok {
		return o, nil
	}

	o, err := i.JS.ObjectStore(ctx, bucket)
	if err != nil {
		return nil, fmt.Errorf("infra: object store %s: %w", bucket, err)
	}

	i.obs[bucket] = o

	return o, nil
}

// Retry runs fn until it succeeds, ctx ends or attempts are exhausted, backing
// off between attempts. It is for transient NATS errors on idempotent calls.
func Retry(ctx context.Context, attempts int, fn func() error) error {
	var err error

	delay := 20 * time.Millisecond //nolint:mnd // initial backoff.

	for range attempts {
		if err = fn(); err == nil {
			return nil
		}

		select {
		case <-ctx.Done():
			return errors.Join(err, ctx.Err())
		case <-time.After(delay):
		}

		delay = min(delay*2, time.Second) //nolint:mnd // exponential backoff capped at 1s.
	}

	return err
}
