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

package packtrail

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/henomis/packtrail/flow"
	"github.com/henomis/packtrail/internal/infra"
)

// Option configures an Engine.
type Option func(*config) error

type scheduleDef struct {
	name, flow, cron, tz string
	input                json.RawMessage
}

type config struct {
	namespace     string
	flows         []*flow.Flow
	flowsDirs     []string
	partitions    int
	owned         []int
	snapshotEvery int
	historyLimit  int
	drain         time.Duration
	logger        *slog.Logger
	cacheSize     int
	clock         func() time.Time
	blobThreshold int
	schedules     []scheduleDef
	noDispatch    bool
	noCommands    bool
	timeouts      timeouts
	replicas      int
}

// timeouts are the engine-side timeouts; zero values keep the defaults.
type timeouts struct {
	ackWait, pullExpiry, read, blob time.Duration
}

func (t timeouts) apply(in *infra.Infra) {
	if t.ackWait > 0 {
		in.AckWait = t.ackWait
	}

	if t.pullExpiry > 0 {
		in.PullExpiry = t.pullExpiry
	}

	if t.read > 0 {
		in.ReadTimeout = t.read
	}

	if t.blob > 0 {
		in.BlobTimeout = t.blob
	}
}

// WithNamespace sets the namespace prefix of every NATS resource (default
// "packtrail"): independent deployments can share a cluster.
func WithNamespace(ns string) Option {
	return func(c *config) error {
		if err := ValidateNamespace(ns); err != nil {
			return err
		}

		c.namespace = ns

		return nil
	}
}

// WithFlow registers a flow definition at Init.
func WithFlow(def *flow.Flow) Option {
	return func(c *config) error {
		if def == nil {
			return fmt.Errorf("%w: nil flow", ErrInvalidArgument)
		}

		cp, err := def.Clone()
		if err != nil {
			return fmt.Errorf("%w: %w", ErrInvalidArgument, err)
		}

		c.flows = append(c.flows, cp)

		return nil
	}
}

// WithFlowYAML parses and registers a flow definition at Init.
func WithFlowYAML(yaml []byte) Option {
	return func(c *config) error {
		def, err := flow.Parse(yaml)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrInvalidArgument, err)
		}

		c.flows = append(c.flows, def)

		return nil
	}
}

// WithFlowsDir registers every *.yaml / *.yml flow of dir at Init.
func WithFlowsDir(dir string) Option {
	return func(c *config) error {
		defs, err := flow.LoadDir(dir)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrInvalidArgument, err)
		}

		for _, d := range defs {
			c.flows = append(c.flows, d)
		}

		c.flowsDirs = append(c.flowsDirs, dir)

		return nil
	}
}

// WithPartitions sets the partition count of a new deployment (default 64).
// It cannot change once the namespace is provisioned.
func WithPartitions(n int) Option {
	return func(c *config) error {
		if n <= 0 || n > maxPartitions {
			return fmt.Errorf("%w: partitions must be in 1..%d", ErrInvalidArgument, maxPartitions)
		}

		c.partitions = n

		return nil
	}
}

const maxPartitions = 1024

// WithOwnedPartitions restricts this engine to some partitions (default: it
// competes for all of them; pinned consumers make one engine active per
// partition and fail over automatically).
func WithOwnedPartitions(ps ...int) Option {
	return func(c *config) error {
		for _, p := range ps {
			if p < 0 {
				return fmt.Errorf("%w: negative partition", ErrInvalidArgument)
			}
		}

		c.owned = ps

		return nil
	}
}

// DefaultHistoryLimit is the live log length (events) past which an execution
// is continued as new under the same id.
const DefaultHistoryLimit = 10000

// WithHistoryLimit continues an execution as new once its live log holds n
// events (default 10 000; negative disables): the log so far is archived as a
// segment and replaced by one event carrying the folded state. Behaviour does
// not change — same id, state, in-flight work and timers — and History,
// StateAt, Fork and Rerun still see everything. A looping execution's log then
// stays bounded (G5-07).
func WithHistoryLimit(n int) Option {
	return func(c *config) error {
		c.historyLimit = n

		return nil
	}
}

// WithSnapshotEvery snapshots states every n events (default 100; negative
// disables snapshots).
func WithSnapshotEvery(n int) Option {
	return func(c *config) error {
		c.snapshotEvery = n

		return nil
	}
}

// WithDrainTimeout bounds the graceful drain on shutdown (default 30s). A
// non-positive value keeps the default.
func WithDrainTimeout(d time.Duration) Option {
	return func(c *config) error {
		if d > 0 {
			c.drain = d
		}

		return nil
	}
}

// WithReplicas sets the replication factor of every stream, KV bucket and
// object store. Without it, existing resources keep the replication they are
// deployed with and new ones get 1, so an engine started without the option
// never scales a replicated deployment down. Use 3 in production: the events
// stream is the source of truth. Changing it scales the resources at the next
// Init; the cluster must have enough JetStream servers.
func WithReplicas(n int) Option {
	return func(c *config) error {
		if n < 1 || n > maxReplicas {
			return fmt.Errorf("%w: replicas must be between 1 and %d", ErrInvalidArgument, maxReplicas)
		}

		c.replicas = n

		return nil
	}
}

const maxReplicas = 5

// WithAckWait sets the ack wait of the engine's consumers (commands, events,
// cron, triggers). Handlers heartbeat while they run, so this is not a limit
// on how long a command may take: it is how long a message held by a crashed
// process waits before another engine gets it (default 5s).
func WithAckWait(d time.Duration) Option {
	return func(c *config) error {
		if d < time.Second {
			return fmt.Errorf("%w: ack wait must be at least 1s", ErrInvalidArgument)
		}

		c.timeouts.ackWait = d

		return nil
	}
}

// WithPullExpiry sets how long the engine's pull requests live on the server
// (default 5s). Shorter values hand partitions over faster after a crash.
func WithPullExpiry(d time.Duration) Option {
	return func(c *config) error {
		if d < time.Second {
			return fmt.Errorf("%w: pull expiry must be at least 1s", ErrInvalidArgument)
		}

		c.timeouts.pullExpiry = d

		return nil
	}
}

// WithReadTimeout bounds one batched read of an event log (default 5s).
func WithReadTimeout(d time.Duration) Option {
	return func(c *config) error {
		if d <= 0 {
			return fmt.Errorf("%w: read timeout must be positive", ErrInvalidArgument)
		}

		c.timeouts.read = d

		return nil
	}
}

// WithBlobTimeout bounds one claim-check transfer when the caller's context
// has no deadline (default 2m). Raise it for very large payloads on slow links.
func WithBlobTimeout(d time.Duration) Option {
	return func(c *config) error {
		if d <= 0 {
			return fmt.Errorf("%w: blob timeout must be positive", ErrInvalidArgument)
		}

		c.timeouts.blob = d

		return nil
	}
}

// WithLogger sets the logger.
func WithLogger(l *slog.Logger) Option {
	return func(c *config) error {
		c.logger = l

		return nil
	}
}

// WithStateCacheSize bounds the in-memory state caches (default 10000).
func WithStateCacheSize(n int) Option {
	return func(c *config) error {
		c.cacheSize = n

		return nil
	}
}

// WithClock injects the clock decisions use (tests).
func WithClock(now func() time.Time) Option {
	return func(c *config) error {
		c.clock = now

		return nil
	}
}

// WithBlobThreshold sets the body size above which messages use the
// claim-check (default: server max_payload minus a margin).
func WithBlobThreshold(n int) Option {
	return func(c *config) error {
		if n <= 0 {
			return fmt.Errorf("%w: blob threshold must be positive", ErrInvalidArgument)
		}

		c.blobThreshold = n

		return nil
	}
}

// WithSchedule installs a cron schedule at Init. The expression is validated
// here, so a typo fails at New instead of inside the engine (I-22).
func WithSchedule(name, flowName, cron string, input any) Option {
	return func(c *config) error {
		if err := checkSchedule(name, flowName, cron); err != nil {
			return err
		}

		b, err := marshalObject(input)
		if err != nil {
			return err
		}

		c.schedules = append(c.schedules, scheduleDef{name: name, flow: flowName, cron: cron, input: b})

		return nil
	}
}

func checkSchedule(name, flowName, cron string) error {
	if err := ValidateName("schedule name", name); err != nil {
		return err
	}

	if err := ValidateName("flow name", flowName); err != nil {
		return err
	}

	return ValidateCron(cron)
}

// WithoutDispatcher runs only the command processors (scale the roles
// independently).
func WithoutDispatcher() Option {
	return func(c *config) error {
		c.noDispatch = true

		return nil
	}
}

// WithoutCommands runs only the dispatcher.
func WithoutCommands() Option {
	return func(c *config) error {
		c.noCommands = true

		return nil
	}
}

// marshalObject encodes v as a JSON object (nil = {}).
func marshalObject(v any) (json.RawMessage, error) {
	if v == nil {
		return json.RawMessage(`{}`), nil
	}

	var b []byte

	switch x := v.(type) {
	case json.RawMessage:
		b = x
	case []byte:
		b = x
	default:
		var err error
		if b, err = json.Marshal(v); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrInvalidArgument, err)
		}
	}

	if len(b) == 0 {
		return json.RawMessage(`{}`), nil
	}

	var m map[string]json.RawMessage
	if err := json.Unmarshal(b, &m); err != nil || m == nil {
		return nil, fmt.Errorf("%w: payload must be a JSON object", ErrInvalidArgument)
	}

	return b, nil
}

// marshalAny encodes v as JSON (nil = null).
func marshalAny(v any) (json.RawMessage, error) {
	switch x := v.(type) {
	case nil:
		return nil, nil
	case json.RawMessage:
		if !json.Valid(x) {
			return nil, fmt.Errorf("%w: invalid JSON", ErrInvalidArgument)
		}

		return x, nil
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrInvalidArgument, err)
		}

		return b, nil
	}
}
