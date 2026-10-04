# packtrail

[![Build Status](https://github.com/henomis/packtrail/actions/workflows/checks.yml/badge.svg)](https://github.com/henomis/packtrail/actions/workflows/checks.yml) [![GoDoc](https://godoc.org/github.com/henomis/packtrail?status.svg)](https://godoc.org/github.com/henomis/packtrail) [![Go Report Card](https://goreportcard.com/badge/github.com/henomis/packtrail)](https://goreportcard.com/report/github.com/henomis/packtrail) [![GitHub release](https://img.shields.io/github/release/henomis/packtrail.svg)](https://github.com/henomis/packtrail/releases)

A durable workflow engine built only on [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream).

**Website and full documentation: <https://henomis.github.io/packtrail/>**

- **Event-sourced.** Every execution is an ordered log of events on its own
  subject. State, snapshots, the visibility index, jobs and timers are all
  derived from it, so time travel, forks, replays and audit come for free.
- **A declarative graph, not workflow-as-code.** Tasks, choices, fan-out/join,
  awaits (human in the loop), dynamic maps, subflows, dynamic edges,
  interrupts and failure routing (`on_failure`, for compensation); typed
  state channels with reducers. No determinism rules for
  your code.
- **Workers in any language.** A task is a job on `<ns>.work.<kind>`; a
  worker answers with a command over a small, versioned JSON protocol
  ([protocol reference](https://henomis.github.io/packtrail/docs.html#protocol)). The Go SDK is in `worker/`.
- **Agnostic.** No agents, LLMs or tokens in the core. Budgets are generic
  counters; an "agent" is just a worker.
- **Only NATS (≥ 2.12).** One message per decision with an expected-sequence
  check for single-writer appends,
  message schedules for durable timers and cron, KV and object stores for
  everything else. No database, no extra coordinator.

**What it is, and is not.** packtrail is a *workflow-as-data* engine: you
declare the graph (YAML or Go structs) and packtrail interprets it. In that it
is closer to other graph-based engines — than to the
workflow-as-code model where workflows are replayed
deterministically. What it shares with other durable engines is the durability model:
event history, durable timers, workers in any language.

## Install

```sh
go get github.com/henomis/packtrail
go install github.com/henomis/packtrail/cmd/packtrail@latest      # CLI
go install github.com/henomis/packtrail/cmd/packtrail-ui@latest   # dashboard
```

Requires Go 1.26+ and NATS Server 2.12+ with JetStream enabled
(`docker run --rm -p 4222:4222 nats:2.14.2 -js`).

## Quick look

```yaml
# review.yaml
name: review
channels:
  notes: {reducer: append}
nodes:
  - id: draft
    type: task
    kind: writer
    timeout: 1m
    retry: {max_attempts: 3, backoff: exponential}
    next: check
  - id: check
    type: choice
    rules:
      - {when: "results.draft.score >= 8", to: approve}
      - {default: true, to: draft}
  - id: approve
    type: await
    signal: approval
    timeout: 48h
    next: publish
  - {id: publish, type: task, kind: publisher}
start: draft
```

```go
eng, _ := packtrail.New(nc, packtrail.WithFlowsDir("flows"))
go eng.Run(ctx) // provisions the namespace and processes commands
<-eng.Ready()   // closed once every consumer pulls (readiness probes)

w, _ := worker.New(nc, "writer", func(ctx context.Context, j *worker.Job) (*worker.Result, error) {
	var in struct{ Topic string }
	if err := j.Input(&in); err != nil {
		return nil, worker.Permanent(err)
	}
	return &worker.Result{
		Output: map[string]any{"text": "…", "score": 9},
		Writes: map[string]any{"notes": "drafted " + in.Topic},
	}, nil
})
go w.Run(ctx)

c := eng.Client()
id, _ := c.Start(ctx, "review", map[string]any{"topic": "NATS"}) // returns once the execution exists
_ = c.Signal(ctx, id, "approval", map[string]any{"by": "ana"})  // buffered until the await reads it
st, _ := c.Wait(ctx, id) // final state: channels, results, counters, …

var draft struct{ Text string }
_ = st.Result("draft", &draft) // also st.Channel, st.Signal, st.DecodeInput, st.DecodeOutput
```

`c.WaitUntil(ctx, id, cond)` waits for a point inside an execution — parked
at an await, a task interrupted for a human — instead of polling `Get`.

A worker can stream intermediate results with `j.Progress(v)`; clients follow
them with `c.Progress(ctx, id)` (or `packtrail progress <exec>`). Progress is
never stored: the log keeps the final result only.

The CLI does the same from a shell:

```sh
packtrail run -flows flows/ &
packtrail start review -input '{"topic":"NATS"}' -wait
packtrail history <exec>          # every event
packtrail get <exec> -seq 12      # state right after event 12
packtrail fork <exec> 12          # continue from there in a new execution
packtrail fork <exec> 12 -writes '{"notes":"try again"}'  # … with edited state
packtrail update <exec> -writes '{"notes":"from ops"}'     # write channels now, synchronously
packtrail rerun <exec> draft      # run a node (and what follows) again
```

`packtrail-ui` serves a debugging dashboard (graph, timeline, state at any
point, actions) for every namespace on the NATS account; `-namespaces a,b`
restricts it to a list. It has no authentication and binds to loopback by
default.

## Concepts

| Concept | |
|---|---|
| Flow | Immutable, versioned (`<name>.<hash>`); executions keep the version they started with |
| Execution | An event log; status `running`, `waiting`, `completed`, `failed`, `cancelled` |
| Engine | Processes commands per partition: load state → decide → append (optimistic concurrency) → ack |
| Dispatcher | Turns events into effects: jobs, timer schedules, child starts, parent notifications, index updates |
| Worker | Serves a kind; heartbeats long jobs; reports complete / fail / interrupt |
| Channels | Typed state written by tasks as deltas, folded by reducers |
| Context | What a worker and expressions see: `input`, `channels`, `results`, `last_node`, `visits`, `signals`, `branches`, `counters`, `errors`, `item`, `resume` |

Delivery is at-least-once: a task can run more than once after a crash. Make
side effects idempotent, or enable the result cache. Work that is no longer
wanted is stopped: when an execution ends, a task is cancelled (a join
settled, a map aborted) or an attempt times out, the running job's context is
cancelled with cause `worker.ErrCancelled` — the handler should return
promptly, and its result is dropped. The engine
itself is exactly-once per decision: duplicates and stale results are absorbed
by the fold.

## Operations

- **Replication.** Use `WithReplicas(3)` on a JetStream cluster: the events
  stream is the source of truth. Asking for replicas on a standalone server
  fails at `Init`.
- **Partitions are permanent** for a namespace (they are part of every event
  subject). Pick the count up front (`WithPartitions`, default 64); to change
  it, run a new namespace alongside and drain the old one.
- **Quarantine.** An execution whose events the dispatcher can never process
  is quarantined (`packtrail quarantined`, dead letters) instead of stalling
  its partition; it can still be cancelled and closed. Once the cause is fixed
  (typically an upgrade), `packtrail unquarantine <exec>` replays what was
  skipped. Transient outages never quarantine: dispatching waits.
- **Alert on stalls.** The dispatcher retries infrastructure errors forever
  (it never gives up on an execution for a transient reason). Export
  `Engine.Metrics().DispatchStall` — the longest time a partition with
  pending events has gone without progress — and alert when it exceeds a
  minute or so.
- **Validate offline.** `packtrail.ValidateOptions(opts...)` runs every
  check `New` does (flows, schedules, namespace, tuning) without a NATS
  connection, so CI or a `validate` command can reject a bad configuration.
  `ValidateNamespace`, `ValidateName` and `ValidateCron` expose the
  identifier and cron rules, so callers don't have to copy the patterns.
- **Long histories.** Once an execution's live log holds 10 000 events
  (`WithHistoryLimit`), it is continued as new under the same id: the log so
  far is archived as a segment and replaced by one event carrying the state.
  Nothing else changes, and `History`, `StateAt`, `Fork` and `Rerun` still see
  every event. `max_steps` still bounds the total number of node entries.

## Examples

Runnable programs in [examples/](examples/README.md): human in the loop,
parallel research with reducers, map-reduce, retries and timeouts, time
travel and forks, an agent-style tool loop, subflows, cron and message
triggers. Start NATS with `docker run --rm -p 4222:4222 nats:2.14.2 -js`,
then `make examples` runs them all.

## Documentation

The [documentation](https://henomis.github.io/packtrail/docs.html) covers:

- [flow definitions](https://henomis.github.io/packtrail/docs.html#flow-anatomy) — every node type, channels and reducers, expressions
- [workers](https://henomis.github.io/packtrail/docs.html#workers) — the Go SDK: results, errors, interrupts, progress, cancellation
- [the client](https://henomis.github.io/packtrail/docs.html#client) — start, signal, update, time travel, fork, rerun
- [operations](https://henomis.github.io/packtrail/docs.html#operations) — options, replication, partitions, quarantine, metrics
- [the protocol](https://henomis.github.io/packtrail/docs.html#protocol) and [the event log](https://henomis.github.io/packtrail/docs.html#events) — for clients and workers in other languages

API reference on [pkg.go.dev](https://pkg.go.dev/github.com/henomis/packtrail).

## Development

```sh
make check   # go test -race + golangci-lint + go vet
```

Tests run against a real embedded nats-server; `packtrailtest.Start(t)` gives
your own tests the same one (with `Restart` to simulate an outage). The acceptance suite
(`internal/acceptance`) injects faults: engine kills, NATS restarts, duplicate
and reordered completions. `PT_CONFORMANCE_WORKER="<command>"` runs the
conformance suite against a worker written in another language.

## License

Apache 2.0.
