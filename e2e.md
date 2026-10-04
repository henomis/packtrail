# E2E coverage work — handoff

State as of 2026-10-03. Everything below is **uncommitted** in the working tree
on `main` (last pushed commit: `c2d8805`).

## Goal

Extend `e2e/` with realistic, multi-feature workflows covering what LangGraph,
CrewAI (workflow side only, no AI) and Temporal offer, and fix the public API
wherever a scenario exposed a gap.

Rules for `e2e/` and `examples/` (from CLAUDE.md): public API only — no
`internal/` packages, no direct NATS/JetStream access, no extra mechanisms, no
helpers that should be public API (add the API instead). The embedded server
comes from the public `packtrailtest` package. Publishing an application
message on a trigger subject is allowed: it is the trigger's input.

## Verification status

- `go test -race` on every package (except `examples/`): pass (22 packages).
- `go test -race -count=3 ./e2e/` (`make e2e`, chaos included): pass, ~400 s.
- `golangci-lint run ./...`: 0 issues.

## Files

New:

| File | Scenario |
|---|---|
| `e2e/deploy_test.go` | #1 `TestRollingDeploy` |
| `e2e/outage_test.go` | #2 `TestTimersFireAfterOutage`, `TestRetryTimersSurviveOutage` |
| `e2e/pipeline_test.go` | #4 `TestEventDrivenPipeline` |
| `e2e/race_test.go` | #5 `TestFirstResponderWins`, `TestAbandonedChildOutlivesParent` |
| `e2e/stream_test.go` | #6 `TestStreamingAcrossCrash` |
| `e2e/namespace_test.go` | #8 `TestNamespacesAreIsolated` |
| `e2e/retention_test.go` | #9 `TestRetentionAndArchive` |
| `e2e/memory_test.go` | #10 `TestMemoryAcrossExecutions` |
| `internal/acceptance/wait_restart_test.go` | regression for the `Wait` hang (fails without the fix) |

Modified:

| File | Change |
|---|---|
| `e2e/timetravel_test.go` | #3 `TestTimeTravelAcrossContinuation`, `TestRerunAcrossSubflow`, `TestForkWhileRunning` |
| `e2e/agent_test.go` | #7 `TestAgentRecursionLimit` |
| `e2e/harness_test.go` | namespaces, shared server, redeploy, stop all engines, `parkedAt` |
| `e2e/order_test.go` | `orderWorkers` lost an always-nil `opts` param (lint) |
| `client.go` | `Rerun` fix |
| `client_more.go` | bounded ordered-consumer creation (`Wait`/`Watch`/`WatchTerminal`) |
| `packtrail.go` | `Engine.Archive` errors |

## Harness additions (`e2e/harness_test.go`)

- `newClusterOn(t, server, ns, flows, opts...)`: several clusters (namespaces)
  on one `packtrailtest.Server`; `newCluster` wraps it with a fresh server and
  the default namespace. Workers and the client follow `cl.ns`.
- `cluster.tuning` (engine options without flows) + `cluster.flows`;
  `deploy(flows)` changes what engines started afterwards register.
- `stopEngines()`: graceful stop of every engine (outage scenarios).
- `parkedAt(id, awaitNode)`: wait until the execution waits at that await.

## Scenarios (what each asserts)

1. **Rolling deploy** — v1 executions parked at an await; engines replaced one
   by one with v2 (different graph and `meta`). v1 executions finish on v1 graph
   and meta; new starts (also mid-deploy) get v2; `WithVersion(v1)` pins v1;
   fork, rerun of a task and rerun of an await stay on v1; `Flows` lists both,
   v2 latest.
2. **Durable timers through a full outage** — all engines stopped (and, in a
   subtest, NATS restarted) while await timeouts / retry delays are pending.
   Each timeout fires exactly once and routes; signals sent during the outage
   before the deadline win; retries run exactly as the policy allows.
3. **Time travel** — `StateAt`/`Fork` before and after continue-as-new
   boundaries; rerun after a child (no new child, same invoice) and of the
   subflow node (new child linked to the rerun, original child untouched);
   fork of an execution waiting on an interrupt, both answered independently.
4. **Event-driven pipeline** — cron + core-subject trigger (each message sent
   twice with one `Nats-Msg-Id`), per-tenant `concurrency` across two render
   processes, `cache`; engine crash + NATS restart in between. One execution
   per request id, repeated requests are cache hits after the outage, render
   peak 1 per tenant, nothing fires after `Unschedule`.
5. **Join policies / parent close** — `any` and `quorum:2`: books with exactly
   the winners, running losers stopped with `ErrCancelled`, a loser answering
   late changes nothing. `on_parent_close: abandon` child completes after its
   parent is cancelled; `cancel` child is cancelled; `WatchTerminal` agrees.
6. **Streaming** — progress subscribed before start; worker crashes mid-job,
   NATS restarts; the stream ends with all frames of the redelivery in order
   and closes at the end; live `Watch` == `History`; the crash cost a
   redelivery, not a retry.
7. **Recursion limit** — runaway planner stops at exactly `max_steps`
   (`ReasonMaxSteps`) across continuations and an engine crash.
8. **Namespaces** — same flow names, kinds and execution id in two namespaces:
   signals, cancels, flows, the index, `WatchTerminal`, `Store` stay separate.
9. **Retention/archive** — retention and `Engine.Archive`: archived executions
   readable (state, full history, `StateAt`, `OutputHistory`, index with
   `Archived`), refuse cancel/signal/resume/update, id not restartable, fork
   works; a waiting execution is never archived.
10. **Long-term memory** — workers read/extend per-user memory in `Store`
    (worker process has its own `NewClient`); survives a NATS restart between
    rounds; operator edit/wipe visible on the next run.

Every scenario also runs `cl.check(id)` (event-model invariants, incl.
`StateAt` of every decision and `Watch` == `History`).

## Bugs found and fixed (public API)

1. **`Client.Wait` could hang forever** when started as the server went down.
   `follow` created its ordered consumer with the caller's ctx (usually no
   deadline); the create request sent to the dying server was never answered
   and `RequestWithContext` blocked forever. Fix: `Client.orderedConsumer`
   bounds creation with `ReadTimeout`; used by `Wait`/`WaitUntil`, `Watch`,
   `WatchTerminal`. Consequence: `Watch` now returns an error (instead of
   hanging) if called while the server is unreachable. Regression test:
   `TestWaitStartedDuringServerRestart`. (Diagnosed via goroutine dump: stuck
   in `jetStream.OrderedConsumer` → `CreateOrUpdateConsumer` →
   `requestWithContext`.)
2. **`Rerun` failed on subflow/await/choice/fan nodes** ("never ran"): it
   searched `NodeScheduled` (tasks/maps only) although documented as "where the
   node was last entered". Now uses `NodeEntered`. Doc comment extended.
3. **`Engine.Archive` was a silent no-op** for running or missing executions.
   Now `ErrInvalidArgument` (running) / `ErrNotFound` (missing); no-op on an
   archived one.

## Open items (not done, need a decision)

- **Fan-out branches must be task nodes**: no parallel subflows (LangGraph and
  CrewAI support parallel sub-graphs). #5 was restructured to two parents.
  Implementing it is an engine change (`flow` validation + fold/dispatch).
- **Result cache has no single-flight**: concurrent runs with the same key all
  miss (by design). #4 asserts hits only for runs fetching ≥1 s after an
  earlier cron fetch completed.
- ~~No `Store` access from a job~~ — done: `worker.Job.Store()` (same
  bucket/keys as `Client.Store()`, shared code in `internal/store`, shared
  sentinels in `internal/apierr` so `errors.Is(err, packtrail.ErrNotFound)`
  works on both). #10 uses it; acceptance `TestJobStore`; documented in
  `web/docs.html`.
- **Core triggers are at-most-once**: #4 publishes only while engines are
  known ready (replaced after the NATS restart). The durable stream trigger
  would need the test to create an application JetStream stream directly,
  which the e2e rules forbid.
- ~~Pre-existing: `gofmt -l` flags `internal/fold/fold_test.go`~~ — fixed.

## Test-writing pitfalls learned

- Harness helpers (`wait`, `check`, `waitUntil`) call `t.Fatalf`: never call
  them from spawned goroutines. Start executions concurrently (Start does not
  block on the run), then wait/check from the test goroutine.
- Don't run invariant checks (`check` → `Watch`/`StateAt`) concurrently with a
  deliberate NATS restart: reads may legitimately fail while it is down.
- Progress is best effort and not stored: subscribe before `Start` (choose the
  id with `WithExecutionID`), and leave time for resubscription after a NATS
  restart before asserting completeness.
- After a NATS restart, core trigger subscriptions are only known to be back
  on engines started afterwards (`Engine.Ready`).

## How to run

```sh
go test -race -run 'TestRollingDeploy' ./e2e/          # one scenario
go test -short ./e2e/                                   # skips TestChaos
make e2e                                                # -race -count=3, ~7 min
go test -race -run TestWaitStartedDuringServerRestart ./internal/acceptance/
```

## Suggested next steps

1. Review and commit (suggested split: the three API fixes + acceptance
   regression; then the e2e scenarios and harness).
2. Decide on parallel subflows.
