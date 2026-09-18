# Review backlog

Findings deferred by a bounded review: real, but not blocking. Append, don't rewrite.

- [ ] 2026-09-18 `internal/scheduler/cron.go` — `ValidateCron` accepts `@every`/`@at` on shape
      alone, but the server rejects `@every 500ms` (<1s), `@every foo` and non-RFC3339 `@at`, so
      those still pass `New` and fail later inside the engine. Both server checks are one stdlib
      call (`time.ParseDuration`, `time.Parse(time.RFC3339, …)`). It also rejects things the
      server accepts: empty comma terms (`1,,2`) and `*-5`.
- [ ] 2026-09-18 `invoker/asyncqueue/worker.go` `deadLetter` — when the execution no longer
      exists (cancelled then archived), `FailActivity` returns `ErrNotFound`. The job is then
      Nak'd for up to 3×maxDeliver with Error logs before it is dropped. Treat not-found as a
      guard miss (nothing is parked).
- [ ] 2026-09-18 `packtrail.go` `Server.FailActivity` — no `validExecID` check, unlike
      `Cancel`, `Resume` and `Results` (F-022 parity).
- [ ] 2026-09-18 `internal/runtime/engine.go` `cancelAbsent` — a transient archive-lookup error
      is reported as `ErrNotFound` (404 in the UI). This is deliberate and documented, but
      operators may read it as "no such execution".
- [ ] 2026-09-18 `packtrail.go` `reconcilePayloadCap` — against a small server the default is
      tightened to exactly `max_payload`, with no headroom for KV headers or subject. An entry
      right at the cap can still fail as an opaque write error.
