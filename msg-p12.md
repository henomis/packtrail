PT-12 is accurate. One addition: the same gap exists for failed attempts, not just interrupts.

  Confirmed:
  - An interrupt can't carry usage. InterruptError (worker/job.go:187) has only Payload, and the interrupt command (InterruptData, internal/cmd/cmd.go:131) and the NodeInterrupted event have no usage field.
  - Usage only reaches the counters when a step completes (applyNodeDone, internal/fold/state.go:515). The budget is also checked only at completion (decide.go:517) and when a subflow finishes (decide.go:1134).

  Missing from the finding:
  - Failed attempts lose usage too. The failure command (FailData) has no usage field either. An agent whose LLM call returns unusable output and is retried has its tokens dropped, which hits budgets harder than pauses do.
  - The workaround is weaker than stated. The budget is checked only when the step completes, so a run of question-and-answer rounds can go past the budget without being stopped until the end. Besides a step that never completes, usage is also lost when the execution is cancelled, times out or fails while paused.

  Proposal: fix both gaps with one helper instead of an interrupt-only InterruptWithUsage:
  - Worker SDK: worker.WithUsage(err, usage) error attaches usage to whatever error a handler returns, whether that's Interrupt(...), a Permanent error or a retryable one. It also works through errors.As.
  - Commands and events: the interrupt and fail commands get a usage field, and so do the NodeInterrupted and NodeFailed events. Applying either event adds the usage to the counters, the same way completion does.
  - Budget: check it after an interrupt or a failure as well. If the budget is exceeded, the execution fails with reason budget instead of pausing or retrying.
  - No double counting: duplicate or stale results already produce no events, so a redelivered job's usage is only counted once.
  - Docs and tests: update the protocol and event specs, add invariant tests (usage counted on interrupt and on a retried failure, budget exceeded while paused, no double count on redelivery), and an invariants.md entry.

  With this in place, stiggy can drop the usage it keeps in the PT-11 transcript and report it on each run instead.

---

## Reply from stiggy

Agreed on all of it. The failed-attempt case is the bigger leak for stiggy: an LLM answer that breaks the output schema is retried, and every retry spends tokens that never reach the budget. You're also right that my workaround is weaker than I described. It can only count at completion, so a long question-and-answer exchange can run past its budget unnoticed, and cancellation, timeout or failure while paused all lose the usage.

**One helper for every outcome: yes.** `worker.WithUsage(err, usage) error` is better than an interrupt-only variant. What stiggy needs from it:

1. **It composes in any order with packtrail's other wrappers.** `WithUsage(Interrupt(p), u)`, `WithUsage(Permanent(e), u)`, `Permanent(WithUsage(e, u))` and a wrapped `fmt.Errorf("...: %w", WithUsage(...))` must all still be classified correctly (interrupt / permanent / retryable), and the usage must be found with `errors.As`. A table test over these combinations would pin it down.
2. **Every attempt counts.** Usage from each failed attempt is added, not just the last one's, because each retry is real spend. That matches your event design (one `NodeFailed` per attempt); please state it in the docs.
3. **Budget semantics.**
   - Exceeded on an interrupt: the execution fails with reason `budget` instead of pausing. Good. A human shouldn't be asked a question the budget can no longer pay for.
   - Exceeded on a retryable failure: fail with `budget` instead of scheduling the retry. Good.
   - Please make the failed node and the reason visible in `State` (`FailedNode`, `Reason`), the same way a budget failure at completion is, so stiggy's CLI and `packtrail-ui` show *where* the money ran out.
4. **Map items and fan-out branches.** Usage from a failed or interrupted map item or branch is counted toward the parent execution's counters, like a completed item's.
5. **No double counting, including `Result.Usage`.** Redeliveries are covered, as you say. Please also define what happens if a handler returns a successful `*Result` with `Usage` *and* the error path is never taken. Nothing changes there, but say it in the docs so nobody wraps a nil error expecting it to count.

**Not needed:** usage for jobs cancelled from outside (`Client.Cancel`, a losing join branch, a timeout that kills the job). The worker never returns from those runs in time, so dropping that usage is acceptable. A sentence in the docs is enough.

**What stiggy will do when it lands:**
- Every agent run reports its own usage at the point it ends:
  - success → `Result.Usage`;
  - interrupt → `WithUsage(Interrupt(payload), u)`;
  - failure → `WithUsage(classify(err), u)`.
- phero emits its run summary, including token usage, to the tracer even when a run fails (`AgentRunSummaryEvent`, `agent/agent.go:303`). So stiggy can attach real token counts to failed runs with a per-job tracer, not just `agent_steps: 1`.
- The `usage` field leaves stiggy's interrupt payload. The transcript stays there (PT-11).

Until then, stiggy keeps carrying the usage of paused runs in the payload, and failed runs report nothing.
