# Changelog

## Unreleased

### Added

- **`Server.OutputHistory(ctx, execID, node)`** (#11): every output a node produced,
  oldest first, with its write time and whether it is the committed version.
  `Results` keeps one output per node, so a loop's earlier attempts were
  unreadable — although each visit had always written its own versioned entry.
  Uncommitted candidates (a stale attempt, a lost lease) are included, and
  `Current` marks the one the flow used.

### Fixed

- **`VarVisits` exported** (#10). `visits` shipped in v0.2.1 without a `Var*`
  constant, so a layer compiling its own syntax down to `when` expressions had
  to restate the string literal. It now sits alongside the other context
  variables.

## v0.2.1

Loop semantics and the end of an execution's life.

### Fixed

- **`last_node` after a revisit** (#5). `Outputs` is in settle order and
  `last_node` is its final element, but a node settling again in a cycle kept
  the position of its first visit. The step after a loop was handed an older
  node's output as "the previous step", and a choice rule on
  `results[last_node]` evaluated against the wrong document — so a loop gated on
  it could never exit. Every flow with a cycle was affected.

### Added

- **Per-node visit counts** (#6). `visits` — how many times each node has been
  entered, keyed by node id — is now part of the execution document, the
  assembled invocation context, and the variables a choice rule may reference,
  so a cycle can bound itself:

  ```yaml
  - {when: 'visits.verify >= 3', to: escalate}
  ```

  Visits count entries, not attempts: a node retried three times on one visit
  counts once, and a `Resume` re-enters the node it failed on. Fan-out branches
  are counted although they never become the current node.

### Changed

- **Cancelling a failed execution retires it** (#7). Failed is terminal but
  resumable, and `Cancel` skipped every terminal status, so a failed execution
  could be revived forever and retired never — and stayed in the hot bucket
  while it waited, since failed is not archivable. `Cancel` now moves a failed
  execution to `cancelled`, which `Resume` refuses and the archive sweep can
  collect. Completed and cancelled remain no-ops, and an unknown id still
  returns `ErrNotFound`. The failure reason is preserved alongside the cancel
  reason rather than overwritten.

### Upgrading

No migration. `visits` is absent from executions started before this version;
a rule reading it on one of those sees a missing field, which under the default
`on_error` counts as no match.
