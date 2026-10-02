# Examples

Each example is a single `main.go` that connects to a JetStream-enabled NATS
server (`$NATS_URL`, default `nats://localhost:4222`), starts an engine and its
workers, and prints what happens:

```sh
docker run --rm -p 4222:4222 nats:2.14.2 -js
go run ./examples/hello
make examples          # run them all
```

| Example | Shows |
|---|---|
| [hello](hello/main.go) | The smallest program: a two-step flow, a worker, start and wait; reading a previous node's output |
| [approval](approval/main.go) | Human in the loop: a task that interrupts with a question and is resumed with the answer; an await node with a timeout route; a choice on the signal |
| [research](research/main.go) | Parallel branches (fanout + quorum join) writing to shared state through reducer channels (append, merge, sum); a budget on a counter |
| [mapreduce](mapreduce/main.go) | A dynamic map over a list computed at run time, bounded parallelism, ordered results, reducers as the reduce step |
| [resilience](resilience/main.go) | Retries with backoff, per-attempt timeouts (the late result is ignored), output JSON Schema, permanent errors |
| [timetravel](timetravel/main.go) | History, state at any sequence, rerun of a failed node after a fix, fork from the past |
| [agentloop](agentloop/main.go) | A tool-using loop: a planner routing with dynamic edges, a message channel, a recursion limit and a call budget — no LLM required |
| [subflow](subflow/main.go) | A flow calling another flow; child output and counters flow back; cancelling the parent cancels the child |
| [events](events/main.go) | Executions started by a cron schedule and by messages on a subject (deduplicated by `Nats-Msg-Id`) |

Each example uses its own namespace (`example-<name>`), so they do not
interfere. While an example runs you can inspect it with the CLI and the UI:

```sh
go run ./cmd/packtrail -ns example-timetravel list
go run ./cmd/packtrail-ui -namespace example-timetravel
```
