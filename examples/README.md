# Packtrail examples

Each example is a self-contained `main` package with its own flow definitions
and NATS namespace, so they can run against the same server without colliding.
All of them need a reachable **NATS Server 2.12+ with JetStream enabled**:

```sh
nats-server -js
```

Run the examples **from the repo root** (two of them load flow YAML from a
relative `flows/` directory) and pass `--nats` if the server is not on
`nats://127.0.0.1:4222`.

| Example | Namespace | Shows |
|---|---|---|
| [`embedded`](embedded/) | `acme` | Engine + built-in nats-task workers in one process: `WithFlowsDir`, `Server.Handle`, fanout/fanin, a signal gate, reconcile schedules, `Server.Results`. |
| [`worker`](worker/) | `packtrail` | A standalone external task service speaking `pkg/protocol` — no engine import. Run it next to `embedded` (same `--namespace`) and the two load-balance the same subjects via the shared queue group. |
| [`custom-invoker`](custom-invoker/) | `agents` | The pluggable Invoker seam: `WithInvoker("agent", …)` drives task nodes by target name, a choice node routes on the triage agent's output (`results.triage.category`), `WithResultCache` dedupes redelivered side effects. |
| [`async`](async/) | `async-demo` | Long-running work off the critical path: `WithAsyncInvoker` + `invoker/asyncqueue` run slow fanout branches on a durable work-queue while the execution parks as `waiting`; flow built as a Go-struct `FlowDef`. |
| [`approval`](approval/) | `orders` | Human-in-the-loop: a `signal` node with an `on_timeout` fallback, `Server.Signal` to approve, `Server.Cancel` to abandon, and `WithHistory` + `Server.History` for the durable step-by-step trace. |

```sh
go run ./examples/embedded        # engine + in-process workers, one execution
go run ./examples/worker         --namespace acme   # optional: external workers for the run above
go run ./examples/custom-invoker  # two executions routed by a simulated agent
go run ./examples/async           # slow branches on the async work-queue
go run ./examples/approval        # one approved order, one cancelled order
```

To watch any of them in the dashboard, point packtrail-ui at the example's
namespace:

```sh
go run ./cmd/packtrail-ui --namespace agents --addr :8088
```
