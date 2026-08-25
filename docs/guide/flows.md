# Parent-child flows

A flow is a parent job that fans out N child jobs, parks until all of them settle, then resumes with their typed results. The children can live on a **different store** than the parent: a cron-scheduled parent in Postgres can fan out ten thousand idempotent email sends into Redis and collect the outcomes back in Postgres.

```ts
import { Effect, Schema } from "effect"
import { Flow, Job, JobStore } from "effect-mq"

const EmailStore = JobStore.named("emails")     // Redis in prod

class SendEmail extends Job.make("SendEmail", {
  payload: { userId: Schema.String },
  success: Schema.String,
  queue: "email",
  store: EmailStore
}) {}

class SendDigest extends Job.make("SendDigest", {
  payload: { edition: Schema.String },
  success: Schema.Struct({ sent: Schema.Number, failed: Schema.Number })
}) {}                                           // default store: Postgres in prod

const DigestFlow = Flow.make("daily-digest", {
  parent: SendDigest,
  children: [SendEmail],
  onChildFailure: "continue"                    // default; or "fail"
})
```

The producer surface is the parent's: `DigestFlow.enqueue`, `.execute`, `.poll`, `.awaitResult`, `.cancel`, and `.schedule` all delegate to `SendDigest`. A flow parent is a real job row in the parent's store, so a cron schedule needs no flow awareness; the flow behavior comes from the registered handler.

## The two-phase handler

Instead of one handler, the parent registers two:

```ts
const DigestWorker = DigestFlow.toLayer({
  fanOut: (payload) =>
    Effect.gen(function*() {
      const users = yield* Users.active
      return Flow.children(SendEmail, users.map((user) => ({
        key: user.id,                           // unique within the flow
        payload: { userId: user.id }
      })))
    }),

  collect: (payload, results) =>
    Effect.succeed({
      sent: results.counts.completed,
      failed: results.counts.failed
    })
})
```

`fanOut` runs when the parent is first claimed and returns the children (one `Flow.children` group, or an array of groups for flows with several child types). The store persists the child manifest and parks the parent in the `waiting-children` state. Once every child settles, the parent becomes runnable again and `collect` runs with the results.

Phase dispatch is persisted rather than inferred: the fan-out ack writes a `flow` marker onto the parent record, and a re-claimed parent with the marker runs `collect`. A crash after the manifest lands can never fan out twice.

Both phases draw on the parent's attempt budget. A failing `fanOut` retries until the manifest lands; a failing `collect` retries with whatever budget remains. The fan-out itself consumes no attempt.

`Flow.toLayer` requires the parent's store **and every child's store** in context. That requirement is the design: the worker running the parent phases is the one process guaranteed able to repair and cancel work across the whole flow (see [reconciliation](#how-it-stays-correct) below).

## Results flow back through the outbox

Every terminal child transition — a completed or failed ack, a cancel, even stall exhaustion — appends the child's report to its own store's **outbox**, atomically with the transition itself. Workers then run a relay that drains the outbox into the parent stores in batches. Declaring the flow is what gives a worker those parent stores:

```ts
const EmailWorker = SendEmail.toLayer(handleEmail).pipe(
  Layer.provide(Worker.layer({ store: EmailStore, flows: [DigestFlow] }))
)
```

The relay drains the moment a child result is acked, so results reach the parent with no added latency; under load they batch into single parent-store writes. Because the report is written in the same atomic operation as the ack, nothing can be lost between them — and children keep completing even while the parent store is down, with their reports queuing up for the relay.

`flows` is a latency knob, not a correctness one. A worker without the registration still runs children and still records their reports; they just wait for a relay on another worker or for the parent-side sweeper to read the child's state directly. Workers running the parent phases via `Flow.toLayer` relay on their own store automatically, so same-store flows need no `flows` entry.

## Children and idempotency

The child `key` is the idempotency mechanism. Each child gets the deterministic id `flow/<parentStoreKey>/<flowId>/<childKey>`, so a re-run of `fanOut` after a crash re-produces the same children rather than a second batch. Duplicate keys within one fan-out fail the parent immediately (no retry burn: it is a deterministic bug).

Because the key carries identity, the child definition's `idempotencyKey` and `dedupe` callbacks **do not apply** to flow children. Per-child `options` accept the usual knobs (`priority`, `attempts`, `backoff`, `keep`, `timeout`, `metadata`); children run immediately, so there is no `delay`.

Child handlers need no flow awareness. They are plain jobs with a `parent` envelope on the record, retried by their own budget like any other job.

## Typed results

`collect` receives three views of the settled children, each decoded through the members' schemas (`name` discriminates child types in multi-child flows):

```ts
collect: (payload, results) =>
  Effect.gen(function*() {
    // counts: plain numbers off the parent record — zero reads.
    if (results.counts.failed === 0) {
      return { sent: results.counts.completed, failed: 0 }
    }

    // all: the materialized buckets — fine into the tens of thousands.
    const settled = yield* results.all
    for (const child of settled.failed) {
      // child.cause: Cause of the child's typed error. Store-side failures
      // (stall exhaustion) surface as defects with the store's failedReason.
    }

    // stream: one page at a time, for flows too large to materialize.
    yield* Stream.runForEach(results.stream, (child) =>
      child.outcome === "completed" ? recordDelivery(child.value) : Effect.void
    )
    return { sent: results.counts.completed, failed: results.counts.failed }
  })
```

Outside the handler, `DigestFlow.childResults(flowId)` returns the same accessors for dashboards and debugging, in any parent state; children still pending count in `counts.pending` and are absent from `all`/`stream`.

## Failure policy

`onChildFailure` decides what a failed child does to the flow:

- **`"continue"`** (default): every child settles, and `collect` sees the failures in its results. The flow completes with whatever `collect` returns.
- **`"fail"`**: the first failed child settles the parent as `failed` immediately. The `failedReason` names the child; there is no parent exit, so `awaitResult` dies (the same shape as stall exhaustion), and the failed child's own exit stays inspectable through `childResults`. The remaining children are cancelled: still-pending rows are marked in the same atomic settle, and the flow sweeper delivers real cancels into the child stores, interrupting running handlers. `collect` does not run.

An admin `retry` of a failed flow parent keeps the manifest and re-enters `collect` with the recorded (mixed) results. A parent whose `fanOut` never landed retries `fanOut` as usual.

Cancelling a flow parent while it waits (`DigestFlow.cancel(flowId)`) settles it the same way: remaining children are cancelled and cascaded, and late child reports drop.

## Nesting

A child may itself be another flow's parent. Fan out groups of work, let each group fan out its items, and collect upward level by level — an inner flow that settles reports to its outer parent through the same outbox machinery, and cancelling the outer flow cascades down through each level:

```ts
const InnerFlow = Flow.make("send-group", { parent: SendBatch, children: [SendEmail] })
const OuterFlow = Flow.make("daily-digest", { parent: SendDigest, children: [SendBatch] })
```

Definitions cannot be cycle-checked statically (a job does not know which flows parent it), so depth is capped at 8: a cyclic definition fails its fan-out with a clear error instead of recursing forever.

## How it stays correct

The parent's store owns the flow: the manifest, per-child results, and the outcome counters live there, so "the flow settles exactly once" is a single-store atomic decision even when children live elsewhere. Cross-store coordination then needs only two at-least-once, idempotent mechanisms:

1. **The outbox** (push path): the child's terminal transition and its report are one atomic write in the child's store; relays deliver the reports in batches and delete what landed. A relay crash means redelivery, and redelivery is free — the dependency row dedups it.
2. **Reconciliation** (repair path): every parent worker runs a flow sweeper (default every 30 seconds, `Worker.layer({ flowSweepInterval })`). For each child still pending past the sweep age it checks the child's store directly: a missing child is (re-)enqueued from the persisted spec, covering crashes between the fan-out ack and the enqueue; a terminal child gets its report synthesized from the child store's own record, covering outbox entries no relay can reach. In-flight children are left alone, and each sweep page rotates, so a large flow's healthy children never starve another flow's repair work.

Every step dedups: enqueues on the deterministic id, reports on the dependency row, cancels on child state, outbox deletes on entry ids. A crash anywhere leaves work the next relay pass or sweep finishes.

::: warning Child retention
Results are copied into the parent store, so child stores can prune terminal children aggressively, with one floor: keep terminal children longer than the flow sweep interval (with margin), or the sweeper can find a pruned, never-reported child missing and re-run it. `keep: { completed: { age: "1 hour" } }` is comfortable against the default 30-second sweep.
:::

## Scale and observability

Fan-out inserts dependency rows and enqueues children in chunks, so ten-thousand-child flows work out of the box. Reports batch: a relay under load delivers hundreds of child results in one parent-store write, so the parent-row serialization that bounds per-flow throughput amortizes across the batch instead of costing one transaction per child. `collect` chooses its own memory profile — `counts` for tallies, `all` to materialize, `stream` to fold page by page.

One Redis caveat: a fail-fast settle or parent cancel marks every remaining child inside one atomic script, which briefly holds the whole (single-threaded) server at very large fan-outs. If you routinely fan out far past ten thousand children, put the parent on Postgres.

Child spans attach to the fan-out run's span context. For large fan-outs, one trace with ten thousand children renders badly; consider `traceLinking: "link"` on the child workers.

Metrics: `flow_fanouts`, `flow_child_reports` (tagged by `outcome` and `source: report | reconcile`), `flow_cascades`, and `flow_outbox_skipped`. A nonzero `reconcile` share is normal during deploys; a sustained one — or a growing `flow_outbox_skipped` — means no worker with the right `flows` registration is reaching those child stores.

## Where to next

- [Workers](/guide/workers): the maintenance loops behind the sweeper, heartbeats, and stall recovery.
- [Repeatable jobs](/guide/repeatable-jobs): put a flow parent on a cron.
- [Retries & timeouts](/guide/retries-and-timeouts): the budget both phases draw on.
- [Reference: options](/reference/options): every flow, enqueue, and worker knob in one table.
