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

A flow parent registers two handlers:

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

`fanOut` runs on the parent's first claim and returns the children (one `Flow.children` group, or an array of groups for flows with several child types). The store persists the child manifest and parks the parent in the `waiting-children` state. Once every child settles, the parent becomes runnable again and `collect` runs with the results.

The fan-out ack also writes a `flow` marker onto the parent record, and a re-claimed parent carrying the marker runs `collect` instead of `fanOut`. A crash after the manifest lands can never fan out twice.

Both phases draw on the parent's attempt budget. A failing `fanOut` retries until the manifest lands; a failing `collect` retries with whatever budget remains. The fan-out itself consumes no attempt.

`Flow.toLayer` requires the parent's store **and every child's store** in context, which makes the worker running the parent phases the one process guaranteed able to repair and cancel work across the whole flow (see [reconciliation](#how-it-stays-correct) below).

## Results come back through the outbox

The child's store appends the child's report to its **outbox** in the same atomic operation as every terminal child transition: a completed or failed ack, a cancel, stall exhaustion. Workers then run a relay that drains the outbox into the parent stores in batches. Declaring the flow gives a worker those parent stores:

```ts
const EmailWorker = SendEmail.toLayer(handleEmail).pipe(
  Layer.provide(Worker.layer({ store: EmailStore, flows: [DigestFlow] }))
)
```

The relay drains the moment a child result is acked, so results reach the parent with no added latency; under load they batch into single parent-store writes. The ack and the report share one atomic write, so a crash cannot separate them. Children keep completing while the parent store is down; their reports queue in the outbox for the relay.

`flows` affects latency alone. A worker without the registration runs children and records their reports all the same; the reports wait for a relay on another worker, or for the parent-side sweeper to read the child's state directly. `Flow.toLayer` registers the parent worker's relay on its own store, so same-store flows need no `flows` entry.

## Children and idempotency

The child `key` is the idempotency mechanism. Each child gets a deterministic id derived from the parent store, the flow id, and the key, so a re-run of `fanOut` after a crash re-produces the same children rather than a second batch. Duplicate keys within one fan-out fail the parent without retries: retrying a deterministic bug burns budget for nothing.

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
- **`"fail"`**: the first failed child settles the parent as `failed`. The `failedReason` names the child; there is no parent exit, so `awaitResult` dies (the same shape as stall exhaustion), and the failed child's own exit stays inspectable through `childResults`. The settle also cancels the remaining children: it marks still-pending rows in the same atomic operation, and the flow sweeper delivers real cancels into the child stores, interrupting running handlers. `collect` does not run.

An admin `retry` of a failed flow parent keeps the manifest and re-enters `collect` with the recorded (mixed) results. A parent whose `fanOut` never landed retries `fanOut` as usual.

Cancelling a flow parent while it waits (`DigestFlow.cancel(flowId)`) settles it the same way: the same cascade cancels the remaining children, and late child reports drop.

## Nesting

A child may itself be another flow's parent: fan out groups of work, let each group fan out its items, and collect upward level by level. A settling inner flow reports to its outer parent through the same outbox machinery, and cancelling the outer flow cascades down through each level:

```ts
const InnerFlow = Flow.make("send-group", { parent: SendBatch, children: [SendEmail] })
const OuterFlow = Flow.make("daily-digest", { parent: SendDigest, children: [SendBatch] })
```

Nothing can cycle-check the definitions statically (a job does not know which flows parent it), so a depth cap of 8 backstops cycles: a cyclic definition fails its fan-out with a clear error instead of recursing forever.

## How it stays correct

The parent's store owns the flow: the manifest, per-child results, and the outcome counters live there, so "the flow settles exactly once" is a single-store atomic decision even when children live elsewhere. Cross-store coordination then needs only two at-least-once, idempotent mechanisms:

1. **The outbox** (push path): the child's terminal transition and its report are one atomic write in the child's store; relays deliver the reports in batches and delete what landed. A relay crash means redelivery, and redelivery is free: the dependency row dedups it.
2. **Reconciliation** (repair path): every parent worker runs a flow sweeper (default every 30 seconds, `Worker.layer({ flowSweepInterval })`). For each child still pending past the sweep age, the sweeper checks the child's store directly: it (re-)enqueues a missing child from the persisted spec (covering crashes between the fan-out ack and the enqueue) and synthesizes a terminal child's report from the child store's own record (covering outbox entries no relay can reach). It leaves in-flight children alone, and sweep pages rotate, so a large flow's healthy children never starve another flow's repair work.

Every step dedups: enqueues on the deterministic id, reports on the dependency row, cancels on child state, outbox deletes on entry ids. A crash anywhere leaves work the next relay pass or sweep finishes.

::: warning Child retention
The parent store keeps its own copy of every result, so child stores can prune terminal children on a short clock, with one floor: keep terminal children longer than the flow sweep interval (with margin), or the sweeper can find a pruned, never-reported child missing and re-run it. `keep: { completed: { age: "1 hour" } }` is comfortable against the default 30-second sweep.
:::

## Scale and observability

Fan-out inserts dependency rows and enqueues children in chunks, so ten-thousand-child flows work out of the box. Reports batch: a relay under load delivers hundreds of child results in one parent-store write, so the parent-row serialization that bounds per-flow throughput amortizes across the batch instead of costing one transaction per child. `collect` chooses its own memory profile: `counts` for tallies, `all` to materialize, `stream` to fold page by page.

One Redis caveat: a fail-fast settle or parent cancel marks every remaining child inside one atomic script, and at very large fan-outs that script holds the whole (single-threaded) server while it runs. If your fan-outs run far past ten thousand children, put the parent on Postgres.

Child spans attach to the fan-out run's span context. For large fan-outs, one trace with ten thousand children renders badly; consider `traceLinking: "link"` on the child workers.

Metrics: `flow_fanouts`, `flow_child_reports` (tagged by `outcome` and `source: report | reconcile`), `flow_cascades`, and `flow_outbox_skipped`. A nonzero `reconcile` share is normal during deploys; a sustained one, or a growing `flow_outbox_skipped`, means no worker with the right `flows` registration is reaching those child stores.

## Where to next

- [Workers](/guide/workers): the maintenance loops behind the sweeper, heartbeats, and stall recovery.
- [Repeatable jobs](/guide/repeatable-jobs): put a flow parent on a cron.
- [Retries & timeouts](/guide/retries-and-timeouts): the budget both phases draw on.
- [Reference: options](/reference/options): every flow, enqueue, and worker knob in one table.
