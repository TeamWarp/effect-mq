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
      sent: results.completed.length,
      failed: results.failed.length
    })
})
```

`fanOut` runs when the parent is first claimed and returns the children (one `Flow.children` group, or an array of groups for flows with several child types). The store persists the child manifest and parks the parent in the `waiting-children` state. Once every child settles, the parent becomes runnable again and `collect` runs with the results.

Phase dispatch is persisted rather than inferred: the fan-out ack writes a `flow` marker onto the parent record, and a re-claimed parent with the marker runs `collect`. A crash after the manifest lands can never fan out twice.

Both phases draw on the parent's attempt budget. A failing `fanOut` retries until the manifest lands; a failing `collect` retries with whatever budget remains. The fan-out itself consumes no attempt.

`Flow.toLayer` requires the parent's store **and every child's store** in context. That requirement is the design: the worker running the parent phases is the one process guaranteed able to repair and cancel work across the whole flow (see [reconciliation](#how-it-stays-correct) below).

## Child workers report back

Any worker that runs a flow's children declares the flow:

```ts
const EmailWorker = SendEmail.toLayer(handleEmail).pipe(
  Layer.provide(Worker.layer({ store: EmailStore, flows: [DigestFlow] }))
)
```

`flows` gives the worker the parent store, and the worker writes each child's terminal result into it *before* acking the child. A worker that claims a flow child without the registration fails it with a clear message instead of silently looping; the flow sweeper converts that failure into a failed report, so the parent learns about the misconfiguration instead of hanging. Workers that run the parent phases via `Flow.toLayer` get reporting rights on their own store for free, so same-store flows need no `flows` entry.

## Children and idempotency

The child `key` is the idempotency mechanism. Each child gets the deterministic id `flow/<parentStoreKey>/<flowId>/<childKey>`, so a re-run of `fanOut` after a crash re-produces the same children rather than a second batch. Duplicate keys within one fan-out fail the parent immediately (no retry burn: it is a deterministic bug).

Because the key carries identity, the child definition's `idempotencyKey` and `dedupe` callbacks **do not apply** to flow children. Per-child `options` accept the usual knobs (`priority`, `attempts`, `backoff`, `keep`, `timeout`, `metadata`); children run immediately, so there is no `delay`.

Child handlers need no flow awareness. They are plain jobs with a `parent` envelope on the record, retried by their own budget like any other job.

## Typed results

`collect` receives the settled children bucketed by outcome, decoded through each child's schemas. The `name` field discriminates child types in multi-child flows:

```ts
collect: (payload, results) =>
  Effect.gen(function*() {
    for (const child of results.completed) {
      // child.value: the child's decoded success value
    }
    for (const child of results.failed) {
      // child.cause: Cause of the child's typed error. Store-side failures
      // (stall exhaustion, unreportable workers) surface as defects with
      // the store's failedReason.
    }
    return { sent: results.completed.length, failed: results.failed.length }
  })
```

Outside the handler, `DigestFlow.childResults(flowId)` returns the same buckets for dashboards and debugging, in any parent state; children that have not settled yet are absent from all three buckets.

## Failure policy

`onChildFailure` decides what a failed child does to the flow:

- **`"continue"`** (default): every child settles, and `collect` sees the failures in `results.failed`. The flow completes with whatever `collect` returns.
- **`"fail"`**: the first failed child settles the parent as `failed` immediately. The `failedReason` names the child; there is no parent exit, so `awaitResult` dies (the same shape as stall exhaustion), and the failed child's own exit stays inspectable through `childResults`. The remaining children are cancelled: still-pending rows are marked in the same atomic settle, and the flow sweeper delivers real cancels into the child stores, interrupting running handlers. `collect` does not run.

An admin `retry` of a failed flow parent keeps the manifest and re-enters `collect` with the recorded (mixed) results. A parent whose `fanOut` never landed retries `fanOut` as usual.

Cancelling a flow parent while it waits (`DigestFlow.cancel(flowId)`) settles it the same way: remaining children are cancelled and cascaded, and late child reports drop.

## How it stays correct

The parent's store owns the flow: the manifest, per-child results, and the pending counter live there, so "the flow settles exactly once" is a single-store atomic decision even when children live elsewhere. Cross-store coordination then needs only two at-least-once, idempotent mechanisms:

1. **Reports** (fast path): the child's worker writes the result into the parent store before acking the child. If the parent store is down, the child is never acked; it stalls and re-runs, and the duplicate report drops on the recorded row.
2. **Reconciliation** (repair path): every parent worker runs a flow sweeper (default every 30 seconds, `Worker.layer({ flowSweepInterval })`). For each child still pending past the sweep age it checks the child's store directly: a missing child is (re-)enqueued from the persisted spec, covering crashes between the fan-out ack and the enqueue; a terminal child gets its report synthesized from the child store's own record, covering stall-exhausted children, direct cancels, and misconfigured workers. In-flight children are left alone.

Every step dedups: enqueues on the deterministic id, reports on the dependency row, cancels on child state. A crash anywhere leaves work the next sweep finishes.

::: warning Child retention
Results are copied into the parent store, so child stores can prune terminal children aggressively, with one floor: keep terminal children longer than the flow sweep interval (with margin), or the sweeper can find a pruned, never-reported child missing and re-run it. `keep: { completed: { age: "1 hour" } }` is comfortable against the default 30-second sweep.
:::

## Scale and observability

Fan-out inserts dependency rows and enqueues children in chunks, so ten-thousand-child flows work out of the box. Each child completion writes one report into the parent store; the dependency rows update in parallel, while the pending-counter decrement serializes on the parent row. That lock is held sub-millisecond, so thousands of reports per second through one flow are fine, and it is a per-flow ceiling rather than a store-wide one.

Child spans attach to the fan-out run's span context. For large fan-outs, one trace with ten thousand children renders badly; consider `traceLinking: "link"` on the child workers.

Metrics: `flow_fanouts`, `flow_child_reports` (tagged by `outcome` and `source: report | reconcile`), `flow_cascades`, and `flow_unreportable_children`. A nonzero `reconcile` share is normal during deploys; a sustained one means child workers cannot reach the parent store.

## Where to next

- [Workers](/guide/workers): the maintenance loops behind the sweeper, heartbeats, and stall recovery.
- [Repeatable jobs](/guide/repeatable-jobs): put a flow parent on a cron.
- [Retries & timeouts](/guide/retries-and-timeouts): the budget both phases draw on.
- [Reference: options](/reference/options): every flow, enqueue, and worker knob in one table.
