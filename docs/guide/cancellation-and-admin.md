# Cancellation & admin

Every admin verb exists twice: typed on the job definition (`MyJob.cancel`, `MyJob.retry`, ...) and definition-free on the `JobStore` service, for dashboards that operate on jobs generically. Both routes go through the store, so lock tokens, attempt accounting, and wake-up notifications stay coherent — never mutate job rows directly.

## Cancelling a job

`cancel(id)` behaves by state:

| State | Effect |
| --- | --- |
| `waiting` / `delayed` | terminal `cancelled` immediately, with a `cancelled` ledger entry |
| `active` | `cancelRequested` is flagged in the store; the owning worker interrupts the handler fiber on its next heartbeat |
| terminal | fails with `JobNotCancellableError` |

Because handlers are Effect fibers, cancelling a *running* job is real: the worker interrupts the fiber, finalizers run, and the job is acked `Cancelled`. The flag rides on the lock heartbeat, so cancel latency for running jobs is at most one `lockRenewInterval` (default: half of `lockDuration`, so 15 seconds) — and it works across processes: cancel from your API server, interrupt on the worker machine.

```ts
import { Effect } from "effect"

const admin = Effect.gen(function*() {
  yield* GenerateInvoice.cancel(jobId)
  // waiting/delayed: already terminal. running: interrupted within one heartbeat.
})
```

Two races are pinned by the driver conformance suite:

- **Completion wins over a pending cancel.** If the handler finishes before the heartbeat delivers the cancel, the completion ack lands and the job is `completed` — a result that already exists is never clobbered.
- **Cancel wins over retries.** If the handler fails on its own while a cancel is pending, the retry ack is converted: the job lands `cancelled` instead of being revived for another attempt.

`awaitResult` treats a cancelled job as a defect (`JobCancelledError`), not a typed failure — cancellation is an operator action, not part of your error contract.

## Cancel by dedup key

When a job was enqueued under a [dedup key](/guide/deduplication), cancel whatever is pending without tracking job ids:

```ts
const wasPending = yield* SendInvite.cancelByKey(employeeId)
```

`cancelByKey` is idempotent by design: it returns `false` when no pending job holds the key, so "cancel it if anything is scheduled" needs no existence check. Pending states are cancelled exactly like `cancel` — waiting/delayed become terminal, active gets the heartbeat flag.

## Retry and promote

```ts
yield* GenerateInvoice.retry(jobId)     // failed -> waiting, fresh attempt budget
yield* GenerateInvoice.promote(jobId)   // delayed -> waiting now
```

`retry` is the verb behind a dashboard's "retry" button. It moves a `failed` job back to `waiting` with a fresh attempt budget — `attemptsMade` and `stalledCount` reset, terminal fields cleared — while the attempts ledger is preserved and keeps numbering monotonically: the first run after a manual retry of a 3-attempt job is attempt 4, not attempt 1. Calling it on a non-failed job fails with `JobNotRetryableError`.

`promote` moves a `delayed` job to `waiting` immediately; any other state fails with `JobNotPromotableError`.

## Pausing queues

Pause lives on the store service, not the worker — it is durable state that affects every worker on the store, and it survives restarts:

```ts
import { JobStore } from "effect-mq"

const ops = Effect.gen(function*() {
  const store = yield* JobStore.JobStore
  yield* store.pause(JobStore.QueueName("email"))    // claims return empty
  const paused = yield* store.pausedQueues()          // [QueueName("email")]
  yield* store.resume(JobStore.QueueName("email"))   // wakes idle workers immediately
})
```

While paused, claims return empty but producers are unaffected: enqueues still land, and delayed jobs still promote to `waiting` — they just are not handed out. `resume` wakes idle workers immediately rather than waiting for the poll fallback.

## The dashboard data layer

The store is queryable enough to back an ops UI without extra infrastructure. `list` is keyset-paginated, newest first (`enqueuedAt` desc, then id desc):

```ts
const page = yield* store.list({
  queue: JobStore.QueueName("billing"),
  name: "generate-invoice",
  states: ["failed"],
  metadata: { customerId },   // every entry must match exactly (AND semantics)
  limit: 50                   // the default
})
// page.items, plus page.cursor to pass back for the next page
```

| Method | Returns |
| --- | --- |
| `list(options)` | a page of `JobRecord`s and a `cursor` when more may exist |
| `counts(queue?)` | live depth: `Record<JobState, number>` |
| `getAttempts(id)` | the raw run ledger, oldest first (empty for unknown ids) |
| `remove(id)` | deletes a job and its ledger; returns `false` for active jobs |

Store-level records carry *encoded* payloads and exits; the typed equivalents live on the definition (`MyJob.poll`, `MyJob.attempts`). On Postgres you can also read the tables directly with drizzle — reads yes, writes no; see [Postgres](/storage/postgres).

## Errors

The definition-level verbs surface these as typed failures (`Effect.catchTag`-able); driver failures (`JobStoreError`) become defects there:

| Error | Raised by | When |
| --- | --- | --- |
| `JobNotFoundError` | `cancel` / `retry` / `promote` | the id does not exist |
| `JobNotCancellableError` | `cancel` | the job is already terminal |
| `JobNotRetryableError` | `retry` | the job is not `failed` |
| `JobNotPromotableError` | `promote` | the job is not `delayed` |
| `JobCancelledError` | `awaitResult` (as a defect) | the awaited job was cancelled |

All are tagged errors exported from the `JobStore` module.

## Where to next

- [Retries & timeouts](/guide/retries-and-timeouts) — automatic retry routing, the counterpart to manual `retry`.
- [Workers & handlers](/guide/workers) — heartbeats, locks, and the shutdown path.
- [Retention & history](/guide/retention) — how long cancelled/failed records stick around.
- [Deduplication](/guide/deduplication) — the key lifecycle behind `cancelByKey`.
