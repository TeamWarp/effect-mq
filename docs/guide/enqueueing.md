# Enqueueing

Producers call `MyJob.enqueue(payload, options?)`. It validates the payload against the schema, encodes it, and writes one record to the job's store. Enqueue needs only the store in context, never the `Worker`, so any process that can reach storage can produce.

```ts
import { Effect } from "effect"

const program = Effect.gen(function*() {
  const jobId = yield* GenerateInvoice.enqueue(
    { invoiceId: "inv-42", companyId: "acme" },
    { priority: 2, attempts: 3 }
  )
})
```

`enqueue` returns the `JobId`. Enqueueing an id that already exists, via an explicit `jobId` or the definition's `idempotencyKey`, is a silent no-op that returns the existing id.

## Options

Per-enqueue options override the definition's `defaults` per option:

| Option | Purpose |
| --- | --- |
| `jobId` | explicit id; overrides the definition's `idempotencyKey` |
| `queue` | send to a different queue than the definition's |
| `metadata` | queryable context, merged over the definition's `metadata` callback |
| `dedupe` | dedup key + mode; overrides the definition's; see [Deduplication](/guide/deduplication) |
| `delay` / `at` | when the job becomes runnable (mutually exclusive, below) |
| `priority` | higher runs first; ties are FIFO |
| `attempts` | total attempts including the first run |
| `backoff` | `{ type: "fixed" \| "exponential", delay, factor? }` between retries |
| `keep` | terminal-record retention; see [Retention](/guide/retention) |
| `timeout` | per-run limit; the worker interrupts the handler past it |

The full option matrix, including worker and store knobs, is in the [options reference](/reference/options).

## When it runs: `delay` vs `at`

Scheduling takes a relative `delay` (any `Duration.Input`) *or* an absolute `at`. The options type is a union, so setting both is a **compile error**. `at` accepts any `DateTime.Input`: a `Date`, ISO string, epoch millis, `{ year, month, ... }` parts, or a zoned `DateTime` for wall-clock instants in a timezone, so you skip the duration math:

```ts
import { DateTime } from "effect"

yield* SendInvite.enqueue({ userId }, { delay: "5 minutes" })

yield* SendInvite.enqueue({ userId }, {
  at: DateTime.makeZonedUnsafe(
    { year: 2026, month: 8, day: 24, hours: 9 },
    { timeZone: "America/New_York", adjustForTimeZone: true }
  )
})
```

An `at` in the past runs immediately (the delay clamps to zero). Delayed jobs sit in the `delayed` state until due; `MyJob.promote(id)` makes one runnable now, and a `dedupe` key with `replace: true` lets a later enqueue move a still-delayed job: the schedule/reschedule/cancel pattern in [Deduplication](/guide/deduplication). For recurring work, use durable schedules instead: [Repeatable jobs](/guide/repeatable-jobs).

## Batch enqueue: `enqueueMany`

`MyJob.enqueueMany(payloads, options?)` fans out a whole batch in one store round trip per chunk: a multi-row `INSERT` on Postgres, one Lua script on Redis (drivers chunk large batches). Ids come back aligned with the payloads:

```ts
const ids = yield* GenerateInvoice.enqueueMany(
  companies.map((company) => ({ invoiceId: company.nextInvoice, companyId: company.id })),
  { queue: "billing", at: nextBillingRun }   // shared options
)
```

Every item keeps full single-enqueue semantics: the definition's `idempotencyKey`, `dedupe`, and `metadata` callbacks run per payload, and duplicates are silent no-ops returning the existing id. Items whose definition derives a dedup key fall back to individual enqueues, in order; plain items ride the batch path.

Options apply batch-wide and exclude the per-job fields at the type level: `jobId` (a shared id would make every item after the first a duplicate) and `dedupe` (a shared key would collapse the batch into one job).

::: warning
The batch is **not** one transaction, by design: a mid-batch store failure can leave a subset (not necessarily a prefix) enqueued. That is safe under at-least-once. Re-running the batch skips already-inserted items when ids are deterministic via `idempotencyKey`; store-assigned ids may re-insert.
:::

## Reading results

Three producer verbs read a job back, all decoded through the definition's schemas:

- **`poll(jobId)`**: `Option<JobStatus>` carrying the current `state`, `attemptsMade`, `metadata`, the decoded `exit` (present for completed/failed jobs), and `failedReason`.
- **`awaitResult(jobId)`**: polls until terminal, then returns the typed success or fails with the typed error. It dies (defect, not typed failure) when the id does not exist, when the job was cancelled (`JobCancelledError`), or when the store itself failed the job without a handler exit (e.g. exhausted stalls). The default poll cadence backs off exponentially from 10ms and caps at 1 second; pass `{ pollSchedule }` to change it.
- **`execute(payload, options?)`**: `enqueue` + `awaitResult` in one call. Combined with `idempotencyKey`, a duplicate `execute` awaits the *existing* job's result.

```ts
const status = yield* GenerateInvoice.poll(jobId)      // Option<JobStatus>
const total = yield* GenerateInvoice.execute({ invoiceId, companyId })
```

## The attempts ledger

Every run (success, retry, failure, stall, cancellation) persists as a ledger entry. `MyJob.attempts(jobId)` returns them decoded, oldest first:

```ts
const runs = yield* GenerateInvoice.attempts(jobId)
// [{ attempt: 1, startedAt, finishedAt, outcome: "retried", exit: Option<Exit<...>> }, ...]
```

Each entry carries the attempt number, `startedAt`/`finishedAt` timestamps, an `outcome` (`completed`, `retried`, `failed`, `stalled`, `cancelled`), and the decoded typed exit (absent for `stalled` and `cancelled` entries). The ledger survives `retry`: a re-run appends, never rewrites, so it is the durable per-run history a dashboard renders.

## Where to next

- [Deduplication](/guide/deduplication): throttle, debounce, and replace via dedup keys.
- [Repeatable jobs](/guide/repeatable-jobs): durable cron/interval schedules.
- [Workers & handlers](/guide/workers): how enqueued jobs get claimed and run.
- [Options reference](/reference/options): every knob in one table.
