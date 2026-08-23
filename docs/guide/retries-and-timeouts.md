# Retries & timeouts

Failures are routed by two pieces of per-job configuration — an attempt budget and a backoff policy — plus a per-run `timeout`. All three live on the job record in the store, so any worker routes a retry the same way, and every run lands in a durable ledger you can decode back to typed exits.

```ts
import { Job } from "effect-mq"
import { Schema } from "effect"

class GenerateInvoice extends Job.make("generate-invoice", {
  payload: { invoiceId: Schema.String },
  error: InvoiceError,
  retryable: (e) => e.reason !== "invoice-voided",
  defaults: {
    attempts: 5,
    backoff: { type: "exponential", delay: "1 second" },
    timeout: "2 minutes"
  }
}) {}
```

## The attempt budget

`attempts` is the **total number of runs including the first** — the default of 1 means no retries. Set it in the definition's `defaults` or override it per enqueue. When a run fails with budget remaining, the worker writes a `retried` entry to the ledger and hands the job back to the store with its backoff delay; when the budget is exhausted, the run is recorded as `failed` and the job lands in the terminal `failed` state.

A retry is not held by the failing worker. The job goes back to `delayed` (or straight to `waiting` for zero backoff) and is re-claimed after the delay by *any* worker on the store — no lock or worker slot is occupied while waiting, and the original worker can die in between without losing the retry.

An exhausted budget is not the end: `GenerateInvoice.retry(jobId)` grants a fresh budget while preserving the ledger — see [Cancellation & admin](/guide/cancellation-and-admin).

## Backoff

`backoff` sets the delay between a failed run and its retry:

| Policy | Delay after the n-th run fails |
| --- | --- |
| `{ type: "fixed", delay }` | `delay` |
| `{ type: "exponential", delay, factor? }` | `delay × factor^(n−1)` — `factor` defaults to 2 |

So `attempts: 4` with `{ type: "exponential", delay: "1 second" }` waits 1s, 2s, then 4s between runs. Without a `backoff`, retries are immediate. The policy is persisted on the record, so the worker that claims the retry does not need the definition's defaults to route the next failure consistently.

## Timeouts

`timeout` is a per-run limit. Because handlers are Effect fibers, exceeding it **interrupts the handler cleanly** — finalizers run, resources release — and the run is recorded as a `TimeoutError` defect in the ledger, consuming an attempt like any other failure. Retry accounting then proceeds normally: backoff, budget, terminal `failed` at exhaustion.

```ts
const RunnerLive = GenerateInvoice.toLayer((payload) =>
  renderInvoice(payload.invoiceId).pipe(
    Effect.onInterrupt(() => releaseRenderSlot(payload.invoiceId))
  )
)
```

Two edges worth knowing: a handler that fails with its *own* typed `TimeoutError` stays a typed failure (the runtime's limit surfaces as a defect, not by tag), and a handler that interrupts itself is treated as a failed attempt rather than a shutdown — otherwise the job would re-run in a hot loop.

## Skipping the budget

When retrying cannot help, spend zero further attempts. Two mechanisms, both routing the job straight to `failed`:

- **`Job.unrecoverable(error)`** — mark a specific failure inside the handler; the error value is returned unchanged, so your typed error channel and schema are untouched:

```ts
Effect.fail(Job.unrecoverable(new InvoiceError({ reason: "customer deleted" })))
```

- **The `retryable` predicate** — declared on the definition; a typed failure for which it returns `false` skips the remaining budget. A predicate that throws is treated as retryable, so a bug in it can never leave a job un-acked.

::: tip
`unrecoverable` marks the error by identity, so it only works for object errors — a primitive (string, number) failure retries normally. Use `retryable` for those.
:::

## Stalled runs

When a worker dies mid-run, the [stalled sweep](/guide/workers#stalled-recovery) recovers the job: the ledger gets an entry with outcome `stalled` (and no exit — nothing was acked), and the job's stall counter increments. Stalls deliberately do not consume the retry budget — the handler may never have misbehaved — but a job exceeding the worker's `maxStalledCount` is failed outright with a `failedReason` and no exit, so `awaitResult` surfaces it as a defect rather than a typed failure.

## Typed errors and the ledger

Give the definition an `error` schema and handler failures round-trip through storage: exits are persisted schema-encoded, and the producer-side verbs decode them back.

```ts
const status = yield* GenerateInvoice.poll(jobId)      // Option<JobStatus> — exit is Option<Exit<A, E>>
const runs = yield* GenerateInvoice.attempts(jobId)    // the decoded run ledger, oldest first
const result = yield* GenerateInvoice.awaitResult(jobId) // fails with the typed InvoiceError
```

Each `JobAttempt` carries a 1-based `attempt` number (monotonic per job — it survives `retry`), `startedAt`/`finishedAt` timestamps, an `outcome`, and the decoded exit:

| Outcome | Meaning | Exit |
| --- | --- | --- |
| `completed` | the handler succeeded | present |
| `retried` | failed with budget remaining | present |
| `failed` | budget exhausted or unrecoverable | present |
| `stalled` | lock expired; recovered by the sweep — also the final entry when the stall limit fails the job | absent |
| `cancelled` | a cancel request landed | absent |

Everything the schema can express — `Redacted`, `DateTime`, branded types, tagged error unions — comes back as real instances, not stored JSON.

## Where to next

- [Cancellation & admin](/guide/cancellation-and-admin) — `retry`, `cancel`, `promote`, pause/resume.
- [Workers & handlers](/guide/workers) — lock heartbeats and stall configuration.
- [Observability](/guide/observability) — the `job_runs` outcome counter and duration histograms.
- [Options reference](/reference/options) — every retry-related knob in one place.
