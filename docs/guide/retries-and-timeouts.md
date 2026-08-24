# Retries & timeouts

Per-job configuration routes failures: an attempt budget and a backoff policy, plus a per-run `timeout`. All three live on the job record in the store, so any worker routes a retry the same way, and every run lands in a durable ledger you can decode back to typed exits.

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

`attempts` is the **total number of runs including the first**: the default of 1 means no retries. Set it in the definition's `defaults` or override it per enqueue. When a run fails with budget remaining, the worker writes a `retried` entry to the ledger and hands the job back to the store with its backoff delay; when the budget is exhausted, the worker records the run as `failed` and the job lands in the terminal `failed` state.

The failing worker does not hold the retry. The job goes back to `delayed` (or straight to `waiting` for zero backoff) and *any* worker on the store re-claims it after the delay: the wait occupies no lock or worker slot, and the original worker can die in between without losing the retry.

An exhausted budget stays repairable: `GenerateInvoice.retry(jobId)` grants a fresh budget while preserving the ledger. See [Cancellation & admin](/guide/cancellation-and-admin).

## Backoff

`backoff` sets the delay between a failed run and its retry:

| Policy | Delay after the n-th run fails |
| --- | --- |
| `{ type: "fixed", delay }` | `delay` |
| `{ type: "exponential", delay, factor? }` | `delay × factor^(n−1)`; `factor` defaults to 2 |

`attempts: 4` with `{ type: "exponential", delay: "1 second" }` waits 1s, 2s, then 4s between runs. Without a `backoff`, retries are immediate. The record carries the policy, so the worker that claims the retry routes the next failure the same way without needing the definition's defaults.

## Timeouts

`timeout` is a per-run limit. Because handlers are Effect fibers, exceeding it **interrupts the handler cleanly** (finalizers run, resources release), and the worker records the run as a `TimeoutError` defect in the ledger, consuming an attempt like any other failure. Retry accounting then proceeds as usual: backoff, budget, terminal `failed` at exhaustion.

```ts
const RunnerLive = GenerateInvoice.toLayer((payload) =>
  renderInvoice(payload.invoiceId).pipe(
    Effect.onInterrupt(() => releaseRenderSlot(payload.invoiceId))
  )
)
```

Two edge cases: a handler that fails with its *own* typed `TimeoutError` stays a typed failure (the runtime's limit surfaces as a defect rather than by tag), and the worker treats a handler that interrupts itself as a failed attempt rather than a shutdown; otherwise the job would re-run in a hot loop.

## Skipping the budget

When retrying cannot help, spend zero further attempts. Two mechanisms, both routing the job straight to `failed`:

- **`Job.unrecoverable(error)`**: mark a specific failure inside the handler; the call returns the error value unchanged, so your typed error channel and schema stay intact:

```ts
Effect.fail(Job.unrecoverable(new InvoiceError({ reason: "customer deleted" })))
```

- **The `retryable` predicate**: declared on the definition; a typed failure for which it returns `false` skips the remaining budget. The worker treats a predicate that throws as retryable, so a bug in it can never leave a job un-acked.

::: tip
`unrecoverable` marks the error by identity, so it only works for object errors: a primitive (string, number) failure retries as usual. Use `retryable` for those.
:::

## Stalled runs

When a worker dies mid-run, the [stalled sweep](/guide/workers#stalled-recovery) recovers the job: the ledger gets an entry with outcome `stalled` (and no exit, since no worker acked the run), and the job's stall counter increments. Stalls do not consume the retry budget, since the handler may never have misbehaved, but the worker fails a job exceeding `maxStalledCount` outright with a `failedReason` and no exit, so `awaitResult` surfaces it as a defect rather than a typed failure.

## Typed errors and the ledger

Give the definition an `error` schema and handler failures round-trip through storage: the store persists exits schema-encoded, and the producer-side verbs decode them back.

```ts
const status = yield* GenerateInvoice.poll(jobId)      // Option<JobStatus>; exit is Option<Exit<A, E>>
const runs = yield* GenerateInvoice.attempts(jobId)    // the decoded run ledger, oldest first
const result = yield* GenerateInvoice.awaitResult(jobId) // fails with the typed InvoiceError
```

Each `JobAttempt` carries a 1-based `attempt` number (monotonic per job; it survives `retry`), `startedAt`/`finishedAt` timestamps, an `outcome`, and the decoded exit:

| Outcome | Meaning | Exit |
| --- | --- | --- |
| `completed` | the handler succeeded | present |
| `retried` | failed with budget remaining | present |
| `failed` | budget exhausted or unrecoverable | present |
| `stalled` | lock expired; recovered by the sweep, and also the final entry when the stall limit fails the job | absent |
| `cancelled` | a cancel request landed | absent |

Everything the schema can express (`Redacted`, `DateTime`, branded types, tagged error unions) comes back as real instances rather than stored JSON.

## Where to next

- [Cancellation & admin](/guide/cancellation-and-admin): `retry`, `cancel`, `promote`, pause/resume.
- [Workers & handlers](/guide/workers): lock heartbeats and stall configuration.
- [Observability](/guide/observability): the `job_runs` outcome counter and duration histograms.
- [Options reference](/reference/options): every retry-related knob in one place.
