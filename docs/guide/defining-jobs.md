# Defining jobs

A job definition is a class produced by `Job.make(name, options)`: a unique name (the `_tag`), a schema-typed payload, and everything both sides of the contract need. Producers get `enqueue`/`execute`/`poll`; runners get `toLayer`. Define it once in shared code; the name is also the persistence key, so keep it stable.

```ts
import { Job } from "effect-mq"
import { Schema } from "effect"

class GenerateInvoice extends Job.make("generate-invoice", {
  payload: { invoiceId: Schema.String, companyId: Schema.String },
  success: Schema.String,
  error: InvoiceError,
  idempotencyKey: ({ invoiceId }) => invoiceId,
  metadata: ({ companyId }) => ({ companyId }),
  retryable: (e) => e.reason !== "invoice-voided",
  queue: "billing",
  defaults: {
    attempts: 5,
    backoff: { type: "exponential", delay: "1 second" },
    timeout: "2 minutes"
  }
}) {}
```

Everything `Job.make` accepts:

| Option | Type | Purpose |
| --- | --- | --- |
| `payload` | struct fields or a `Schema.Struct` | what producers pass in, what handlers receive |
| `success` | schema (default `Schema.Void`) | the handler's result, decodable via `awaitResult`/`attempts` |
| `error` | schema (default `Schema.Never`) | the handler's typed failure, round-trips through storage |
| `idempotencyKey` | `(payload) => string` | derive a stable job id from business data |
| `dedupe` | `(payload) => DedupeInput` | temporal dedup key; see [Deduplication](/guide/deduplication) |
| `metadata` | `(payload) => Record<string, string>` | queryable business context, indexed by every driver |
| `retryable` | `(error) => boolean` | return `false` to skip the remaining retry budget |
| `queue` | string (default `"default"`) | ordering/concurrency domain within the store |
| `store` | `JobStore.named(...)` key | which named store hosts this job |
| `defaults` | `JobOptions` | default enqueue options, overridable per enqueue |

## Payload schemas

`payload` takes either bare struct fields (shorthand, wrapped in `Schema.Struct` for you) or any struct schema you already have. Enqueue persists payloads **schema-encoded** as JSON and the worker decodes them back, so the full schema toolkit round-trips: branded types, unions, `Schema.DateTimeUtc`, `Schema.Redacted`. Your handler receives real domain values.

```ts
class Remind extends Job.make("remind", {
  payload: {
    userId: UserId,                 // branded: enforced at the enqueue site
    remindAt: Schema.DateTimeUtc    // a real DateTime in the handler
  }
}) {}
```

`success` and `error` follow the same rule: the worker encodes handler exits through an exit schema and persists them in the attempt ledger, so `MyJob.awaitResult(id)` in another process returns the typed success or the typed failure.

## Redacted fields

`Schema.Redacted` has **transport semantics**. It does not encrypt values at rest:

- `Schema.Redacted(Schema.String)` round-trips. Enqueue writes the underlying value into the payload JSON in your store; the worker-side decode re-wraps it, so handlers receive a real `Redacted` value that prints `<redacted>` in logs. Redaction protects logs and inspection, not the database.
- `Schema.Redacted(inner, { disallowJsonEncode: true })` refuses persistence entirely: enqueueing dies with `Cannot serialize Redacted` before anything reaches the store. Use it for values that must never be written down, and hand them to handlers via context/services instead.

Tests pin both behaviors.

## Idempotency

`idempotencyKey` derives the job id from the payload: the id becomes `<name>/<key>` (e.g. `generate-invoice/inv-42`). Two things fall out of a deterministic id:

- **Dedup by id.** Enqueueing while a job with that id exists is a silent no-op returning the existing id. Retrying an API request cannot double-enqueue.
- **A natural join key.** Because the id is computable from business data, your own tables can reference queue records without storing job ids: the queue's retention can prune freely while your domain history keeps the key. See [Metadata vs. your own tables](/storage/stores#metadata-vs-your-own-tables) for the pattern.

```ts
idempotencyKey: ({ invoiceId }) => invoiceId
// GenerateInvoice.enqueue({ invoiceId: "inv-42", ... }) always yields "generate-invoice/inv-42"
```

An explicit per-enqueue `jobId` overrides the derived id. For *temporal* policy (throttle, debounce, replace) that leaves your ids alone, use `dedupe`, a separate key with its own lifecycle: [Deduplication](/guide/deduplication).

## Metadata

`metadata` projects the payload into a flat `Record<string, string>` at enqueue time. Every driver indexes it, so it powers `store.list({ metadata: { companyId } })`, dashboard filters, and (on Postgres) GIN-indexed containment queries, all without deserializing payloads. Per-enqueue `metadata` merges over the definition's.

Keep it to ops-UI context ("list runs for company X"). Domain results belong in your own tables, joined by the idempotency-keyed id.

## Retry control

`retryable` inspects a typed handler failure; returning `false` skips the remaining retry budget and fails the job immediately. Use it for errors where retrying cannot help (a voided invoice, a deleted customer). Its per-failure counterpart is `Effect.fail(Job.unrecoverable(error))` inside the handler. Full retry semantics live in [Retries & timeouts](/guide/retries-and-timeouts).

## Queue and store

`queue` names an ordering/concurrency domain *within* a store; workers set per-queue concurrency with `Worker.layer({ queues: { billing: { concurrency: 5 } } })`. `store` binds the job to a *named store* (`JobStore.named("durable")`), an infrastructure/durability domain hosting many queues; forgetting to provide that store's layer is a compile error at every enqueue site. See [Named stores](/storage/stores) for running Postgres and Redis side by side.

## Defaults

`defaults` accepts any per-enqueue tuning option; per-enqueue values override per option:

| Option | Default | Meaning |
| --- | --- | --- |
| `delay` | 0 | run this long after enqueue (per-enqueue also offers absolute `at`) |
| `priority` | 0 | higher runs first; ties are FIFO |
| `attempts` | 1 | total attempts including the first run (1 = no retries) |
| `backoff` | immediate | `{ type: "fixed" \| "exponential", delay, factor? }`; `factor` defaults to 2 |
| `keep` | forever | terminal-record retention: flat `{ count, age }` or split per state |
| `timeout` | none | per-run limit; the worker interrupts the handler fiber and the run counts as a failed attempt |

All durations take `Duration.Input` (`"30 seconds"`, `Duration.minutes(2)`, millis). `keep` splits by terminal state when completed jobs are noise and failed ones evidence: `{ completed: { age: "1 day" }, failed: { age: "30 days" } }`. Details in [Retention](/guide/retention).

::: tip
Put policy that belongs to the job (its retry budget, its timeout) in `defaults`, and keep per-enqueue options for what varies per call (`delay`/`at`, `priority`). Every enqueue site inherits the definition, so tuning lives in one place.
:::

## Where to next

- [Enqueueing](/guide/enqueueing): per-enqueue options, `delay` vs `at`, batches.
- [Deduplication](/guide/deduplication): throttle, debounce, replace-while-delayed.
- [Named stores](/storage/stores): bind jobs to different infrastructure.
- [Retries & timeouts](/guide/retries-and-timeouts): backoff, `retryable`, `unrecoverable`.
