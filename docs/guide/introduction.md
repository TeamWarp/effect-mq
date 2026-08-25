# What is effect-mq?

effect-mq is a background job library for [Effect](https://effect.website). You define a job once: a name, a schema-typed payload, optional success/error schemas, and defaults. You then enqueue it from producers and handle it in workers, in the same process or on different machines, against shared storage.

```ts
class SendEmail extends Job.make("SendEmail", {
  payload: { to: Schema.String, subject: Schema.String },
  success: Schema.String,
  queue: "email",
  defaults: { attempts: 3, backoff: { type: "exponential", delay: "1 second" } }
}) {}
```

The library is one npm package, `effect-mq`, with tree-shakeable subpath modules:

| Module | What it is |
| --- | --- |
| `effect-mq` | Core: `Job`, `Worker`, `JobStore` contract, `MemoryJobStore` |
| `effect-mq/drizzle-postgres` | Postgres store + drizzle schema factories |
| `effect-mq/redis` | Redis store over Effect's `Redis` service |
| `effect-mq/testing` | `TestJobStore` for unit tests, driver conformance suite |

The non-core subpaths declare their dependencies as optional peers, so install only what you use.

## Why Effect-native matters

Most job libraries treat the handler as an opaque callback and manage it with process-level machinery. effect-mq handlers are Effect fibers, so the runtime controls them:

- **Timeouts interrupt the handler**: finalizers run, the worker records the attempt, and retry accounting proceeds. A callback-based processor cannot do that to itself.
- **Cancellation reaches running jobs across processes**: `MyJob.cancel(id)` flags the store; the owning worker interrupts the fiber on its next heartbeat.
- **Graceful shutdown releases in-flight jobs** back to `waiting` without consuming an attempt.
- **Schemas own the payload boundary**: the library validates and encodes what you enqueue, and your handler receives it decoded and typed, including `Redacted` and `DateTime` fields.

## The two-sided contract

Both sides use the same job definition, and the type system keeps them apart:

- **Producers** call `MyJob.enqueue(payload)` / `MyJob.execute(payload)`. These require the job's *store* in context, never the `Worker`. The type system enforces this, so your API server can enqueue without depending on handler code.
- **Runners** provide `MyJob.toLayer(handler)` on top of `Worker.layer()`, bound to the same store.

## Storage is a seam, not a backend

Everything the library does goes through one interface, `JobStore`, and each of its operations is atomic: claim with locks, token-guarded acks, dedup, schedule ticks. Three drivers ship in the box (Postgres via your drizzle schema, Redis via atomic Lua scripts, in-memory), and a public conformance suite of a hundred-odd behavioral tests keeps any driver honest, including [yours](/storage/writing-a-driver).

Delivery is **at-least-once**: a claimed job holds a lock that its worker heartbeats; if the worker dies, the stalled sweeper recovers the job for another attempt. Design handlers to be idempotent; the [idempotency key](/guide/defining-jobs#idempotency) and [dedup](/guide/deduplication) primitives make that easy.

## Where to next

- [Getting started](/guide/getting-started): install to running worker in five minutes.
- [Defining jobs](/guide/defining-jobs): schemas, defaults, idempotency.
- [Postgres](/storage/postgres) or [Redis](/storage/redis): production storage.
