# Memory & multiple stores

`MemoryJobStore` is the reference `JobStore` driver: a full implementation of the storage contract that lives in process memory. It passes the same conformance suite as Postgres and Redis, so everything — retries, schedules, dedup, retention — behaves identically. What it lacks is durability and cross-process reach: state disappears on restart, and only workers in the same process can see it.

Use it for tests, demos, and truly disposable jobs. For anything you'd mind losing, use [Postgres](/storage/postgres) or [Redis](/storage/redis).

## MemoryJobStore

Three constructors, all producing a fresh, empty store per build:

```ts
import { MemoryJobStore } from "effect-mq"

// The default JobStore, no options:
const StoreLive = MemoryJobStore.layer

// With options:
const TunedLive = MemoryJobStore.layerWith({
  historyTtl: { completed: "1 hour", failed: "1 day" },
  historySweepInterval: "30 seconds",
  idGenerator: ({ name }) => `${name}_${crypto.randomUUID()}`
})

// Bound to a named store key instead of the default (see below):
const EphemeralLive = MemoryJobStore.layerFor(Ephemeral, { historyTtl: "1 hour" })
```

| Option | Default | Meaning |
| --- | --- | --- |
| `historyTtl` | off | Retention ceiling for terminal records — one duration for all terminal states, or a per-state split (`{ completed: "1 day", failed: "30 days" }`) |
| `historySweepInterval` | 1 minute | How often the background sweep prunes expired history |
| `idGenerator` | `j-<n>` counter | Generator for store-assigned job ids — sync or Effect-returning |

The history sweeper (when `historyTtl` is set) runs in the layer's scope and also honours stricter per-job [`keep.age` rules](/guide/retention). There is also `MemoryJobStore.make` / `makeWith` if you need the raw `Service` outside a layer.

## Named stores

By default every job, producer, and worker shares the single `JobStore` service. Named stores split that into several independent service keys, so different jobs can live on different infrastructure — and the type system routes them:

```ts
import { Job, JobStore } from "effect-mq"
import { Schema } from "effect"

const Durable = JobStore.named("durable")       // -> Postgres in prod
const Ephemeral = JobStore.named("ephemeral")   // -> Redis or memory

class GenerateInvoice extends Job.make("generate-invoice", {
  payload: { invoiceId: Schema.String },
  idempotencyKey: ({ invoiceId }) => invoiceId,
  store: Durable
}) {}

class WarmCache extends Job.make("warm-cache", {
  payload: { key: Schema.String },
  store: Ephemeral
}) {}
```

`JobStore.named("durable")` creates a distinct `Context.Key` — keys are identified by their name string, so two `named("durable")` calls in different files are interchangeable. Binding a job with `Job.make({ store: Durable })` changes its requirements: `GenerateInvoice.enqueue(...)` now requires the *Durable* store in context, not the default `JobStore`. Forgetting to provide the Postgres layer for it is a compile error at every enqueue site, not a runtime surprise.

A **queue** and a **store** are different axes: a queue is an ordering/concurrency domain *within* a store (`Job.make({ queue })`); a store is an infrastructure/durability domain hosting many queues.

## One store, one driver layer

Each driver can construct a layer for a named key — `store` as an option on the Postgres layer, `layerFor` on Redis and memory:

```ts
import { DrizzleJobStore } from "effect-mq/drizzle-postgres"
import { RedisJobStore } from "effect-mq/redis"
import { MemoryJobStore } from "effect-mq"

const DurableLive = DrizzleJobStore.layer({
  jobs, attempts: jobAttempts, schedules, queues, dedupe,
  store: Durable
})

const EphemeralLive = RedisJobStore.layerFor(Ephemeral)
// ...or, in tests / small deployments:
const EphemeralTest = MemoryJobStore.layerFor(Ephemeral)
```

## Two workers in one process

Workers bind to exactly one store via `Worker.layer({ store })`. To run workers for several stores in the same process, provide each worker layer *locally* to its handler group with `Layer.provide` — a merged global `Worker` would be ambiguous:

```ts
import { Worker } from "effect-mq"
import { Layer } from "effect"

const Workers = Layer.mergeAll(
  GenerateInvoice.toLayer(handleInvoice).pipe(
    Layer.provide(Worker.layer({ store: Durable, concurrency: 4 }))
  ),
  WarmCache.toLayer(handleWarmCache).pipe(
    Layer.provide(Worker.layer({ store: Ephemeral, concurrency: 16 }))
  )
).pipe(Layer.provide(Layer.mergeAll(DurableLive, EphemeralLive)))
```

The pattern this enables: business-critical runs (invoices, payouts) on Postgres with long retention, high-volume disposable work (cache warming, previews) on Redis or memory with aggressive pruning — one codebase, compile-time routing.

::: tip
Producers stay worker-free either way: an API server that enqueues `GenerateInvoice` needs only `DurableLive` in its layer stack, never a `Worker`.
:::

## Metadata vs. your own tables

Two kinds of "business context" belong in two different homes:

- **Operational context** — "list invoice runs for customer X, retry that one". Use the definition's `metadata` projection: a flat `Record<string, string>` derived from the payload, indexed by every driver, filterable via `store.list({ metadata: { customerId } })` or raw SQL on Postgres.
- **Domain history** — "what did this invoice run actually produce". That belongs in your own tables, joined by the *deterministic* job id from `idempotencyKey`. Because the id is computable from business data, your domain rows can reference queue records without any id bookkeeping.

The payoff is lifecycle independence: job [retention](/guide/retention) can prune terminal records freely because your business history lives in tables you own. Don't let queue infrastructure own business data lifecycles.

## Where to next

- [Postgres](/storage/postgres) — the drizzle-schema workflow for the durable store.
- [Redis](/storage/redis) — Lua-script atomicity for the fast store.
- [Writing a driver](/storage/writing-a-driver) — implement `JobStore` for anything else.
- [Testing your app](/guide/testing) — `TestJobStore.layerFor` covers named stores too.
