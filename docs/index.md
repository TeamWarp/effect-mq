---
layout: home

hero:
  name: effect-mq
  text: Background jobs for Effect.
  tagline: Schema-typed payloads. Swappable storage. At-least-once execution. One package.
  actions:
    - theme: brand
      text: Getting started
      link: /guide/getting-started
    - theme: alt
      text: What is effect-mq?
      link: /guide/introduction
    - theme: alt
      text: GitHub
      link: https://github.com/TeamWarp/effect-mq

features:
  - title: Effect-native
    details: Jobs are schemas, handlers are Effects, workers are Layers. Interruption, retries, and timeouts use the runtime — not bolted-on process machinery.
  - title: Storage is a seam
    details: Postgres through your drizzle schema, Redis through atomic Lua scripts, memory for tests. One conformance suite keeps every driver honest.
  - title: Producer / runner split
    details: Enqueueing never requires a Worker — enforced at the type level. Ship producers in your API and runners on separate machines.
  - title: Operable
    details: Run ledger, metadata queries, repeatable schedules with exactly-once ticks, dedup modes, cancellation of running jobs, tracing and metrics.
---

## The whole idea, in one file

```ts
import { Job, Worker } from "effect-mq"
import { DrizzleJobStore } from "effect-mq/drizzle-postgres"
import { Effect, Layer, Schema } from "effect"

// 1. Define once — shared by producers and runners.
class GenerateInvoice extends Job.make("generate-invoice", {
  payload: { invoiceId: Schema.String },
  idempotencyKey: ({ invoiceId }) => invoiceId,
  defaults: { attempts: 5, backoff: { type: "exponential", delay: "1 second" } }
}) {}

// 2. Produce — needs the store, never the worker.
const jobId = yield* GenerateInvoice.enqueue({ invoiceId: "inv_123" })

// 3. Run — a layer, wherever you deploy it.
const RunnerLive = GenerateInvoice.toLayer(
  ({ invoiceId }) => Effect.log(`rendering ${invoiceId}`),
  { concurrency: 5 }
).pipe(Layer.provideMerge(Worker.layer()))
```

```sh
bun add effect-mq   # or npm / pnpm / yarn
```
