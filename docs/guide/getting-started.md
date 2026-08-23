# Getting started

Install the package:

```sh
bun add effect-mq        # or npm install / pnpm add / yarn add
```

`effect-mq` targets **Effect v4** (`effect@4.0.0-rc`). The Postgres and Redis stores have optional peer dependencies — you'll add those in the storage step.

## 1. Define a job

A job is a class: a tag, a payload schema, and defaults. The definition is shared by everyone who enqueues it and whoever runs it.

```ts
import { Job } from "effect-mq"
import { Schema } from "effect"

class SendEmail extends Job.make("SendEmail", {
  payload: { to: Schema.String, subject: Schema.String },
  success: Schema.String,                            // typed result
  idempotencyKey: ({ to, subject }) => `${to}:${subject}`,
  metadata: ({ to }) => ({ to }),                    // queryable context
  queue: "email",
  defaults: {
    attempts: 3,
    backoff: { type: "exponential", delay: "1 second" }
  }
}) {}
```

Payloads are real schemas: `Redacted`, `DateTimeUtc`, branded types, unions — all round-trip through storage and come back typed in the handler.

## 2. Enqueue from anywhere

Producers need the job's **store** in context — never the worker. That means your API server can enqueue without depending on any handler code.

```ts
import { Effect } from "effect"

const program = Effect.gen(function*() {
  // fire and forget — returns the JobId
  const jobId = yield* SendEmail.enqueue({ to: "ada@example.com", subject: "hi" })

  // ...or enqueue and await the typed result
  const messageId = yield* SendEmail.execute(
    { to: "grace@example.com", subject: "now" },
    { delay: "5 seconds", priority: 2 }
  )
})
```

Thanks to `idempotencyKey`, enqueueing the same email twice is a no-op that returns the existing id.

## 3. Run a worker

Handlers are Effects; workers are Layers. Compose them wherever you deploy — same process or a dedicated machine.

```ts
import { Worker, MemoryJobStore } from "effect-mq"
import { Effect, Layer } from "effect"

const RunnerLive = SendEmail.toLayer(
  (payload, ctx) => Effect.succeed(`message-${ctx.jobId}`),
  { concurrency: 5 }
).pipe(
  Layer.provideMerge(Worker.layer()),
  Layer.provideMerge(MemoryJobStore.layer)   // swap for Postgres/Redis below
)
```

Provide `RunnerLive` to your app and the worker claims, runs, retries, and records jobs. Interrupting the layer's scope shuts down gracefully: in-flight jobs are released back to `waiting` without consuming an attempt.

## 4. Pick real storage

`MemoryJobStore` is for tests and demos. For production, swap the layer — nothing else changes:

::: code-group

```ts [Postgres (drizzle)]
import { DrizzleJobStore, mqJobs, mqJobAttempts, mqSchedules, mqQueueControl, mqDedupe } from "effect-mq/drizzle-postgres"

// The tables live in YOUR drizzle schema; drizzle-kit owns migrations.
export const jobs = mqJobs()
export const jobAttempts = mqJobAttempts(jobs)
export const schedules = mqSchedules()
export const queues = mqQueueControl()
export const dedupe = mqDedupe()

const StoreLive = DrizzleJobStore.layer({ jobs, attempts: jobAttempts, schedules, queues, dedupe })
```

```ts [Redis]
import { RedisJobStore } from "effect-mq/redis"
import { NodeRedis } from "@effect/platform-node"

const StoreLive = RedisJobStore.layer().pipe(
  Layer.provide(NodeRedis.layer({ url: "redis://localhost:6379" }))
)
```

:::

See [Postgres](/storage/postgres) for the migration workflow and [Redis](/storage/redis) for client setup.

## 5. Check on your jobs

```ts
const status = yield* SendEmail.poll(jobId)        // Option<JobStatus>
const runs = yield* SendEmail.attempts(jobId)      // the decoded run ledger
```

Every attempt — success, retry, failure, stall — is persisted and decodes back to a typed exit.

## Where to next

- [Defining jobs](/guide/defining-jobs) — everything `Job.make` accepts.
- [Enqueueing](/guide/enqueueing) — delays, absolute times, priorities, batches.
- [Workers & handlers](/guide/workers) — concurrency, locks, shutdown.
- [Testing your app](/guide/testing) — assert what your services enqueue, without a worker.
