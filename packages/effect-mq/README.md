# effect-mq

Effect-native background jobs. Schema-first job definitions, a storage-agnostic
queue core, a worker runtime, and a Postgres store that lives inside your
drizzle schema — inspired by BullMQ's semantics and `effect/workflow`'s DX.
Built on Effect v4.

```
bun add effect-mq effect            # or npm / pnpm / yarn
```

One package, tree-shakeable modules:

| Import | Contents | Extra peers |
| --- | --- | --- |
| `effect-mq` | `Job`, `JobStore`, `MemoryJobStore`, `Worker` | — |
| `effect-mq/drizzle-postgres` | drizzle-postgres schema factories + the Postgres `JobStore` | `drizzle-orm` (v1), `@effect/sql-pg` |
| `effect-mq/redis` | the Redis `JobStore` (Lua-script atomicity) | a `Redis` service (`@effect/platform-node`/`-bun`) |
| `effect-mq/testing` | the `JobStore` conformance suite for driver authors | `@effect/vitest` |

## Five-minute tour

```ts
import { Job, MemoryJobStore, Worker } from "effect-mq"
import { Effect, Layer, Schema } from "effect"

// 1. Define a job — shared by producers and runners.
class SendEmail extends Job.make("SendEmail", {
  payload: { to: Schema.String, subject: Schema.String },
  success: Schema.String,
  idempotencyKey: ({ to, subject }) => `${to}:${subject}`,
  metadata: ({ to }) => ({ to }),                    // indexed, queryable
  queue: "email",
  defaults: { attempts: 3, backoff: { type: "exponential", delay: "1 second" } }
}) {}

// 2. Produce. Only the store is required — no worker anywhere in sight.
const producer = Effect.gen(function*() {
  const jobId = yield* SendEmail.enqueue({ to: "ada@example.com", subject: "hi" })
  //    ^ JobId — deterministic here, thanks to idempotencyKey

  // ...or enqueue-and-wait for the typed result:
  const messageId = yield* SendEmail.execute(
    { to: "grace@example.com", subject: "now" },
    { delay: "5 seconds", priority: 2 }
  )
})

// 3. Run. Workers are layers — deploy them in the same process or across
//    machines against shared storage.
const RunnerLive = SendEmail.toLayer(
  (payload, ctx) => Effect.succeed(`message-${ctx.jobId}`),
  { concurrency: 5 }
).pipe(
  Layer.provideMerge(Worker.layer()),
  Layer.provideMerge(MemoryJobStore.layer)   // swap for Postgres below
)
```

What you get out of the box:

- **At-least-once execution** — token-guarded locks, heartbeat renewal, and a
  stalled-job sweeper that recovers work from crashed workers.
- **Durable retries** — a failed attempt is written back to the store and
  re-claimed after its backoff by *any* worker; no lock or worker slot is held
  while waiting.
- **A full run ledger** — every run (success, retry, failure, stall) persists
  as an `AttemptRecord`; `Job.attempts(id)` decodes them back to typed exits.
- **Idempotency** — `idempotencyKey` makes enqueue a no-op while a job with
  that key exists, and makes the job id *computable from business data* — the
  natural join key between the queue and your own domain tables.
- **A dashboard data layer** — `store.list({ name, states, metadata, cursor })`,
  `Job.poll`, `Job.retry(id)` (failed → fresh attempt budget, ledger intact),
  and per-job retention via `keep: { count, age }`.
- **Graceful shutdown** — interrupting a worker releases in-flight jobs back
  to `waiting` without consuming an attempt.
- **Repeatable jobs** — durable cron/interval schedules
  (`MyJob.schedule(key, { cron })`) that fire exactly once per occurrence
  across any number of workers.
- **Admin verbs** — `cancel` (including *running* jobs, whose handler fiber is
  interrupted cross-process), `promote` (delayed → now), and queue-level
  `pause`/`resume`.
- **Handler timeouts** — `defaults: { timeout: "30 seconds" }` interrupts the
  handler cleanly and routes through normal retry accounting; something BullMQ
  can't do to a running processor.
- **Unrecoverable errors** — `Job.unrecoverable(error)` or a `retryable`
  predicate skips the remaining retry budget when retrying can't help.

## Repeatable jobs

Schedules are durable rows in the store — not process-local timers — so they
survive restarts and coordinate across workers. Ticks enqueue with a
slot-deterministic job id (`sched/<key>/<slot>`), which makes every occurrence
exactly-once no matter how many workers sweep:

```ts
// Create or replace (same key = replace; upsert is idempotent to deploy).
yield* SendDigest.schedule("daily", {
  cron: "0 9 * * *",              // or: every: "10 minutes"
  tz: "America/New_York",         // IANA zone, cron only
  payload: { edition: "morning" }
})

yield* SendDigest.unschedule("daily")
```

`cron` fires at the next matching occurrence; `every` first fires one interval
from now and stays on its original grid. If workers are down over several
occurrences, missed slots collapse into one run (the next sweep enqueues the
overdue slot once, then advances past `now`). Options mirror `enqueue`:
`metadata`, `priority`, `attempts`, `backoff`, `keep`, `timeout`.

## Timeouts, cancellation, and unrecoverable errors

Because handlers are Effect fibers, the runtime can *actually stop them* —
these verbs interrupt cleanly (finalizers run) instead of abandoning work:

```ts
class GenerateInvoice extends Job.make("generate-invoice", {
  payload: { invoiceId: Schema.String },
  error: InvoiceError,
  defaults: { attempts: 5, timeout: "2 minutes" },  // per-run limit
  retryable: (e) => e.reason !== "invoice-voided"   // skip retries when futile
}) {}

// Inside a handler, mark a specific failure as not worth retrying:
Effect.fail(Job.unrecoverable(new InvoiceError({ reason: "customer deleted" })))

// From anywhere (a dashboard, another process):
yield* GenerateInvoice.cancel(jobId)   // waiting/delayed: terminal immediately;
                                       // running: the worker's next heartbeat
                                       // interrupts the handler fiber
yield* GenerateInvoice.promote(jobId)  // delayed -> runnable now

// Store-level (definition-free) equivalents for generic dashboards:
const store = yield* JobStore.JobStore
yield* store.cancel(jobId)
yield* store.pause(QueueName("email"))   // claims stop; producers unaffected
yield* store.resume(QueueName("email"))  // wakes idle workers immediately
```

A timed-out run records a `TimeoutError` defect in the attempt ledger and
consumes an attempt like any other failure. A cancelled job lands in the
terminal `cancelled` state with a `cancelled` ledger entry; `awaitResult`
treats it as a defect (`JobCancelledError`), not a typed failure.

## History retention

Two layers of control:

- **Per job**: `keep: { count, age }` prunes terminal records per name+state
  at ack time.
- **Per store**: a retention ceiling applied to *all* terminal records,
  swept periodically in the background:

```ts
MemoryJobStore.layerWith({ historyTtl: "7 days" })
DrizzleJobStore.layer({ jobs, attempts, schedules, queues, historyTtl: "90 days" })
```

## Custom job ids

Store-assigned ids default to a compact sequence (`j-<n>`). Bring your own
generator when you want globally unique or prefixed ids:

```ts
import { ulid } from "ulid"

DrizzleJobStore.layer({
  jobs, attempts, schedules, queues,
  idGenerator: ({ name }) => `${name}_${ulid()}`   // sync or Effect-returning
})
```

The generator only runs for store-assigned ids — `idempotencyKey` ids and
repeatable-schedule tick ids stay deterministic (exactly-once depends on
them). Collisions are retried a bounded number of times, then the enqueue
fails; bring real entropy.

## Postgres through drizzle

The Postgres store runs on drizzle v1's Effect driver
(`drizzle-orm/effect-postgres`, built on `@effect/sql-pg` — works on Node and
Bun). Claims use `FOR UPDATE SKIP LOCKED`; wake-ups use LISTEN/NOTIFY, so
workers in other processes pick jobs up promptly.

```
bun add drizzle-orm@rc @effect/sql-pg
```

### 1. Put the tables in your drizzle schema

The factories are the single source of truth for the table layout. Re-export
them from your schema file and **your drizzle-kit pipeline owns the
migrations** — no library-run DDL, no parallel migration system:

```ts
// db/schema.ts
import { mqJobAttempts, mqJobs, mqQueueControl, mqSchedules } from "effect-mq/drizzle-postgres"

// The `name` column is typed to your job tags (derived, not hand-written):
type JobNames = typeof GenerateInvoice._tag | typeof SendEmail._tag

export const jobs = mqJobs<JobNames>()          // default table: effect_mq_jobs
export const jobAttempts = mqJobAttempts(jobs)  // default: effect_mq_job_attempts
export const jobSchedules = mqSchedules()       // default: effect_mq_schedules
export const jobQueues = mqQueueControl()       // default: effect_mq_queue_control
```

```
drizzle-kit generate   # emits the CREATE TABLE migration next to your others
drizzle-kit migrate
```

When a future effect-mq version changes the layout, the factory changes and
`drizzle-kit generate` diffs it — you get a normal, reviewable migration.

### 2. Provide the store layer

```ts
import { DrizzleJobStore } from "effect-mq/drizzle-postgres"
import { PgClient } from "@effect/sql-pg"
import { Layer, Redacted } from "effect"
import { jobAttempts, jobQueues, jobs, jobSchedules } from "./db/schema.ts"

const JobStoreLive = DrizzleJobStore.layer({
  jobs,
  attempts: jobAttempts,
  schedules: jobSchedules,
  queues: jobQueues
}).pipe(
  Layer.provide(PgClient.layer({ url: Redacted.make(process.env.DATABASE_URL!) }))
)
```

The layer probes the tables at startup and fails fast with a clear message if
migrations haven't run (`validate: false` defers that to first use).

### 3. Query it like any other table

Product UIs read the tables directly with drizzle — fully typed:

```ts
db.select().from(jobs).where(and(
  eq(jobs.name, "generate-invoice"),                 // a typo is a compile error
  sql`${jobs.metadata} @> ${{ customerId }}::jsonb`  // GIN-indexed containment
))

db.select().from(jobAttempts)
  .innerJoin(jobs, eq(jobAttempts.jobId, jobs.id))
  .where(eq(jobAttempts.outcome, "failed"))
```

**Reads yes, writes no.** Mutations must go through the store —
`MyJob.retry(id)`, `store.remove(id)` — so lock tokens, attempt accounting,
and wake-up notifications stay coherent.

Worker tip: `awaitWake` is LISTEN/NOTIFY-driven, and the worker's
`pollInterval` is the fallback — `Worker.layer({ pollInterval: "500 millis" })`
is a good Postgres setting.

## Redis store

The Redis store (`effect-mq/redis`) implements every `JobStore` operation as
one atomic Lua script, so it is safe across any number of producer and worker
processes. It builds on Effect's client-agnostic `Redis` service — provide it
from your platform package (no extra peers beyond what you already run):

```ts
import { RedisJobStore } from "effect-mq/redis"
import { NodeRedis } from "@effect/platform-node"   // node-redis under the hood
// import { BunRedis } from "@effect/platform-bun"  // Bun.redis under the hood
import { Layer } from "effect"

const JobStoreLive = RedisJobStore.layer({
  prefix: "myapp-jobs",          // key namespace (default "effect-mq")
  historyTtl: "30 days"          // optional retention ceiling
}).pipe(
  Layer.provide(NodeRedis.layer({ url: process.env.REDIS_URL }))
)
```

Wake-ups ride pub/sub (`<prefix>:wake`), so idle workers in other processes
pick new jobs up promptly; the worker's `pollInterval` is the fallback. Notes:

- Keys are plain-prefixed (no hash tags) — point it at a single Redis /
  Valkey node or a cluster-unaware proxy, not Redis Cluster.
- `list` filters scan server-side in Lua: fine for dashboards, not for
  millions of terminal rows — set `historyTtl`/`keep` accordingly. `counts`
  is O(1) (maintained counters).
- The same conformance suite that runs against Postgres runs against a real
  Redis in this repo, TestClock included (scripts take time via ARGV).

## Multiple stores on different infrastructure

Bind jobs to *named stores* so business-critical runs live in Postgres while
disposable ones live elsewhere — enforced by the type system:

```ts
import { Job, JobStore, MemoryJobStore, Worker } from "effect-mq"

const Durable = JobStore.named("durable")       // -> Postgres in prod
const Ephemeral = JobStore.named("ephemeral")   // -> Redis or memory

class GenerateInvoice extends Job.make("generate-invoice", {
  payload: { invoiceId: Schema.String },
  idempotencyKey: ({ invoiceId }) => invoiceId,
  store: Durable
}) {}

// Forgetting the Durable layer is now a COMPILE error at every enqueue site.
// Workers bind to one store:
const durableWorkers = GenerateInvoice.toLayer(handler).pipe(
  Layer.provide(Worker.layer({ store: Durable }))     // local provide: several
)                                                     // workers can coexist
```

A **queue** is an ordering/concurrency domain *within* a store
(`Job.make({ queue })`, `Worker.layer({ queues: { email: { concurrency: 5 } } })`);
a **store** is an infrastructure/durability domain hosting many queues.

## Metadata vs. your own tables

Two kinds of "business context", two homes:

- **Ops UI** ("list invoice runs for customer X, retry that one"): use the
  `metadata` projection — a flat `Record<string, string>` derived from the
  payload, indexed by every driver, filterable via `store.list` or raw SQL.
- **Domain history** ("what did this invoice run actually produce"): your own
  table, joined by the *deterministic* job id from `idempotencyKey`. The queue's
  retention (`keep`) can then prune freely while your business history lives
  forever. Don't let queue infrastructure own business data lifecycles.

## Secrets and redaction

Payloads and results are persisted **schema-encoded** — nothing reaches a
store un-encoded. For `Schema.Redacted` fields, Effect's semantics apply:

- `Schema.Redacted(Schema.String)` **round-trips**: handlers receive a real
  `Redacted` value (safe to log — it prints `<redacted>`), but the underlying
  value *is* stored in the payload/exit JSON. Redaction protects logs and
  inspection, not the database at rest.
- `Schema.Redacted(inner, { disallowJsonEncode: true })` **refuses
  persistence**: enqueueing such a payload dies with `Cannot serialize
  Redacted` before anything reaches the store. Use it for values that must
  never be written down; pass them to handlers via context/services instead.

Both behaviors are pinned by tests.

## Writing a storage driver

Implement the `JobStore` service (one atomic seam: `enqueue`, `claim`, `ack`,
`release`, `extendLocks`, `recoverStalled`, `awaitWake`, `getJob`,
`getAttempts`, `list`, `retry`, `counts`, `remove`, `cancel`, `promote`,
`pause`/`resume`/`pausedQueues`, and the schedule ops
`upsertSchedule`/`removeSchedule`/`listSchedules`/`dueSchedules`/`advanceSchedule`)
and run the conformance suite against it:

```ts
import { jobStoreConformance } from "effect-mq/testing"

jobStoreConformance("MyDriver", () => MyDriver.layer)
```

Two rules make the suite work against real storage: derive **all time from
the Effect `Clock`** (pass `now` as a bind parameter — never SQL `now()`), so
tests run under `TestClock`; and keep every operation atomic. The in-memory
driver (`MemoryJobStore`) is the reference implementation, and the Postgres
suite in this repo runs the same conformance tests against a real database.

## Roadmap

Next up: richer deduplication (throttle/debounce), trace propagation, a
cross-process event stream, batch enqueue, drizzle schema customization, and
parent-child fan-out. Full prioritized list:
[ROADMAP.md](https://github.com/TeamWarp/effect-mq/blob/main/ROADMAP.md).

## License

MIT
