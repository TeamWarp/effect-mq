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
| `effect-mq/drizzle` | drizzle schema factories + the Postgres `JobStore` | `drizzle-orm` (v1), `@effect/sql-pg` |
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
import { mqJobAttempts, mqJobs } from "effect-mq/drizzle"

// The `name` column is typed to your job tags (derived, not hand-written):
type JobNames = typeof SyncBenefits._tag | typeof GenerateReport._tag

export const jobs = mqJobs<JobNames>()          // default table: effect_mq_jobs
export const jobAttempts = mqJobAttempts(jobs)  // default: effect_mq_job_attempts
```

```
drizzle-kit generate   # emits the CREATE TABLE migration next to your others
drizzle-kit migrate
```

When a future effect-mq version changes the layout, the factory changes and
`drizzle-kit generate` diffs it — you get a normal, reviewable migration.

### 2. Provide the store layer

```ts
import { DrizzleJobStore } from "effect-mq/drizzle"
import { PgClient } from "@effect/sql-pg"
import { Layer, Redacted } from "effect"
import { jobAttempts, jobs } from "./db/schema.ts"

const JobStoreLive = DrizzleJobStore.layer({ jobs, attempts: jobAttempts }).pipe(
  Layer.provide(PgClient.layer({ url: Redacted.make(process.env.DATABASE_URL!) }))
)
```

The layer probes the tables at startup and fails fast with a clear message if
migrations haven't run (`validate: false` defers that to first use).

### 3. Query it like any other table

Product UIs read the tables directly with drizzle — fully typed:

```ts
db.select().from(jobs).where(and(
  eq(jobs.name, "sync-benefits"),                    // a typo is a compile error
  sql`${jobs.metadata} @> ${{ employerId }}::jsonb`  // GIN-indexed containment
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

## Multiple stores on different infrastructure

Bind jobs to *named stores* so business-critical runs live in Postgres while
disposable ones live elsewhere — enforced by the type system:

```ts
import { Job, JobStore, MemoryJobStore, Worker } from "effect-mq"

const Durable = JobStore.named("durable")       // -> Postgres in prod
const Ephemeral = JobStore.named("ephemeral")   // -> memory/Redis

class SyncBenefits extends Job.make("sync-benefits", {
  payload: { employerId: Schema.String },
  idempotencyKey: ({ employerId }) => employerId,
  store: Durable
}) {}

// Forgetting the Durable layer is now a COMPILE error at every enqueue site.
// Workers bind to one store:
const durableWorkers = SyncBenefits.toLayer(handler).pipe(
  Layer.provide(Worker.layer({ store: Durable }))     // local provide: several
)                                                     // workers can coexist
```

A **queue** is an ordering/concurrency domain *within* a store
(`Job.make({ queue })`, `Worker.layer({ queues: { email: { concurrency: 5 } } })`);
a **store** is an infrastructure/durability domain hosting many queues.

## Metadata vs. your own tables

Two kinds of "business context", two homes:

- **Ops UI** ("list sync runs for employer X, retry that one"): use the
  `metadata` projection — a flat `Record<string, string>` derived from the
  payload, indexed by every driver, filterable via `store.list` or raw SQL.
- **Domain history** ("what did this sync actually change"): your own table,
  joined by the *deterministic* job id from `idempotencyKey`. The queue's
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
`getAttempts`, `list`, `retry`, `counts`, `remove`) and run the conformance
suite against it:

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

Next up: repeatable/cron jobs, handler timeouts + unrecoverable errors,
dashboard admin verbs (pause/promote/cancel), per-store history TTL, richer
deduplication (throttle/debounce), trace propagation and an event stream, a
Redis store, and parent-child fan-out. Full prioritized list:
[ROADMAP.md](https://github.com/TeamWarp/effect-mq/blob/main/ROADMAP.md).

## License

MIT
