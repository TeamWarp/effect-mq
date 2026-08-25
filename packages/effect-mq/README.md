# effect-mq

Effect-native background jobs. Schema-first job definitions, a storage-agnostic
queue core, a worker runtime, and a Postgres store that lives inside your
drizzle schema — inspired by BullMQ's semantics and `effect/workflow`'s DX.
Built on Effect v4.

**Documentation: [effect-mq.com](https://www.effect-mq.com)**

```
bun add effect-mq effect            # or npm / pnpm / yarn
```

One package, tree-shakeable modules:

| Import | Contents | Extra peers |
| --- | --- | --- |
| `effect-mq` | `Job`, `JobStore`, `MemoryJobStore`, `Worker` | — |
| `effect-mq/drizzle-postgres` | drizzle-postgres schema factories + the Postgres `JobStore` | `drizzle-orm` (v1), `@effect/sql-pg` |
| `effect-mq/redis` | the Redis `JobStore` (Lua-script atomicity) | a `Redis` service (`@effect/platform-node`/`-bun`) |
| `effect-mq/testing` | `TestJobStore` (assert enqueues in unit tests) + the driver conformance suite | `@effect/vitest` (conformance only) |

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
  (payload) => Effect.map(Worker.CurrentJob, ({ jobId }) => `message-${jobId}`),
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
  (`MyJob.schedule(key, { cron })`); each occurrence is claimed and enqueued
  in one atomic store op, so ticks are exactly-once across any number of
  workers.
- **Batch enqueue** — `MyJob.enqueueMany(payloads, options?)` inserts a
  whole batch of plain items in one store round trip per chunk (multi-row
  `INSERT` / one Lua script), with per-item idempotency and dedup semantics
  intact (dedup-keyed items fall back to individual enqueues).
- **Admin verbs** — `cancel` (including *running* jobs, whose handler fiber is
  interrupted cross-process), `promote` (delayed → now), and queue-level
  `pause`/`resume`.
- **Handler timeouts** — `defaults: { timeout: "30 seconds" }` interrupts the
  handler cleanly and routes through normal retry accounting; something BullMQ
  can't do to a running processor.
- **Unrecoverable errors** — `Job.unrecoverable(error)` or a `retryable`
  predicate skips the remaining retry budget when retrying can't help.
- **Deduplication** — pending-dedup, throttle, debounce, and
  replace-while-delayed via a dedup key that never touches your job ids.

## Batch enqueue

Fan-out inserts a whole batch of plain items in **one store round trip per
chunk** — a multi-row `INSERT` on Postgres, one Lua script on Redis (drivers
chunk very large batches):

```ts
const ids = yield* GenerateInvoice.enqueueMany(
  companies.map((company) => ({ companyId: company.id })),
  { queue: "billing", at: nextBillingRun }   // shared options
)
```

Ids come back aligned with the payloads. Every item keeps full single-enqueue
semantics — `idempotencyKey`, `dedupe`, and `metadata` callbacks run per
payload, and duplicates are silent no-ops returning the existing id (items
that derive a dedup key run through the single-enqueue path individually, in
order). Options apply batch-wide; per-job `jobId`/`dedupe` are excluded at
the type level. The batch is intentionally *not* one transaction: a
mid-batch store failure can leave a subset applied, which is safe under
at-least-once — re-running a batch with deterministic ids skips what already
landed (store-assigned ids may re-insert).

## Repeatable jobs

Schedules are durable rows in the store — not process-local timers — so they
survive restarts and coordinate across workers. Each occurrence is claimed
and its job (id `sched/<key>/<slot>`) enqueued in **one atomic store op** —
a compare-and-swap on the schedule's next occurrence — so every occurrence
fires exactly once no matter how many workers sweep, and no matter how
aggressively history retention prunes old tick jobs:

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

## Parent-child flows

A flow fans a parent job out into N children, parks the parent until every
child settles, then resumes it with their typed results. Children can live
on a **different store** than the parent (a cron parent in Postgres fanning
out 10k idempotent sends into Redis, collecting the outcomes back):

```ts
import { Flow } from "effect-mq"

const DigestFlow = Flow.make("daily-digest", {
  parent: SendDigest,            // Postgres
  children: [SendEmail],         // Redis
  onChildFailure: "continue"     // or "fail": first failure settles the flow
})

// The parent worker runs two phases (requires parent AND child stores):
const DigestWorker = DigestFlow.toLayer({
  fanOut: (payload) =>
    Effect.map(Users.active, (users) =>
      Flow.children(SendEmail, users.map((user) => ({
        key: user.id,                       // unique in the flow = idempotency
        payload: { userId: user.id }
      })))),
  collect: (payload, results) =>
    Effect.succeed({ sent: results.counts.completed, failed: results.counts.failed })
})

// Workers that run the children declare the flow so their relay can push
// results to the parent store the moment each child acks:
Worker.layer({ store: EmailStore, flows: [DigestFlow] })
```

The parent's store owns the flow (manifest, per-child results, outcome
counters), so "settle exactly once" is single-store atomic. Cross-store,
every terminal child transition appends its report to the child store's
**outbox** in the same atomic operation; worker relays push those in
batches (children keep completing through parent-store outages) and a
reconciliation sweeper repairs anything the push path misses, from storage
alone. Flows nest (a child can be another flow's parent), and `collect`
reads results as plain `counts`, materialized buckets, or a paged `Stream`.
Docs: [Parent-child flows](https://www.effect-mq.com/guide/flows).

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

Two layers of control, both splittable by terminal state (completed jobs are
usually noise, failed ones evidence):

- **Per job**: `keep` prunes terminal records per name+state — flat
  `{ count, age }` applies to all states, or split per state:

```ts
keep: { count: 100 }                                              // all states
keep: { completed: { age: "1 day" }, failed: { age: "30 days" } } // split
```

- **Per store**: a retention ceiling swept periodically in the background —
  one duration or a per-state split (an absent state has no ceiling; the
  timer touches its rows only when they carry their own `keep.age`):

```ts
MemoryJobStore.layerWith({ historyTtl: "7 days" })
DrizzleJobStore.layer({ ...tables, historyTtl: { completed: "1 day", failed: "90 days" } })
```

The sweep honours `min(per-row keep.age, ceiling)`, so a job name that goes
quiet is still pruned on the timer, not only when its group is next acked.

## Tracing

Producer → handler traces connect **across processes and storage**: the
enqueue span's context (`traceId`/`spanId`/`sampled`) is persisted on the
job record, and the worker wraps every handler run in a span whose parent
is that external context (`Tracer.externalSpan`) — your invite handler's
span appears as a child of the HTTP request that scheduled it, even when
they ran hours apart on different machines. Run spans are named
`` `${name}.run` `` by default (configurable via
`Worker.layer({ handlerSpanName: (ctx) => ... })`) and carry
`effectMqJobId`, `effectMqQueue`, and `effectMqAttempt` attributes.

How the handler span attaches follows the delay: **immediate enqueues
continue the producer trace** (parent-child — the email handler sits inside
the signup request's waterfall), while **explicitly delayed/`at`-scheduled
jobs start their own trace with a causal link** back to the producer (a
five-day-wide parent-child trace renders badly and defeats tail sampling).
The policy keys off scheduling *intent* captured at enqueue — queue backlog
never changes your trace shapes — and every retry attempt of a job keeps
its mode. Override per worker with
`Worker.layer({ traceLinking: "auto" | "parent" | "link" | "none" })`. All
producer verbs (`enqueue`, `cancel`, `schedule`, ...) already run in their
own spans. Wire up any Effect tracer/exporter; without one, the overhead is
negligible. Poll-loop iterations are deliberately unspanned — the handler
run is the meaningful trace unit; per-claim spans would flood your backend.

## Metrics

Workers and producers emit Effect `Metric` instruments (exported as the
`Metrics` module). They are **process-local operational signal, not
persisted state** — wire up any Effect-compatible exporter (the Otlp modules
from `effect/unstable/observability`, `@effect/opentelemetry`, Prometheus)
and retention lives in your metrics backend. The *durable* analogues stay in
the store: `store.counts()` for live depth, the attempt ledger for per-run
history — both queryable forever.

| Metric | Type | Tags |
| --- | --- | --- |
| `effect_mq_jobs_enqueued` | counter | `name`, `queue`, `duplicate` |
| `effect_mq_job_runs` | counter | `name`, `queue`, `outcome` |
| `effect_mq_job_run_duration_ms` | histogram | `name`, `queue`, `outcome` |
| `effect_mq_job_wait_duration_ms` | histogram | `name`, `queue` |
| `effect_mq_claims` | counter | `queue`, `result` (claimed/empty) |
| `effect_mq_jobs_in_flight` | gauge | `queue` |
| `effect_mq_queue_depth` | gauge | `queue`, `state` (opt-in sampler) |
| `effect_mq_locks_lost` / `effect_mq_cancel_interrupts` | counter | — |
| `effect_mq_stalled_recovered` | counter | `outcome` |
| `effect_mq_schedule_ticks` | counter | `name` |

`job_wait_duration_ms` is the queue-latency headline: time between a job
becoming runnable and its claim. Depth sampling costs one `counts()` query
per queue per tick, so it is opt-in:
`Worker.layer({ queueMetricsInterval: "15 seconds" })`.

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

## Deduplication

Dedup is a **separate key**, not id derivation — your ids (explicit,
`idGenerator`, or store-assigned) are never rewritten. Keys are scoped per
job name and picked per definition or per enqueue:

```ts
class SyncBenefits extends Job.make("sync-benefits", {
  payload: { employerId: Schema.String },
  dedupe: ({ employerId }) => employerId          // string shorthand = { key }
}) {}

// Per-enqueue, with modes:
yield* SyncBenefits.enqueue(payload, { dedupe: { key: "emp-1", ttl: "1 minute" } })
```

Four behaviors, composable from three fields:

| Options | Behavior |
| --- | --- |
| `{ key }` | dedupe while the keyed job is pending; finishing frees the key |
| `{ key, ttl }` | throttle: at most one job per window, even after completion |
| `{ key, ttl, extend: true }` | debounce: every dropped enqueue pushes the window out |
| `{ key, replace: true }` | while the keyed job is still delayed, the newest payload/metadata/priority/attempts/backoff/keep/timeout/delay replace it (same id; a `ttl` window re-arms) |

A deduplicated enqueue returns the keyed job's id (`duplicate: true` at the
store level). `idempotencyKey` still exists and is different on purpose: it
makes the job id *itself* deterministic (permanent identity, joinable from
your domain tables), while `dedupe` is temporal policy with its own lifecycle.

Keys also power the schedule/reschedule/cancel lifecycle for one-shot future
work — no job-id bookkeeping in your business logic:

```ts
class SendInvite extends Job.make("send-invite", {
  payload: { employeeId: Schema.String },
  dedupe: ({ employeeId }) => ({ key: employeeId, replace: true })
}) {}

// Schedule for a wall-clock instant (any DateTime.Input — zero duration math):
yield* SendInvite.enqueue({ employeeId }, {
  at: DateTime.makeZonedUnsafe(
    { year: 2026, month: 8, day: 24, hours: 9 },
    { timeZone: "America/New_York", adjustForTimeZone: true }
  )
})

// Reschedule: the same idempotent call with a new time (replace moves it).
yield* SendInvite.enqueue({ employeeId }, { at: nextDay })

// They are not onboarding after all — cancel whatever is pending, if anything:
const wasPending = yield* SendInvite.cancelByKey(employeeId)
```

`delay` and `at` are mutually exclusive (a compile error via the option
union); an `at` in the past runs immediately.

Postgres users: dedup adds one table and one jobs column — add
`export const jobDedupe = mqDedupe()` to your schema and `drizzle-kit
generate` diffs both (the table and the new `dedupe_key` column) into one
migration. Memory and Redis need nothing.

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
import {
  mqDedupe,
  mqFlowChildren,
  mqFlowOutbox,
  mqJobAttempts,
  mqJobs,
  mqQueueControl,
  mqSchedules
} from "effect-mq/drizzle-postgres"

// The `name` column is typed to your job tags (derived, not hand-written):
type JobNames = typeof GenerateInvoice._tag | typeof SendEmail._tag

export const jobs = mqJobs<JobNames>()            // default table: effect_mq_jobs
export const jobAttempts = mqJobAttempts(jobs)    // default: effect_mq_job_attempts
export const jobSchedules = mqSchedules()         // default: effect_mq_schedules
export const jobQueues = mqQueueControl()         // default: effect_mq_queue_control
export const jobDedupe = mqDedupe()               // default: effect_mq_dedupe
export const jobFlowChildren = mqFlowChildren()   // default: effect_mq_flow_children
export const jobFlowOutbox = mqFlowOutbox()       // default: effect_mq_flow_outbox
```

Need more indexes (the built-ins cover claiming, listing, metadata
containment, and `name`/`state`/`finishedAt` history)? Every factory takes an
`extraConfig` callback — the same shape as drizzle's third `pgTable` argument —
appended after the built-in indexes and owned by your migrations like
everything else:

```ts
export const jobs = mqJobs<JobNames>("effect_mq_jobs", {
  extraConfig: (t) => [index("jobs_name_recent_idx").on(t.name, t.enqueuedAt.desc())]
})
```

### Custom columns

Need real columns — a tenant id you can FK, RLS-scope, and join — populated
at job creation instead of patched in from job logic? `extend` the jobs
table; at enqueue the store fills each extended column from the job's
`metadata` entry with the same TS key (the definition's
`metadata: (payload) => ...` is your creation-time hook), NULL when absent:

```ts
class SyncPayments extends Job.make("sync-payments", {
  payload: { companyId: Schema.String, objectId: Schema.String },
  metadata: ({ companyId, objectId }) => ({ companyId, objectId })
}) {}

export const jobs = mqJobs<JobNames>("effect_mq_jobs", {
  extend: {
    companyId: text("company_id").notNull(),
    objectId: text("object_id")
  },
  extraConfig: (t) => [index("jobs_company_idx").on(t.companyId, t.state)]
})

// Coercion/renames when the metadata convention isn't enough:
DrizzleJobStore.layer({ ...tables, extraValues: ({ metadata }) => ({ companyId: metadata.companyId }) })
```

`db.select().from(jobs).where(eq(jobs.companyId, tenant))` is fully typed; a
dedupe `replace` rewrites the extended columns with the latest values; a
missing metadata key on a `NOT NULL` column fails the enqueue loudly. Memory
and Redis need nothing — there the metadata projection is already the
queryable surface (`store.list({ metadata: { companyId } })`).

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
import {
  jobAttempts,
  jobDedupe,
  jobFlowChildren,
  jobFlowOutbox,
  jobQueues,
  jobs,
  jobSchedules
} from "./db/schema.ts"

const JobStoreLive = DrizzleJobStore.layer({
  jobs,
  attempts: jobAttempts,
  schedules: jobSchedules,
  queues: jobQueues,
  dedupe: jobDedupe,
  flowChildren: jobFlowChildren,
  flowOutbox: jobFlowOutbox
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

Worker tip: `awaitWake` is LISTEN/NOTIFY-driven and **queue-filtered** (the
NOTIFY payload names the queue, so an enqueue wakes only the takers watching
that queue — many queues don't amplify into many claims). The worker's
`pollInterval` is just the fallback; the 5s default is fine.

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

Wake-ups ride pub/sub (`<prefix>:wake`) with queue-filtered messages, so
idle workers in other processes pick new jobs up promptly and an enqueue
wakes only the takers watching its queue; the worker's `pollInterval` is the
fallback. Notes:

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

## Reference: knobs at a glance

Everything a job definition, an enqueue, and a worker can be tuned with:

**`Job.make(name, options)`** — `payload` (schema or struct fields),
`success`/`error` schemas, `idempotencyKey(payload)`, `dedupe(payload)`,
`metadata(payload)`, `retryable(error)`, `queue`, `store`, and `defaults`
(any per-enqueue option below).

**Per enqueue** (`enqueue`/`execute` options) — `jobId`, `queue`,
`metadata`, `dedupe`, `delay` OR `at` (absolute `DateTime.Input`; exclusive
by type), `priority` (higher first), `attempts`, `backoff`
(`fixed`/`exponential`), `keep` (`count`/`age`), `timeout`.

**Job verbs** — `enqueue`, `enqueueMany` (a whole batch, one store round
trip per chunk; same options minus per-job `jobId`/`dedupe`), `execute` (enqueue +
await the typed result), `poll`, `awaitResult`, `attempts` (the decoded run
ledger), `retry`, `cancel`, `cancelByKey` (by dedup key, idempotent),
`promote`, `schedule`/`unschedule`, `toLayer` (register the handler).

**`Worker.layer(options)`** — all durations take `Duration.Input`:

| Option | Default | Meaning |
| --- | --- | --- |
| `store` | default `JobStore` | which named store this worker claims from |
| `concurrency` | 1 | taker fibers per queue |
| `queues` | — | per-queue overrides: `{ email: { concurrency: 5 } }` |
| `lockDuration` | 30s | how long a claim's lock lasts before it counts as stalled |
| `lockRenewInterval` | half of `lockDuration` | heartbeat cadence (also delivers cross-process cancels) |
| `stalledInterval` | 30s | how often to sweep for stalled jobs |
| `maxStalledCount` | 1 | stalls tolerated before a job is failed outright |
| `pollInterval` | 5s | idle fallback when no wake-up arrives (wake-ups are queue-filtered and push-based, so the default is fine even on Postgres) |
| `scheduleSweepInterval` | 15s | how often to tick due repeatable-job schedules |
| `queueMetricsInterval` | off | sample `store.counts()` per queue into the depth gauge |
| `handlerSpanName` | `` `${name}.run` `` | name of the span wrapping each handler run |
| `traceLinking` | `auto` | parent for immediate jobs, causal link for delayed ones (`parent`/`link`/`none` force a mode) |
| `onJobFailure` | — | callback after each failed run is acked; runs isolated |
| `flows` | — | flows whose children this worker runs (lets its relay push results) |
| `flowSweepInterval` | 30s | flow sweeper cadence + the relay's fallback drain cadence |
| `id` | random | identifier used in lock tokens |

**Store construction** — every driver accepts `idGenerator`, `historyTtl`,
and `historySweepInterval`; drizzle-postgres additionally takes the table
instances (+ `validate`), Redis a key `prefix`.

## Testing your app

Unit-test that services enqueue correctly — no worker, no boilerplate, and
payloads come back **decoded through the job's schema** (so `Redacted`,
`DateTime`, and branded values are real instances, not stored JSON):

```ts
import { TestJobStore } from "effect-mq/testing"

it.effect("signup enqueues a welcome email", () =>
  Effect.gen(function*() {
    yield* SignupService.register({ email: "ada@example.com" })

    const emails = yield* TestJobStore.enqueuedOf(SendEmail)
    expect(emails).toHaveLength(1)
    expect(emails[0]?.payload.to).toBe("ada@example.com")
    expect(emails[0]?.state).toBe("waiting")
  }).pipe(Effect.provide(TestJobStore.layer)))
```

`TestJobStore.layer` provides a fresh in-memory store as both the default
`JobStore` (for the code under test) and the inspection service; jobs just
accumulate in `waiting`/`delayed` since nothing claims them. Named stores
use `TestJobStore.layerFor(Durable)`. Records surface scheduling detail
(`state`, `priority`, `runAt`, `metadata`, `dedupeKey`), and the raw store
is exposed for simulating claims/acks. No `@effect/vitest` required — it is
plain Effect, so it works with any test runner.

## Writing a storage driver

Implement the `JobStore` service (one atomic seam: `enqueue`/`enqueueMany`,
`claim`, `ack`, `release`, `extendLocks`, `recoverStalled`, `awaitWake`,
`getJob`, `getAttempts`, `list`, `retry`, `counts`, `remove`, `cancel`,
`promote`, `pause`/`resume`/`pausedQueues`, `cancelByDedupe`, the schedule
ops `upsertSchedule`/`removeSchedule`/`listSchedules`/`dueSchedules`/
`tickSchedule`/`advanceSchedule`, and the flow ops `recordChildResults`/
`listChildResults`/`flowSweepWork`/`markChildrenCascaded`/`peekOutbox`/
`deleteOutbox`) and run the conformance suite against it:

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

Next up: drizzle schema customization (column renames, native id and
timestamp column types, a typed queue registry), a cross-process event
stream, and global queue concurrency/rate limits. Full prioritized list:
[ROADMAP.md](https://github.com/TeamWarp/effect-mq/blob/main/ROADMAP.md);
release history:
[CHANGELOG.md](https://github.com/TeamWarp/effect-mq/blob/main/CHANGELOG.md).

## License

MIT
