# Postgres (drizzle)

The Postgres store (`effect-mq/drizzle-postgres`) runs on drizzle v1's Effect driver (`drizzle-orm/effect-postgres`, built on `@effect/sql-pg`; works on Node and Bun). Its tables live inside *your* drizzle schema, so your migration pipeline owns the DDL and your product code queries the queue like any other table.

Install the optional peers:

```sh
bun add drizzle-orm@rc @effect/sql-pg@rc
```

| Peer | Range |
| --- | --- |
| `drizzle-orm` | `>=1.0.0-rc <2` |
| `@effect/sql-pg` | `>=4.0.0-rc <5` |

## 1. Put the tables in your drizzle schema

The factories are the single source of truth for the table layout. Re-export them from your schema file. The library never runs DDL, so there is no parallel migration system to reconcile with yours:

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

export const jobs = mqJobs<JobNames>()              // default table: effect_mq_jobs
export const jobAttempts = mqJobAttempts(jobs)      // default: effect_mq_job_attempts
export const jobSchedules = mqSchedules()           // default: effect_mq_schedules
export const jobQueues = mqQueueControl()           // default: effect_mq_queue_control
export const jobDedupe = mqDedupe()                 // default: effect_mq_dedupe
export const jobFlowChildren = mqFlowChildren()     // default: effect_mq_flow_children
export const jobFlowOutbox = mqFlowOutbox()         // default: effect_mq_flow_outbox
```

Then generate and run the migration like any other schema change:

```sh
drizzle-kit generate   # emits the CREATE TABLE migration next to your others
drizzle-kit migrate
```

When a future effect-mq version changes the layout, the factory changes and `drizzle-kit generate` diffs it: you get a normal, reviewable migration.

Every factory takes an optional table name as the first argument (`mqJobAttempts` takes the jobs table first) and an `extraConfig` callback, the same shape as drizzle's third `pgTable` argument, for your own indexes on top of the built-ins (which cover claiming, listing, metadata containment, and `name`/`state`/`finishedAt` history):

```ts
export const jobs = mqJobs<JobNames>("effect_mq_jobs", {
  extraConfig: (t) => [index("jobs_name_recent_idx").on(t.name, t.enqueuedAt.desc())]
})
```

## 2. Provide the store layer

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

`DrizzleJobStore.layer` accepts:

| Option | Default | Meaning |
| --- | --- | --- |
| `jobs`, `attempts`, `schedules`, `queues`, `dedupe`, `flowChildren`, `flowOutbox` | required | the table instances from the factories above |
| `store` | default `JobStore` | bind to a [named store](/storage/stores) key |
| `historyTtl` | off | store-level retention ceiling: one duration or a per-state split (see [Retention](/guide/retention)) |
| `historySweepInterval` | 1 minute | history sweep cadence |
| `idGenerator` | `j-<seq>` | generator for store-assigned job ids |
| `extraValues` | metadata convention | override values for `extend`ed columns |
| `validate` | `true` | probe the tables at startup, fail fast if migrations haven't run |

The startup probe fails with a clear message when the tables are missing or mismatched; `validate: false` defers that to first use.

## Custom columns

To get real columns (a tenant id you can FK, RLS-scope, and join) populated at job creation rather than patched in from job logic, `extend` the jobs table. At enqueue the store fills each extended column from the job's `metadata` entry with the same TS key (the definition's `metadata: (payload) => ...` is your creation-time hook), NULL when absent:

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
```

When the metadata convention isn't enough (coercions, renames), override the mapping at the store level:

```ts
DrizzleJobStore.layer({ ...tables, extraValues: ({ metadata }) => ({ companyId: metadata.companyId }) })
```

The rules:

- `db.select().from(jobs).where(eq(jobs.companyId, tenant))` is fully typed.
- A dedupe `replace` rewrites the extended columns with the latest values.
- A missing metadata key on a `NOT NULL` column fails the enqueue with a `JobStoreError`; no silent partial rows.

Memory and Redis need nothing here: there the metadata projection is the queryable surface (`store.list({ metadata: { companyId } })`).

## Custom job ids

Store-assigned ids default to a compact sequence (`j-<n>`). Bring your own generator (sync or Effect-returning) when you want globally unique or prefixed ids:

```ts
import { ulid } from "ulid"

DrizzleJobStore.layer({
  ...tables,
  idGenerator: ({ name }) => `${name}_${ulid()}`
})
```

The generator only runs for store-assigned ids: `idempotencyKey` ids and repeatable-schedule tick ids stay deterministic (exactly-once depends on them). The store retries collisions a bounded number of times, then fails the enqueue; bring real entropy.

## How it works

Three mechanics worth knowing when you operate this store:

- **Claims use `FOR UPDATE SKIP LOCKED`**: any number of worker processes claim concurrently without lock contention; a partial index on `(queue, priority desc, seq)` serves the claim path.
- **Wake-ups use LISTEN/NOTIFY, queue-filtered**: the NOTIFY payload names the queue, so an enqueue wakes only the takers watching that queue; many queues don't amplify into many claims. The worker's `pollInterval` is the fallback (the 5s default is fine). If the LISTEN subscription drops, wake-ups degrade to polling until it resubscribes.
- **All time arrives as bind parameters from the Effect Clock**, never SQL `now()`, so the [conformance suite](/storage/writing-a-driver) runs against real Postgres under `TestClock`.

## Reads yes, writes no

Product UIs read the tables directly with drizzle, fully typed:

```ts
db.select().from(jobs).where(and(
  eq(jobs.name, "generate-invoice"),                 // a typo is a compile error
  sql`${jobs.metadata} @> ${{ customerId }}::jsonb`  // GIN-indexed containment
))

db.select().from(jobAttempts)
  .innerJoin(jobs, eq(jobAttempts.jobId, jobs.id))
  .where(eq(jobAttempts.outcome, "failed"))
```

::: warning
Never mutate job rows yourself. Mutations must go through the store (`MyJob.retry(id)`, `store.remove(id)`) so lock tokens, attempt accounting, and wake-up notifications stay coherent.
:::

## Upgrading

Ship layout changes through your normal migration flow: bump effect-mq, run `drizzle-kit generate`, review the diff. 0.4.0 added the `trace` jsonb column to the jobs table (cross-process [trace propagation](/guide/observability)); 0.5.0 added the nullable `group_name` column to the schedules table ([schedule reconciliation](/guide/repeatable-jobs#declaring-the-full-set-reconciliation)). `drizzle-kit generate` picks each up as one small migration; migrate before deploying the new version.

0.6.0 ([parent-child flows](/guide/flows)) is a bigger step, and it applies even if you never define a flow:

1. The jobs table gains nullable columns: `parent` jsonb plus the flow bookkeeping (`flow_fail_fast` boolean and the `flow_pending`/`flow_completed`/`flow_failed`/`flow_cancelled` integers).
2. Two new tables arrive as schema factories: export `mqFlowChildren()` and `mqFlowOutbox()` from your schema as shown above.
3. `DrizzleJobStore.layer` now **requires** the `flowChildren` and `flowOutbox` table options; upgrading without them is a compile error, which is the intended nudge to run the migration first.

`drizzle-kit generate` emits all of it as one migration once the factory exports are in your schema; migrate before deploying 0.6.0.

## Where to next

- [Multiple stores](/storage/stores): bind jobs to named stores across infrastructures.
- [Retention](/guide/retention): `keep` and `historyTtl` interplay.
- [Redis](/storage/redis): the other production store.
- [Writing a driver](/storage/writing-a-driver): the conformance suite this store passes.
