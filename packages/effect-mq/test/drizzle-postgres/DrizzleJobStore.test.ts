import { Job, JobStore, Worker } from "../../src/index.ts"
import { PgClient } from "@effect/sql-pg"
import { jobStoreConformance } from "../../src/testing/index.ts"
import { assert, describe, expect, it } from "@effect/vitest"
import { EffectDrizzleQueryError } from "drizzle-orm/effect-core/errors"
import { getTableConfig, index, text } from "drizzle-orm/pg-core"
import { Cause, Effect, Exit, Fiber, Layer, Option, Schedule, Schema } from "effect"
import { DeadlockError, SqlError, UnknownError } from "effect/unstable/sql/SqlError"
import {
  DrizzleJobStore,
  mqDedupe,
  mqFlowChildren,
  mqFlowOutbox,
  mqJobAttempts,
  mqJobs,
  mqQueueControl,
  mqSchedules
} from "../../src/drizzle-postgres/index.ts"
import {
  createTablesSql,
  dropTablesSql,
  freshStoreEffect,
  freshStoreLayer,
  freshTableNames,
  pgAvailable,
  pgClientLive,
  pgUrl
} from "./support.ts"

const available = await pgAvailable()

if (!available) {
  describe("DrizzleJobStore (Postgres)", () => {
    it.skip(`skipped: no Postgres at ${pgUrl} — run \`docker compose up -d --wait\``, () => {})
  })
} else {
  // The shared contract, against real Postgres. Works under TestClock because
  // the driver derives all time from the Effect Clock as bind parameters.
  jobStoreConformance("DrizzleJobStore (Postgres)", freshStoreLayer)

  describe("DrizzleJobStore specifics", () => {
    it.effect("schema factories match the driver's expected DDL (drift guard)", () =>
      Effect.gen(function*() {
        const client = yield* PgClient.PgClient
        const names = freshTableNames()
        for (const statement of createTablesSql(names)) {
          yield* client.unsafe(statement).pipe(Effect.orDie)
        }
        yield* Effect.addFinalizer(() => client.unsafe(dropTablesSql(names)).pipe(Effect.ignore))

        const typeMap = new Map<string, string>([
          ["PgBoolean", "boolean"],
          ["PgText", "text"],
          ["PgInteger", "integer"],
          ["PgBigInt53", "bigint"],
          ["PgBigSerial53", "bigint"],
          ["PgJsonb", "jsonb"],
          ["PgTimestamp", "timestamp with time zone"]
        ])
        for (
          const [table, tableName] of [
            [mqJobs(names.jobs), names.jobs],
            [mqJobAttempts(mqJobs(names.jobs), names.attempts), names.attempts],
            [mqSchedules(names.schedules), names.schedules],
            [mqQueueControl(names.queues), names.queues],
            [mqDedupe(names.dedupe), names.dedupe],
            [mqFlowChildren(names.flowChildren), names.flowChildren],
            [mqFlowOutbox(names.flowOutbox), names.flowOutbox]
          ] as const
        ) {
          const config = getTableConfig(table)
          const rows = yield* client.unsafe<{
            column_name: string
            data_type: string
            is_nullable: string
          }>(
            `SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = '${tableName}'`
          ).pipe(Effect.orDie)
          const actual = new Map(rows.map((row) => [row.column_name, row]))
          expect(actual.size).toBe(config.columns.length)
          for (const column of config.columns) {
            const dbColumn = actual.get(column.name)
            expect(dbColumn, `column ${tableName}.${column.name} missing in DDL`).toBeDefined()
            const expectedType = typeMap.get(column.columnType)
            expect(dbColumn?.data_type, `${tableName}.${column.name} type`).toBe(expectedType)
            expect(dbColumn?.is_nullable, `${tableName}.${column.name} nullability`).toBe(
              column.notNull ? "NO" : "YES"
            )
          }
        }
      }).pipe(Effect.scoped, Effect.provide(pgClientLive())))

    it.live("runs jobs end-to-end through the Worker against real Postgres", () =>
      Effect.gen(function*() {
        const { jobs, store } = yield* freshStoreEffect
        void jobs
        const storeLayer = Layer.succeed(JobStore.JobStore, store)

        class SendReport extends Job.make("SendReport", {
          payload: { month: Schema.String },
          success: Schema.String,
          metadata: ({ month }) => ({ month }),
          defaults: { attempts: 2, backoff: { type: "fixed", delay: "30 millis" } }
        }) {}

        let attempt = 0
        const handlers = SendReport.toLayer((payload) =>
          Effect.gen(function*() {
            attempt++
            const current = yield* Worker.CurrentJob
            return current.attempt === 1
              ? yield* Effect.die("transient failure")
              : `report ${payload.month} sent`
          })
        )

        const result = yield* SendReport.execute({ month: "2026-08" }).pipe(
          Effect.timeout("10 seconds"),
          Effect.provide(
            handlers.pipe(
              Layer.provideMerge(Worker.layer({ pollInterval: "100 millis", lockDuration: "5 seconds" })),
              Layer.provideMerge(storeLayer)
            )
          )
        )
        expect(result).toBe("report 2026-08 sent")
        expect(attempt).toBe(2)

        // The run ledger and metadata landed in Postgres. The id is
        // deterministic because... there is no idempotencyKey, so find it via
        // the metadata projection instead.
        const listed = yield* store.list({ metadata: { month: "2026-08" } })
        expect(listed.items).toHaveLength(1)
        const record = listed.items[0]
        assert(record !== undefined)
        expect(record.state).toBe("completed")
        expect(record.attemptsMade).toBe(2)

        const ledger = yield* store.getAttempts(record.id)
        expect(ledger.map((entry) => entry.outcome)).toEqual(["retried", "completed"])
      }).pipe(Effect.scoped, Effect.provide(pgClientLive())))

    it.effect("two named stores can live in one database as separate table pairs", () =>
      Effect.gen(function*() {
        const Durable = JobStore.named("pg-durable")
        const Ephemeral = JobStore.named("pg-ephemeral")
        const first = yield* freshStoreEffect
        const second = yield* freshStoreEffect

        yield* Effect.gen(function*() {
          const durable = yield* Durable
          const ephemeral = yield* Ephemeral
          yield* durable.enqueue({
            id: undefined,
            name: "A",
            queue: JobStore.QueueName("default"),
            payload: {},
            metadata: {},
            priority: 0,
            attemptsMax: 1,
            backoff: undefined,
            keep: undefined,
            timeoutMs: undefined,
            dedupe: undefined,
            trace: undefined,
            parent: undefined,
            delayMs: 0
          })
          expect((yield* durable.counts()).waiting).toBe(1)
          expect((yield* ephemeral.counts()).waiting).toBe(0)
        }).pipe(
          Effect.provide(
            Layer.mergeAll(
              Layer.succeed(Durable, first.store),
              Layer.succeed(Ephemeral, second.store)
            )
          )
        )
      }).pipe(Effect.scoped, Effect.provide(pgClientLive())))

    it.effect("store-assigned ids come from the configured idGenerator; collisions fail after bounded retries", () =>
      Effect.gen(function*() {
        const client = yield* PgClient.PgClient
        const names = freshTableNames()
        const jobs = mqJobs(names.jobs)
        const attempts = mqJobAttempts(jobs, names.attempts)
        const schedules = mqSchedules(names.schedules)
        const queues = mqQueueControl(names.queues)
        const dedupe = mqDedupe(names.dedupe)
        const flowChildren = mqFlowChildren(names.flowChildren)
        const flowOutbox = mqFlowOutbox(names.flowOutbox)
        for (const statement of createTablesSql(names)) {
          yield* client.unsafe(statement).pipe(Effect.orDie)
        }
        yield* Effect.addFinalizer(() => client.unsafe(dropTablesSql(names)).pipe(Effect.ignore))
        let n = 0
        const store = yield* DrizzleJobStore.make({
          jobs,
          attempts,
          schedules,
          queues,
          dedupe,
          flowChildren,
          flowOutbox,
          idGenerator: ({ name }) => `job_${name}_${++n}`
        }).pipe(Effect.orDie)

        const base = {
          id: undefined,
          queue: JobStore.QueueName("default"),
          payload: {},
          metadata: {},
          priority: 0,
          attemptsMax: 1,
          backoff: undefined,
          keep: undefined,
          timeoutMs: undefined,
          dedupe: undefined,
          trace: undefined,
          parent: undefined,
          delayMs: 0
        }
        const first = yield* store.enqueue({ ...base, name: "Gen" })
        expect(first.id).toBe("job_Gen_1")

        // User-supplied ids never consult the generator.
        const custom = yield* store.enqueue({ ...base, name: "Gen", id: JobStore.JobId("mine") })
        expect(custom.id).toBe("mine")
        expect(n).toBe(1)

        // Occupy every id the replayed generator will propose: the bounded
        // retry loop must exhaust and fail rather than spin.
        for (let i = 2; i <= 5; i++) {
          yield* store.enqueue({ ...base, name: "Gen", id: JobStore.JobId(`job_Gen_${i}`) })
        }
        n = 0
        const again = yield* Effect.flip(store.enqueue({ ...base, name: "Gen" }))
        expect(again._tag).toBe("JobStoreError")
        expect(n).toBe(5)
      }).pipe(Effect.scoped, Effect.provide(pgClientLive())))

    it.effect("concurrent first-enqueues on one dedup key create exactly one job", () =>
      Effect.gen(function*() {
        const { store } = yield* freshStoreEffect
        const request = {
          id: undefined,
          name: "Race",
          queue: JobStore.QueueName("default"),
          payload: {},
          metadata: {},
          priority: 0,
          attemptsMax: 1,
          backoff: undefined,
          keep: undefined,
          timeoutMs: undefined,
          dedupe: { key: "race", ttlMs: undefined, extend: false, replace: false },
          trace: undefined,
          parent: undefined,
          delayMs: 0
        }
        const results = yield* Effect.all(
          [store.enqueue(request), store.enqueue(request), store.enqueue(request)],
          { concurrency: 3 }
        )
        expect(results.filter((r) => !r.duplicate)).toHaveLength(1)
        expect(new Set(results.map((r) => r.id)).size).toBe(1)
        expect((yield* store.counts()).waiting).toBe(1)
      }).pipe(Effect.scoped, Effect.provide(pgClientLive())))

    it.effect("extended columns fill from metadata at enqueue; extraValues and replace override", () =>
      Effect.gen(function*() {
        const client = yield* PgClient.PgClient
        const names = freshTableNames()
        const jobs = mqJobs(names.jobs, {
          extend: {
            companyId: text("company_id").notNull(),
            objectId: text("object_id")
          },
          // Extended columns are visible to extraConfig, fully typed.
          extraConfig: (t) => [index(`${names.jobs}_company_idx`).on(t.companyId, t.state)]
        })
        const attempts = mqJobAttempts(jobs, names.attempts)
        const schedules = mqSchedules(names.schedules)
        const queues = mqQueueControl(names.queues)
        const dedupe = mqDedupe(names.dedupe)
        const flowChildren = mqFlowChildren(names.flowChildren)
        const flowOutbox = mqFlowOutbox(names.flowOutbox)
        for (const statement of createTablesSql(names)) {
          yield* client.unsafe(statement).pipe(Effect.orDie)
        }
        yield* client.unsafe(
          `ALTER TABLE "${names.jobs}" ADD COLUMN company_id text NOT NULL, ADD COLUMN object_id text`
        ).pipe(Effect.orDie)
        yield* Effect.addFinalizer(() => client.unsafe(dropTablesSql(names)).pipe(Effect.ignore))
        const store = yield* DrizzleJobStore.make({ jobs, attempts, schedules, queues, dedupe, flowChildren, flowOutbox }).pipe(Effect.orDie)

        const base = {
          id: undefined,
          name: "Tenant",
          queue: JobStore.QueueName("default"),
          payload: {},
          metadata: { companyId: "acme", objectId: "obj-1" },
          priority: 0,
          attemptsMax: 1,
          backoff: undefined,
          keep: undefined,
          timeoutMs: undefined,
          dedupe: undefined,
          trace: undefined,
          parent: undefined,
          delayMs: 0
        }
        // Default mapping: metadata[<TS key>] lands in the column at INSERT.
        const first = yield* store.enqueue(base)
        const row = yield* client.unsafe(
          `SELECT company_id, object_id FROM "${names.jobs}" WHERE id = '${first.id}'`
        ).pipe(Effect.orDie)
        expect(row[0]).toEqual({ company_id: "acme", object_id: "obj-1" })

        // Absent metadata key -> NULL for nullable columns...
        const partial = yield* store.enqueue({ ...base, metadata: { companyId: "acme" } })
        const partialRow = yield* client.unsafe(
          `SELECT object_id FROM "${names.jobs}" WHERE id = '${partial.id}'`
        ).pipe(Effect.orDie)
        expect(partialRow[0]).toEqual({ object_id: null })

        // ...and a loud JobStoreError for NOT NULL ones.
        const violated = yield* Effect.flip(store.enqueue({ ...base, metadata: {} }))
        expect(violated._tag).toBe("JobStoreError")

        // extraValues overrides the metadata convention.
        const mapped = yield* DrizzleJobStore.make({
          jobs,
          attempts,
          schedules,
          queues,
          dedupe,
          flowChildren,
          flowOutbox,
          extraValues: ({ metadata }) => ({ companyId: `tenant-${metadata.companyId}` })
        }).pipe(Effect.orDie)
        const overridden = yield* mapped.enqueue(base)
        const overriddenRow = yield* client.unsafe(
          `SELECT company_id, object_id FROM "${names.jobs}" WHERE id = '${overridden.id}'`
        ).pipe(Effect.orDie)
        expect(overriddenRow[0]).toEqual({ company_id: "tenant-acme", object_id: "obj-1" })

        // A dedupe replace rewrites the extended columns with the latest values.
        const keyed = {
          ...base,
          metadata: { companyId: "acme", objectId: "v1" },
          dedupe: { key: "obj", ttlMs: undefined, extend: false, replace: true },
          trace: undefined,
          parent: undefined,
          delayMs: 60_000
        }
        const job = yield* store.enqueue(keyed)
        yield* store.enqueue({ ...keyed, metadata: { companyId: "acme", objectId: "v2" } })
        const replacedRow = yield* client.unsafe(
          `SELECT object_id FROM "${names.jobs}" WHERE id = '${job.id}'`
        ).pipe(Effect.orDie)
        expect(replacedRow[0]).toEqual({ object_id: "v2" })
      }).pipe(Effect.scoped, Effect.provide(pgClientLive())))

    it.effect("extended columns fill through enqueueMany and tickSchedule", () =>
      Effect.gen(function*() {
        const client = yield* PgClient.PgClient
        const names = freshTableNames()
        const jobs = mqJobs(names.jobs, {
          extend: {
            companyId: text("company_id").notNull(),
            objectId: text("object_id")
          }
        })
        const attempts = mqJobAttempts(jobs, names.attempts)
        const schedules = mqSchedules(names.schedules)
        const queues = mqQueueControl(names.queues)
        const dedupe = mqDedupe(names.dedupe)
        const flowChildren = mqFlowChildren(names.flowChildren)
        const flowOutbox = mqFlowOutbox(names.flowOutbox)
        for (const statement of createTablesSql(names)) {
          yield* client.unsafe(statement).pipe(Effect.orDie)
        }
        yield* client.unsafe(
          `ALTER TABLE "${names.jobs}" ADD COLUMN company_id text NOT NULL, ADD COLUMN object_id text`
        ).pipe(Effect.orDie)
        yield* Effect.addFinalizer(() => client.unsafe(dropTablesSql(names)).pipe(Effect.ignore))
        const store = yield* DrizzleJobStore.make({ jobs, attempts, schedules, queues, dedupe, flowChildren, flowOutbox }).pipe(Effect.orDie)

        const base = {
          id: undefined,
          name: "Tenant",
          queue: JobStore.QueueName("default"),
          payload: {},
          metadata: { companyId: "acme", objectId: "obj-1" },
          priority: 0,
          attemptsMax: 1,
          backoff: undefined,
          keep: undefined,
          timeoutMs: undefined,
          dedupe: undefined,
          trace: undefined,
          parent: undefined,
          delayMs: 0
        }
        // The batch INSERT is a second hand-rolled VALUES list; it must fill
        // extras exactly like the single-enqueue path (metadata mapping,
        // NULL for absent nullable keys).
        const batch = yield* store.enqueueMany([
          base,
          { ...base, metadata: { companyId: "acme" } }
        ])
        const rows = yield* client.unsafe(
          `SELECT company_id, object_id FROM "${names.jobs}" WHERE id IN ('${batch[0]?.id}', '${batch[1]?.id}') ORDER BY seq`
        ).pipe(Effect.orDie)
        expect(rows).toEqual([
          { company_id: "acme", object_id: "obj-1" },
          { company_id: "acme", object_id: null }
        ])

        // NOT NULL violations stay loud through the batch path.
        const violated = yield* Effect.flip(store.enqueueMany([{ ...base, metadata: {} }]))
        expect(violated._tag).toBe("JobStoreError")

        // extraValues overrides apply per batch row too.
        const mapped = yield* DrizzleJobStore.make({
          jobs,
          attempts,
          schedules,
          queues,
          dedupe,
          flowChildren,
          flowOutbox,
          extraValues: ({ metadata }) => ({ companyId: `tenant-${metadata.companyId}` })
        }).pipe(Effect.orDie)
        const overridden = yield* mapped.enqueueMany([base])
        const overriddenRow = yield* client.unsafe(
          `SELECT company_id FROM "${names.jobs}" WHERE id = '${overridden[0]?.id}'`
        ).pipe(Effect.orDie)
        expect(overriddenRow[0]).toEqual({ company_id: "tenant-acme" })

        // tickSchedule's insert fills extras from the tick request metadata.
        yield* store.upsertSchedule({
          key: JobStore.ScheduleKey("Tenant/minutely"),
          jobName: "Tenant",
          queue: JobStore.QueueName("default"),
          cron: undefined,
          tz: undefined,
          everyMs: 60_000,
          payload: {},
          metadata: { companyId: "acme" },
          priority: 0,
          attemptsMax: 1,
          backoff: undefined,
          keep: undefined,
          timeoutMs: undefined,
          group: undefined,
          nextRunAt: 0
        })
        const fired = yield* store.tickSchedule(
          JobStore.ScheduleKey("Tenant/minutely"),
          0,
          60_000,
          { ...base, id: JobStore.JobId("sched/Tenant/minutely/0"), metadata: { companyId: "sched-acme" } }
        )
        expect(fired).toBe(true)
        const tickRow = yield* client.unsafe(
          `SELECT company_id, object_id FROM "${names.jobs}" WHERE id = 'sched/Tenant/minutely/0'`
        ).pipe(Effect.orDie)
        expect(tickRow[0]).toEqual({ company_id: "sched-acme", object_id: null })
      }).pipe(Effect.scoped, Effect.provide(pgClientLive())))

    it.live("historyTtl sweeps terminal rows; live rows survive", () =>
      Effect.gen(function*() {
        const client = yield* PgClient.PgClient
        const names = freshTableNames()
        const jobs = mqJobs(names.jobs)
        const attempts = mqJobAttempts(jobs, names.attempts)
        const schedules = mqSchedules(names.schedules)
        const queues = mqQueueControl(names.queues)
        const dedupe = mqDedupe(names.dedupe)
        const flowChildren = mqFlowChildren(names.flowChildren)
        const flowOutbox = mqFlowOutbox(names.flowOutbox)
        for (const statement of createTablesSql(names)) {
          yield* client.unsafe(statement).pipe(Effect.orDie)
        }
        yield* Effect.addFinalizer(() => client.unsafe(dropTablesSql(names)).pipe(Effect.ignore))
        const store = yield* DrizzleJobStore.make({
          jobs,
          attempts,
          schedules,
          queues,
          dedupe,
          flowChildren,
          flowOutbox,
          historyTtl: "80 millis",
          historySweepInterval: "40 millis"
        }).pipe(Effect.orDie)

        const base = {
          id: undefined,
          queue: JobStore.QueueName("default"),
          payload: {},
          metadata: {},
          priority: 0,
          attemptsMax: 1,
          backoff: undefined,
          keep: undefined,
          timeoutMs: undefined,
          dedupe: undefined,
          trace: undefined,
          parent: undefined,
          delayMs: 0
        }
        const done = yield* store.enqueue({ ...base, name: "Swept" })
        const claim = yield* store.claim({
          queue: JobStore.QueueName("default"),
          names: ["Swept"],
          token: "t-ttl",
          lockDurationMs: 5_000
        })
        assert(claim._tag === "Claimed")
        yield* store.ack(done.id, "t-ttl", { _tag: "Complete", exit: undefined })
        const waiting = yield* store.enqueue({ ...base, name: "Waits" })

        // Poll until the sweeper fires (bounded — the suite must not hang).
        yield* store.getJob(done.id).pipe(
          Effect.flatMap((record) =>
            Option.isNone(record) ? Effect.void : Effect.fail(new Error("not swept yet"))
          ),
          Effect.retry({ schedule: Schedule.spaced("40 millis"), times: 50 })
        )
        expect(Option.isSome(yield* store.getJob(waiting.id))).toBe(true)
      }).pipe(Effect.scoped, Effect.provide(pgClientLive())))

    it.effect("payloads and exits survive Postgres storage as decodable JSON", () =>
      Effect.gen(function*() {
        const { store } = yield* freshStoreEffect
        const Typed = Job.make("Typed", {
          payload: { n: Schema.Number },
          success: Schema.String
        })
        const storeLayer = Layer.succeed(JobStore.JobStore, store)

        const id = yield* Typed.enqueue({ n: 42 }).pipe(Effect.provide(storeLayer))
        const claim = yield* store.claim({
          queue: JobStore.QueueName("default"),
          names: ["Typed"],
          token: "t-1",
          lockDurationMs: 5_000
        })
        assert(claim._tag === "Claimed")
        expect(claim.job.payload).toEqual({ n: 42 })

        const encodedExit = yield* Schema.encodeEffect(Typed.exitSchema)(
          yield* Effect.exit(Effect.succeed("done"))
        ).pipe(Effect.orDie)
        yield* store.ack(id, "t-1", { _tag: "Complete", exit: encodedExit })

        const status = yield* Typed.poll(id).pipe(Effect.provide(storeLayer))
        assert(Option.isSome(status))
        assert(Option.isSome(status.value.exit))
        expect(status.value.state).toBe("completed")
      }).pipe(Effect.scoped, Effect.provide(pgClientLive())))

    it.live("cross-process NOTIFY wakes a waiter in another store instance", () =>
      Effect.gen(function*() {
        // Two make() instances over the SAME tables simulate two processes:
        // instance A's local wakeVersion bump can't help — only the NOTIFY
        // delivered through LISTEN can wake it.
        const a = yield* freshStoreEffect
        const b = yield* DrizzleJobStore.make({
          jobs: a.jobs,
          attempts: a.attempts,
          schedules: a.schedules,
          queues: a.queues,
          dedupe: a.dedupe,
          flowChildren: a.flowChildren,
          flowOutbox: a.flowOutbox
        })

        const empty = yield* a.store.claim({
          queue: JobStore.QueueName("default"),
          names: ["Cross"],
          token: "t-a",
          lockDurationMs: 5_000
        })
        assert(empty._tag === "Empty")

        const waiter = yield* Effect.forkChild(
          a.store.awaitWake([JobStore.QueueName("default")], empty.wakeToken).pipe(
            Effect.timeoutOption("5 seconds")
          )
        )
        // Give the LISTEN subscription a moment to be established.
        yield* Effect.sleep("300 millis")
        yield* b.enqueue({
          id: undefined,
          name: "Cross",
          queue: JobStore.QueueName("default"),
          payload: {},
          metadata: {},
          priority: 0,
          attemptsMax: 1,
          backoff: undefined,
          keep: undefined,
          timeoutMs: undefined,
          dedupe: undefined,
          trace: undefined,
          parent: undefined,
          delayMs: 0
        })
        const woken = yield* Fiber.join(waiter)
        expect(Option.isSome(woken)).toBe(true)
      }).pipe(Effect.scoped, Effect.provide(pgClientLive())))

    it.live("concurrent claims never hand out the same job twice", () =>
      Effect.gen(function*() {
        const { store } = yield* freshStoreEffect
        for (let i = 0; i < 8; i++) {
          yield* store.enqueue({
            id: undefined,
            name: "Par",
            queue: JobStore.QueueName("default"),
            payload: { i },
            metadata: {},
            priority: 0,
            attemptsMax: 1,
            backoff: undefined,
            keep: undefined,
            timeoutMs: undefined,
            dedupe: undefined,
            trace: undefined,
            parent: undefined,
            delayMs: 0
          })
        }
        const results = yield* Effect.all(
          Array.from({ length: 16 }, (_, i) =>
            store.claim({
              queue: JobStore.QueueName("default"),
              names: ["Par"],
              token: `t-${i}`,
              lockDurationMs: 5_000
            })),
          { concurrency: "unbounded" }
        )
        const claimedIds = results.flatMap((result) =>
          result._tag === "Claimed" ? [result.job.id] : []
        )
        expect(claimedIds).toHaveLength(8)
        expect(new Set(claimedIds).size).toBe(8)
      }).pipe(Effect.scoped, Effect.provide(pgClientLive())))

    it.live("DrizzleJobStore.layer + a named-store Worker drain jobs end-to-end (with UI retry)", () =>
      Effect.gen(function*() {
        const names = freshTableNames()
        const client = yield* PgClient.PgClient
        for (const statement of createTablesSql(names)) {
          yield* client.unsafe(statement).pipe(Effect.orDie)
        }
        yield* Effect.addFinalizer(() => client.unsafe(dropTablesSql(names)).pipe(Effect.ignore))
        const jobs = mqJobs(names.jobs)
        const attempts = mqJobAttempts(jobs, names.attempts)
        const schedules = mqSchedules(names.schedules)
        const queues = mqQueueControl(names.queues)
        const dedupe = mqDedupe(names.dedupe)
        const flowChildren = mqFlowChildren(names.flowChildren)
        const flowOutbox = mqFlowOutbox(names.flowOutbox)

        const Durable = JobStore.named("pg-live-durable")
        const Flaky = Job.make("PgFlaky", {
          payload: { n: Schema.Number },
          success: Schema.Number,
          store: Durable
        })
        let runs = 0
        const handlers = Flaky.toLayer(({ n }) =>
          Effect.suspend(() => {
            runs++
            return runs === 1 ? Effect.die("boom") : Effect.succeed(n * 2)
          })
        )
        const stack = handlers.pipe(
          Layer.provideMerge(Worker.layer({ store: Durable, pollInterval: "100 millis" })),
          Layer.provideMerge(
            DrizzleJobStore.layer({ jobs, attempts, schedules, queues, dedupe, flowChildren, flowOutbox, store: Durable }).pipe(Layer.orDie)
          ),
          Layer.provide(pgClientLive())
        )

        yield* Effect.gen(function*() {
          // attempts=1 (default): the first run's defect fails the job.
          const id = yield* Flaky.enqueue({ n: 21 })
          const failed = yield* Effect.exit(
            Flaky.awaitResult(id).pipe(Effect.timeout("10 seconds"))
          )
          assert(Exit.isFailure(failed))

          // The dashboard "retry" button gives it a fresh budget.
          yield* Flaky.retry(id)
          const result = yield* Flaky.awaitResult(id).pipe(Effect.timeout("10 seconds"))
          expect(result).toBe(42)

          const ledger = yield* Flaky.attempts(id)
          expect(ledger.map((entry) => entry.outcome)).toEqual(["failed", "completed"])
        }).pipe(Effect.provide(stack))
      }).pipe(Effect.scoped, Effect.provide(pgClientLive())))

    it.effect("validate fails fast with a helpful error when tables are missing", () =>
      Effect.gen(function*() {
        const names = freshTableNames()
        const jobs = mqJobs(names.jobs)
        const attempts = mqJobAttempts(jobs, names.attempts)
        const schedules = mqSchedules(names.schedules)
        const queues = mqQueueControl(names.queues)
        const dedupe = mqDedupe(names.dedupe)
        const flowChildren = mqFlowChildren(names.flowChildren)
        const flowOutbox = mqFlowOutbox(names.flowOutbox)
        const result = yield* Effect.exit(DrizzleJobStore.make({ jobs, attempts, schedules, queues, dedupe, flowChildren, flowOutbox }))
        assert(Exit.isFailure(result))
        expect(JSON.stringify(result.cause)).toContain("drizzle-kit generate")

        // validate: false defers the failure to first use.
        const store = yield* DrizzleJobStore.make({ jobs, attempts, schedules, queues, dedupe, flowChildren, flowOutbox, validate: false })
        const first = yield* Effect.exit(store.counts())
        assert(Exit.isFailure(first))
      }).pipe(Effect.scoped, Effect.provide(pgClientLive())))

    it("the deadlock retry predicate recognizes sql-pg's wrapped 40P01 shape", () => {
      // The exact construction path of a real deadlock failure, mirrored
      // from the installed sources: @effect/sql-pg's `classifyError` turns
      // pg code 40P01 into `new DeadlockError({ cause, message, operation })`
      // inside `new SqlError({ reason })` (PgClient.ts, "Failed to execute
      // statement"/"execute"), and drizzle's Effect session wraps that as
      // `new EffectDrizzleQueryError({ query, params, cause: Cause.fail(e) })`
      // (pg-core/effect/session.ts). String(error) renders neither the code
      // nor the reason tag, which is why the predicate walks the structure.
      const pgError = Object.assign(new Error("deadlock detected"), { code: "40P01" })
      const sqlError = new SqlError({
        reason: new DeadlockError({
          cause: pgError,
          message: "Failed to execute statement",
          operation: "execute"
        })
      })
      const wrapped = new EffectDrizzleQueryError({
        query: "UPDATE jobs SET ...",
        params: [],
        cause: Cause.fail(sqlError)
      })
      expect(DrizzleJobStore.isDeadlockError(wrapped)).toBe(true)
      expect(DrizzleJobStore.isDeadlockError(sqlError)).toBe(true)
      // String rendering hides both deadlock signals, so a substring
      // predicate can never fire on this shape — the structural walk is
      // load-bearing.
      expect(String(wrapped).includes("40P01")).toBe(false)
      expect(String(wrapped).includes("deadlock detected")).toBe(false)

      // Fallback: an unclassified reason still carrying the raw pg error.
      const unclassified = new SqlError({
        reason: new UnknownError({
          cause: pgError,
          message: "Failed to execute statement",
          operation: "execute"
        })
      })
      expect(DrizzleJobStore.isDeadlockError(unclassified)).toBe(true)

      // Non-deadlock failures must not retry.
      const boring = new SqlError({
        reason: new UnknownError({
          cause: new Error("connection reset"),
          message: "Failed to execute statement",
          operation: "execute"
        })
      })
      expect(DrizzleJobStore.isDeadlockError(boring)).toBe(false)
      expect(DrizzleJobStore.isDeadlockError(new Error("40P01"))).toBe(false)
      expect(DrizzleJobStore.isDeadlockError(undefined)).toBe(false)
    })

    it("the jobs table `name` column is typed to the job-tag union", () => {
      const table = mqJobs<"generate-invoice" | "send-email">("typed_jobs")
      type NameType = typeof table.name._.data
      const accepts: NameType = "generate-invoice"
      // @ts-expect-error - not part of the union
      const rejects: NameType = "typo-job"
      void rejects
      expect(accepts).toBe("generate-invoice")
      expect(getTableConfig(table).name).toBe("typed_jobs")
    })

    it("the `queue` and schedule/dedupe name columns accept user-supplied brands", () => {
      type WarpQueue = "critical" | "default"
      type JobNames = "generate-invoice" | "send-email"

      const jobs = mqJobs<JobNames, WarpQueue>("typed_queue_jobs")
      type JobsQueueType = typeof jobs.queue._.data
      const jobsQueue: JobsQueueType = "critical"
      // @ts-expect-error - not one of the declared queues
      const badQueue: JobsQueueType = "typo-queue"
      void badQueue

      const schedules = mqSchedules<JobNames, WarpQueue>("typed_queue_sched")
      type SchedJobName = typeof schedules.jobName._.data
      type SchedQueue = typeof schedules.queue._.data
      const schedName: SchedJobName = "send-email"
      const schedQueue: SchedQueue = "default"
      // @ts-expect-error - not part of the job-tag union
      const badSchedName: SchedJobName = "typo-job"
      void badSchedName

      const dedupe = mqDedupe<JobNames>("typed_queue_dedupe")
      type DedupeName = typeof dedupe.name._.data
      const dedupeName: DedupeName = "generate-invoice"
      // @ts-expect-error - not part of the job-tag union
      const badDedupeName: DedupeName = "typo-job"
      void badDedupeName

      const queues = mqQueueControl<WarpQueue>("typed_queue_control")
      type ControlQueue = typeof queues.queue._.data
      const controlQueue: ControlQueue = "critical"
      // @ts-expect-error - not one of the declared queues
      const badControlQueue: ControlQueue = "typo-queue"
      void badControlQueue

      expect([jobsQueue, schedName, schedQueue, dedupeName, controlQueue])
        .toEqual(["critical", "send-email", "default", "generate-invoice", "critical"])
    })

    it("extraConfig appends user indexes to the built-in ones on every factory", () => {
      const jobs = mqJobs("extra_idx_jobs", {
        extraConfig: (t) => [
          index("extra_idx_jobs_name_recent_idx").on(t.name, t.enqueuedAt.desc())
        ]
      })
      const jobIndexes = getTableConfig(jobs).indexes.map((entry) => entry.config.name)
      // The six built-ins survive, the user's index is appended.
      expect(jobIndexes).toHaveLength(7)
      expect(jobIndexes).toContain("extra_idx_jobs_name_recent_idx")
      expect(jobIndexes).toContain("extra_idx_jobs_ready_idx")

      const attempts = mqJobAttempts(jobs, "extra_idx_attempts", {
        extraConfig: (t) => [index("extra_idx_attempts_outcome_idx").on(t.outcome, t.finishedAt)]
      })
      expect(getTableConfig(attempts).indexes.map((entry) => entry.config.name))
        .toEqual(["extra_idx_attempts_outcome_idx"])

      const schedules = mqSchedules("extra_idx_sched", {
        extraConfig: (t) => [index("extra_idx_sched_queue_idx").on(t.queue)]
      })
      expect(getTableConfig(schedules).indexes).toHaveLength(2)

      const queues = mqQueueControl("extra_idx_queues", {
        extraConfig: (t) => [index("extra_idx_queues_paused_idx").on(t.paused)]
      })
      expect(getTableConfig(queues).indexes).toHaveLength(1)
    })
  })
}
