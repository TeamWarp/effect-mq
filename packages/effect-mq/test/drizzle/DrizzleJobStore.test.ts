import { Job, JobStore, Worker } from "../../src/index.ts"
import { PgClient } from "@effect/sql-pg"
import { jobStoreConformance } from "../../src/testing/index.ts"
import { assert, describe, expect, it } from "@effect/vitest"
import { getTableConfig } from "drizzle-orm/pg-core"
import { Effect, Exit, Fiber, Layer, Option, Schedule, Schema } from "effect"
import { DrizzleJobStore, mqJobAttempts, mqJobs, mqQueueControl, mqSchedules } from "../../src/drizzle/index.ts"
import { createTablesSql, freshStoreEffect, freshStoreLayer, freshTableNames, pgAvailable, pgClientLive, pgUrl } from "./support.ts"

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
        for (
          const statement of createTablesSql(names.jobs, names.attempts, names.schedules, names.queues)
        ) {
          yield* client.unsafe(statement).pipe(Effect.orDie)
        }
        yield* Effect.addFinalizer(() =>
          client.unsafe(
            `DROP TABLE IF EXISTS "${names.attempts}", "${names.jobs}", "${names.schedules}", "${names.queues}" CASCADE`
          ).pipe(Effect.ignore)
        )

        const typeMap = new Map<string, string>([
          ["PgBoolean", "boolean"],
          ["PgText", "text"],
          ["PgInteger", "integer"],
          ["PgBigInt53", "bigint"],
          ["PgJsonb", "jsonb"],
          ["PgTimestamp", "timestamp with time zone"]
        ])
        for (
          const [table, tableName] of [
            [mqJobs(names.jobs), names.jobs],
            [mqJobAttempts(mqJobs(names.jobs), names.attempts), names.attempts],
            [mqSchedules(names.schedules), names.schedules],
            [mqQueueControl(names.queues), names.queues]
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
        const handlers = SendReport.toLayer((payload, context) =>
          Effect.suspend(() => {
            attempt++
            return context.attempt === 1
              ? Effect.die("transient failure")
              : Effect.succeed(`report ${payload.month} sent`)
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
        for (
          const statement of createTablesSql(names.jobs, names.attempts, names.schedules, names.queues)
        ) {
          yield* client.unsafe(statement).pipe(Effect.orDie)
        }
        yield* Effect.addFinalizer(() =>
          client.unsafe(
            `DROP TABLE IF EXISTS "${names.attempts}", "${names.jobs}", "${names.schedules}", "${names.queues}" CASCADE`
          ).pipe(Effect.ignore)
        )
        let n = 0
        const store = yield* DrizzleJobStore.make({
          jobs,
          attempts,
          schedules,
          queues,
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

    it.live("historyTtl sweeps terminal rows; live rows survive", () =>
      Effect.gen(function*() {
        const client = yield* PgClient.PgClient
        const names = freshTableNames()
        const jobs = mqJobs(names.jobs)
        const attempts = mqJobAttempts(jobs, names.attempts)
        const schedules = mqSchedules(names.schedules)
        const queues = mqQueueControl(names.queues)
        for (
          const statement of createTablesSql(names.jobs, names.attempts, names.schedules, names.queues)
        ) {
          yield* client.unsafe(statement).pipe(Effect.orDie)
        }
        yield* Effect.addFinalizer(() =>
          client.unsafe(
            `DROP TABLE IF EXISTS "${names.attempts}", "${names.jobs}", "${names.schedules}", "${names.queues}" CASCADE`
          ).pipe(Effect.ignore)
        )
        const store = yield* DrizzleJobStore.make({
          jobs,
          attempts,
          schedules,
          queues,
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
          queues: a.queues
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
        for (
          const statement of createTablesSql(names.jobs, names.attempts, names.schedules, names.queues)
        ) {
          yield* client.unsafe(statement).pipe(Effect.orDie)
        }
        yield* Effect.addFinalizer(() =>
          client.unsafe(
            `DROP TABLE IF EXISTS "${names.attempts}", "${names.jobs}", "${names.schedules}", "${names.queues}" CASCADE`
          ).pipe(Effect.ignore)
        )
        const jobs = mqJobs(names.jobs)
        const attempts = mqJobAttempts(jobs, names.attempts)
        const schedules = mqSchedules(names.schedules)
        const queues = mqQueueControl(names.queues)

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
            DrizzleJobStore.layer({ jobs, attempts, schedules, queues, store: Durable }).pipe(Layer.orDie)
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
        const result = yield* Effect.exit(DrizzleJobStore.make({ jobs, attempts, schedules, queues }))
        assert(Exit.isFailure(result))
        expect(JSON.stringify(result.cause)).toContain("drizzle-kit generate")

        // validate: false defers the failure to first use.
        const store = yield* DrizzleJobStore.make({ jobs, attempts, schedules, queues, validate: false })
        const first = yield* Effect.exit(store.counts())
        assert(Exit.isFailure(first))
      }).pipe(Effect.scoped, Effect.provide(pgClientLive())))

    it("the jobs table `name` column is typed to the job-tag union", () => {
      const table = mqJobs<"sync-benefits" | "send-email">("typed_jobs")
      type NameType = typeof table.name._.data
      const accepts: NameType = "sync-benefits"
      // @ts-expect-error - not part of the union
      const rejects: NameType = "typo-job"
      void rejects
      expect(accepts).toBe("sync-benefits")
      expect(getTableConfig(table).name).toBe("typed_jobs")
    })
  })
}
