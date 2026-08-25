import { Job, JobStore, Worker } from "../../src/index.ts"
import { jobStoreConformance } from "../../src/testing/index.ts"
import { assert, describe, expect, it } from "@effect/vitest"
import { Effect, Fiber, Layer, Option, Schedule, Schema } from "effect"
import { RedisJobStore } from "../../src/redis/index.ts"
import { freshPrefix, redisAvailable, redisLive, redisUrl } from "./support.ts"

const available = await redisAvailable()

if (!available) {
  describe("RedisJobStore", () => {
    it.skip(`skipped: no Redis at ${redisUrl} — run \`docker compose up -d --wait\``, () => {})
  })
} else {
  // The shared contract, against a real Redis. Works under TestClock because
  // every Lua script receives time via ARGV from the Effect Clock.
  jobStoreConformance("RedisJobStore", () =>
    RedisJobStore.layer({ prefix: freshPrefix() }).pipe(
      Layer.provide(redisLive())
    ))

  describe("RedisJobStore specifics", () => {
    it.live("a full job lifecycle round-trips through Redis end to end", () =>
      Effect.gen(function*() {
        class Report extends Job.make("Report", {
          payload: { month: Schema.String },
          success: Schema.String,
          error: Schema.String,
          metadata: ({ month }) => ({ month }),
          defaults: { attempts: 2 }
        }) {}
        let attempt = 0
        const handlers = Report.toLayer((payload) =>
          Effect.gen(function*() {
            const current = yield* Worker.CurrentJob
            attempt = current.attempt
            return current.attempt === 1
              ? yield* Effect.fail("flaky")
              : `report ${payload.month} sent`
          })
        )

        const store = yield* RedisJobStore.make({ prefix: freshPrefix() })
        const storeLayer = Layer.succeed(JobStore.JobStore, store)

        const result = yield* Report.execute({ month: "2026-08" }).pipe(
          Effect.provide(
            handlers.pipe(
              Layer.provideMerge(Worker.layer({ pollInterval: "100 millis", lockDuration: "5 seconds" })),
              Layer.provideMerge(storeLayer)
            )
          )
        )
        expect(result).toBe("report 2026-08 sent")
        expect(attempt).toBe(2)

        const listed = yield* store.list({ metadata: { month: "2026-08" } })
        expect(listed.items).toHaveLength(1)
        const record = listed.items[0]
        assert(record !== undefined)
        expect(record.state).toBe("completed")
        expect(record.attemptsMade).toBe(2)

        const ledger = yield* store.getAttempts(record.id)
        expect(ledger.map((entry) => entry.outcome)).toEqual(["retried", "completed"])
      }).pipe(Effect.scoped, Effect.provide(redisLive())))

    it.live("a pub/sub wake reaches a waiter in another store instance", () =>
      Effect.gen(function*() {
        // Two make() instances over the SAME prefix simulate two processes:
        // instance A's local wake version cannot help — only the published
        // message delivered through SUBSCRIBE can wake it.
        const prefix = freshPrefix()
        const a = yield* RedisJobStore.make({ prefix })
        const b = yield* RedisJobStore.make({ prefix })

        const empty = yield* a.claim({
          queue: JobStore.QueueName("default"),
          names: ["Cross"],
          token: "t-a",
          lockDurationMs: 5_000
        })
        assert(empty._tag === "Empty")
        const waiter = yield* Effect.forkChild(
          a.awaitWake([JobStore.QueueName("default")], empty.wakeToken)
        )
        yield* Effect.sleep("100 millis")

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
        yield* Fiber.join(waiter).pipe(Effect.timeout("5 seconds"))
      }).pipe(Effect.scoped, Effect.provide(redisLive())))

    it.effect("store-assigned ids come from the configured idGenerator", () =>
      Effect.gen(function*() {
        let n = 0
        const store = yield* RedisJobStore.make({
          prefix: freshPrefix(),
          idGenerator: ({ name }) => `job_${name}_${++n}`
        })
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
      }).pipe(Effect.scoped, Effect.provide(redisLive())))

    it.effect("two stores on one prefix contend atomically; other prefixes are isolated", () =>
      Effect.gen(function*() {
        const prefix = freshPrefix()
        const a = yield* RedisJobStore.make({ prefix })
        const b = yield* RedisJobStore.make({ prefix })
        const other = yield* RedisJobStore.make({ prefix: freshPrefix() })

        const base = {
          id: undefined,
          name: "Contended",
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
        const { id } = yield* a.enqueue(base)

        // The same data is visible through both same-prefix instances...
        expect(Option.isSome(yield* b.getJob(id))).toBe(true)
        // ...and invisible through a different prefix.
        expect(Option.isNone(yield* other.getJob(id))).toBe(true)
        expect((yield* other.counts()).waiting).toBe(0)

        // Exactly one contender wins the claim; Lua atomicity, not luck.
        const claims = yield* Effect.all(
          [
            a.claim({ queue: base.queue, names: ["Contended"], token: "t-a", lockDurationMs: 5_000 }),
            b.claim({ queue: base.queue, names: ["Contended"], token: "t-b", lockDurationMs: 5_000 })
          ],
          { concurrency: 2 }
        )
        expect(claims.filter((claim) => claim._tag === "Claimed")).toHaveLength(1)
      }).pipe(Effect.scoped, Effect.provide(redisLive())))

    it.effect("a FanOut past the 500-child chunk size stages atomically and lands one manifest", () =>
      Effect.gen(function*() {
        const store = yield* RedisJobStore.make({ prefix: freshPrefix() })
        const queue = JobStore.QueueName("default")
        const base = {
          id: undefined,
          name: "BigFlow",
          queue,
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
        const { id } = yield* store.enqueue(base)
        const claim = yield* store.claim({
          queue,
          names: ["BigFlow"],
          token: "t-big",
          lockDurationMs: 30_000
        })
        assert(claim._tag === "Claimed")

        // 750 children forces the driver through two staging chunks (500 +
        // 250, the second carrying the manifest + state flip). Zero-padded
        // keys make childKey order equal numeric order.
        const children: Array<JobStore.FlowChildSpec> = Array.from({ length: 750 }, (_, i) => {
          const key = `c${String(i).padStart(4, "0")}`
          return {
            childKey: key,
            storeKey: "effect-mq/JobStore/children",
            request: {
              ...base,
              id: JobStore.JobId(`flow/main/${id}/${key}`),
              name: "Child",
              parent: {
                flowName: "big",
                flowId: id,
                childKey: key,
                parentStoreKey: "main",
                depth: 1
              }
            }
          }
        })
        yield* store.ack(id, "t-big", { _tag: "FanOut", failFast: false, children })

        const parent = yield* store.getJob(id)
        assert(Option.isSome(parent))
        expect(parent.value.state).toBe("waiting-children")
        expect(parent.value.flow).toEqual({
          failFast: false,
          pending: 750,
          completed: 0,
          failed: 0,
          cancelled: 0
        })

        // The full manifest paginates in childKey order.
        const seen: Array<string> = []
        let cursor: string | undefined = undefined
        do {
          const page: {
            items: ReadonlyArray<JobStore.FlowChildRecord>
            cursor: string | undefined
          } = yield* store.listChildResults(id, { cursor, limit: 300 })
          for (const row of page.items) seen.push(row.childKey)
          cursor = page.cursor
        } while (cursor !== undefined)
        expect(seen).toEqual(children.map((child) => child.childKey))

        // A report batch moves the counters; the in-batch duplicate drops.
        const results = yield* store.recordChildResults([
          {
            flowId: id,
            childKey: "c0000",
            outcome: "completed",
            exit: { ok: true },
            failedReason: undefined
          },
          {
            flowId: id,
            childKey: "c0000",
            outcome: "failed",
            exit: undefined,
            failedReason: undefined
          },
          {
            flowId: id,
            childKey: "c0001",
            outcome: "completed",
            exit: { ok: true },
            failedReason: undefined
          }
        ])
        expect(results).toEqual([
          { applied: true, parentSettled: false },
          { applied: false, parentSettled: false },
          { applied: true, parentSettled: false }
        ])
        const after = yield* store.getJob(id)
        assert(Option.isSome(after))
        expect(after.value.state).toBe("waiting-children")
        expect(after.value.flow).toEqual({
          failFast: false,
          pending: 748,
          completed: 2,
          failed: 0,
          cancelled: 0
        })
      }).pipe(Effect.scoped, Effect.provide(redisLive())))

    it.live("historyTtl sweeps terminal jobs; live jobs survive", () =>
      Effect.gen(function*() {
        const store = yield* RedisJobStore.make({
          prefix: freshPrefix(),
          historyTtl: "80 millis",
          historySweepInterval: "40 millis"
        })
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

        yield* store.getJob(done.id).pipe(
          Effect.flatMap((record) =>
            Option.isNone(record) ? Effect.void : Effect.fail(new Error("not swept yet"))
          ),
          Effect.retry({ schedule: Schedule.spaced("40 millis"), times: 50 })
        )
        expect(Option.isSome(yield* store.getJob(waiting.id))).toBe(true)
      }).pipe(Effect.scoped, Effect.provide(redisLive())))
  })
}
