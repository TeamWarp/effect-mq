import { Job, JobStore, Worker } from "../../src/index.ts"
import { jobStoreConformance } from "../../src/testing/index.ts"
import { assert, describe, expect, it } from "@effect/vitest"
import { Cause, Effect, Exit, Fiber, Layer, Option, Schedule, Schema } from "effect"
import { TestClock } from "effect/testing"
import { Redis } from "effect/unstable/persistence"
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

  describe("RedisJobStore indexed lists", () => {
    const request = (overrides?: Partial<JobStore.EnqueueRequest>): JobStore.EnqueueRequest => ({
      id: undefined,
      name: "IdxJob",
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
      delayMs: 0,
      ...overrides
    })

    // Assert an exit died with a defect of the given class and hand it back
    // narrowed for field assertions.
    const dieDefectOf = <A, E, T>(exit: Exit.Exit<A, E>, ctor: new(...args: ReadonlyArray<never>) => T): T => {
      assert(Exit.isFailure(exit))
      const defect = exit.cause.reasons.find(Cause.isDieReason)?.defect
      assert(defect instanceof ctor)
      return defect
    }

    it.effect("list routes every (filter, orderBy) combination to the right structure", () =>
      Effect.gen(function*() {
        const store = yield* RedisJobStore.make({ prefix: freshPrefix() })
        const q1 = JobStore.QueueName("q1")
        const q2 = JobStore.QueueName("q2")
        // Timeline (1s apart): a1(A,q1), b1(B,q1), a2(A,q2,tenant),
        // a3(A,q1,delayed 60s), b2(B,q2,delayed 30s); then a1 completes and
        // b1 fails, 1s apart.
        const a1 = yield* store.enqueue(request({ name: "A", queue: q1 }))
        yield* TestClock.adjust(1_000)
        const b1 = yield* store.enqueue(request({ name: "B", queue: q1 }))
        yield* TestClock.adjust(1_000)
        const a2 = yield* store.enqueue(request({ name: "A", queue: q2, metadata: { tenant: "t1" } }))
        yield* TestClock.adjust(1_000)
        const a3 = yield* store.enqueue(request({ name: "A", queue: q1, delayMs: 60_000 }))
        yield* TestClock.adjust(1_000)
        const b2 = yield* store.enqueue(request({ name: "B", queue: q2, delayMs: 30_000 }))
        const claim1 = yield* store.claim({ queue: q1, names: ["A"], token: "t-1", lockDurationMs: 5_000 })
        assert(claim1._tag === "Claimed")
        yield* store.ack(a1.id, "t-1", { _tag: "Complete", exit: null })
        yield* TestClock.adjust(1_000)
        const claim2 = yield* store.claim({ queue: q1, names: ["B"], token: "t-2", lockDurationMs: 5_000 })
        assert(claim2._tag === "Claimed")
        yield* store.ack(b1.id, "t-2", { _tag: "Fail", exit: null })

        // byname:<name>, both directions.
        const byName = yield* store.list({ name: "A" })
        expect(byName.items.map((job) => job.id)).toEqual([a3.id, a2.id, a1.id])
        const byNameAsc = yield* store.list({ name: "A", order: "asc" })
        expect(byNameAsc.items.map((job) => job.id)).toEqual([a1.id, a2.id, a3.id])

        // byname with residual queue/states/metadata predicates.
        const aInQ1 = yield* store.list({ name: "A", queue: q1 })
        expect(aInQ1.items.map((job) => job.id)).toEqual([a3.id, a1.id])
        const aWaiting = yield* store.list({ name: "A", states: ["waiting"] })
        expect(aWaiting.items.map((job) => job.id)).toEqual([a2.id])
        const aTenant = yield* store.list({ name: "A", metadata: { tenant: "t1" } })
        expect(aTenant.items.map((job) => job.id)).toEqual([a2.id])

        // byname pagination: exclusive keyset cursor.
        const page1 = yield* store.list({ name: "A", limit: 2 })
        expect(page1.items.map((job) => job.id)).toEqual([a3.id, a2.id])
        assert(page1.cursor !== undefined)
        const page2 = yield* store.list({ name: "A", limit: 2, cursor: page1.cursor })
        expect(page2.items.map((job) => job.id)).toEqual([a1.id])
        expect(page2.cursor).toBeUndefined()

        // byqueue:<queue> with a residual states predicate.
        const inQ2 = yield* store.list({ queue: q2 })
        expect(inQ2.items.map((job) => job.id)).toEqual([b2.id, a2.id])
        const q1Delayed = yield* store.list({ queue: q1, states: ["delayed"] })
        expect(q1Delayed.items.map((job) => job.id)).toEqual([a3.id])

        // delayed:<queue> for runAt, with a residual name predicate.
        const upcoming = yield* store.list({ queue: q2, states: ["delayed"], orderBy: "runAt", order: "asc" })
        expect(upcoming.items.map((job) => job.id)).toEqual([b2.id])
        const upcomingA = yield* store.list({
          queue: q1,
          states: ["delayed"],
          name: "B",
          orderBy: "runAt",
          order: "asc"
        })
        expect(upcomingA.items).toHaveLength(0)

        // finished:<state> merge across states; terminal:<name>:<state> when
        // a name pins the group.
        const recent = yield* store.list({ states: ["completed", "failed"], orderBy: "finishedAt" })
        expect(recent.items.map((job) => job.id)).toEqual([b1.id, a1.id])
        const aDone = yield* store.list({ name: "A", states: ["completed"], orderBy: "finishedAt" })
        expect(aDone.items.map((job) => job.id)).toEqual([a1.id])

        // The keyset cursor spans the merge: page one from finished:failed,
        // page two resumes into finished:completed.
        const mergedPage1 = yield* store.list({ states: ["completed", "failed"], orderBy: "finishedAt", limit: 1 })
        expect(mergedPage1.items.map((job) => job.id)).toEqual([b1.id])
        assert(mergedPage1.cursor !== undefined)
        const mergedPage2 = yield* store.list({
          states: ["completed", "failed"],
          orderBy: "finishedAt",
          limit: 1,
          cursor: mergedPage1.cursor
        })
        expect(mergedPage2.items.map((job) => job.id)).toEqual([a1.id])
        expect(mergedPage2.cursor).toBeUndefined()

        // The unfiltered default still rides `all`.
        const everything = yield* store.list({})
        expect(everything.items).toHaveLength(5)
      }).pipe(Effect.scoped, Effect.provide(redisLive())))

    it.effect("a query that routes to a disabled index dies; other routes still serve the filter", () =>
      Effect.gen(function*() {
        const store = yield* RedisJobStore.make({ prefix: freshPrefix(), indexes: { name: false } })
        const { id } = yield* store.enqueue(request({ name: "A" }))
        const claim = yield* store.claim({
          queue: JobStore.QueueName("default"),
          names: ["A"],
          token: "t-1",
          lockDurationMs: 5_000
        })
        assert(claim._tag === "Claimed")
        yield* store.ack(id, "t-1", { _tag: "Complete", exit: null })

        const defect = dieDefectOf(
          yield* Effect.exit(store.list({ name: "A" })),
          RedisJobStore.ListIndexDisabledError
        )
        expect(defect.index).toBe("name")
        expect(defect.message).toContain("indexes.name")

        // Routing runs first: the same name filter with terminal states and
        // finishedAt ordering rides terminal:<name>:<state> — no index used.
        const served = yield* store.list({ name: "A", states: ["completed"], orderBy: "finishedAt" })
        expect(served.items.map((job) => job.id)).toEqual([id])

        // The queue index is independent and still on.
        const byQueue = yield* store.list({ queue: JobStore.QueueName("default") })
        expect(byQueue.items.map((job) => job.id)).toEqual([id])

        // indexes: false disables both; the unfiltered default still works.
        const dark = yield* RedisJobStore.make({ prefix: freshPrefix(), indexes: false })
        const darkDefect = dieDefectOf(
          yield* Effect.exit(dark.list({ queue: JobStore.QueueName("default") })),
          RedisJobStore.ListIndexDisabledError
        )
        expect(darkDefect.index).toBe("queue")
        expect((yield* dark.list({})).items).toHaveLength(0)
      }).pipe(Effect.scoped, Effect.provide(redisLive())))

    it.effect("orderBy combinations outside the Redis matrix die with ListOrderUnsupportedError", () =>
      Effect.gen(function*() {
        const store = yield* RedisJobStore.make({ prefix: freshPrefix() })

        // runAt requires states: ["delayed"] AND a queue.
        const noQueue = dieDefectOf(
          yield* Effect.exit(store.list({ states: ["delayed"], orderBy: "runAt" })),
          JobStore.ListOrderUnsupportedError
        )
        expect(noQueue.orderBy).toBe("runAt")
        dieDefectOf(
          yield* Effect.exit(store.list({ queue: JobStore.QueueName("default"), orderBy: "runAt" })),
          JobStore.ListOrderUnsupportedError
        )

        // finishedAt requires states ⊆ terminal.
        const nonTerminal = dieDefectOf(
          yield* Effect.exit(store.list({ states: ["waiting", "completed"], orderBy: "finishedAt" })),
          JobStore.ListOrderUnsupportedError
        )
        expect(nonTerminal.orderBy).toBe("finishedAt")
      }).pipe(Effect.scoped, Effect.provide(redisLive())))

    it.effect("init rebuilds a marker-less index via ZSCAN, and the marker confines later boots to the tail", () =>
      Effect.gen(function*() {
        const prefix = freshPrefix()
        const redis = yield* Redis.Redis
        // Rows written while the indexes are off have no index entries.
        const dark = yield* RedisJobStore.make({ prefix, indexes: false })
        const a1 = yield* dark.enqueue(request({ name: "A" }))
        yield* TestClock.adjust(1_000)
        const b1 = yield* dark.enqueue(request({ name: "B", queue: JobStore.QueueName("other") }))
        yield* TestClock.adjust(1_000)
        const a2 = yield* dark.enqueue(request({ name: "A", queue: JobStore.QueueName("other") }))
        // A bulk batch with one shared enqueuedAt: the ZSCAN rebuild is
        // linear and tie-immune.
        yield* dark.enqueueMany(Array.from({ length: 1_100 }, () => request({ name: "Bulk" })))
        expect(Number(yield* redis.send("EXISTS", `${prefix}:byname:A`, `${prefix}:byname:Bulk`))).toBe(0)

        // Re-init with the indexes on: the missing markers trigger a full
        // rebuild, stamped with this boot's clock time.
        yield* TestClock.adjust(120_000)
        const indexed = yield* RedisJobStore.make({ prefix })
        const byName = yield* indexed.list({ name: "A", order: "asc" })
        expect(byName.items.map((job) => job.id)).toEqual([a1.id, a2.id])
        const byQueue = yield* indexed.list({ queue: JobStore.QueueName("other"), order: "asc" })
        expect(byQueue.items.map((job) => job.id)).toEqual([b1.id, a2.id])
        expect(Number(yield* redis.send("ZCARD", `${prefix}:byname:Bulk`))).toBe(1_100)
        expect(yield* redis.send("GET", `${prefix}:index:name:ready`)).toBe("122000")

        // A marker keeps the next boot off the full scan: it only heals the
        // tail (marker minus 60s onward), so a hole poked below that window
        // stays — proving nothing rescanned `all` — and the marker advances.
        yield* redis.send("ZREM", `${prefix}:byname:A`, a1.id)
        yield* TestClock.adjust(5_000)
        const third = yield* RedisJobStore.make({ prefix })
        const afterSkip = yield* third.list({ name: "A" })
        expect(afterSkip.items.map((job) => job.id)).toEqual([a2.id])
        expect(yield* redis.send("GET", `${prefix}:index:name:ready`)).toBe("127000")
      }).pipe(Effect.scoped, Effect.provide(redisLive())))

    it.effect("a boot with a marker heals unindexed rows enqueued since the marker's window", () =>
      Effect.gen(function*() {
        const prefix = freshPrefix()
        const redis = yield* Redis.Redis
        const first = yield* RedisJobStore.make({ prefix })
        const old = yield* first.enqueue(request({ name: "A" }))
        // A later boot re-stamps the marker well past `old`'s enqueuedAt.
        yield* TestClock.adjust(200_000)
        const second = yield* RedisJobStore.make({ prefix })
        const fresh = yield* second.enqueue(request({ name: "A" }))
        // Simulate an index-less writer (an old version mid-rolling-deploy)
        // having inserted both rows: strip their byname entries.
        yield* redis.send("ZREM", `${prefix}:byname:A`, old.id, fresh.id)

        yield* TestClock.adjust(5_000)
        const third = yield* RedisJobStore.make({ prefix })
        // fresh (enqueuedAt 200_000 >= marker 200_000 - 60_000) is healed;
        // old (enqueuedAt 0) is outside the tail window and stays missing —
        // the heal is a tail heal, not a rescan.
        const healed = yield* third.list({ name: "A" })
        expect(healed.items.map((job) => job.id)).toEqual([fresh.id])
        expect(yield* redis.send("GET", `${prefix}:index:name:ready`)).toBe("205000")
      }).pipe(Effect.scoped, Effect.provide(redisLive())))

    it.effect("disabling an index deletes only its marker; re-enabling does a full rebuild", () =>
      Effect.gen(function*() {
        const prefix = freshPrefix()
        const redis = yield* Redis.Redis
        const indexed = yield* RedisJobStore.make({ prefix })
        const a1 = yield* indexed.enqueue(request({ name: "A" }))
        yield* TestClock.adjust(1_000)
        const a2 = yield* indexed.enqueue(request({ name: "A", queue: JobStore.QueueName("other") }))
        yield* indexed.enqueue(request({ name: "B" }))

        // Disabling name drops ONLY its marker — the zsets stay, because an
        // enabled sibling on the prefix may be serving reads from them.
        const cleaned = yield* RedisJobStore.make({ prefix, indexes: { name: false } })
        expect(Number(yield* redis.send("EXISTS", `${prefix}:index:name:ready`))).toBe(0)
        expect(Number(yield* redis.send("EXISTS", `${prefix}:index:queue:ready`))).toBe(1)
        expect(Number(yield* redis.send("EXISTS", `${prefix}:byname:A`, `${prefix}:byname:B`))).toBe(2)
        const stillByQueue = yield* cleaned.list({ queue: JobStore.QueueName("other") })
        expect(stillByQueue.items.map((job) => job.id)).toEqual([a2.id])
        expect(Option.isSome(yield* cleaned.getJob(a1.id))).toBe(true)

        // Rows inserted while disabled never reach the (stale) zsets...
        yield* TestClock.adjust(1_000)
        const a3 = yield* cleaned.enqueue(request({ name: "A" }))
        expect(yield* redis.send("ZSCORE", `${prefix}:byname:A`, a3.id)).toBeNull()

        // ...and re-enabling does a full ZSCAN rebuild (no marker to trust),
        // so lists are correct again, stale-era rows included.
        const reopened = yield* RedisJobStore.make({ prefix })
        const byName = yield* reopened.list({ name: "A", order: "asc" })
        expect(byName.items.map((job) => job.id)).toEqual([a1.id, a2.id, a3.id])
      }).pipe(Effect.scoped, Effect.provide(redisLive())))

    it.effect("an index member whose job hash is gone is skipped and removed", () =>
      Effect.gen(function*() {
        const prefix = freshPrefix()
        const redis = yield* Redis.Redis
        const store = yield* RedisJobStore.make({ prefix })
        const ghost = yield* store.enqueue(request({ name: "A" }))
        yield* TestClock.adjust(1_000)
        const kept = yield* store.enqueue(request({ name: "A" }))
        // Remove the hash out of band; every structure still references it.
        yield* redis.send("DEL", `${prefix}:job:${ghost.id}`)

        // Each routed scan skips the orphan and heals the structure it
        // walked — and only that one.
        const byName = yield* store.list({ name: "A" })
        expect(byName.items.map((job) => job.id)).toEqual([kept.id])
        expect(yield* redis.send("ZSCORE", `${prefix}:byname:A`, ghost.id)).toBeNull()
        expect(yield* redis.send("ZSCORE", `${prefix}:all`, ghost.id)).not.toBeNull()

        const everything = yield* store.list({})
        expect(everything.items.map((job) => job.id)).toEqual([kept.id])
        expect(yield* redis.send("ZSCORE", `${prefix}:all`, ghost.id)).toBeNull()

        const byQueue = yield* store.list({ queue: JobStore.QueueName("default") })
        expect(byQueue.items.map((job) => job.id)).toEqual([kept.id])
        expect(yield* redis.send("ZSCORE", `${prefix}:byqueue:default`, ghost.id)).toBeNull()
      }).pipe(Effect.scoped, Effect.provide(redisLive())))
  })
}
