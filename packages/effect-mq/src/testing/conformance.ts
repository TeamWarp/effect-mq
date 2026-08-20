/**
 * Conformance suite for `JobStore` implementations.
 *
 * Every storage driver must pass this suite. Run it from a vitest file:
 *
 * ```ts
 * import { jobStoreConformance } from "effect-mq/testing"
 * import { MemoryJobStore } from "effect-mq"
 *
 * jobStoreConformance("MemoryJobStore", () => MemoryJobStore.layer)
 * ```
 *
 * The suite runs under `TestClock`. Drivers must derive ALL time from the
 * Effect `Clock` (e.g. pass `now` into queries as a bind parameter) — never
 * from the database server's clock — so this works against real storage too.
 *
 * @since 0.1.0
 */
import * as JobStore from "../JobStore.ts"
import { assert, describe, expect, it } from "@effect/vitest"
import { Effect, Exit, Fiber, type Layer, Option } from "effect"
import { TestClock } from "effect/testing"

const { JobId, QueueName } = JobStore

const baseRequest = (
  overrides?: Partial<JobStore.EnqueueRequest>
): JobStore.EnqueueRequest => ({
  id: undefined,
  name: "TestJob",
  queue: QueueName("default"),
  payload: { n: 1 },
  metadata: {},
  priority: 0,
  attemptsMax: 1,
  backoff: undefined,
  keep: undefined,
  delayMs: 0,
  ...overrides
})

const claimOptions = (
  overrides?: Partial<JobStore.ClaimOptions>
): JobStore.ClaimOptions => ({
  queue: QueueName("default"),
  names: ["TestJob"],
  token: "t-1",
  lockDurationMs: 30_000,
  ...overrides
})

/**
 * Assert a `JobStore` implementation behaves according to the contract.
 *
 * @since 0.1.0
 */
export const jobStoreConformance = (
  name: string,
  storeLayer: () => Layer.Layer<JobStore.JobStore>
): void => {
  describe(`JobStore conformance: ${name}`, () => {
    const withStore = <A, E>(
      body: (store: JobStore.Service) => Effect.Effect<A, E>
    ) =>
      Effect.flatMap(JobStore.JobStore, body).pipe(
        Effect.provide(storeLayer())
      )

    it.effect("enqueue lands in waiting and is claimable", () =>
      withStore((store) =>
        Effect.gen(function*() {
          const result = yield* store.enqueue(baseRequest())
          expect(result.duplicate).toBe(false)
          const counts = yield* store.counts()
          expect(counts.waiting).toBe(1)

          const claim = yield* store.claim(claimOptions())
          assert(claim._tag === "Claimed")
          expect(claim.job.id).toBe(result.id)
          expect(claim.job.state).toBe("active")
          expect(claim.job.payload).toEqual({ n: 1 })
        })
      ))

    it.effect("enqueue with delay lands in delayed and promotes when due", () =>
      withStore((store) =>
        Effect.gen(function*() {
          yield* store.enqueue(baseRequest({ delayMs: 5_000 }))
          expect((yield* store.counts()).delayed).toBe(1)

          const early = yield* store.claim(claimOptions())
          assert(early._tag === "Empty")
          expect(early.nextRunAt).toBeDefined()

          yield* TestClock.adjust(5_000)
          const due = yield* store.claim(claimOptions({ token: "t-2" }))
          assert(due._tag === "Claimed")
        })
      ))

    it.effect("duplicate ids are a no-op", () =>
      withStore((store) =>
        Effect.gen(function*() {
          const first = yield* store.enqueue(
            baseRequest({ id: JobId("custom-1"), payload: { n: 1 } })
          )
          const second = yield* store.enqueue(
            baseRequest({ id: JobId("custom-1"), payload: { n: 999 }, priority: 9 })
          )
          expect(first).toEqual({ id: "custom-1", duplicate: false })
          expect(second).toEqual({ id: "custom-1", duplicate: true })

          const job = yield* store.getJob(JobId("custom-1"))
          assert(Option.isSome(job))
          expect(job.value.payload).toEqual({ n: 1 })
          expect(job.value.priority).toBe(0)
          expect((yield* store.counts()).waiting).toBe(1)
        })
      ))

    it.effect("store-assigned ids never collide with user-supplied ids", () =>
      withStore((store) =>
        Effect.gen(function*() {
          // Deliberately occupy an id shaped like a store-generated one.
          const custom = yield* store.enqueue(baseRequest({ id: JobId("j-1") }))
          expect(custom).toEqual({ id: "j-1", duplicate: false })

          const auto = yield* store.enqueue(baseRequest({ payload: { n: 2 } }))
          expect(auto.duplicate).toBe(false)
          expect(auto.id).not.toBe(custom.id)
          expect((yield* store.counts()).waiting).toBe(2)
        })
      ))

    it.effect("metadata round-trips on the record", () =>
      withStore((store) =>
        Effect.gen(function*() {
          const { id } = yield* store.enqueue(
            baseRequest({ metadata: { employerId: "emp-1", region: "us" } })
          )
          const job = yield* store.getJob(id)
          assert(Option.isSome(job))
          expect(job.value.metadata).toEqual({ employerId: "emp-1", region: "us" })
        })
      ))

    it.effect("claims are FIFO within a priority, higher priority first", () =>
      withStore((store) =>
        Effect.gen(function*() {
          const a = yield* store.enqueue(baseRequest({ payload: { n: 1 } }))
          const b = yield* store.enqueue(baseRequest({ payload: { n: 2 } }))
          const c = yield* store.enqueue(
            baseRequest({ payload: { n: 3 }, priority: 5 })
          )

          const claimed: Array<string> = []
          for (const token of ["t-1", "t-2", "t-3"]) {
            const claim = yield* store.claim(claimOptions({ token }))
            assert(claim._tag === "Claimed")
            claimed.push(claim.job.id)
          }
          expect(claimed).toEqual([c.id, a.id, b.id])
        })
      ))

    it.effect("claim filters by queue and by name", () =>
      withStore((store) =>
        Effect.gen(function*() {
          yield* store.enqueue(baseRequest({ queue: QueueName("other") }))
          yield* store.enqueue(baseRequest({ name: "OtherJob" }))

          const wrongBoth = yield* store.claim(claimOptions())
          assert(wrongBoth._tag === "Empty")

          const byQueue = yield* store.claim(
            claimOptions({ queue: QueueName("other"), token: "t-2" })
          )
          assert(byQueue._tag === "Claimed")
          expect(byQueue.job.queue).toBe("other")

          const byName = yield* store.claim(
            claimOptions({ names: ["OtherJob"], token: "t-3" })
          )
          assert(byName._tag === "Claimed")
          expect(byName.job.name).toBe("OtherJob")
        })
      ))

    it.effect("ack Complete stores the exit, finishes the job, and records the run", () =>
      withStore((store) =>
        Effect.gen(function*() {
          const { id } = yield* store.enqueue(baseRequest())
          const claim = yield* store.claim(claimOptions())
          assert(claim._tag === "Claimed")

          yield* store.ack(id, "t-1", { _tag: "Complete", exit: { ok: true } })
          const job = yield* store.getJob(id)
          assert(Option.isSome(job))
          expect(job.value.state).toBe("completed")
          expect(job.value.exit).toEqual({ ok: true })
          expect(job.value.attemptsMade).toBe(1)
          expect(job.value.finishedAt).toBeDefined()

          const attempts = yield* store.getAttempts(id)
          expect(attempts).toHaveLength(1)
          expect(attempts[0]?.attempt).toBe(1)
          expect(attempts[0]?.outcome).toBe("completed")
          expect(attempts[0]?.exit).toEqual({ ok: true })
          expect(attempts[0]?.startedAt).toBeDefined()
          expect(attempts[0]?.finishedAt).toBeDefined()
        })
      ))

    it.effect("ack Retry re-queues with delay and records the failed run", () =>
      withStore((store) =>
        Effect.gen(function*() {
          const { id } = yield* store.enqueue(baseRequest({ attemptsMax: 3 }))
          const claim = yield* store.claim(claimOptions())
          assert(claim._tag === "Claimed")

          yield* store.ack(id, "t-1", {
            _tag: "Retry",
            delayMs: 1_000,
            exit: { boom: 1 }
          })
          const afterRetry = yield* store.getJob(id)
          assert(Option.isSome(afterRetry))
          expect(afterRetry.value.state).toBe("delayed")
          expect(afterRetry.value.attemptsMade).toBe(1)

          // The failed run is persisted (durable tapError before rerun).
          const attempts = yield* store.getAttempts(id)
          expect(attempts).toHaveLength(1)
          expect(attempts[0]?.attempt).toBe(1)
          expect(attempts[0]?.outcome).toBe("retried")
          expect(attempts[0]?.exit).toEqual({ boom: 1 })

          const early = yield* store.claim(claimOptions({ token: "t-2" }))
          assert(early._tag === "Empty")
          yield* TestClock.adjust(1_000)
          const due = yield* store.claim(claimOptions({ token: "t-3" }))
          assert(due._tag === "Claimed")
          expect(due.job.attemptsMade).toBe(1)
        })
      ))

    it.effect("ack Fail is terminal and records the run", () =>
      withStore((store) =>
        Effect.gen(function*() {
          const { id } = yield* store.enqueue(baseRequest())
          const claim = yield* store.claim(claimOptions())
          assert(claim._tag === "Claimed")

          yield* store.ack(id, "t-1", { _tag: "Fail", exit: { failed: true } })
          const job = yield* store.getJob(id)
          assert(Option.isSome(job))
          expect(job.value.state).toBe("failed")
          expect(job.value.exit).toEqual({ failed: true })

          const attempts = yield* store.getAttempts(id)
          expect(attempts).toHaveLength(1)
          expect(attempts[0]?.outcome).toBe("failed")
          expect(attempts[0]?.exit).toEqual({ failed: true })

          const after = yield* store.claim(claimOptions({ token: "t-2" }))
          assert(after._tag === "Empty")
        })
      ))

    it.effect("ack with a wrong token fails with LockLostError", () =>
      withStore((store) =>
        Effect.gen(function*() {
          const { id } = yield* store.enqueue(baseRequest())
          const claim = yield* store.claim(claimOptions())
          assert(claim._tag === "Claimed")

          const result = yield* Effect.exit(
            store.ack(id, "wrong-token", { _tag: "Complete", exit: null })
          )
          assert(Exit.isFailure(result))

          const state = yield* store.getJob(id)
          assert(Option.isSome(state))
          expect(state.value.state).toBe("active")
        })
      ))

    it.effect("ack of an unknown id fails with JobNotFoundError", () =>
      withStore((store) =>
        Effect.gen(function*() {
          const result = yield* Effect.exit(
            store.ack(JobId("nope"), "t-1", { _tag: "Complete", exit: null })
          )
          assert(Exit.isFailure(result))
        })
      ))

    it.effect("release returns the job to waiting without consuming an attempt or recording a run", () =>
      withStore((store) =>
        Effect.gen(function*() {
          const { id } = yield* store.enqueue(baseRequest())
          const claim = yield* store.claim(claimOptions())
          assert(claim._tag === "Claimed")

          yield* store.release(id, "t-1")
          const job = yield* store.getJob(id)
          assert(Option.isSome(job))
          expect(job.value.state).toBe("waiting")
          expect(job.value.attemptsMade).toBe(0)
          expect(yield* store.getAttempts(id)).toHaveLength(0)
        })
      ))

    it.effect("extendLocks extends live locks and reports lost ones", () =>
      withStore((store) =>
        Effect.gen(function*() {
          const { id } = yield* store.enqueue(baseRequest())
          const claim = yield* store.claim(
            claimOptions({ lockDurationMs: 10_000 })
          )
          assert(claim._tag === "Claimed")

          const lost = yield* store.extendLocks(
            [{ id, token: "t-1" }, { id: JobId("ghost"), token: "t-9" }],
            20_000
          )
          expect(lost).toEqual(["ghost"])

          // The extension outlives the original lock duration.
          yield* TestClock.adjust(15_000)
          const recovered = yield* store.recoverStalled({ maxStalledCount: 1 })
          expect(recovered).toEqual([])
        })
      ))

    it.effect("recoverStalled requeues expired locks, records runs, and fails repeat offenders", () =>
      withStore((store) =>
        Effect.gen(function*() {
          const { id } = yield* store.enqueue(baseRequest())

          // First stall: back to waiting.
          const first = yield* store.claim(
            claimOptions({ lockDurationMs: 1_000 })
          )
          assert(first._tag === "Claimed")
          yield* TestClock.adjust(1_000)
          const recovered = yield* store.recoverStalled({ maxStalledCount: 1 })
          expect(recovered).toEqual([{ id, failed: false }])
          const afterFirst = yield* store.getJob(id)
          assert(Option.isSome(afterFirst))
          expect(afterFirst.value.state).toBe("waiting")

          // Second stall exceeds maxStalledCount: failed.
          const second = yield* store.claim(
            claimOptions({ token: "t-2", lockDurationMs: 1_000 })
          )
          assert(second._tag === "Claimed")
          yield* TestClock.adjust(1_000)
          const failed = yield* store.recoverStalled({ maxStalledCount: 1 })
          expect(failed).toEqual([{ id, failed: true }])
          const afterSecond = yield* store.getJob(id)
          assert(Option.isSome(afterSecond))
          expect(afterSecond.value.state).toBe("failed")
          expect(afterSecond.value.failedReason).toBeDefined()

          const attempts = yield* store.getAttempts(id)
          expect(attempts.map((attempt) => attempt.outcome)).toEqual(["stalled", "stalled"])
          expect(attempts.map((attempt) => attempt.attempt)).toEqual([1, 2])

          // The old token no longer acks.
          const ack = yield* Effect.exit(
            store.ack(id, "t-2", { _tag: "Complete", exit: null })
          )
          assert(Exit.isFailure(ack))
        })
      ))

    it.effect("retry re-runs a failed job with a fresh budget and a preserved ledger", () =>
      withStore((store) =>
        Effect.gen(function*() {
          const { id } = yield* store.enqueue(baseRequest({ attemptsMax: 1 }))
          const claim = yield* store.claim(claimOptions())
          assert(claim._tag === "Claimed")
          yield* store.ack(id, "t-1", { _tag: "Fail", exit: { failed: 1 } })

          yield* store.retry(id)
          const job = yield* store.getJob(id)
          assert(Option.isSome(job))
          expect(job.value.state).toBe("waiting")
          expect(job.value.attemptsMade).toBe(0)
          expect(job.value.exit).toBeUndefined()
          expect(job.value.failedReason).toBeUndefined()

          // The job is claimable again; the ledger keeps counting monotonically.
          const again = yield* store.claim(claimOptions({ token: "t-2" }))
          assert(again._tag === "Claimed")
          yield* store.ack(id, "t-2", { _tag: "Complete", exit: { ok: 1 } })
          const attempts = yield* store.getAttempts(id)
          expect(attempts.map((attempt) => [attempt.attempt, attempt.outcome])).toEqual([
            [1, "failed"],
            [2, "completed"]
          ])
        })
      ))

    it.effect("retry rejects non-failed jobs and unknown ids", () =>
      withStore((store) =>
        Effect.gen(function*() {
          const { id } = yield* store.enqueue(baseRequest())
          const wrongState = yield* Effect.flip(store.retry(id))
          expect(wrongState._tag).toBe("JobNotRetryableError")

          const missing = yield* Effect.flip(store.retry(JobId("nope")))
          expect(missing._tag).toBe("JobNotFoundError")
        })
      ))

    it.effect("list filters by name/queue/state/metadata and paginates newest-first", () =>
      withStore((store) =>
        Effect.gen(function*() {
          for (let i = 0; i < 5; i++) {
            yield* store.enqueue(baseRequest({
              payload: { n: i },
              metadata: { employerId: i % 2 === 0 ? "even" : "odd" }
            }))
            yield* TestClock.adjust(1) // distinct enqueuedAt for stable order
          }
          yield* store.enqueue(baseRequest({ name: "OtherJob" }))
          yield* TestClock.adjust(1)
          yield* store.enqueue(baseRequest({ queue: QueueName("other") }))

          const byName = yield* store.list({ name: "TestJob" })
          expect(byName.items).toHaveLength(6)
          // Newest first.
          expect(byName.items[0]?.queue).toBe("other")

          const byQueue = yield* store.list({ name: "TestJob", queue: QueueName("default") })
          expect(byQueue.items).toHaveLength(5)
          expect(byQueue.items.map((job) => job.payload)).toEqual([
            { n: 4 },
            { n: 3 },
            { n: 2 },
            { n: 1 },
            { n: 0 }
          ])

          const byMetadata = yield* store.list({ metadata: { employerId: "even" } })
          expect(byMetadata.items.map((job) => job.payload)).toEqual([
            { n: 4 },
            { n: 2 },
            { n: 0 }
          ])

          const byState = yield* store.list({ states: ["waiting"] })
          expect(byState.items).toHaveLength(7)

          // Pagination walks the full set without overlap.
          const page1 = yield* store.list({ name: "TestJob", limit: 4 })
          expect(page1.items).toHaveLength(4)
          expect(page1.cursor).toBeDefined()
          const page2 = yield* store.list({ name: "TestJob", limit: 4, cursor: page1.cursor })
          expect(page2.items).toHaveLength(2)
          const ids = [...page1.items, ...page2.items].map((job) => job.id)
          expect(new Set(ids).size).toBe(6)
        })
      ))

    it.effect("keep count prunes older terminal records of the same name and state", () =>
      withStore((store) =>
        Effect.gen(function*() {
          for (let i = 0; i < 4; i++) {
            const { id } = yield* store.enqueue(
              baseRequest({ payload: { n: i }, keep: { count: 2, ageMs: undefined } })
            )
            const claim = yield* store.claim(claimOptions({ token: `t-${i}` }))
            assert(claim._tag === "Claimed")
            yield* store.ack(id, `t-${i}`, { _tag: "Complete", exit: null })
            yield* TestClock.adjust(1)
          }
          const listed = yield* store.list({ name: "TestJob", states: ["completed"] })
          expect(listed.items).toHaveLength(2)
          expect(listed.items.map((job) => job.payload)).toEqual([{ n: 3 }, { n: 2 }])
        })
      ))

    it.effect("keep age prunes terminal records older than the window", () =>
      withStore((store) =>
        Effect.gen(function*() {
          const keep = { count: undefined, ageMs: 10_000 }
          const first = yield* store.enqueue(baseRequest({ payload: { n: 1 }, keep }))
          const claim1 = yield* store.claim(claimOptions())
          assert(claim1._tag === "Claimed")
          yield* store.ack(first.id, "t-1", { _tag: "Complete", exit: null })

          yield* TestClock.adjust(20_000)
          const second = yield* store.enqueue(baseRequest({ payload: { n: 2 }, keep }))
          const claim2 = yield* store.claim(claimOptions({ token: "t-2" }))
          assert(claim2._tag === "Claimed")
          yield* store.ack(second.id, "t-2", { _tag: "Complete", exit: null })

          expect(Option.isNone(yield* store.getJob(first.id))).toBe(true)
          expect(Option.isSome(yield* store.getJob(second.id))).toBe(true)
        })
      ))

    it.effect("list with an empty states filter matches nothing", () =>
      withStore((store) =>
        Effect.gen(function*() {
          yield* store.enqueue(baseRequest())
          const listed = yield* store.list({ states: [] })
          expect(listed.items).toHaveLength(0)
          expect(listed.cursor).toBeUndefined()
        })
      ))

    it.effect("list pagination is lossless when enqueue timestamps tie", () =>
      withStore((store) =>
        Effect.gen(function*() {
          // No clock adjustment: every record shares one enqueuedAt, so
          // ordering and the cursor fall back entirely to the id tie-break.
          for (let i = 0; i < 7; i++) {
            yield* store.enqueue(baseRequest({ payload: { n: i } }))
          }
          const seen = new Set<string>()
          let cursor: string | undefined
          do {
            const page: JobStore.ListResult = yield* store.list({ limit: 3, cursor })
            for (const item of page.items) {
              expect(seen.has(item.id)).toBe(false)
              seen.add(item.id)
            }
            cursor = page.cursor
          } while (cursor !== undefined)
          expect(seen.size).toBe(7)
        })
      ))

    it.effect("keep count ties on finishedAt keep the most recently acked records", () =>
      withStore((store) =>
        Effect.gen(function*() {
          // Two jobs acked at the SAME TestClock instant: the tie must break
          // on enqueue/seq order identically in every driver.
          const first = yield* store.enqueue(
            baseRequest({ payload: { n: 1 }, keep: { count: 1, ageMs: undefined } })
          )
          const second = yield* store.enqueue(
            baseRequest({ payload: { n: 2 }, keep: { count: 1, ageMs: undefined } })
          )
          const claimA = yield* store.claim(claimOptions({ token: "t-a" }))
          const claimB = yield* store.claim(claimOptions({ token: "t-b" }))
          assert(claimA._tag === "Claimed" && claimB._tag === "Claimed")
          yield* store.ack(first.id, "t-a", { _tag: "Complete", exit: null })
          yield* store.ack(second.id, "t-b", { _tag: "Complete", exit: null })

          expect(Option.isNone(yield* store.getJob(first.id))).toBe(true)
          expect(Option.isSome(yield* store.getJob(second.id))).toBe(true)
        })
      ))

    it.effect("keep applies count and age together", () =>
      withStore((store) =>
        Effect.gen(function*() {
          const keep = { count: 2, ageMs: 10_000 }
          const ids: Array<JobStore.JobId> = []
          for (let i = 0; i < 3; i++) {
            const { id } = yield* store.enqueue(baseRequest({ payload: { n: i }, keep }))
            const claim = yield* store.claim(claimOptions({ token: `t-${i}` }))
            assert(claim._tag === "Claimed")
            yield* store.ack(id, `t-${i}`, { _tag: "Complete", exit: null })
            ids.push(id)
            yield* TestClock.adjust(6_000)
          }
          const [oldest, middle, newest] = ids
          assert(oldest !== undefined && middle !== undefined && newest !== undefined)
          // At the final ack (t=12s): the age clause prunes #0 (finished 12s
          // ago > 10s) and the count clause independently keeps the newest 2,
          // so #1 (6s old) and #2 survive under both clauses.
          expect(Option.isNone(yield* store.getJob(oldest))).toBe(true)
          expect(Option.isSome(yield* store.getJob(middle))).toBe(true)
          expect(Option.isSome(yield* store.getJob(newest))).toBe(true)
        })
      ))

    it.effect("awaitWake resolves on new work and honours the wake token", () =>
      withStore((store) =>
        Effect.gen(function*() {
          const empty = yield* store.claim(claimOptions())
          assert(empty._tag === "Empty")

          // Wake-up arriving *before* awaitWake is not lost thanks to the token.
          yield* store.enqueue(baseRequest())
          yield* store.awaitWake([QueueName("default")], empty.wakeToken)

          // And a waiter blocked on a fresh token is woken by a later enqueue.
          const claimed = yield* store.claim(claimOptions({ token: "t-2" }))
          assert(claimed._tag === "Claimed")
          const emptyAgain = yield* store.claim(claimOptions({ token: "t-3" }))
          assert(emptyAgain._tag === "Empty")
          const waiter = yield* Effect.forkChild(
            store.awaitWake([QueueName("default")], emptyAgain.wakeToken)
          )
          yield* Effect.yieldNow
          yield* store.enqueue(baseRequest({ payload: { n: 2 } }))
          yield* Fiber.join(waiter)
        })
      ))

    it.effect("counts groups by state and can filter by queue", () =>
      withStore((store) =>
        Effect.gen(function*() {
          yield* store.enqueue(baseRequest())
          yield* store.enqueue(baseRequest({ delayMs: 1_000 }))
          yield* store.enqueue(baseRequest({ queue: QueueName("other") }))
          const claim = yield* store.claim(claimOptions())
          assert(claim._tag === "Claimed")

          const all = yield* store.counts()
          expect(all).toEqual({
            waiting: 1,
            delayed: 1,
            active: 1,
            completed: 0,
            failed: 0
          })
          const other = yield* store.counts(QueueName("other"))
          expect(other.waiting).toBe(1)
          expect(other.active).toBe(0)
        })
      ))

    it.effect("remove deletes non-active jobs but refuses active ones", () =>
      withStore((store) =>
        Effect.gen(function*() {
          const first = yield* store.enqueue(baseRequest())
          const second = yield* store.enqueue(baseRequest({ payload: { n: 2 } }))
          const claim = yield* store.claim(claimOptions())
          assert(claim._tag === "Claimed")
          expect(claim.job.id).toBe(first.id)

          expect(yield* store.remove(second.id)).toBe(true)
          expect(yield* store.remove(first.id)).toBe(false)
          expect(yield* store.remove(JobId("ghost"))).toBe(false)
          expect((yield* store.counts()).waiting).toBe(0)
        })
      ))

    it.effect("getAttempts returns an empty ledger for unknown ids", () =>
      withStore((store) =>
        Effect.gen(function*() {
          expect(yield* store.getAttempts(JobId("nope"))).toEqual([])
        })
      ))
  })
}
