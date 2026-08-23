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
  timeoutMs: undefined,
  dedupe: undefined,
  trace: undefined,
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

          const result = yield* store.extendLocks(
            [{ id, token: "t-1" }, { id: JobId("ghost"), token: "t-9" }],
            20_000
          )
          expect(result.lost).toEqual(["ghost"])
          expect(result.cancelRequested).toEqual([])

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
              baseRequest({ payload: { n: i }, keep: { completed: { count: 2, ageMs: undefined } } })
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
          const keep = { completed: { count: undefined, ageMs: 10_000 } }
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
            baseRequest({ payload: { n: 1 }, keep: { completed: { count: 1, ageMs: undefined } } })
          )
          const second = yield* store.enqueue(
            baseRequest({ payload: { n: 2 }, keep: { completed: { count: 1, ageMs: undefined } } })
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
          const keep = { completed: { count: 2, ageMs: 10_000 } }
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
            failed: 0,
            cancelled: 0
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

    it.effect("cancel makes waiting and delayed jobs terminal with a ledger entry", () =>
      withStore((store) =>
        Effect.gen(function*() {
          const waiting = yield* store.enqueue(baseRequest())
          const delayed = yield* store.enqueue(baseRequest({ delayMs: 60_000 }))
          yield* store.cancel(waiting.id)
          yield* store.cancel(delayed.id)

          for (const id of [waiting.id, delayed.id]) {
            const job = yield* store.getJob(id)
            assert(Option.isSome(job))
            expect(job.value.state).toBe("cancelled")
            expect(job.value.finishedAt).toBeDefined()
            const attempts = yield* store.getAttempts(id)
            expect(attempts.map((attempt) => attempt.outcome)).toEqual(["cancelled"])
          }
          const claim = yield* store.claim(claimOptions())
          assert(claim._tag === "Empty")

          // Terminal jobs cannot be cancelled again; unknown ids are reported.
          const again = yield* Effect.flip(store.cancel(waiting.id))
          expect(again._tag).toBe("JobNotCancellableError")
          const missing = yield* Effect.flip(store.cancel(JobId("nope")))
          expect(missing._tag).toBe("JobNotFoundError")
        })
      ))

    it.effect("cancel on an active job flags it; the heartbeat reports it; ack Cancelled finishes it", () =>
      withStore((store) =>
        Effect.gen(function*() {
          const { id } = yield* store.enqueue(baseRequest())
          const claim = yield* store.claim(claimOptions())
          assert(claim._tag === "Claimed")

          yield* store.cancel(id)
          const flagged = yield* store.getJob(id)
          assert(Option.isSome(flagged))
          expect(flagged.value.state).toBe("active")
          expect(flagged.value.cancelRequested).toBe(true)

          // The heartbeat surfaces the request instead of extending the lock.
          const heartbeat = yield* store.extendLocks([{ id, token: "t-1" }], 20_000)
          expect(heartbeat.cancelRequested).toEqual([id])
          expect(heartbeat.lost).toEqual([])

          yield* store.ack(id, "t-1", { _tag: "Cancelled" })
          const done = yield* store.getJob(id)
          assert(Option.isSome(done))
          expect(done.value.state).toBe("cancelled")
          expect(done.value.cancelRequested).toBe(false)
          const attempts = yield* store.getAttempts(id)
          expect(attempts.map((attempt) => attempt.outcome)).toEqual(["cancelled"])
        })
      ))

    it.effect("release and stall recovery honour a pending cancel request", () =>
      withStore((store) =>
        Effect.gen(function*() {
          // release path
          const first = yield* store.enqueue(baseRequest())
          const claimA = yield* store.claim(claimOptions())
          assert(claimA._tag === "Claimed")
          yield* store.cancel(first.id)
          yield* store.release(first.id, "t-1")
          const released = yield* store.getJob(first.id)
          assert(Option.isSome(released))
          expect(released.value.state).toBe("cancelled")

          // stall path
          const second = yield* store.enqueue(baseRequest({ payload: { n: 2 } }))
          const claimB = yield* store.claim(claimOptions({ token: "t-2", lockDurationMs: 1_000 }))
          assert(claimB._tag === "Claimed")
          yield* store.cancel(second.id)
          yield* TestClock.adjust(1_000)
          const recovered = yield* store.recoverStalled({ maxStalledCount: 5 })
          // Cancelled-by-recovery jobs are not reported as stalled recoveries.
          expect(recovered).toEqual([])
          const swept = yield* store.getJob(second.id)
          assert(Option.isSome(swept))
          expect(swept.value.state).toBe("cancelled")
        })
      ))

    it.effect("promote runs a delayed job now and rejects other states", () =>
      withStore((store) =>
        Effect.gen(function*() {
          const { id } = yield* store.enqueue(baseRequest({ delayMs: 60_000 }))
          const early = yield* store.claim(claimOptions())
          assert(early._tag === "Empty")

          yield* store.promote(id)
          const claim = yield* store.claim(claimOptions({ token: "t-2" }))
          assert(claim._tag === "Claimed")
          expect(claim.job.id).toBe(id)

          const wrongState = yield* Effect.flip(store.promote(id))
          expect(wrongState._tag).toBe("JobNotPromotableError")
          const missing = yield* Effect.flip(store.promote(JobId("nope")))
          expect(missing._tag).toBe("JobNotFoundError")
        })
      ))

    it.effect("pause stops claims for a queue until resume", () =>
      withStore((store) =>
        Effect.gen(function*() {
          yield* store.enqueue(baseRequest())
          yield* store.pause(QueueName("default"))
          expect(yield* store.pausedQueues()).toEqual(["default"])

          const paused = yield* store.claim(claimOptions())
          assert(paused._tag === "Empty")

          // Other queues are unaffected.
          yield* store.enqueue(baseRequest({ queue: QueueName("other") }))
          const other = yield* store.claim(claimOptions({ queue: QueueName("other"), token: "t-2" }))
          assert(other._tag === "Claimed")

          // Resume wakes idle workers and claims flow again.
          const empty = yield* store.claim(claimOptions({ token: "t-3" }))
          assert(empty._tag === "Empty")
          const waiter = yield* Effect.forkChild(
            store.awaitWake([QueueName("default")], empty.wakeToken)
          )
          yield* Effect.yieldNow
          yield* store.resume(QueueName("default"))
          yield* Fiber.join(waiter)
          expect(yield* store.pausedQueues()).toEqual([])
          const resumed = yield* store.claim(claimOptions({ token: "t-4" }))
          assert(resumed._tag === "Claimed")
        })
      ))

    it.effect("schedules: upsert, list, due, conditional advance, remove", () =>
      withStore((store) =>
        Effect.gen(function*() {
          const schedule: JobStore.ScheduleRecord = {
            key: JobStore.ScheduleKey("TestJob/hourly"),
            jobName: "TestJob",
            queue: QueueName("default"),
            cron: "0 * * * *",
            tz: undefined,
            everyMs: undefined,
            payload: { n: 1 },
            metadata: { source: "conformance" },
            priority: 2,
            attemptsMax: 3,
            backoff: { _tag: "fixed", delayMs: 1_000 },
            keep: undefined,
            timeoutMs: 5_000,
            nextRunAt: 60_000
          }
          yield* store.upsertSchedule(schedule)
          expect(yield* store.listSchedules()).toEqual([schedule])
          expect(yield* store.listSchedules({ jobName: "OtherJob" })).toEqual([])

          // Not due yet (TestClock starts at 0).
          expect(yield* store.dueSchedules()).toEqual([])
          yield* TestClock.adjust(60_000)
          expect(yield* store.dueSchedules()).toEqual([schedule])

          // Upsert replaces in place.
          yield* store.upsertSchedule({ ...schedule, priority: 9 })
          expect((yield* store.listSchedules())[0]?.priority).toBe(9)

          // Conditional advance: a stale expectation is a no-op.
          yield* store.advanceSchedule(schedule.key, 999, 120_000)
          expect((yield* store.listSchedules())[0]?.nextRunAt).toBe(60_000)
          yield* store.advanceSchedule(schedule.key, 60_000, 120_000)
          expect((yield* store.listSchedules())[0]?.nextRunAt).toBe(120_000)
          expect(yield* store.dueSchedules()).toEqual([])

          expect(yield* store.removeSchedule(schedule.key)).toBe(true)
          expect(yield* store.removeSchedule(schedule.key)).toBe(false)
          expect(yield* store.listSchedules()).toEqual([])
        })
      ))

    const minutelySchedule = (): JobStore.ScheduleRecord => ({
      key: JobStore.ScheduleKey("TestJob/minutely"),
      jobName: "TestJob",
      queue: QueueName("default"),
      cron: undefined,
      tz: undefined,
      everyMs: 60_000,
      payload: { n: 7 },
      metadata: {},
      priority: 0,
      attemptsMax: 1,
      backoff: undefined,
      keep: undefined,
      timeoutMs: undefined,
      nextRunAt: 60_000
    })

    it.effect("tickSchedule fires a slot exactly once and advances atomically", () =>
      withStore((store) =>
        Effect.gen(function*() {
          const schedule = minutelySchedule()
          yield* store.upsertSchedule(schedule)
          yield* TestClock.adjust(60_000)
          const request = baseRequest({
            id: JobId("sched/TestJob/minutely/60000"),
            payload: { n: 7 }
          })

          expect(yield* store.tickSchedule(schedule.key, 60_000, 120_000, request)).toBe(true)
          expect((yield* store.listSchedules())[0]?.nextRunAt).toBe(120_000)
          expect((yield* store.counts()).waiting).toBe(1)

          // A concurrent sweeper holding the same stale expectation loses the
          // CAS: nothing fires, nothing advances further.
          expect(yield* store.tickSchedule(schedule.key, 60_000, 180_000, request)).toBe(false)
          expect((yield* store.listSchedules())[0]?.nextRunAt).toBe(120_000)
          expect((yield* store.counts()).waiting).toBe(1)

          const claim = yield* store.claim(claimOptions())
          assert(claim._tag === "Claimed")
          expect(claim.job.id).toBe("sched/TestJob/minutely/60000")
          expect(claim.job.payload).toEqual({ n: 7 })
        })
      ))

    it.effect("a stale tick cannot re-fire a slot whose job was pruned", () =>
      withStore((store) =>
        Effect.gen(function*() {
          const schedule = minutelySchedule()
          yield* store.upsertSchedule(schedule)
          yield* TestClock.adjust(60_000)
          const slotId = JobId("sched/TestJob/minutely/60000")
          const request = baseRequest({ id: slotId, payload: { n: 7 } })
          expect(yield* store.tickSchedule(schedule.key, 60_000, 120_000, request)).toBe(true)

          // Retention prunes the slot job — the old non-atomic design would
          // now let a stale sweeper re-insert it. The CAS must still refuse.
          expect(yield* store.remove(slotId)).toBe(true)
          expect(yield* store.tickSchedule(schedule.key, 60_000, 180_000, request)).toBe(false)
          expect((yield* store.counts()).waiting).toBe(0)
          expect((yield* store.listSchedules())[0]?.nextRunAt).toBe(120_000)
        })
      ))

    it.effect("tickSchedule advances without firing when the slot job already exists", () =>
      withStore((store) =>
        Effect.gen(function*() {
          const schedule = minutelySchedule()
          yield* store.upsertSchedule(schedule)
          yield* TestClock.adjust(60_000)
          const slotId = JobId("sched/TestJob/minutely/60000")
          // A pre-0.4 worker crashed between its enqueue and advance: the
          // slot row exists but nextRunAt is stale. The tick must advance the
          // schedule (or it stays due forever) yet report nothing new fired.
          yield* store.enqueue(baseRequest({ id: slotId, payload: { n: 7 } }))
          const request = baseRequest({ id: slotId, payload: { n: 7 } })
          expect(yield* store.tickSchedule(schedule.key, 60_000, 120_000, request)).toBe(false)
          expect((yield* store.listSchedules())[0]?.nextRunAt).toBe(120_000)
          expect((yield* store.counts()).waiting).toBe(1)
        })
      ))

    it.effect("tickSchedule rejects requests without an id and unknown keys", () =>
      withStore((store) =>
        Effect.gen(function*() {
          const missing = yield* Effect.flip(
            store.tickSchedule(JobStore.ScheduleKey("TestJob/minutely"), 60_000, 120_000, baseRequest())
          )
          expect(missing._tag).toBe("JobStoreError")
          expect(
            yield* store.tickSchedule(
              JobStore.ScheduleKey("ghost"),
              0,
              60_000,
              baseRequest({ id: JobId("sched/ghost/0") })
            )
          ).toBe(false)
          expect((yield* store.counts()).waiting).toBe(0)
        })
      ))

    it.effect("tickSchedule wakes parked takers", () =>
      withStore((store) =>
        Effect.gen(function*() {
          const schedule = minutelySchedule()
          yield* store.upsertSchedule(schedule)
          const empty = yield* store.claim(claimOptions())
          assert(empty._tag === "Empty")
          const waiter = yield* Effect.forkChild(
            store.awaitWake([QueueName("default")], empty.wakeToken)
          )
          yield* Effect.yieldNow
          yield* TestClock.adjust(60_000)
          const request = baseRequest({
            id: JobId("sched/TestJob/minutely/60000"),
            payload: { n: 7 }
          })
          expect(yield* store.tickSchedule(schedule.key, 60_000, 120_000, request)).toBe(true)
          yield* Fiber.join(waiter)
          const claim = yield* store.claim(claimOptions({ token: "t-2" }))
          assert(claim._tag === "Claimed")
        })
      ))

    it.effect("enqueueMany inserts a batch with positional results", () =>
      withStore((store) =>
        Effect.gen(function*() {
          const results = yield* store.enqueueMany([
            baseRequest({ payload: { n: 1 } }),
            baseRequest({ payload: { n: 2 }, delayMs: 60_000 }),
            baseRequest({ id: JobId("batch-3"), payload: { n: 3 }, priority: 5 })
          ])
          expect(results).toHaveLength(3)
          expect(results.map((result) => result.duplicate)).toEqual([false, false, false])
          expect(new Set(results.map((result) => result.id)).size).toBe(3)
          expect(results[2]?.id).toBe("batch-3")

          const counts = yield* store.counts()
          expect(counts.waiting).toBe(2)
          expect(counts.delayed).toBe(1)

          const explicit = yield* store.getJob(JobId("batch-3"))
          assert(Option.isSome(explicit))
          expect(explicit.value.payload).toEqual({ n: 3 })
          expect(explicit.value.priority).toBe(5)

          const delayedId = results[1]?.id
          assert(delayedId !== undefined)
          const delayed = yield* store.getJob(delayedId)
          assert(Option.isSome(delayed))
          expect(delayed.value.state).toBe("delayed")
          expect(delayed.value.runAt).toBe(60_000)

          // Priority order survives the batch path.
          const claim = yield* store.claim(claimOptions())
          assert(claim._tag === "Claimed")
          expect(claim.job.id).toBe("batch-3")
        })
      ))

    it.effect("enqueueMany reports duplicates positionally without clobbering", () =>
      withStore((store) =>
        Effect.gen(function*() {
          yield* store.enqueue(baseRequest({ id: JobId("dup-1"), payload: { n: 0 } }))
          const results = yield* store.enqueueMany([
            baseRequest({ payload: { n: 1 } }),
            baseRequest({ id: JobId("dup-1"), payload: { n: 99 } }),
            baseRequest({ payload: { n: 2 } })
          ])
          expect(results.map((result) => result.duplicate)).toEqual([false, true, false])
          expect(results[1]?.id).toBe("dup-1")
          const kept = yield* store.getJob(JobId("dup-1"))
          assert(Option.isSome(kept))
          expect(kept.value.payload).toEqual({ n: 0 })
          expect((yield* store.counts()).waiting).toBe(3)
        })
      ))

    it.effect("enqueueMany resolves intra-batch repeats of one id like separate enqueues", () =>
      withStore((store) =>
        Effect.gen(function*() {
          const results = yield* store.enqueueMany([
            baseRequest({ id: JobId("same"), payload: { n: 1 } }),
            baseRequest({ id: JobId("same"), payload: { n: 2 } })
          ])
          expect(results.map((result) => result.duplicate)).toEqual([false, true])
          expect(results[1]?.id).toBe("same")
          expect((yield* store.counts()).waiting).toBe(1)
          const job = yield* store.getJob(JobId("same"))
          assert(Option.isSome(job))
          expect(job.value.payload).toEqual({ n: 1 })
        })
      ))

    it.effect("enqueueMany applies per-item dedup policies in order", () =>
      withStore((store) =>
        Effect.gen(function*() {
          const dedupe = { key: "k", ttlMs: undefined, extend: false, replace: false }
          const results = yield* store.enqueueMany([
            baseRequest({ payload: { n: 1 }, dedupe }),
            baseRequest({ payload: { n: 2 } }),
            baseRequest({ payload: { n: 3 }, dedupe })
          ])
          expect(results.map((result) => result.duplicate)).toEqual([false, false, true])
          expect(results[2]?.id).toBe(results[0]?.id)
          expect((yield* store.counts()).waiting).toBe(2)
        })
      ))

    it.effect("enqueueMany with an empty batch returns an empty array", () =>
      withStore((store) =>
        Effect.gen(function*() {
          expect(yield* store.enqueueMany([])).toEqual([])
        })
      ))

    // Pins the hand-duplicated field plumbing in batch/tick insert paths
    // (positional Lua ARGV strides, multi-row VALUES lists): every persisted
    // field must come out identical to the single-enqueue path. A swapped
    // pair of ARGVs or a dropped column fails this even though the simpler
    // tests (payload/priority/state only) stay green.
    it.effect("batch and tick inserts persist every field exactly like enqueue", () =>
      withStore((store) =>
        Effect.gen(function*() {
          const richRequest = (id: string): JobStore.EnqueueRequest =>
            baseRequest({
              id: JobId(id),
              payload: { big: 1234567890123456, nested: { arr: [1, 2, 3] } },
              metadata: { tenant: "acme", region: "us" },
              priority: 3,
              attemptsMax: 4,
              backoff: { _tag: "fixed", delayMs: 2_000 },
              keep: { completed: { count: 2, ageMs: undefined } },
              timeoutMs: 9_000,
              trace: { traceId: "trace-1", spanId: "span-1", sampled: true, delayed: false }
            })
          yield* store.enqueue(richRequest("rich-single"))
          expect(yield* store.enqueueMany([richRequest("rich-batch")]))
            .toEqual([{ id: "rich-batch", duplicate: false }])
          const schedule = {
            ...minutelySchedule(),
            key: JobStore.ScheduleKey("TestJob/parity"),
            nextRunAt: 0
          }
          yield* store.upsertSchedule(schedule)
          expect(yield* store.tickSchedule(schedule.key, 0, 60_000, richRequest("rich-tick")))
            .toBe(true)

          const project = (job: JobStore.JobRecord) => ({
            name: job.name,
            queue: job.queue,
            state: job.state,
            payload: job.payload,
            metadata: job.metadata,
            priority: job.priority,
            attemptsMax: job.attemptsMax,
            attemptsMade: job.attemptsMade,
            backoff: job.backoff,
            keep: job.keep,
            timeoutMs: job.timeoutMs,
            cancelRequested: job.cancelRequested,
            trace: job.trace,
            runAt: job.runAt,
            enqueuedAt: job.enqueuedAt
          })
          const single = yield* store.getJob(JobId("rich-single"))
          assert(Option.isSome(single))
          const expected = project(single.value)
          // The reference row itself must carry the rich fields — otherwise
          // three empty rows would compare equal and prove nothing.
          expect(expected.backoff).toEqual({ _tag: "fixed", delayMs: 2_000 })
          expect(expected.keep).toEqual({ completed: { count: 2 } })
          expect(expected.timeoutMs).toBe(9_000)
          expect(expected.trace).toEqual({
            traceId: "trace-1",
            spanId: "span-1",
            sampled: true,
            delayed: false
          })
          expect(expected.payload).toEqual({ big: 1234567890123456, nested: { arr: [1, 2, 3] } })
          for (const id of [JobId("rich-batch"), JobId("rich-tick")]) {
            const job = yield* store.getJob(id)
            assert(Option.isSome(job))
            expect(project(job.value)).toEqual(expected)
          }
        })
      ))

    it.effect("enqueueMany wakes parked takers", () =>
      withStore((store) =>
        Effect.gen(function*() {
          const empty = yield* store.claim(claimOptions())
          assert(empty._tag === "Empty")
          const waiter = yield* Effect.forkChild(
            store.awaitWake([QueueName("default")], empty.wakeToken)
          )
          yield* Effect.yieldNow
          yield* store.enqueueMany([baseRequest()])
          yield* Fiber.join(waiter)
          const claim = yield* store.claim(claimOptions({ token: "t-2" }))
          assert(claim._tag === "Claimed")
        })
      ))

    it.effect("completion wins over a pending cancel request", () =>
      withStore((store) =>
        Effect.gen(function*() {
          const { id } = yield* store.enqueue(baseRequest())
          const claim = yield* store.claim(claimOptions())
          assert(claim._tag === "Claimed")
          yield* store.cancel(id)

          yield* store.ack(id, "t-1", { _tag: "Complete", exit: { ok: true } })
          const job = yield* store.getJob(id)
          assert(Option.isSome(job))
          expect(job.value.state).toBe("completed")
          expect(job.value.cancelRequested).toBe(false)
        })
      ))

    it.effect("a natural failure racing a cancel is cancelled, not revived", () =>
      withStore((store) =>
        Effect.gen(function*() {
          const { id } = yield* store.enqueue(baseRequest({ attemptsMax: 5 }))
          const claim = yield* store.claim(claimOptions())
          assert(claim._tag === "Claimed")
          yield* store.cancel(id)

          // The handler failed on its own before the heartbeat could
          // interrupt it; the retry ack must honour the pending cancel.
          yield* store.ack(id, "t-1", { _tag: "Retry", exit: { boom: 1 }, delayMs: 1_000 })
          const job = yield* store.getJob(id)
          assert(Option.isSome(job))
          expect(job.value.state).toBe("cancelled")
          expect(job.value.cancelRequested).toBe(false)
          const attempts = yield* store.getAttempts(id)
          expect(attempts.map((attempt) => attempt.outcome)).toEqual(["cancelled"])
          const nothing = yield* store.claim(claimOptions({ token: "t-2" }))
          assert(nothing._tag === "Empty")
        })
      ))

    it.effect("ack Cancelled with a wrong token fails with LockLostError", () =>
      withStore((store) =>
        Effect.gen(function*() {
          const { id } = yield* store.enqueue(baseRequest())
          const claim = yield* store.claim(claimOptions())
          assert(claim._tag === "Claimed")
          yield* store.cancel(id)

          const wrong = yield* Effect.flip(store.ack(id, "not-mine", { _tag: "Cancelled" }))
          expect(wrong._tag).toBe("LockLostError")
          const job = yield* store.getJob(id)
          assert(Option.isSome(job))
          expect(job.value.state).toBe("active")
          expect(job.value.cancelRequested).toBe(true)
        })
      ))

    it.effect("cancel applies the keep retention policy", () =>
      withStore((store) =>
        Effect.gen(function*() {
          const keep = { cancelled: { count: 1 } }
          const ids: Array<JobStore.JobId> = []
          for (let i = 0; i < 3; i++) {
            const { id } = yield* store.enqueue(baseRequest({ payload: { n: i }, keep }))
            ids.push(id)
            yield* store.cancel(id)
            yield* TestClock.adjust(10)
          }
          const listed = yield* store.list({ states: ["cancelled"] })
          expect(listed.items).toHaveLength(1)
          expect(listed.items[0]?.id).toBe(ids[2])
        })
      ))

    it.effect("upserting an unchanged cadence preserves the next occurrence; a changed one resets it", () =>
      withStore((store) =>
        Effect.gen(function*() {
          const base: JobStore.ScheduleRecord = {
            key: JobStore.ScheduleKey("TestJob/pulse"),
            jobName: "TestJob",
            queue: QueueName("default"),
            cron: undefined,
            tz: undefined,
            everyMs: 60_000,
            payload: {},
            metadata: {},
            priority: 0,
            attemptsMax: 1,
            backoff: undefined,
            keep: undefined,
            timeoutMs: undefined,
            nextRunAt: 60_000
          }
          yield* store.upsertSchedule(base)

          // A deploy-time re-registration recomputes nextRunAt from "now" —
          // the store must keep the original grid point.
          yield* TestClock.adjust(30_000)
          yield* store.upsertSchedule({ ...base, priority: 5, nextRunAt: 90_000 })
          const preserved = (yield* store.listSchedules())[0]
          expect(preserved?.nextRunAt).toBe(60_000)
          expect(preserved?.priority).toBe(5)

          // Changing the cadence takes the caller's fresh nextRunAt.
          yield* store.upsertSchedule({ ...base, everyMs: 120_000, nextRunAt: 150_000 })
          expect((yield* store.listSchedules())[0]?.nextRunAt).toBe(150_000)
        })
      ))

    it.effect("schedule records round-trip every/tz fields; due order is by nextRunAt; list filters by queue", () =>
      withStore((store) =>
        Effect.gen(function*() {
          const everySchedule: JobStore.ScheduleRecord = {
            key: JobStore.ScheduleKey("TestJob/every"),
            jobName: "TestJob",
            queue: QueueName("default"),
            cron: undefined,
            tz: undefined,
            everyMs: 90_000,
            payload: { n: 1 },
            metadata: {},
            priority: 0,
            attemptsMax: 1,
            backoff: undefined,
            keep: undefined,
            timeoutMs: undefined,
            nextRunAt: 90_000
          }
          const cronSchedule: JobStore.ScheduleRecord = {
            ...everySchedule,
            key: JobStore.ScheduleKey("TestJob/tz"),
            queue: QueueName("other"),
            cron: "0 9 * * *",
            tz: "America/New_York",
            everyMs: undefined,
            nextRunAt: 30_000
          }
          yield* store.upsertSchedule(everySchedule)
          yield* store.upsertSchedule(cronSchedule)

          const byQueue = yield* store.listSchedules({ queue: QueueName("other") })
          expect(byQueue).toEqual([cronSchedule])
          expect(byQueue[0]?.tz).toBe("America/New_York")
          const roundTripped = (yield* store.listSchedules({ queue: QueueName("default") }))[0]
          expect(roundTripped?.everyMs).toBe(90_000)

          // Both due: ordered by nextRunAt ascending.
          yield* TestClock.adjust(90_000)
          const due = yield* store.dueSchedules()
          expect(due.map((schedule) => schedule.key)).toEqual([
            "TestJob/tz",
            "TestJob/every"
          ])
        })
      ))

    it.effect("counts stay consistent across cancel, retry, keep pruning, and remove", () =>
      withStore((store) =>
        Effect.gen(function*() {
          // Three completions with keep {count: 1}: two get pruned.
          for (let i = 0; i < 3; i++) {
            const { id } = yield* store.enqueue(baseRequest({ payload: { n: i }, keep: { completed: { count: 1 } } }))
            const claim = yield* store.claim(claimOptions({ token: `t-${i}` }))
            assert(claim._tag === "Claimed")
            yield* store.ack(id, `t-${i}`, { _tag: "Complete", exit: undefined })
            yield* TestClock.adjust(10)
          }
          expect((yield* store.counts()).completed).toBe(1)

          // Cancel a waiting job.
          const doomed = yield* store.enqueue(baseRequest({ name: "Doomed" }))
          yield* store.cancel(doomed.id)
          expect((yield* store.counts()).cancelled).toBe(1)

          // Fail a job, then admin-retry it: failed -> waiting.
          const flaky = yield* store.enqueue(baseRequest())
          const claim = yield* store.claim(claimOptions({ token: "t-f" }))
          assert(claim._tag === "Claimed")
          yield* store.ack(flaky.id, "t-f", { _tag: "Fail", exit: undefined })
          expect((yield* store.counts()).failed).toBe(1)
          yield* store.retry(flaky.id)

          // Remove the surviving completed record.
          const completed = yield* store.list({ states: ["completed"] })
          const survivor = completed.items[0]
          assert(survivor !== undefined)
          expect(yield* store.remove(survivor.id)).toBe(true)

          expect(yield* store.counts()).toEqual({
            waiting: 1,
            delayed: 0,
            active: 0,
            completed: 0,
            failed: 0,
            cancelled: 1
          })
        })
      ))

    it.effect("schedule payloads round-trip high-precision numbers and empty arrays exactly", () =>
      withStore((store) =>
        Effect.gen(function*() {
          const payload = { big: 1234567890123456, third: 1 / 3, empty: [] }
          const schedule: JobStore.ScheduleRecord = {
            key: JobStore.ScheduleKey("TestJob/precise"),
            jobName: "TestJob",
            queue: QueueName("default"),
            cron: undefined,
            tz: undefined,
            everyMs: 60_000,
            payload,
            metadata: {},
            priority: 0,
            attemptsMax: 1,
            backoff: undefined,
            keep: undefined,
            timeoutMs: undefined,
            nextRunAt: 60_000
          }
          yield* store.upsertSchedule(schedule)
          const stored = (yield* store.listSchedules())[0]
          expect(stored?.payload).toEqual(payload)
        })
      ))

    it.effect("dedupe never changes the job id, and keys are scoped per name", () =>
      withStore((store) =>
        Effect.gen(function*() {
          const dedupe = { key: "emp-1", ttlMs: undefined, extend: false, replace: false }
          const first = yield* store.enqueue(baseRequest({ id: JobId("my-ulid-1"), dedupe }))
          expect(first).toEqual({ id: "my-ulid-1", duplicate: false })
          const job = yield* store.getJob(first.id)
          assert(Option.isSome(job))
          expect(job.value.dedupeKey).toBe("emp-1")

          // Same key, same name: deduplicated to the FIRST job's id.
          const second = yield* store.enqueue(baseRequest({ id: JobId("my-ulid-2"), dedupe }))
          expect(second).toEqual({ id: "my-ulid-1", duplicate: true })
          expect(Option.isNone(yield* store.getJob(JobId("my-ulid-2")))).toBe(true)

          // Same key, different name: no interference.
          const other = yield* store.enqueue(baseRequest({ name: "OtherJob", dedupe }))
          expect(other.duplicate).toBe(false)
        })
      ))

    it.effect("pending dedupe holds while the keyed job is unfinished and frees on completion", () =>
      withStore((store) =>
        Effect.gen(function*() {
          const dedupe = { key: "k", ttlMs: undefined, extend: false, replace: false }
          const first = yield* store.enqueue(baseRequest({ dedupe }))
          expect((yield* store.enqueue(baseRequest({ dedupe }))).duplicate).toBe(true)

          // Still deduped while active.
          const claim = yield* store.claim(claimOptions())
          assert(claim._tag === "Claimed")
          expect((yield* store.enqueue(baseRequest({ dedupe }))).duplicate).toBe(true)

          // A terminal ack frees the key immediately.
          yield* store.ack(first.id, "t-1", { _tag: "Complete", exit: undefined })
          const fresh = yield* store.enqueue(baseRequest({ dedupe }))
          expect(fresh.duplicate).toBe(false)
          expect(fresh.id).not.toBe(first.id)
        })
      ))

    it.effect("cancellation also frees a pending dedupe key", () =>
      withStore((store) =>
        Effect.gen(function*() {
          const dedupe = { key: "k", ttlMs: undefined, extend: false, replace: false }
          const first = yield* store.enqueue(baseRequest({ dedupe }))
          yield* store.cancel(first.id)
          expect((yield* store.enqueue(baseRequest({ dedupe }))).duplicate).toBe(false)
        })
      ))

    it.effect("a ttl dedupe window throttles even past completion, then expires", () =>
      withStore((store) =>
        Effect.gen(function*() {
          const dedupe = { key: "k", ttlMs: 60_000, extend: false, replace: false }
          const first = yield* store.enqueue(baseRequest({ dedupe }))
          const claim = yield* store.claim(claimOptions())
          assert(claim._tag === "Claimed")
          yield* store.ack(first.id, "t-1", { _tag: "Complete", exit: undefined })

          // Completed, but the window still throttles...
          yield* TestClock.adjust(30_000)
          expect((yield* store.enqueue(baseRequest({ dedupe }))).duplicate).toBe(true)

          // ...and a plain (non-extend) window is NOT pushed out by drops.
          yield* TestClock.adjust(30_000)
          const fresh = yield* store.enqueue(baseRequest({ dedupe }))
          expect(fresh.duplicate).toBe(false)
          expect(fresh.id).not.toBe(first.id)
        })
      ))

    it.effect("an extend dedupe window is pushed out by each deduplicated enqueue", () =>
      withStore((store) =>
        Effect.gen(function*() {
          const dedupe = { key: "k", ttlMs: 60_000, extend: true, replace: false }
          yield* store.enqueue(baseRequest({ dedupe }))

          yield* TestClock.adjust(45_000)
          expect((yield* store.enqueue(baseRequest({ dedupe }))).duplicate).toBe(true)

          // 75s after the FIRST enqueue — past the original window, inside
          // the extended one.
          yield* TestClock.adjust(30_000)
          expect((yield* store.enqueue(baseRequest({ dedupe }))).duplicate).toBe(true)

          // Past the latest extension: free again.
          yield* TestClock.adjust(60_001)
          expect((yield* store.enqueue(baseRequest({ dedupe }))).duplicate).toBe(false)
        })
      ))

    it.effect("replace dedupe rewrites a still-delayed job in place, latest content wins", () =>
      withStore((store) =>
        Effect.gen(function*() {
          const dedupe = { key: "k", ttlMs: undefined, extend: false, replace: true }
          const first = yield* store.enqueue(
            baseRequest({ dedupe, payload: { n: 1 }, priority: 1, delayMs: 60_000 })
          )

          const replaced = yield* store.enqueue(
            baseRequest({ dedupe, payload: { n: 2 }, priority: 7, delayMs: 5_000 })
          )
          expect(replaced).toEqual({ id: first.id, duplicate: true })
          const job = yield* store.getJob(first.id)
          assert(Option.isSome(job))
          expect(job.value.payload).toEqual({ n: 2 })
          expect(job.value.priority).toBe(7)
          expect(job.value.state).toBe("delayed")

          // The rewritten delay is live: due after 5s, not the original 60s.
          yield* TestClock.adjust(5_000)
          const claim = yield* store.claim(claimOptions())
          assert(claim._tag === "Claimed")
          expect(claim.job.id).toBe(first.id)
          expect(claim.job.payload).toEqual({ n: 2 })

          // Once claimed (active), replace degrades to plain dedup.
          const during = yield* store.enqueue(baseRequest({ dedupe, payload: { n: 3 } }))
          expect(during).toEqual({ id: first.id, duplicate: true })
          const active = yield* store.getJob(first.id)
          assert(Option.isSome(active))
          expect(active.value.payload).toEqual({ n: 2 })
        })
      ))

    it.effect("an existing explicit id wins over the dedup tree in every driver", () =>
      withStore((store) =>
        Effect.gen(function*() {
          const key = { key: "k", ttlMs: undefined, extend: false, replace: false }
          // X runs under key k and completes, freeing the key but staying in
          // history.
          const x = yield* store.enqueue(baseRequest({ id: JobId("X"), dedupe: key }))
          const claim = yield* store.claim(claimOptions())
          assert(claim._tag === "Claimed")
          yield* store.ack(x.id, "t-1", { _tag: "Complete", exit: undefined })

          // J takes over the key, delayed.
          const j = yield* store.enqueue(
            baseRequest({ dedupe: key, payload: { n: 9 }, delayMs: 60_000 })
          )

          // A retried enqueue of X (id exists) must return X untouched — the
          // id check precedes the dedup tree, so J is neither returned nor
          // replaced.
          const retried = yield* store.enqueue(baseRequest({
            id: JobId("X"),
            payload: { n: 1 },
            dedupe: { ...key, replace: true }
          }))
          expect(retried).toEqual({ id: "X", duplicate: true })
          const job = yield* store.getJob(j.id)
          assert(Option.isSome(job))
          expect(job.value.payload).toEqual({ n: 9 })
          expect(job.value.state).toBe("delayed")
        })
      ))

    it.effect("a landed replace re-arms the ttl window", () =>
      withStore((store) =>
        Effect.gen(function*() {
          const dedupe = { key: "k", ttlMs: 60_000, extend: false, replace: true }
          const first = yield* store.enqueue(baseRequest({ dedupe, delayMs: 300_000 }))

          // 50s in: replace lands; the window restarts from here.
          yield* TestClock.adjust(50_000)
          const replaced = yield* store.enqueue(
            baseRequest({ dedupe, payload: { n: 2 }, delayMs: 300_000 })
          )
          expect(replaced).toEqual({ id: first.id, duplicate: true })

          // 100s after the FIRST enqueue — past the original window, inside
          // the re-armed one: still the same job.
          yield* TestClock.adjust(50_000)
          const again = yield* store.enqueue(
            baseRequest({ dedupe, payload: { n: 3 }, delayMs: 300_000 })
          )
          expect(again).toEqual({ id: first.id, duplicate: true })
        })
      ))

    it.effect("keep policies are independent per terminal state", () =>
      withStore((store) =>
        Effect.gen(function*() {
          // Completed records keep only the newest 1; failed keep everything.
          const keep = { completed: { count: 1 } }
          const completed: Array<JobStore.JobId> = []
          const failed: Array<JobStore.JobId> = []
          for (let i = 0; i < 2; i++) {
            const done = yield* store.enqueue(baseRequest({ payload: { n: i }, keep }))
            const claimA = yield* store.claim(claimOptions({ token: `tc-${i}` }))
            assert(claimA._tag === "Claimed")
            yield* store.ack(done.id, `tc-${i}`, { _tag: "Complete", exit: undefined })
            completed.push(done.id)

            const bad = yield* store.enqueue(baseRequest({ payload: { n: 10 + i }, keep }))
            const claimB = yield* store.claim(claimOptions({ token: `tf-${i}` }))
            assert(claimB._tag === "Claimed")
            yield* store.ack(bad.id, `tf-${i}`, { _tag: "Fail", exit: undefined })
            failed.push(bad.id)
            yield* TestClock.adjust(10)
          }
          const counts = yield* store.counts()
          expect(counts.completed).toBe(1)
          expect(counts.failed).toBe(2)
          expect(Option.isNone(yield* store.getJob(completed[0] ?? JobId("?")))).toBe(true)
          expect(Option.isSome(yield* store.getJob(failed[0] ?? JobId("?")))).toBe(true)
        })
      ))

    it.effect("an enqueue wakes only waiters watching its queue", () =>
      withStore((store) =>
        Effect.gen(function*() {
          const empty = yield* store.claim(claimOptions())
          assert(empty._tag === "Empty")

          let woke = false
          const waiter = yield* Effect.forkChild(
            store.awaitWake([QueueName("default")], empty.wakeToken).pipe(
              Effect.tap(() => Effect.sync(() => void (woke = true)))
            )
          )
          yield* Effect.yieldNow

          // Work on ANOTHER queue must not wake the default-queue waiter.
          yield* store.enqueue(baseRequest({ queue: QueueName("other") }))
          for (let i = 0; i < 10; i++) {
            yield* Effect.yieldNow
          }
          expect(woke).toBe(false)

          // Matching-queue work does.
          yield* store.enqueue(baseRequest())
          yield* Fiber.join(waiter)
          expect(woke).toBe(true)
        })
      ))

    it.effect("cancelByDedupe cancels pending keyed jobs and is idempotent", () =>
      withStore((store) =>
        Effect.gen(function*() {
          const dedupe = { key: "emp-1", ttlMs: undefined, extend: false, replace: false }
          // Unknown key: nothing pending, no error.
          expect(yield* store.cancelByDedupe("TestJob", "emp-1")).toBe(false)

          // Delayed keyed job: cancelled terminally.
          const delayed = yield* store.enqueue(baseRequest({ dedupe, delayMs: 60_000 }))
          expect(yield* store.cancelByDedupe("TestJob", "emp-1")).toBe(true)
          const job = yield* store.getJob(delayed.id)
          assert(Option.isSome(job))
          expect(job.value.state).toBe("cancelled")
          expect(yield* store.cancelByDedupe("TestJob", "emp-1")).toBe(false)

          // Active keyed job: flagged for the heartbeat.
          const active = yield* store.enqueue(baseRequest({ dedupe, payload: { n: 2 } }))
          const claim = yield* store.claim(claimOptions())
          assert(claim._tag === "Claimed")
          expect(yield* store.cancelByDedupe("TestJob", "emp-1")).toBe(true)
          const flagged = yield* store.getJob(active.id)
          assert(Option.isSome(flagged))
          expect(flagged.value.cancelRequested).toBe(true)

          // Name scoping: same key under another name is untouched.
          expect(yield* store.cancelByDedupe("OtherJob", "emp-1")).toBe(false)
        })
      ))

    it.effect("the producer's trace context round-trips on the record", () =>
      withStore((store) =>
        Effect.gen(function*() {
          const trace = { traceId: "trace-abc", spanId: "span-def", sampled: true, delayed: false }
          const { id } = yield* store.enqueue(baseRequest({ trace }))
          const job = yield* store.getJob(id)
          assert(Option.isSome(job))
          expect(job.value.trace).toEqual(trace)

          const bare = yield* store.enqueue(baseRequest({ payload: { n: 2 } }))
          const none = yield* store.getJob(bare.id)
          assert(Option.isSome(none))
          expect(none.value.trace).toBeUndefined()
        })
      ))

    it.effect("delayed jobs still promote to waiting while their queue is paused", () =>
      withStore((store) =>
        Effect.gen(function*() {
          const { id } = yield* store.enqueue(baseRequest({ delayMs: 1_000 }))
          yield* store.pause(QueueName("default"))
          yield* TestClock.adjust(1_000)

          const claim = yield* store.claim(claimOptions())
          assert(claim._tag === "Empty")
          const job = yield* store.getJob(id)
          assert(Option.isSome(job))
          expect(job.value.state).toBe("waiting")
        })
      ))
  })
}
