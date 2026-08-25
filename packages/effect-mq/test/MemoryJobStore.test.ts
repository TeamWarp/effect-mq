import { jobStoreConformance } from "../src/testing/index.ts"
import { JobStore, MemoryJobStore } from "../src/index.ts"
import { assert, describe, expect, it } from "@effect/vitest"
import { Effect } from "effect"
import { TestClock } from "effect/testing"

jobStoreConformance("MemoryJobStore", () => MemoryJobStore.layer)

const { JobId, QueueName } = JobStore

const request = (overrides?: Partial<JobStore.EnqueueRequest>): JobStore.EnqueueRequest => ({
  id: undefined,
  name: "TestJob",
  queue: QueueName("default"),
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

describe("MemoryJobStore list ordering beyond the required surface", () => {
  // Redis constrains finishedAt ordering to terminal states; memory (like
  // Postgres) serves the mixed case, with missing finishedAt sorting as 0.
  it.effect("finishedAt ordering over mixed states sorts unfinished rows as 0", () =>
    Effect.gen(function*() {
      const store = yield* MemoryJobStore.makeWith()
      // Delayed: never claimable here, so it stays without a finishedAt.
      const pending = yield* store.enqueue(request({ payload: { n: 1 }, delayMs: 60_000 }))
      const finish = (token: string) =>
        Effect.gen(function*() {
          yield* store.enqueue(request())
          const claim = yield* store.claim({
            queue: QueueName("default"),
            names: ["TestJob"],
            token,
            lockDurationMs: 30_000
          })
          assert(claim._tag === "Claimed")
          yield* store.ack(claim.job.id, token, { _tag: "Complete", exit: undefined })
          yield* TestClock.adjust(1_000)
          return claim.job.id
        })
      const older = yield* finish("t-1")
      const newer = yield* finish("t-2")

      const desc = yield* store.list({ orderBy: "finishedAt", order: "desc" })
      expect(desc.items.map((job) => job.id)).toEqual([newer, older, pending.id])
      const asc = yield* store.list({ orderBy: "finishedAt", order: "asc" })
      expect(asc.items.map((job) => job.id)).toEqual([pending.id, older, newer])
    }).pipe(Effect.scoped))
})

describe("MemoryJobStore idGenerator", () => {
  it.effect("store-assigned ids come from the configured generator", () =>
    Effect.gen(function*() {
      let n = 0
      const calls: Array<string> = []
      const store = yield* MemoryJobStore.makeWith({
        idGenerator: ({ name }) => {
          calls.push(name)
          return `job_${name}_${++n}`
        }
      })

      const first = yield* store.enqueue(request())
      expect(first.id).toBe("job_TestJob_1")
      const second = yield* store.enqueue(request())
      expect(second.id).toBe("job_TestJob_2")

      // User-supplied (idempotency) ids never consult the generator.
      const custom = yield* store.enqueue(request({ id: JobId("my-key") }))
      expect(custom.id).toBe("my-key")
      expect(calls).toHaveLength(2)
    }).pipe(Effect.scoped))

  it.effect("effectful generators work and collisions are retried", () =>
    Effect.gen(function*() {
      let n = 0
      const store = yield* MemoryJobStore.makeWith({
        // Yields "dup" twice per job before a unique id — the bounded retry
        // loop must absorb the duplicates.
        idGenerator: () => Effect.sync(() => (++n % 3 === 0 ? `u-${n}` : "dup"))
      })
      yield* store.enqueue(request({ id: JobId("dup") }))

      const result = yield* store.enqueue(request())
      expect(result.id).toBe("u-3")
    }).pipe(Effect.scoped))

  it.effect("a generator that cannot produce a unique id fails the enqueue", () =>
    Effect.gen(function*() {
      const store = yield* MemoryJobStore.makeWith({ idGenerator: () => "constant" })
      const first = yield* store.enqueue(request())
      expect(first.id).toBe("constant")

      const stuck = yield* Effect.flip(store.enqueue(request()))
      assert(stuck._tag === "JobStoreError")
      expect(stuck.message).toContain("unique job id")
    }).pipe(Effect.scoped))
})
