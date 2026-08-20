import { describe, expect, it } from "@effect/vitest"
import { Effect, Layer, Schema } from "effect"
import { Job, MemoryJobStore, Worker } from "../src/index.ts"

/**
 * Live-clock smoke tests: everything else runs under TestClock, so these
 * guard the "live-clock correct" invariant (real timers, real races) on the
 * actual runtime. Timings are kept tiny to stay fast but non-flaky.
 */
describe("Worker (live clock)", () => {
  it.live("executes, delays, and retries on real timers", () =>
    Effect.gen(function*() {
      const Live = Job.make("LiveSmoke", {
        payload: { n: Schema.Number },
        success: Schema.Number,
        defaults: { attempts: 2, backoff: { type: "fixed", delay: "30 millis" } }
      })
      let flakyAttempts = 0
      const handlers = Live.toLayer(({ n }, context) =>
        Effect.suspend(() => {
          if (n === 2) {
            flakyAttempts++
            if (context.attempt === 1) return Effect.die("first attempt fails")
          }
          return Effect.succeed(n * 10)
        })
      )

      yield* Effect.gen(function*() {
        // Plain round-trip.
        const first = yield* Live.execute({ n: 1 }).pipe(
          Effect.timeout("5 seconds")
        )
        expect(first).toBe(10)

        // Delayed job runs after its (real) delay.
        const delayed = yield* Live.execute({ n: 3 }, { delay: "50 millis" }).pipe(
          Effect.timeout("5 seconds")
        )
        expect(delayed).toBe(30)

        // Defect on attempt 1 -> real-time backoff -> success on attempt 2.
        const retried = yield* Live.execute({ n: 2 }).pipe(
          Effect.timeout("5 seconds")
        )
        expect(retried).toBe(20)
        expect(flakyAttempts).toBe(2)
      }).pipe(
        Effect.provide(
          handlers.pipe(
            Layer.provideMerge(Worker.layer({
              pollInterval: "20 millis",
              lockDuration: "2 seconds"
            })),
            Layer.provideMerge(MemoryJobStore.layer)
          )
        )
      )
    }))
})
