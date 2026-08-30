import { describe, expect, it } from "@effect/vitest"
import { Clock, Effect, Fiber, type LogLevel, Logger, References } from "effect"
import { TestClock } from "effect/testing"
import { DrizzleJobStore } from "../../src/drizzle-postgres/index.ts"

/** Let all currently runnable fibers make progress. */
const settle = Effect.gen(function*() {
  for (let i = 0; i < 10; i++) {
    yield* Effect.yieldNow
  }
})

interface Line {
  readonly level: LogLevel.LogLevel
  readonly text: string
}

/** Capture just this loop's lines, at every level down to debug. */
const captureInto = (lines: Array<Line>) => <A, E, R>(self: Effect.Effect<A, E, R>) =>
  self.pipe(
    Effect.provide(
      Logger.layer([
        Logger.make((options) => {
          const [head] = Array.isArray(options.message) ? options.message : [options.message]
          const text = String(head)
          if (text.startsWith("effect-mq: LISTEN")) {
            lines.push({ level: options.logLevel, text })
          }
        })
      ])
    ),
    Effect.provideService(References.MinimumLogLevel, "Debug")
  )

/**
 * A subscription whose attempts stay up for the given durations before
 * failing, recording the clock time each attempt began. The last duration
 * repeats once the list runs out.
 */
const scriptedSubscribe = (upForMillis: ReadonlyArray<number>, startedAt: Array<number>) => {
  let attempt = 0
  return Effect.gen(function*() {
    const upFor = upForMillis[Math.min(attempt, upForMillis.length - 1)]
    attempt += 1
    startedAt.push(yield* Clock.currentTimeMillis)
    yield* Effect.sleep(upFor)
    return yield* Effect.fail("LISTEN not supported by this connection")
  })
}

describe("DrizzleJobStore.resubscribeForever", () => {
  // A connection that can never support LISTEN (PgBouncer in transaction
  // mode) fails every attempt the same way. The retry has to settle down
  // instead of hammering — and logging — once a second forever.
  it.effect("backs off exponentially to a 30 second cap while attempts keep failing", () =>
    Effect.gen(function*() {
      const startedAt: Array<number> = []
      const fiber = yield* Effect.forkChild(
        DrizzleJobStore.resubscribeForever(scriptedSubscribe([0], startedAt))
      )

      yield* settle
      yield* TestClock.adjust("5 minutes")
      yield* settle
      yield* Fiber.interrupt(fiber)

      // 1s, 2s, 4s, 8s, 16s, then the cap.
      expect(startedAt.slice(0, 10)).toEqual([
        0,
        1_000,
        3_000,
        7_000,
        15_000,
        31_000,
        61_000,
        91_000,
        121_000,
        151_000
      ])
    }))

  // The first failure is the actionable event; the ten-thousandth is not.
  it.effect("logs the first failure of a streak at warning level and the rest at debug", () => {
    const lines: Array<Line> = []
    return Effect.gen(function*() {
      const fiber = yield* Effect.forkChild(
        DrizzleJobStore.resubscribeForever(scriptedSubscribe([0], []))
      )

      yield* settle
      yield* TestClock.adjust("1 minute")
      yield* settle
      yield* Fiber.interrupt(fiber)

      expect(lines.length).toBeGreaterThan(3)
      expect(lines[0].level).toBe("Warn")
      expect(lines[0].text).toContain("wake-ups degraded to polling")
      expect(new Set(lines.slice(1).map((line) => line.level))).toEqual(new Set(["Debug"]))
    }).pipe(captureInto(lines))
  })

  // A subscription that held for longer than the cap was healthy, so the
  // drop that follows it is a new event: full-speed retry, warning of its own.
  it.effect("resets the streak after a subscription that stayed up past the cap", () => {
    const lines: Array<Line> = []
    const startedAt: Array<number> = []
    return Effect.gen(function*() {
      const fiber = yield* Effect.forkChild(
        // Two instant failures, then one that holds a minute, then instant again.
        DrizzleJobStore.resubscribeForever(scriptedSubscribe([0, 0, 60_000, 0], startedAt))
      )

      yield* settle
      yield* TestClock.adjust("2 minutes")
      yield* settle
      yield* Fiber.interrupt(fiber)

      // t=0 fail, +1s fail, then the healthy attempt holds 3s..63s. Its drop
      // restarts the streak, so the retries after it are 1s and 2s again —
      // not the 8s the un-reset streak would have been up to.
      expect(startedAt.slice(0, 5)).toEqual([0, 1_000, 3_000, 64_000, 66_000])
      expect(lines.slice(0, 4).map((line) => line.level)).toEqual(["Warn", "Debug", "Warn", "Debug"])
    }).pipe(captureInto(lines))
  })
})
