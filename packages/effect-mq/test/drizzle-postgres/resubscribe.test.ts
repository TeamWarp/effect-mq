import { describe, expect, it } from "@effect/vitest"
import { Clock, type Duration, Effect, Fiber, type LogLevel, Logger, References } from "effect"
import { TestClock } from "effect/testing"
import { resubscribeForever } from "../../src/drizzle-postgres/resubscribe.ts"

/** Let all currently runnable fibers make progress. */
const settle = Effect.gen(function*() {
  for (let i = 0; i < 10; i++) {
    yield* Effect.yieldNow
  }
})

interface Line {
  readonly level: LogLevel.LogLevel
  readonly text: string
  readonly cause: string
}

/** Capture this loop's lines only, at every level down to debug. */
const captureInto = (lines: Array<Line>) => <A, E, R>(self: Effect.Effect<A, E, R>) =>
  self.pipe(
    Effect.provide(
      Logger.layer([
        Logger.make((options) => {
          const [head] = Array.isArray(options.message) ? options.message : [options.message]
          const text = String(head)
          if (text.startsWith("effect-mq: LISTEN")) {
            lines.push({ level: options.logLevel, text, cause: String(options.cause) })
          }
        })
      ])
    ),
    Effect.provideService(References.MinimumLogLevel, "Debug")
  )

const levels = (lines: ReadonlyArray<Line>) => lines.map((line) => line.level)

interface Attempt {
  /** How long the attempt runs before it settles. */
  readonly upFor: number
  /** How it settles: fail, die, return normally, or fail as a subscription that had been working. */
  readonly outcome: "fail" | "die" | "succeed" | "fail-healthy"
}

/**
 * A subscription that plays the given attempts in order, recording the clock
 * time each one began. The last attempt repeats once the script runs out, so
 * a script is a prefix plus a steady state.
 */
const scripted = (script: ReadonlyArray<Attempt>, startedAt: Array<number> = []) => {
  if (script.length === 0) throw new Error("scripted: need at least one attempt")
  let n = 0
  return Effect.gen(function*() {
    const step = script[Math.min(n, script.length - 1)]
    n += 1
    startedAt.push(yield* Clock.currentTimeMillis)
    yield* Effect.sleep(step.upFor)
    if (step.outcome === "fail") return yield* Effect.fail("LISTEN not supported by this connection")
    if (step.outcome === "die") return yield* Effect.die(new Error("listen client blew up"))
    if (step.outcome === "fail-healthy") return yield* Effect.fail(LOST_AFTER_ECHOING)
  })
}

const LOST_AFTER_ECHOING = "lost after echoing"

const fails = (upFor = 0): Attempt => ({ upFor, outcome: "fail" })
const succeeds = (upFor = 0): Attempt => ({ upFor, outcome: "succeed" })
const failsHealthy = (upFor = 0): Attempt => ({ upFor, outcome: "fail-healthy" })

/** Run the loop for `window`, then interrupt it. */
const runFor = <E>(
  subscribe: Effect.Effect<void, E>,
  window: Duration.Input,
  options?: { readonly wasHealthy?: ((error: E) => boolean) | undefined }
) =>
  Effect.gen(function*() {
    const fiber = yield* Effect.forkChild(resubscribeForever(subscribe, options))
    yield* settle
    yield* TestClock.adjust(window)
    yield* settle
    yield* Fiber.interrupt(fiber)
  })

describe("resubscribeForever", () => {
  describe("backoff", () => {
    // A connection that can never support LISTEN (PgBouncer in transaction
    // mode) fails every attempt the same way, so the retry has to settle
    // down instead of hammering once a second forever.
    it.effect("doubles the delay while failures are consecutive, then holds at the cap", () =>
      Effect.gen(function*() {
        const at: Array<number> = []
        yield* runFor(scripted([fails()], at), "5 minutes")

        // 1s, 2s, 4s, 8s, 16s, then the 30s cap from there on.
        expect(at.slice(0, 10)).toEqual([
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

    // Regression: the health check used to key off elapsed time, so an
    // attempt that took longer than the cap to FAIL looked like a healthy
    // subscription and reset the streak. A hung endpoint then got a
    // full-speed retry and a fresh warning every single time.
    it.effect("keeps backing off when each attempt is slow to fail", () => {
      const lines: Array<Line> = []
      const at: Array<number> = []
      return Effect.gen(function*() {
        // Every attempt hangs for 35s — longer than the 30s cap — then fails.
        yield* runFor(scripted([fails(35_000)], at), "4 minutes")

        // Gaps between attempts must still grow: 35s+1s, 35s+2s, 35s+4s...
        const gaps = at.slice(1).map((t, i) => t - at[i])
        expect(gaps.slice(0, 4)).toEqual([36_000, 37_000, 39_000, 43_000])
        // ...and only the first failure is a warning.
        expect(levels(lines)[0]).toBe("Warn")
        expect(new Set(levels(lines).slice(1))).toEqual(new Set(["Debug"]))
      }).pipe(captureInto(lines))
    })

    it.effect("never sleeps longer than the 30 second cap", () =>
      Effect.gen(function*() {
        const at: Array<number> = []
        yield* runFor(scripted([fails()], at), "10 minutes")

        const gaps = at.slice(1).map((t, i) => t - at[i])
        expect(Math.max(...gaps)).toBe(30_000)
      }))
  })

  describe("log levels", () => {
    // The first failure is the actionable event; the ten-thousandth is not.
    it.effect("warns once per streak and debugs the rest", () => {
      const lines: Array<Line> = []
      return Effect.gen(function*() {
        yield* runFor(scripted([fails()]), "1 minute")

        expect(lines.length).toBeGreaterThan(3)
        expect(levels(lines)[0]).toBe("Warn")
        expect(new Set(levels(lines).slice(1))).toEqual(new Set(["Debug"]))
      }).pipe(captureInto(lines))
    })

    it.effect("attaches the failure cause to the warning", () => {
      const lines: Array<Line> = []
      return Effect.gen(function*() {
        yield* runFor(scripted([fails()]), "2 seconds")

        expect(lines[0].text).toContain("wake-ups degraded to polling")
        expect(lines[0].cause).toContain("LISTEN not supported by this connection")
      }).pipe(captureInto(lines))
    })

    it.effect("treats a defect like a failure instead of tearing the loop down", () => {
      const lines: Array<Line> = []
      const at: Array<number> = []
      return Effect.gen(function*() {
        yield* runFor(scripted([{ upFor: 0, outcome: "die" }], at), "10 seconds")

        // It kept retrying rather than propagating the defect.
        expect(at.length).toBeGreaterThan(2)
        expect(levels(lines)[0]).toBe("Warn")
        expect(lines[0].cause).toContain("listen client blew up")
      }).pipe(captureInto(lines))
    })
  })

  describe("streak resets", () => {
    // Regression: the counter used to be incremented on every attempt,
    // including successful ones, so a single clean completion silently
    // demoted the next real failure to debug and the warning was lost.
    it.effect("a successful attempt restores the warning for the next failure", () => {
      const lines: Array<Line> = []
      return Effect.gen(function*() {
        // fail, fail, succeed, then fail again.
        yield* runFor(scripted([fails(), fails(), succeeds(), fails()]), "1 minute")

        // Warn, Debug for the opening streak; the success clears it; the
        // next failure opens a new streak and warns again.
        expect(levels(lines).slice(0, 3)).toEqual(["Warn", "Debug", "Warn"])
      }).pipe(captureInto(lines))
    })

    it.effect("a successful attempt logs nothing at all", () => {
      const lines: Array<Line> = []
      return Effect.gen(function*() {
        yield* runFor(scripted([succeeds()]), "10 seconds")

        expect(lines).toEqual([])
      }).pipe(captureInto(lines))
    })

    it.effect("a successful attempt restarts the delay at one second", () =>
      Effect.gen(function*() {
        const at: Array<number> = []
        // Four failures walk the delay out to 8s, then one success resets it.
        yield* runFor(scripted([fails(), fails(), fails(), fails(), succeeds()], at), "1 minute")

        // 0, +1s, +2s, +4s, +8s reaches the success at 15s; every attempt
        // after it is a second apart, not eight.
        expect(at.slice(0, 5)).toEqual([0, 1_000, 3_000, 7_000, 15_000])
        const afterSuccess = at.slice(5).map((t, i) => t - at.slice(4)[i])
        expect(new Set(afterSuccess.slice(0, 3))).toEqual(new Set([1_000]))
      }))

    it.effect("consecutive successes do not escalate the delay", () =>
      Effect.gen(function*() {
        const at: Array<number> = []
        yield* runFor(scripted([succeeds()], at), "10 seconds")

        const gaps = at.slice(1).map((t, i) => t - at[i])
        expect(new Set(gaps)).toEqual(new Set([1_000]))
      }))
  })

  describe("wasHealthy", () => {
    // The liveness probe fails a subscription that had been echoing. That is
    // a new incident, not the next step of the streak it interrupted.
    it.effect("a failure of a working subscription opens a new streak: warns and retries at a second", () => {
      const lines: Array<Line> = []
      const at: Array<number> = []
      return Effect.gen(function*() {
        // Two setup failures, then a subscription that lives a minute and is
        // then declared dead, then setup failures again.
        yield* runFor(
          scripted([fails(), fails(), failsHealthy(60_000), fails()], at),
          "5 minutes",
          { wasHealthy: (error) => error === LOST_AFTER_ECHOING }
        )

        // 0, +1s, +2s reaches the healthy attempt at 3s; it dies at 63s and
        // is retried a second later, not four; the streak walks up from
        // there.
        expect(at.slice(0, 6)).toEqual([0, 1_000, 3_000, 64_000, 66_000, 70_000])
        expect(levels(lines).slice(0, 5)).toEqual(["Warn", "Debug", "Warn", "Debug", "Debug"])
      }).pipe(captureInto(lines))
    })

    it.effect("without wasHealthy the same failure continues the streak", () => {
      const lines: Array<Line> = []
      const at: Array<number> = []
      return Effect.gen(function*() {
        yield* runFor(scripted([fails(), fails(), failsHealthy(60_000), fails()], at), "5 minutes")

        expect(at.slice(0, 5)).toEqual([0, 1_000, 3_000, 67_000, 75_000])
        expect(levels(lines).slice(0, 4)).toEqual(["Warn", "Debug", "Debug", "Debug"])
      }).pipe(captureInto(lines))
    })
  })

  describe("shutdown", () => {
    // The store forks this into its scope, so scope close interrupts it.
    // An interrupt is not a subscription failure and must not be logged.
    it.effect("interruption logs nothing and stops the loop", () => {
      const lines: Array<Line> = []
      const at: Array<number> = []
      return Effect.gen(function*() {
        const fiber = yield* Effect.forkChild(resubscribeForever(scripted([fails()], at)))
        yield* settle
        yield* TestClock.adjust("5 seconds")
        yield* settle
        const seen = at.length
        expect(lines.length).toBeGreaterThan(0)

        yield* Fiber.interrupt(fiber)
        yield* settle
        yield* TestClock.adjust("1 minute")
        yield* settle

        // No further attempts, and the interrupt itself was not logged.
        expect(at.length).toBe(seen)
        expect(levels(lines).slice(1).includes("Warn")).toBe(false)
      }).pipe(captureInto(lines))
    })

    it.effect("interruption while the subscription is healthy logs nothing", () => {
      const lines: Array<Line> = []
      return Effect.gen(function*() {
        const fiber = yield* Effect.forkChild(resubscribeForever(Effect.never))
        yield* settle
        yield* TestClock.adjust("10 minutes")
        yield* settle
        yield* Fiber.interrupt(fiber)
        yield* settle

        expect(lines).toEqual([])
      }).pipe(captureInto(lines))
    })
  })
})
