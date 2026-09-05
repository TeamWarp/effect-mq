import { assert, describe, expect, it } from "@effect/vitest"
import { Cause, Clock, Effect, Exit, Fiber, Option } from "effect"
import { TestClock } from "effect/testing"
import { type ListenProbeTimeout, PROBE_PAYLOAD, probed } from "../../src/drizzle-postgres/listenProbe.ts"

/** Let all currently runnable fibers make progress. */
const settle = Effect.gen(function*() {
  for (let i = 0; i < 10; i++) {
    yield* Effect.yieldNow
  }
})

/** Walk the clock in one-second steps so every probe and timeout lands. */
const walk = (seconds: number) =>
  Effect.gen(function*() {
    yield* settle
    for (let i = 0; i < seconds; i++) {
      yield* TestClock.adjust("1 second")
      yield* settle
    }
  })

/**
 * A scripted wake channel. Healthy means a ping is echoed straight back on
 * the subscription; `dead` means it never is; `poolDown` means the ping
 * cannot even be sent.
 */
const fakeChannel = () => {
  const state = { dead: false, poolDown: false }
  let deliver: ((payload: string) => void) | undefined
  const woken: Array<string> = []
  const subscribe = (onPayload: (payload: string) => void) =>
    Effect.suspend(() => {
      deliver = onPayload
      return Effect.never
    })
  const ping = Effect.suspend(() => {
    if (state.poolDown) return Effect.fail("pool down")
    if (!state.dead) deliver?.(PROBE_PAYLOAD)
    return Effect.void
  })
  return {
    state,
    subscribe,
    ping,
    woken,
    onWake: (payload: string) => {
      woken.push(payload)
    },
    notify: (payload: string) => deliver?.(payload)
  }
}

const INTERVAL_MS = 30_000
const TIMEOUT_MS = 10_000

/** Fork the probed subscription, the wall clock following TestClock unless given. */
const start = <E = never>(
  channel: ReturnType<typeof fakeChannel>,
  options: {
    readonly subscribe?: (onPayload: (payload: string) => void) => Effect.Effect<void, E>
    readonly wallClock?: () => number
  } = {}
) =>
  Effect.gen(function*() {
    const clock = yield* Clock.Clock
    return yield* Effect.forkChild(probed({
      subscribe: options.subscribe ?? channel.subscribe,
      onWake: channel.onWake,
      ping: channel.ping,
      intervalMs: INTERVAL_MS,
      timeoutMs: TIMEOUT_MS,
      wallClock: options.wallClock ?? (() => clock.currentTimeMillisUnsafe())
    }))
  })

const probeTimeout = (exit: Exit.Exit<void, ListenProbeTimeout>) => {
  assert(Exit.isFailure(exit))
  const error = Cause.findErrorOption(exit.cause)
  assert(Option.isSome(error))
  return error.value
}

describe("probed", () => {
  it.effect("a subscription that echoes is left alone", () =>
    Effect.gen(function*() {
      const channel = fakeChannel()
      const fiber = yield* start(channel)
      yield* walk(600)

      expect(fiber.pollUnsafe()).toBeUndefined()
      yield* Fiber.interrupt(fiber)
    }))

  it.effect("payloads reach onWake; probes do not", () =>
    Effect.gen(function*() {
      const channel = fakeChannel()
      const fiber = yield* start(channel)
      yield* walk(1)
      channel.notify("orders")
      channel.notify(PROBE_PAYLOAD)
      channel.notify("*")

      expect(channel.woken).toEqual(["orders", "*"])
      yield* Fiber.interrupt(fiber)
    }))

  // The silent-pooler case: LISTEN was accepted, nothing ever arrives.
  it.effect("no echo at all: fails after interval + timeout, having seen zero echoes", () =>
    Effect.gen(function*() {
      const channel = fakeChannel()
      channel.state.dead = true
      const fiber = yield* start(channel)

      // Probe at 30s, deadline at 40s.
      yield* walk(39)
      expect(fiber.pollUnsafe()).toBeUndefined()
      yield* walk(1)
      const error = probeTimeout(yield* Effect.exit(Fiber.join(fiber)))
      expect(error._tag).toBe("ListenProbeTimeout")
      expect(error.echoes).toBe(0)
    }))

  // The dropped-socket case: it worked, then it did not.
  it.effect("a subscription that dies after working fails with the echoes it saw", () =>
    Effect.gen(function*() {
      const channel = fakeChannel()
      const fiber = yield* start(channel)
      // Probes run every interval + timeout: two healthy ones, at 30s and 70s.
      yield* walk(75)
      expect(fiber.pollUnsafe()).toBeUndefined()
      channel.state.dead = true
      // The probe at 110s goes unanswered; deadline at 120s.
      yield* walk(45)

      const error = probeTimeout(yield* Effect.exit(Fiber.join(fiber)))
      expect(error.echoes).toBe(2)
    }))

  it.effect("a probe that cannot be sent is no verdict", () =>
    Effect.gen(function*() {
      const channel = fakeChannel()
      channel.state.dead = true
      channel.state.poolDown = true
      const fiber = yield* start(channel)
      // Ten probes, none of them sendable (an unsendable probe has no
      // timeout to wait out, so they come every interval): nothing is decided.
      yield* walk(300)
      expect(fiber.pollUnsafe()).toBeUndefined()

      // The pool is back, the listen socket is still dead: probe at 330s,
      // deadline at 340s.
      channel.state.poolDown = false
      yield* walk(40)
      const error = probeTimeout(yield* Effect.exit(Fiber.join(fiber)))
      expect(error.echoes).toBe(0)
    }))

  // The conformance suite runs under TestClock against real Postgres: a
  // clock that jumps a day cannot make the wire deliver faster, so a miss
  // that only the Effect Clock vouches for is not a miss.
  it.effect("a TestClock jump alone is never a miss", () =>
    Effect.gen(function*() {
      const channel = fakeChannel()
      channel.state.dead = true
      const fiber = yield* start(channel, { wallClock: () => 0 })
      yield* walk(600)

      expect(fiber.pollUnsafe()).toBeUndefined()
      yield* Fiber.interrupt(fiber)
    }))

  it.effect("a setup failure propagates as itself", () =>
    Effect.gen(function*() {
      const channel = fakeChannel()
      const fiber = yield* start(channel, {
        subscribe: () => Effect.fail("LISTEN not supported by this connection")
      })
      yield* settle

      const exit = yield* Effect.exit(Fiber.join(fiber))
      assert(Exit.isFailure(exit))
      expect(Cause.findErrorOption(exit.cause)).toEqual(Option.some("LISTEN not supported by this connection"))
    }))

  it.effect("a stream that ends ends the probe with it", () =>
    Effect.gen(function*() {
      const channel = fakeChannel()
      const fiber = yield* start(channel, { subscribe: () => Effect.void })
      yield* settle

      expect(Exit.isSuccess(yield* Effect.exit(Fiber.join(fiber)))).toBe(true)
    }))
})
