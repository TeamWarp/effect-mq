/**
 * Liveness probe for the wake-channel subscription.
 *
 * `@effect/sql-pg`'s `listen` stream only fails during setup: the listen
 * client's error handler is a no-op and nothing watches `end`, so once the
 * socket drops the stream stays open and delivers nothing for the life of
 * the process. The same is true on a pooler that accepts `LISTEN` and drops
 * notifications (stock PgBouncer in transaction mode). Either way wake-ups
 * silently degrade to `pollInterval` with no log and no resubscribe.
 *
 * So the store probes. Every `intervalMs` it sends a reserved payload on the
 * wake channel through the pool, and if no echo arrives within `timeoutMs`
 * it fails the subscription with `ListenProbeTimeout`. The resubscribe loop
 * then does what it already does for a setup failure — warn, back off,
 * `LISTEN` again — and because the stream closed, the next `LISTEN` gets a
 * fresh client.
 *
 * Internal: `drizzle-postgres/index.ts` deliberately does not re-export this
 * module, so nothing here is part of the package's public surface.
 *
 * @since 0.7.1
 */
import { Data, Effect, Exit } from "effect"

/**
 * The NOTIFY payload a probe sends. Reserved alongside `"*"`: a queue with
 * this exact name would never receive cross-process wake-ups.
 */
export const PROBE_PAYLOAD = "effect-mq:probe"

/**
 * The subscription stopped echoing probes. `echoes` says whether it ever
 * did: zero means it never delivered anything (a pooler dropping
 * notifications, most likely); more means a socket that died after working.
 */
export class ListenProbeTimeout extends Data.TaggedError("ListenProbeTimeout")<{
  readonly message: string
  readonly echoes: number
}> {}

export interface ProbedOptions<E> {
  /**
   * Run the subscription, handing every payload to `onPayload`. Returns when
   * the stream ends, fails when it fails.
   */
  readonly subscribe: (onPayload: (payload: string) => void) => Effect.Effect<void, E>
  /** Deliver a payload that is not a probe. */
  readonly onWake: (payload: string) => void
  /** NOTIFY `PROBE_PAYLOAD` on the wake channel, from another connection. */
  readonly ping: Effect.Effect<void, unknown>
  /** Milliseconds between probes. */
  readonly intervalMs: number
  /** Milliseconds a probe may take to echo before the subscription is declared dead. */
  readonly timeoutMs: number
  /**
   * Wall-clock milliseconds, default `Date.now`. A miss only counts when this
   * clock agrees the timeout elapsed. The Effect Clock schedules the probe,
   * and under TestClock a day passes in a microsecond, but the wire does not
   * get faster: without this guard the conformance suite would tear down a
   * healthy subscription on every large `TestClock.adjust`. Storage
   * timestamps still come from the Effect Clock alone.
   */
  readonly wallClock?: (() => number) | undefined
}

/**
 * Run `subscribe` under the probe: succeeds when the stream ends, fails with
 * the stream's own error or with `ListenProbeTimeout` once a probe goes
 * unanswered. A probe that could not be sent (the pool is down) says nothing
 * about the listen socket and is simply tried again next interval.
 */
export const probed = <E>(options: ProbedOptions<E>): Effect.Effect<void, E | ListenProbeTimeout> =>
  Effect.suspend(() => {
    const wallClock = options.wallClock ?? Date.now
    let echoes = 0
    const subscription = options.subscribe((payload) => {
      if (payload === PROBE_PAYLOAD) {
        echoes += 1
      } else {
        options.onWake(payload)
      }
    })
    const watchdog: Effect.Effect<never, ListenProbeTimeout> = Effect.gen(function*() {
      while (true) {
        yield* Effect.sleep(options.intervalMs)
        const before = echoes
        const sentAt = wallClock()
        const sent = yield* Effect.exit(options.ping)
        if (Exit.isFailure(sent)) continue
        yield* Effect.sleep(options.timeoutMs)
        if (echoes === before && wallClock() - sentAt >= options.timeoutMs) {
          return yield* new ListenProbeTimeout({
            message: `effect-mq: no NOTIFY echo within ${options.timeoutMs}ms; ` +
              (echoes === 0
                ? "the subscription never delivered anything (a pooler that drops notifications?)"
                : "the LISTEN connection is presumed dead"),
            echoes
          })
        }
      }
    })
    return Effect.raceFirst(subscription, watchdog)
  })
