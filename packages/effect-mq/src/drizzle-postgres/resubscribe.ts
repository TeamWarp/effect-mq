/**
 * The wake-channel resubscribe loop, factored out of `DrizzleJobStore` so it
 * can be driven under `TestClock` without a database.
 *
 * Internal: `drizzle-postgres/index.ts` deliberately does not re-export this
 * module, so nothing here is part of the package's public surface.
 *
 * @since 0.7.1
 */
import { Cause, Effect, Exit, Option, Schedule } from "effect"

/**
 * Resubscribe backoff: 1s, 2s, 4s, 8s, 16s, then a 30 second cap. Same
 * `Schedule.min([exponential, spaced])` shape as `Worker`'s store retry
 * policy.
 */
const retryPolicy = Schedule.min([
  Schedule.exponential("1 second", 2),
  Schedule.spaced("30 seconds")
])

/**
 * Run `subscribe` forever, backing off while consecutive attempts fail.
 *
 * Only the failure that opens a streak logs at warning level. That one is the
 * actionable event — wake-ups have degraded to the worker's `pollInterval` —
 * and the ten-thousandth is not. On a pooled endpoint whose pooler rejects
 * `LISTEN` (PgBouncer in transaction mode cannot pin a session to a
 * connection, and a pooled Neon endpoint is exactly that) every attempt fails
 * identically for the life of the process, which is what turns a flat
 * one-second warn loop into a real log bill. A pooler that accepts `LISTEN`
 * and silently drops notifications never reaches the failure branch at all:
 * the subscription sits open delivering nothing, and the queue runs on
 * `pollInterval`.
 *
 * The streak resets when an attempt does not fail, or when a failure is one
 * `wasHealthy` accepts — never on how long an attempt survived. With
 * `@effect/sql-pg`'s `listen`, a failure the stream raises on its own is a
 * *setup* failure: the stream acquires a connection and issues `LISTEN`
 * inside its setup effect, and once that returns nothing can fail or end the
 * queue — the listen client's upstream `error` handler is empty, so an
 * established subscription simply runs until its scope closes. Uptime
 * therefore says nothing about health: a thirty-second attempt is a slow
 * connect that failed, not a subscription that lived. Resetting on it would
 * hand a hung endpoint a full-speed retry and a fresh warning every time,
 * which is the exact flood this loop exists to stop.
 *
 * The liveness probe (`listenProbe.ts`) is the one thing that fails an
 * established subscription on purpose, and it knows whether that
 * subscription ever delivered anything. `wasHealthy` lets the caller say so:
 * a failure it accepts ends a subscription that had been working, so it is
 * the first failure of a new streak — it warns and retries at a second,
 * whatever came before.
 */
export const resubscribeForever = <E>(
  subscribe: Effect.Effect<void, E>,
  options?: { readonly wasHealthy?: ((error: E) => boolean) | undefined }
): Effect.Effect<never> =>
  Effect.gen(function*() {
    let backoff = yield* Schedule.toStepWithSleep(retryPolicy)
    let failures = 0
    while (true) {
      const exit = yield* Effect.exit(subscribe)
      if (Exit.isFailure(exit)) {
        if (options?.wasHealthy !== undefined) {
          const error = Cause.findErrorOption(exit.cause)
          if (Option.isSome(error) && options.wasHealthy(error.value)) {
            failures = 0
            backoff = yield* Schedule.toStepWithSleep(retryPolicy)
          }
        }
        yield* failures === 0
          ? Effect.logWarning(
            "effect-mq: LISTEN subscription failed; wake-ups degraded to polling until resubscribe",
            exit.cause
          )
          : Effect.logDebug(
            "effect-mq: LISTEN resubscribe failed; wake-ups still degraded to polling",
            exit.cause
          )
        failures += 1
      } else {
        // Not a failure, so not part of a streak: the next attempt starts
        // over at a second, and warns again if it fails.
        failures = 0
        backoff = yield* Schedule.toStepWithSleep(retryPolicy)
      }
      // The policy recurs forever, so the exhausted branch is unreachable.
      yield* backoff(void 0).pipe(
        Effect.catch(() => Effect.die("effect-mq: LISTEN retry schedule exhausted"))
      )
    }
  })
