/**
 * The worker runtime of effect-mq.
 *
 * `Worker` is a service that runs registered job handlers against a
 * `JobStore`. Handlers are registered through `Job.toLayer(handler)`; the
 * worker starts a set of taker fibers per queue (bounded by the queue's
 * concurrency), each running the loop:
 *
 *   claim -> decode payload -> run handler -> ack (complete | retry | fail)
 *
 * plus two maintenance fibers: lock renewal for in-flight jobs, and stalled
 * job recovery. On scope close, in-flight handlers are interrupted and their
 * jobs released back to `waiting` without consuming an attempt.
 *
 * @since 0.1.0
 */
import { Cause, Clock, Context, Deferred, Duration, Effect, Exit, Fiber, FiberSet, Layer, Option, Result, Schedule, Schema, type Scope, Scope as Scope_, Tracer } from "effect"
import {
  type AckOutcome,
  type BackoffPolicy,
  isJobStoreError,
  isMarkedUnrecoverable,
  JobId,
  type JobNotFoundError,
  type JobRecord,
  JobStore,
  type JobStoreError,
  type LockLostError,
  nextOccurrence,
  QueueName,
  type ScheduleRecord,
  type Service as StoreService
} from "./JobStore.ts"

/**
 * Information about the currently running attempt, passed to handlers as the
 * second argument.
 *
 * @since 0.1.0
 */
export interface JobContext {
  readonly jobId: JobId
  readonly name: string
  readonly queue: QueueName
  /** 1-based attempt number. */
  readonly attempt: number
  /** Total attempts allowed for this job. */
  readonly attemptsMax: number
}

/**
 * The structural shape of a job definition the worker needs for
 * registration. `Job.Job` satisfies this — the indirection avoids a module
 * cycle between `Job` and `Worker`.
 *
 * @since 0.1.0
 */
export interface JobDescriptor<
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top
> {
  readonly _tag: string
  readonly queue: QueueName
  /** The store this job is bound to; must match the worker's store. */
  readonly store: Context.Key<any, StoreService>
  readonly payloadSchema: Payload
  /** JSON codec for the payload (what is actually stored). */
  readonly payloadJsonSchema: Schema.Top & {
    readonly Type: Payload["Type"]
  }
  /** JSON codec for handler exits (what is actually stored). */
  readonly exitSchema: Schema.Top & {
    readonly Type: Exit.Exit<Success["Type"], Error["Type"]>
  }
  /** When present and returning false for a typed error, retries are skipped. */
  readonly retryable: ((error: Error["Type"]) => boolean) | undefined
}

/**
 * @since 0.1.0
 */
export interface RegisterOptions {
  /**
   * Taker fibers for this job's queue. The first registration for a queue
   * decides; later values for the same queue are ignored.
   */
  readonly concurrency?: number | undefined
  /** Override the queue this handler consumes (defaults to the job's queue). */
  readonly queue?: string | undefined
}

/**
 * @since 0.1.0
 */
export interface WorkerOptions<StoreId = JobStore> {
  /**
   * The store this worker claims from (a `JobStore.named(...)` key). Default:
   * the default `JobStore`. To run workers for several stores in one process,
   * provide each `Worker.layer({ store })` *locally* to its handler group via
   * `Layer.provide` (not `provideMerge`).
   */
  readonly store?: Context.Key<StoreId, StoreService> | undefined
  /** Default taker fibers per queue (default 1). */
  readonly concurrency?: number | undefined
  /** Per-queue configuration, keyed by queue name. */
  readonly queues?: Readonly<Record<string, { readonly concurrency?: number | undefined }>> | undefined
  /** How long a claim's lock lasts before the job counts as stalled (default 30s). */
  readonly lockDuration?: Duration.Input | undefined
  /** Lock heartbeat interval (default half of `lockDuration`). */
  readonly lockRenewInterval?: Duration.Input | undefined
  /** How often to sweep for stalled jobs (default 30s). */
  readonly stalledInterval?: Duration.Input | undefined
  /** Stalls tolerated before a job is failed outright (default 1). */
  readonly maxStalledCount?: number | undefined
  /** Fallback polling interval when idle and no wake-up arrives (default 5s). */
  readonly pollInterval?: Duration.Input | undefined
  /** How often to tick due repeatable-job schedules (default 15s). */
  readonly scheduleSweepInterval?: Duration.Input | undefined
  /** Identifier used in lock tokens (default: random). */
  readonly id?: string | undefined
}

/**
 * @since 0.1.0
 */
export class Worker extends Context.Service<Worker, {
  readonly register: <
    Payload extends Schema.Top,
    Success extends Schema.Top,
    Error extends Schema.Top,
    R
  >(
    job: JobDescriptor<Payload, Success, Error>,
    handler: (
      payload: Payload["Type"],
      context: JobContext
    ) => Effect.Effect<Success["Type"], Error["Type"], R>,
    options?: RegisterOptions | undefined
  ) => Effect.Effect<
    void,
    never,
    | Scope.Scope
    | R
    | Payload["DecodingServices"]
    | Success["EncodingServices"]
    | Error["EncodingServices"]
  >
}>()("effect-mq/Worker") {}

interface HandlerEntry {
  readonly run: (
    payload: JobRecord["payload"],
    context: JobContext
  ) => Effect.Effect<unknown, unknown>
  readonly encodeExit: (
    exit: Exit.Exit<unknown, unknown>
  ) => Effect.Effect<JobRecord["exit"], unknown>
  readonly unrecoverableFailure:
    | ((cause: Cause.Cause<unknown>) => boolean)
    | undefined
}

// A cause is unrecoverable when its error or defect was marked via
// `Job.unrecoverable` (identity-based, so typed channels stay untouched).
const causeIsMarkedUnrecoverable = (cause: Cause.Cause<unknown>): boolean => {
  const failure = Cause.findErrorOption(cause)
  if (Option.isSome(failure) && isMarkedUnrecoverable(failure.value)) return true
  const die = Cause.findDie(cause)
  return Result.isSuccess(die) && isMarkedUnrecoverable(die.success.defect)
}

/**
 * Delay before attempt `attempt` (1-based) re-runs, per the job's policy.
 *
 * @internal
 */
export const backoffDelayMs = (
  backoff: BackoffPolicy | undefined,
  attempt: number
): number => {
  if (backoff === undefined) return 0
  switch (backoff._tag) {
    case "fixed":
      return backoff.delayMs
    case "exponential":
      return Math.round(backoff.delayMs * (backoff.factor ?? 2) ** (attempt - 1))
  }
}

// Bounded so the "retry, then die/drop" paths are actually reachable and a
// persistently-broken driver cannot hang shutdown forever (worst case ~10s).
const storeRetryPolicy = Schedule.min([
  Schedule.exponential(200, 1.5),
  Schedule.spaced("30 seconds")
]).pipe(Schedule.upTo({ times: 8 }))

type Restore = <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>

/**
 * Build the worker service. Requires a `Scope` (fibers live in it) and the
 * `JobStore`.
 *
 * @since 0.1.0
 */
export const make = <StoreId = JobStore>(
  options?: WorkerOptions<StoreId> | undefined
): Effect.Effect<Worker["Service"], never, Scope.Scope | StoreId> =>
  Effect.gen(function*() {
    // Taker fibers are forked from whichever registration arrives first; pin
    // them to the worker's own context so they never inherit one job's
    // locally provided services.
    const workerContext = yield* Effect.context<never>()
    // SAFETY: when `options.store` is omitted the public signature fixes
    // `StoreId` to its default `JobStore`, so the default key is the right
    // `Context.Key<StoreId>`; when it is present the cast is an identity.
    const storeKey = (options?.store ?? JobStore) as Context.Key<StoreId, StoreService>
    const store: StoreService = yield* storeKey
    const fibers = yield* FiberSet.make()

    const lockDurationMs = Duration.toMillis(options?.lockDuration ?? 30_000)
    const lockRenewMs = options?.lockRenewInterval !== undefined
      ? Duration.toMillis(options.lockRenewInterval)
      : Math.max(1, Math.floor(lockDurationMs / 2))
    const stalledMs = Duration.toMillis(options?.stalledInterval ?? 30_000)
    const scheduleSweepMs = Duration.toMillis(options?.scheduleSweepInterval ?? 15_000)
    const maxStalledCount = options?.maxStalledCount ?? 1
    const pollMs = Duration.toMillis(options?.pollInterval ?? 5_000)
    const workerId = options?.id ?? `worker-${Math.random().toString(36).slice(2, 10)}`

    const handlers = new Map<string, HandlerEntry>()
    const queueNames = new Map<QueueName, Set<string>>()
    const startedQueues = new Set<QueueName>()
    const inflight = new Map<
      JobId,
      { readonly id: JobId; readonly token: string; readonly fiber: Fiber.Fiber<unknown, unknown> }
    >()
    // Jobs this worker is interrupting because of a cancel request; their
    // interrupt-only exits ack as Cancelled instead of shutdown-release.
    const cancelling = new Set<JobId>()

    let tokenCounter = 0
    const nextToken = () => `${workerId}:${++tokenCounter}`

    // Local wake-up for registration changes. Versioned like the store's
    // wakeToken so a pulse firing between "observe" and "await" is not lost.
    let pulseVersion = 0
    let pulse = Deferred.makeUnsafe<void>()
    const firePulse = Effect.suspend(() => {
      pulseVersion += 1
      const current = pulse
      pulse = Deferred.makeUnsafe<void>()
      return Deferred.succeed(current, void 0)
    })
    const awaitPulse = (observed: number) =>
      Effect.suspend(() =>
        pulseVersion > observed ? Effect.void : Deferred.await(pulse)
      )

    const retryStore = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
      effect.pipe(
        Effect.retry({
          // Classified by tag (not instanceof) so errors from a duplicated
          // module instance are still recognized.
          while: (error) => isJobStoreError(error),
          schedule: storeRetryPolicy
        }),
        Effect.orDie
      )

    // Ack-path safety: lost locks and vanished jobs mean another worker owns
    // the job now — log and move on. Driver errors retry, then are dropped.
    const ackSafely = (
      effect: Effect.Effect<
        void,
        JobStoreError | JobNotFoundError | LockLostError
      >,
      what: string
    ) =>
      effect.pipe(
        Effect.retry({
          while: (error) => isJobStoreError(error),
          schedule: storeRetryPolicy
        }),
        Effect.catch((error) =>
          Effect.logWarning(`effect-mq: ${what} dropped (${error._tag})`, error)
        )
      )

    const routeFailure = (record: JobRecord, exit: JobRecord["exit"]): AckOutcome => {
      const attempt = record.attemptsMade + 1
      if (attempt >= record.attemptsMax) {
        return { _tag: "Fail", exit }
      }
      return { _tag: "Retry", delayMs: backoffDelayMs(record.backoff, attempt), exit }
    }

    // Runs inside the taker's uninterruptible region; `restore` re-enables
    // interruption only around the handler itself.
    const processJob = (record: JobRecord, token: string, restore: Restore) =>
      Effect.suspend(() => {
        const entry = handlers.get(record.name)
        if (entry === undefined) {
          // Unregistered between claim and processing — hand the job back.
          return ackSafely(store.release(record.id, token), "release")
        }
        const context: JobContext = {
          jobId: record.id,
          name: record.name,
          queue: record.queue,
          attempt: record.attemptsMade + 1,
          attemptsMax: record.attemptsMax
        }
        return Effect.gen(function*() {
          // The per-run time limit interrupts the handler internally; surface
          // it as a defect so it flows through normal retry accounting.
          // timeoutOrElse (not timeout + a catch on TimeoutError) so a
          // handler's OWN typed TimeoutError failure stays a typed failure.
          const handlerEffect = record.timeoutMs === undefined
            ? entry.run(record.payload, context)
            : entry.run(record.payload, context).pipe(
              Effect.timeoutOrElse({
                duration: record.timeoutMs,
                orElse: () =>
                  Effect.die(
                    new Cause.TimeoutError(
                      `effect-mq: job "${record.name}" timed out after ${record.timeoutMs}ms`
                    )
                  )
              })
            )
          // The handler runs in a child fiber so a cross-process cancel can
          // interrupt THIS job without killing the taker loop; `restore`
          // keeps the child interruptible while the fork itself is masked.
          const fiber = yield* Effect.forkChild(restore(handlerEffect))
          inflight.set(record.id, { id: record.id, token, fiber })
          const exit = yield* Effect.exit(restore(Fiber.join(fiber)))
          inflight.delete(record.id)
          const wasCancelled = cancelling.delete(record.id)

          // A cancel-request interrupt is terminal: ack Cancelled (even if a
          // shutdown races it — cancellation wins, the job must not revive).
          if (wasCancelled && Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)) {
            yield* ackSafely(store.ack(record.id, token, { _tag: "Cancelled" }), "ack")
            return
          }

          // Distinguish worker shutdown from a handler that interrupted
          // itself: entering an interruptible region while an external
          // interrupt is pending fails immediately.
          const shutdown = yield* Effect.exit(restore(Effect.void))
          if (Exit.isFailure(shutdown)) {
            // Worker shutdown: stop the handler (the join above may have been
            // interrupted before the child finished), then give the job back
            // without consuming an attempt.
            yield* Fiber.interrupt(fiber)
            yield* ackSafely(store.release(record.id, token), "release")
            return yield* Effect.interrupt
          }

          // A handler that interrupted itself is a failed attempt, not a
          // shutdown — otherwise the job would hot-loop forever.
          const effective: Exit.Exit<unknown, unknown> =
            Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)
              ? Exit.die(
                new Error(`effect-mq: handler for job "${record.name}" interrupted itself`)
              )
              : exit

          // Never let an encode defect escape: the job would be stuck active.
          let encoded = yield* Effect.exit(entry.encodeExit(effective))
          let encodable = true
          if (Exit.isFailure(encoded)) {
            encodable = false
            yield* Effect.logError(
              `effect-mq: failed to encode handler exit for job "${record.name}"`,
              encoded.cause
            )
            encoded = yield* Effect.exit(entry.encodeExit(Exit.die(
              new Error(`effect-mq: failed to encode handler exit for job "${record.name}"`)
            )))
          }
          const exitValue = Exit.isSuccess(encoded) ? encoded.value : undefined

          const unrecoverable = Exit.isFailure(effective) && (
            causeIsMarkedUnrecoverable(effective.cause) ||
            entry.unrecoverableFailure?.(effective.cause) === true
          )
          const outcome: AckOutcome = Exit.isSuccess(effective)
            ? encodable
              // A success whose value can't be encoded must NOT re-run (its
              // side effects already happened) — fail it with the defect.
              ? { _tag: "Complete", exit: exitValue }
              : { _tag: "Fail", exit: exitValue }
            : unrecoverable
            ? { _tag: "Fail", exit: exitValue }
            : routeFailure(record, exitValue)
          yield* ackSafely(store.ack(record.id, token, outcome), "ack")
        })
      })

    // One loop iteration. The claim and everything after it run
    // uninterruptibly so shutdown can never orphan a just-claimed job; only
    // the idle waits and the handler run are interruptible.
    const takerIteration = (queue: QueueName) =>
      Effect.uninterruptibleMask((restore) =>
        Effect.gen(function*() {
          const observedPulse = pulseVersion
          const names = queueNames.get(queue)
          if (names === undefined || names.size === 0) {
            return yield* restore(
              Effect.race(awaitPulse(observedPulse), Effect.sleep(pollMs))
            )
          }
          const token = nextToken()
          const result = yield* retryStore(store.claim({
            queue,
            names: Array.from(names),
            token,
            lockDurationMs
          }))
          if (result._tag === "Empty") {
            const now = yield* Clock.currentTimeMillis
            const timeout = result.nextRunAt !== undefined
              ? Math.max(0, Math.min(result.nextRunAt - now, pollMs))
              : pollMs
            return yield* restore(Effect.raceAll([
              retryStore(store.awaitWake([queue], result.wakeToken)),
              Effect.sleep(timeout),
              awaitPulse(observedPulse)
            ]))
          }
          yield* processJob(result.job, token, restore)
        })
      )

    const takerLoop = (queue: QueueName) =>
      takerIteration(queue).pipe(
        Effect.catchCause((cause) =>
          Effect.logError(`effect-mq: worker iteration failed (queue "${queue}")`, cause)
        ),
        Effect.forever
      )

    const queueConcurrency = (queue: QueueName, registered: number | undefined) =>
      Math.max(
        1,
        options?.queues?.[queue]?.concurrency ??
          registered ??
          options?.concurrency ??
          1
      )

    const ensureQueueLoop = (queue: QueueName, registered: number | undefined) =>
      Effect.suspend(() => {
        if (startedQueues.has(queue)) return Effect.void
        startedQueues.add(queue)
        const takers = queueConcurrency(queue, registered)
        const loop = takerLoop(queue).pipe(
          Effect.updateContext(() => workerContext)
        )
        return Effect.forEach(
          Array.from({ length: takers }, (_, i) => i),
          () => FiberSet.run(fibers, loop),
          { discard: true }
        )
      })

    const renewalLoop = Effect.gen(function*() {
      yield* Effect.sleep(lockRenewMs)
      const entries = Array.from(inflight.values())
      if (entries.length === 0) return
      const result = yield* retryStore(store.extendLocks(
        entries.map((entry) => ({ id: entry.id, token: entry.token })),
        lockDurationMs
      ))
      if (result.lost.length > 0) {
        yield* Effect.logWarning(
          "effect-mq: failed to renew locks; jobs may run twice",
          result.lost
        )
      }
      // Honour cross-process cancel requests: interrupt the handler fiber;
      // processJob acks the job as Cancelled.
      for (const id of result.cancelRequested) {
        const flight = inflight.get(id)
        if (flight !== undefined) {
          cancelling.add(id)
          // Delivery only — awaiting the handler's exit here would park the
          // heartbeat behind arbitrary user finalizers and starve every other
          // in-flight job's lock renewal. processJob's join observes the exit
          // and acks Cancelled.
          yield* Effect.sync(() => flight.fiber.interruptUnsafe())
        }
      }
    }).pipe(
      Effect.catchCause((cause) => Effect.logError("effect-mq: lock renewal failed", cause)),
      Effect.forever
    )

    const stalledLoop = Effect.gen(function*() {
      yield* Effect.sleep(stalledMs)
      const recovered = yield* retryStore(store.recoverStalled({ maxStalledCount }))
      if (recovered.length > 0) {
        yield* Effect.logWarning("effect-mq: recovered stalled jobs", recovered)
      }
    }).pipe(
      Effect.catchCause((cause) => Effect.logError("effect-mq: stalled sweep failed", cause)),
      Effect.forever
    )

    // Tick one due repeatable-job schedule. The enqueue id is deterministic
    // per (schedule, slot), so concurrent sweepers across workers dedup
    // naturally; advanceSchedule is conditional, so double-advances are
    // no-ops.
    const sweepSchedule = (schedule: ScheduleRecord) =>
      Effect.gen(function*() {
        const now = yield* Clock.currentTimeMillis
        const slot = schedule.nextRunAt
        const next = nextOccurrence(schedule, slot, now)
        if (next === undefined) {
          return yield* Effect.logError(
            `effect-mq: schedule "${schedule.key}" has an invalid cron/every configuration; skipping`
          )
        }
        yield* retryStore(store.enqueue({
          id: JobId(`sched/${schedule.key}/${slot}`),
          name: schedule.jobName,
          queue: schedule.queue,
          payload: schedule.payload,
          metadata: { ...schedule.metadata, scheduledFor: new Date(slot).toISOString() },
          priority: schedule.priority,
          attemptsMax: schedule.attemptsMax,
          backoff: schedule.backoff,
          keep: schedule.keep,
          timeoutMs: schedule.timeoutMs,
          delayMs: 0
        }))
        yield* retryStore(store.advanceSchedule(schedule.key, slot, next))
      })

    // Each due schedule is swept in isolation so one poison row (bad cron,
    // storage error) can never starve the rest of the sweep.
    const scheduleLoop = Effect.gen(function*() {
      yield* Effect.sleep(scheduleSweepMs)
      const due = yield* retryStore(store.dueSchedules())
      for (const schedule of due) {
        yield* sweepSchedule(schedule).pipe(
          Effect.catchCause((cause) =>
            Effect.logError(`effect-mq: sweep of schedule "${schedule.key}" failed`, cause)
          )
        )
      }
    }).pipe(
      Effect.catchCause((cause) => Effect.logError("effect-mq: schedule sweep failed", cause)),
      Effect.forever
    )

    yield* FiberSet.run(fibers, renewalLoop)
    yield* FiberSet.run(fibers, stalledLoop)
    yield* FiberSet.run(fibers, scheduleLoop)

    // SAFETY: the public `register` signature declares Scope, the handler's R
    // and the codec services as requirements; the implementation erases them
    // (the trailing assertion below) because the handler runs with the
    // context captured at registration time via `provideCaptured`.
    return Worker.of({
      register: (job, handler, registerOptions) =>
        Effect.gen(function*() {
          const name = job._tag
          if (handlers.has(name)) {
            return yield* Effect.die(
              new Error(`effect-mq: duplicate handler registered for job "${name}"`)
            )
          }
          if (job.store.key !== storeKey.key) {
            return yield* Effect.die(
              new Error(
                `effect-mq: job "${name}" is bound to store "${job.store.key}" but this worker claims from "${storeKey.key}". ` +
                  `Provide a Worker.layer({ store }) for the job's store (use Layer.provide locally to run several workers in one process).`
              )
            )
          }
          // Everything the handler and codecs require was provided to the
          // registration layer; capture it, minus runtime-ambient keys that
          // must always come from the executing fiber.
          const services = (yield* Effect.context<never>()).pipe(
            Context.omit(Scope_.Scope, Tracer.ParentSpan)
          )
          const decodePayload = Schema.decodeUnknownEffect(job.payloadJsonSchema)
          const encodeExit = Schema.encodeEffect(job.exitSchema)
          // SAFETY: per `register`'s public signature the captured context
          // contains the handler's requirements; merging it OVER the runtime
          // context (captured wins on conflicts, so locally provided services
          // are not shadowed by the worker's) restores those requirements.
          const provideCaptured = <A, E>(effect: Effect.Effect<A, E, unknown>): Effect.Effect<A, E> =>
            effect.pipe(
              Effect.updateContext((input) =>
                Context.merge(input, services) as Context.Context<unknown>
              )
            ) as Effect.Effect<A, E>
          const retryable = job.retryable
          const entry: HandlerEntry = {
            run: (payload, context) =>
              provideCaptured(
                decodePayload(payload).pipe(
                  Effect.orDie,
                  Effect.flatMap((decoded) => handler(decoded, context))
                )
              ),
            encodeExit: (exit) => provideCaptured(encodeExit(exit)),
            unrecoverableFailure: retryable === undefined ? undefined : (cause) => {
              const failure = Cause.findErrorOption(cause)
              if (Option.isNone(failure)) return false
              try {
                // SAFETY: the only typed failures a handler can produce are
                // its declared error type, which is what `retryable` accepts.
                return !retryable(failure.value as Parameters<typeof retryable>[0])
              } catch {
                // A throwing predicate must never leave the job un-acked:
                // treat the failure as retryable and let the budget decide.
                return false
              }
            }
          }
          handlers.set(name, entry)
          const queue = registerOptions?.queue !== undefined
            ? QueueName(registerOptions.queue)
            : job.queue
          let names = queueNames.get(queue)
          if (names === undefined) {
            names = new Set()
            queueNames.set(queue, names)
          }
          names.add(name)
          yield* Effect.addFinalizer(() =>
            Effect.sync(() => {
              handlers.delete(name)
              queueNames.get(queue)?.delete(name)
            })
          )
          yield* ensureQueueLoop(queue, registerOptions?.concurrency)
          yield* firePulse
        }) as Effect.Effect<void, never, never>
    })
  })

/**
 * Run a worker as a layer. Provide handler layers (`Job.toLayer(...)`) on top
 * of this, and a `JobStore` below it.
 *
 * @since 0.1.0
 */
export const layer = <StoreId = JobStore>(
  options?: WorkerOptions<StoreId> | undefined
): Layer.Layer<Worker, never, StoreId> => Layer.effect(Worker, make(options))
