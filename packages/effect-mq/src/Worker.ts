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
 * plus maintenance fibers: lock renewal for in-flight jobs, stalled job
 * recovery, schedule sweeping — and, once flows are registered, the outbox
 * relay and flow sweeper. On scope close, in-flight handlers are interrupted
 * and their jobs released back to `waiting` without consuming an attempt.
 *
 * @since 0.1.0
 */
import { Cause, Clock, Context, Deferred, Duration, Effect, Exit, Fiber, FiberSet, Layer, Metric, Option, Result, Schedule, Schema, type Scope, Scope as Scope_, Tracer } from "effect"
import {
  type AckOutcome,
  type BackoffPolicy,
  type EnqueueRequest,
  type FlowChildSpec,
  type FlowState,
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
import * as Metrics from "./Metrics.ts"

/**
 * Information about the currently running attempt. Handlers (and anything
 * they call, however deeply nested) read it from the `CurrentJob` service.
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
 * The currently running attempt, provided by the worker around every
 * handler run (plain jobs and both flow phases). Declaring it as a
 * requirement makes "this code only runs inside a job" a compile-time
 * fact — `toLayer` subtracts it, since the worker supplies it per run:
 *
 * ```ts
 * const notify = Effect.gen(function*() {
 *   const { jobId, attempt } = yield* Worker.CurrentJob
 *   // ... use them however deep in the call graph this runs
 * })
 * ```
 *
 * Outside a worker (unit tests calling such code directly), provide a
 * value with `Effect.provideService(Worker.CurrentJob, {...})`.
 *
 * @since 0.6.0
 */
export class CurrentJob extends Context.Service<CurrentJob, JobContext>()(
  "effect-mq/Worker/CurrentJob"
) {}

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
 * The structural shape of a flow definition the worker needs — for running
 * flow CHILDREN (the outbox relay routes results by parent store) and, via
 * `FlowDescriptor`, for running the parent's two phases. `Flow.Flow`
 * satisfies this; the indirection avoids a module cycle.
 *
 * @since 0.6.0
 */
export interface FlowAny {
  /** The flow's unique name (what child `parent` envelopes reference). */
  readonly name: string
  readonly parent: {
    readonly store: Context.Key<any, StoreService>
  }
  /** True when the flow's `onChildFailure` policy is `"fail"`. */
  readonly failFast: boolean
}

/**
 * What `Flow.toLayer` hands to `registerFlow`: the parent job's codecs plus
 * the two phase runners, pre-wired by the flow module (fan-out builds
 * complete `FlowChildSpec`s — deterministic ids, `parent` envelopes, trace
 * stamps — and collect consumes the recorded dependency rows).
 *
 * @internal
 */
export interface FlowDescriptor extends FlowAny {
  readonly parent: {
    readonly _tag: string
    readonly queue: QueueName
    readonly store: Context.Key<any, StoreService>
    readonly payloadJsonSchema: Schema.Top
    readonly exitSchema: Schema.Top
    readonly retryable: ((error: never) => boolean) | undefined
  }
  /** Every child store the sweeper/enqueuer must reach, for context lookup. */
  readonly childStores: ReadonlyArray<Context.Key<any, StoreService>>
  readonly fanOut: (
    payload: JobRecord["payload"],
    context: JobContext,
    /** The parent's own nesting depth (0 for top-level flows). */
    parentDepth: number
  ) => Effect.Effect<ReadonlyArray<FlowChildSpec>, unknown, unknown>
  readonly collect: (
    payload: JobRecord["payload"],
    flowState: FlowState,
    context: JobContext
  ) => Effect.Effect<unknown, unknown, unknown>
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
  /**
   * When set, sample `store.counts(queue)` for every registered queue into
   * the `effect_mq_queue_depth` gauge at this cadence (default: off — depth
   * sampling costs one store query per queue per tick).
   */
  readonly queueMetricsInterval?: Duration.Input | undefined
  /**
   * Name for the span wrapping each handler run (default
   * `` `${context.name}.run` ``). The span carries `effectMqJobId`,
   * `effectMqQueue`, and `effectMqAttempt` attributes and — when the
   * producer enqueued inside a span — joins the producing trace as its
   * child.
   */
  readonly handlerSpanName?: ((context: JobContext) => string) | undefined
  /**
   * How the handler span attaches to the producer's persisted trace:
   * - `"auto"` (default): immediate enqueues CONTINUE the producer trace
   *   (parent-child); explicitly delayed/`at`-scheduled ones start their own
   *   trace with a causal LINK back (long-delayed parent-child traces render
   *   badly and defeat tail sampling).
   * - `"parent"` / `"link"`: force one mode for every job.
   * - `"none"`: spans and attributes only, no cross-trace edge.
   */
  readonly traceLinking?: "auto" | "parent" | "link" | "none" | undefined
  /**
   * Called after a failed run is acked — both retryable attempts and
   * terminal failures (`JobFailure.willRetry` tells them apart), including
   * jobs failed by stall exhaustion. Runs isolated on the worker fiber: a
   * failing hook is logged and never disturbs job processing. The worker
   * also logs failures itself (`logWarning` for retries, `logError` for
   * terminal failures), so the hook is for custom reporting (error
   * trackers, paging), not a logging prerequisite.
   */
  readonly onJobFailure?: ((failure: JobFailure) => Effect.Effect<void>) | undefined
  /**
   * Flows whose CHILD jobs this worker runs. Child results are written to
   * the child store's outbox atomically with each terminal transition; this
   * list gives the worker the flows' parent stores (each parent StoreId
   * lands in the layer's requirements), so its relay can push those results
   * immediately. Without it the results wait for a relay elsewhere or for
   * the parent-side sweeper's reconciliation — correct either way, just
   * slower. Workers running a flow's PARENT phases get their registration
   * implicitly from `Flow.toLayer`.
   */
  readonly flows?: ReadonlyArray<FlowAny> | undefined
  /**
   * Cadence of the flow sweeper (default 30s), which repairs whatever the
   * push path missed: (re-)enqueues fanned-out children that never landed
   * in their store, synthesizes reports for children that reached a
   * terminal state without a delivered report, and cascades cancels after
   * a flow settles. Also the age a dependency row must reach before it is
   * reconciled, and the outbox relay's fallback drain cadence (the relay
   * normally drains the moment a child result is acked). The sweeper runs
   * only on workers that registered a flow via `Flow.toLayer`; the relay
   * runs on any worker with flow registrations of either kind.
   */
  readonly flowSweepInterval?: Duration.Input | undefined
  /** Identifier used in lock tokens (default: random). */
  readonly id?: string | undefined
}

/**
 * What `Worker.layer({ onJobFailure })` receives after a failed run is
 * acked: job identity, attempt accounting, whether the store will retry,
 * and the failure cause.
 *
 * @since 0.4.2
 */
export interface JobFailure {
  readonly jobId: JobId
  readonly name: string
  readonly queue: QueueName
  /** The attempt that failed (1-based). */
  readonly attempt: number
  readonly attemptsMax: number
  /** True when the store will re-run the job after its backoff. */
  readonly willRetry: boolean
  readonly cause: Cause.Cause<unknown>
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
      payload: Payload["Type"]
    ) => Effect.Effect<Success["Type"], Error["Type"], R>,
    options?: RegisterOptions | undefined
  ) => Effect.Effect<
    void,
    never,
    | Scope.Scope
    // The worker provides CurrentJob around every run.
    | Exclude<R, CurrentJob>
    | Payload["DecodingServices"]
    | Success["EncodingServices"]
    | Error["EncodingServices"]
  >
  /**
   * Register a flow's parent phases (`fanOut`/`collect`). Called by
   * `Flow.toLayer`, which declares the real requirements (parent + child
   * stores, handler R, codec services) on its own signature.
   *
   * @internal
   */
  readonly registerFlow: (
    flow: FlowDescriptor,
    options?: RegisterOptions | undefined
  ) => Effect.Effect<void, never, Scope.Scope>
}>()("effect-mq/Worker") {}

interface HandlerEntry {
  /** For flow parents this is the `collect` phase (`flowState` carries its tallies). */
  readonly run: (
    payload: JobRecord["payload"],
    context: JobContext,
    flowState: FlowState | undefined
  ) => Effect.Effect<unknown, unknown>
  readonly encodeExit: (
    exit: Exit.Exit<unknown, unknown>
  ) => Effect.Effect<JobRecord["exit"], unknown>
  readonly unrecoverableFailure:
    | ((cause: Cause.Cause<unknown>) => boolean)
    | undefined
  /** Present for flow parents: the fan-out phase and its plumbing. */
  readonly flow: {
    readonly flowName: string
    readonly failFast: boolean
    readonly fanOut: (
      payload: JobRecord["payload"],
      context: JobContext,
      parentDepth: number
    ) => Effect.Effect<ReadonlyArray<FlowChildSpec>, unknown>
    readonly enqueueChildren: (
      children: ReadonlyArray<FlowChildSpec>
    ) => Effect.Effect<void>
  } | undefined
}

// A `retryable` predicate lifted to a cause classifier (false ⇒ skip the
// remaining retry budget).
const toUnrecoverableFailure = (
  retryable: ((error: never) => boolean) | undefined
): ((cause: Cause.Cause<unknown>) => boolean) | undefined =>
  retryable === undefined ? undefined : (cause) => {
    const failure = Cause.findErrorOption(cause)
    if (Option.isNone(failure)) return false
    try {
      // SAFETY: the only typed failures a handler can produce are its
      // declared error type, which is what `retryable` accepts.
      return !retryable(failure.value as never)
    } catch {
      // A throwing predicate must never leave the job un-acked: treat the
      // failure as retryable and let the budget decide.
      return false
    }
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
 * The parent StoreIds of the flows passed via `Worker.layer({ flows })` —
 * a naked type parameter so the union of flows distributes.
 *
 * @since 0.6.0
 */
export type FlowParentStores<F> = F extends {
  readonly parent: { readonly store: Context.Key<infer Id, StoreService> }
} ? Id
  : never

/**
 * Build the worker service. Requires a `Scope` (fibers live in it), the
 * `JobStore`, and the parent store of every flow in `flows`.
 *
 * @since 0.1.0
 */
export const make = <StoreId = JobStore, const Flows extends ReadonlyArray<FlowAny> = ReadonlyArray<never>>(
  options?: (WorkerOptions<StoreId> & { readonly flows?: Flows | undefined }) | undefined
): Effect.Effect<
  Worker["Service"],
  never,
  Scope.Scope | StoreId | FlowParentStores<Flows[number]>
> =>
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
    const flowSweepMs = Duration.toMillis(options?.flowSweepInterval ?? 30_000)
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

    // Flow plumbing. `storesByKey` maps store-key strings to resolved
    // services — this worker's own store, every `flows` entry's parent
    // store, and each registered flow's child stores (a registered flow's
    // parent store IS the worker's own store). The relay
    // routes outbox entries by `parentStoreKey` through it, and the sweeper
    // and post-fan-out enqueue resolve child stores through it.
    // `flowPolicies` carries each known flow's fail-fast bit for settle
    // logging.
    const storesByKey = new Map<string, StoreService>()
    const flowPolicies = new Map<string, { readonly failFast: boolean }>()
    let flowSweeperStarted = false
    let relayStarted = false
    storesByKey.set(storeKey.key, store)
    for (const flow of options?.flows ?? []) {
      storesByKey.set(flow.parent.store.key, yield* flow.parent.store)
      flowPolicies.set(flow.name, { failFast: flow.failFast })
    }

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

    // Like ackSafely, but says whether the ack definitely landed — flow
    // reports and post-fan-out child enqueues must only follow an ack this
    // worker actually won.
    const ackLanded = (
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
        Effect.as(true),
        Effect.catch((error) =>
          Effect.logWarning(`effect-mq: ${what} dropped (${error._tag})`, error).pipe(
            Effect.as(false)
          )
        )
      )

    // Every failed run is logged (warning while retries remain, error once
    // terminal) and handed to the onJobFailure hook. The hook runs isolated:
    // whatever it does, job processing proceeds.
    const reportFailure = (failure: JobFailure, retryDelayMs: number | undefined) =>
      Effect.gen(function*() {
        yield* (failure.willRetry
          ? Effect.logWarning(
            `effect-mq: job "${failure.name}" failed (attempt ${failure.attempt}/${failure.attemptsMax}); ` +
              `retrying in ${retryDelayMs ?? 0}ms`,
            failure.cause
          )
          : Effect.logError(
            `effect-mq: job "${failure.name}" failed terminally ` +
              `(attempt ${failure.attempt}/${failure.attemptsMax})`,
            failure.cause
          )).pipe(Effect.annotateLogs({
            effectMqJobId: failure.jobId,
            effectMqQueue: failure.queue,
            effectMqAttempt: failure.attempt
          }))
        const hook = options?.onJobFailure
        if (hook !== undefined) {
          yield* hook(failure).pipe(
            Effect.catchCause((cause) => Effect.logError("effect-mq: onJobFailure hook failed", cause))
          )
        }
      })

    // The outbox relay. Child results land in this store's outbox
    // atomically with each terminal transition; the relay drains them into
    // their parent stores in batches and deletes what it delivered.
    // Redelivery after a crash is safe (dependency rows dedup), so there
    // are no leases — peek, deliver, delete. A terminal ack nudges the
    // relay so results push immediately; the periodic pass is the fallback.
    let relayPulseVersion = 0
    let relayPulse = Deferred.makeUnsafe<void>()
    const fireRelayPulse = Effect.suspend(() => {
      relayPulseVersion += 1
      const current = relayPulse
      relayPulse = Deferred.makeUnsafe<void>()
      return Deferred.succeed(current, void 0)
    })
    const awaitRelayPulse = (observed: number) =>
      Effect.suspend(() =>
        relayPulseVersion > observed ? Effect.void : Deferred.await(relayPulse)
      )

    const relayDrain = Effect.gen(function*() {
      const pageSize = 500
      // Cursor-walk the whole outbox: `after` pages past everything already
      // seen this drain, so entries this worker cannot route — or could not
      // deliver just now — never blockade the ones behind them. Each drain
      // starts from the head again, which is what retries them.
      let after: string | undefined
      while (true) {
        const entries = yield* retryStore(store.peekOutbox({ limit: pageSize, after }))
        if (entries.length === 0) return
        after = entries[entries.length - 1]?.id
        const byStore = new Map<string, Array<(typeof entries)[number]>>()
        let skipped = 0
        for (const entry of entries) {
          if (!storesByKey.has(entry.parentStoreKey)) {
            // No route from this worker; a worker with the right `flows`
            // registration relays it, and reconciliation keeps the flow
            // correct regardless.
            skipped += 1
            continue
          }
          let group = byStore.get(entry.parentStoreKey)
          if (group === undefined) {
            group = []
            byStore.set(entry.parentStoreKey, group)
          }
          group.push(entry)
        }
        if (skipped > 0) {
          yield* Metric.update(Metrics.flowOutboxSkipped, skipped)
        }
        for (const [key, group] of byStore) {
          const target = storesByKey.get(key)
          if (target === undefined) continue
          // One unreachable parent store must not block deliveries to the
          // others: log, leave the group's entries for the next pass, move
          // on.
          yield* Effect.gen(function*() {
            const results = yield* retryStore(
              target.recordChildResults(group.map((entry) => entry.report))
            )
            for (const [index, result] of results.entries()) {
              const entry = group[index]
              if (entry === undefined) continue
              if (result.applied) {
                yield* Metric.update(
                  Metrics.flowChildReports.pipe(
                    Metric.withAttributes({
                      flow: entry.flowName,
                      outcome: entry.report.outcome,
                      source: "report"
                    })
                  ),
                  1
                )
              }
              if (
                result.parentSettled && entry.report.outcome === "failed" &&
                flowPolicies.get(entry.flowName)?.failFast === true
              ) {
                yield* Effect.logError(
                  `effect-mq: flow "${entry.flowName}" (${entry.report.flowId}) settled failed-fast on child "${entry.report.childKey}"`
                )
              }
            }
            yield* retryStore(store.deleteOutbox(group.map((entry) => entry.id)))
          }).pipe(
            Effect.catchCause((cause) =>
              Effect.logWarning(
                `effect-mq: outbox relay could not deliver to store "${key}"; retrying next pass`,
                cause
              )
            )
          )
        }
        if (entries.length < pageSize) return
      }
    })

    const relayLoop = Effect.gen(function*() {
      const observed = relayPulseVersion
      // Failures are contained per drain so the race below always paces the
      // loop (no hot retry); a pulse fired DURING the drain re-runs it
      // immediately via the version check.
      yield* relayDrain.pipe(
        Effect.catchCause((cause) => Effect.logError("effect-mq: outbox relay drain failed", cause))
      )
      yield* Effect.race(awaitRelayPulse(observed), Effect.sleep(flowSweepMs))
    }).pipe(Effect.forever)

    const ensureRelay = Effect.suspend(() => {
      if (relayStarted) return Effect.void
      relayStarted = true
      return FiberSet.run(fibers, relayLoop.pipe(Effect.updateContext(() => workerContext)))
    })

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
        // One line per finished run: outcome counter + duration histogram
        // (claim time to now, from the Effect Clock).
        const recordRun = (outcome: string) =>
          Effect.gen(function*() {
            const finished = yield* Clock.currentTimeMillis
            const attributes = { name: record.name, queue: record.queue, outcome }
            yield* Metric.update(Metrics.jobRuns.pipe(Metric.withAttributes(attributes)), 1)
            yield* Metric.update(
              Metrics.jobRunDuration.pipe(Metric.withAttributes(attributes)),
              Math.max(0, finished - (record.processedAt ?? finished))
            )
          })
        if (record.flow !== undefined && entry.flow === undefined) {
          // A fanned-out flow parent claimed by a PLAIN handler registration
          // (the flow was re-registered as an ordinary job in a deploy):
          // running the plain handler would silently ack its result as the
          // parent's terminal exit and discard the recorded child results.
          // Fail visibly instead; once the deploy is fixed, an admin retry
          // re-enters collect with the manifest intact.
          return Effect.gen(function*() {
            const cause = Cause.die(new Error(
              `effect-mq: job "${record.name}" carries flow state (collect phase) but is registered as a plain handler; ` +
                `register its flow via Flow.toLayer instead of Job.toLayer`
            ))
            const encoded = yield* Effect.exit(entry.encodeExit(Exit.failCause(cause)))
            yield* ackSafely(
              store.ack(record.id, token, {
                _tag: "Fail",
                exit: Exit.isSuccess(encoded) ? encoded.value : undefined
              }),
              "ack"
            )
            yield* reportFailure({
              jobId: record.id,
              name: record.name,
              queue: record.queue,
              attempt: context.attempt,
              attemptsMax: record.attemptsMax,
              willRetry: false,
              cause
            }, undefined)
            yield* recordRun("failed")
          })
        }
        return Effect.gen(function*() {
          // The per-run time limit interrupts the handler internally; surface
          // it as a defect so it flows through normal retry accounting.
          // timeoutOrElse (not timeout + a catch on TimeoutError) so a
          // handler's OWN typed TimeoutError failure stays a typed failure.
          // Each run gets a span (configurable name) tagged with the job
          // id, parented on the PRODUCER's persisted span context when
          // present — so producer -> handler traces connect across
          // processes.
          const linking = options?.traceLinking ?? "auto"
          const attach = record.trace === undefined
            ? "none"
            : linking === "auto"
            ? (record.trace.delayed ? "link" : "parent")
            : linking
          const producerSpan = record.trace === undefined ? undefined : Tracer.externalSpan({
            traceId: record.trace.traceId,
            spanId: record.trace.spanId,
            sampled: record.trace.sampled
          })
          // Flow-parent phase dispatch is persisted, not inferred: no `flow`
          // bookkeeping on the record means the manifest never landed (run
          // `fanOut`); its presence means a resumed parent (run `collect`,
          // stored in `entry.run`) — a re-claimed parent can never fan out
          // twice.
          const flow = entry.flow
          const isFanOut = flow !== undefined && record.flow === undefined
          const runEffect = (isFanOut
            ? flow.fanOut(record.payload, context, record.parent?.depth ?? 0)
            : entry.run(record.payload, context, record.flow)).pipe(
              // Every log line a handler writes carries the job identity —
              // log-based alerting needs no per-handler setup.
              Effect.annotateLogs({
                effectMqJobId: record.id,
                effectMqQueue: record.queue,
                effectMqAttempt: context.attempt
              })
            )
          const withRunSpan = runEffect.pipe(
            Effect.withSpan(
              options?.handlerSpanName?.(context) ?? `${record.name}.run`,
              {
                attributes: {
                  effectMqJobId: record.id,
                  effectMqQueue: record.queue,
                  effectMqAttempt: context.attempt
                },
                links: attach === "link" && producerSpan !== undefined
                  ? [{ span: producerSpan, attributes: {} }]
                  : undefined
              },
              { captureStackTrace: false }
            )
          )
          const spanned = attach === "parent" && producerSpan !== undefined
            ? withRunSpan.pipe(Effect.withParentSpan(producerSpan))
            : withRunSpan
          const handlerEffect = record.timeoutMs === undefined
            ? spanned
            : spanned.pipe(
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
          const busy = Metrics.jobsInFlight.pipe(Metric.withAttributes({ queue: record.queue }))
          yield* Metric.modify(busy, 1)
          const exit = yield* Effect.exit(restore(Fiber.join(fiber)))
          yield* Metric.modify(busy, -1)
          inflight.delete(record.id)
          const wasCancelled = cancelling.delete(record.id)

          // A cancel-request interrupt is terminal: ack Cancelled (even if a
          // shutdown races it — cancellation wins, the job must not revive).
          if (wasCancelled && Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)) {
            const landed = yield* ackLanded(store.ack(record.id, token, { _tag: "Cancelled" }), "ack")
            if (landed && record.parent !== undefined) {
              // The ack appended this child's report to the outbox; drain now
              // instead of waiting for the periodic pass.
              yield* fireRelayPulse
            }
            return yield* recordRun("cancelled")
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
            yield* recordRun("released")
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

          if (isFanOut && Exit.isSuccess(effective)) {
            // SAFETY: the fan-out runner's success type is the built specs.
            const children = effective.value as ReadonlyArray<FlowChildSpec>
            const acked = yield* Effect.exit(
              store.ack(record.id, token, {
                _tag: "FanOut",
                failFast: flow.failFast,
                children
              }).pipe(
                Effect.retry({
                  while: (error) => isJobStoreError(error),
                  schedule: storeRetryPolicy
                })
              )
            )
            if (Exit.isSuccess(acked)) {
              yield* Metric.update(
                Metrics.flowFanOuts.pipe(Metric.withAttributes({ flow: flow.flowName })),
                1
              )
              // The ack may have settled the parent instead of parking it (a
              // cancel raced the fan-out: cancellation wins and the rows are
              // already marked for cascade) — children of a settled flow must
              // not start. A settle that lands AFTER this check still
              // converges: the marked rows make the sweeper cancel whatever
              // we enqueue below.
              const parked = children.length === 0 ? Option.none() : yield* retryStore(store.getJob(record.id))
              if (Option.isSome(parked) && parked.value.state === "waiting-children") {
                // Fast path only: the flow sweeper re-drives whatever a
                // crash here misses, straight from the persisted specs.
                yield* flow.enqueueChildren(children)
              } else if (children.length > 0) {
                yield* Effect.logInfo(
                  `effect-mq: flow "${flow.flowName}" (${record.id}) settled during fan-out; children not enqueued`
                )
              }
            } else {
              // Lock lost or job vanished: the manifest did NOT land, so the
              // children must not run — another worker re-runs fanOut.
              yield* Effect.logWarning(
                `effect-mq: FanOut ack dropped for flow "${flow.flowName}" (${record.id})`,
                acked.cause
              )
            }
            return yield* recordRun("fanned-out")
          }

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
          const landed = yield* ackLanded(store.ack(record.id, token, outcome), "ack")
          if (
            landed && record.parent !== undefined &&
            (outcome._tag === "Complete" || outcome._tag === "Fail")
          ) {
            // The ack appended this child's report to the outbox; drain now
            // instead of waiting for the periodic pass.
            yield* fireRelayPulse
          }
          if (outcome._tag === "Fail" || outcome._tag === "Retry") {
            const cause = Exit.isFailure(effective)
              ? effective.cause
              : Cause.die(
                new Error(`effect-mq: success value for job "${record.name}" could not be encoded`)
              )
            yield* reportFailure({
              jobId: record.id,
              name: record.name,
              queue: record.queue,
              attempt: context.attempt,
              attemptsMax: record.attemptsMax,
              willRetry: outcome._tag === "Retry",
              cause
            }, outcome._tag === "Retry" ? outcome.delayMs : undefined)
          }
          yield* recordRun(
            outcome._tag === "Complete"
              ? "completed"
              : outcome._tag === "Retry"
              ? "retried"
              : outcome._tag === "Cancelled"
              ? "cancelled"
              : "failed"
          )
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
            yield* Metric.update(
              Metrics.claims.pipe(Metric.withAttributes({ queue, result: "empty" })),
              1
            )
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
          yield* Metric.update(
            Metrics.claims.pipe(Metric.withAttributes({ queue, result: "claimed" })),
            1
          )
          const claimedAt = yield* Clock.currentTimeMillis
          yield* Metric.update(
            Metrics.jobWaitDuration.pipe(
              Metric.withAttributes({ name: result.job.name, queue })
            ),
            Math.max(0, claimedAt - result.job.runAt)
          )
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
        yield* Metric.update(Metrics.locksLost, result.lost.length)
        yield* Effect.logWarning(
          "effect-mq: failed to renew locks; jobs may run twice",
          result.lost
        )
        // A lost lock stays lost: drop the flight so we neither renew nor
        // recount it every heartbeat (the run's eventual ack surfaces
        // LockLostError on its own).
        for (const id of result.lost) {
          inflight.delete(id)
        }
      }
      // Honour cross-process cancel requests: interrupt the handler fiber;
      // processJob acks the job as Cancelled.
      for (const id of result.cancelRequested) {
        const flight = inflight.get(id)
        if (flight !== undefined) {
          // The store re-reports the request every heartbeat until the ack
          // lands; count the first delivery only.
          if (!cancelling.has(id)) {
            yield* Metric.update(Metrics.cancelInterrupts, 1)
          }
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
        const failed = recovered.filter((entry) => entry.failed).length
        if (failed > 0) {
          yield* Metric.update(
            Metrics.stalledRecovered.pipe(Metric.withAttributes({ outcome: "failed" })),
            failed
          )
        }
        if (recovered.length - failed > 0) {
          yield* Metric.update(
            Metrics.stalledRecovered.pipe(Metric.withAttributes({ outcome: "requeued" })),
            recovered.length - failed
          )
        }
        yield* Effect.logWarning("effect-mq: recovered stalled jobs", recovered)
        // Stall exhaustion lands jobs in terminal `failed` without an ack, so
        // report those here like any other terminal failure (whichever
        // worker's sweep recovered them reports them, exactly once).
        for (const entry of recovered) {
          if (!entry.failed) continue
          const job = yield* retryStore(store.getJob(entry.id))
          if (Option.isNone(job)) continue
          yield* reportFailure({
            jobId: job.value.id,
            name: job.value.name,
            queue: job.value.queue,
            attempt: job.value.attemptsMade + 1,
            attemptsMax: job.value.attemptsMax,
            willRetry: false,
            cause: Cause.die(
              new Error(job.value.failedReason ?? "effect-mq: job stalled past maxStalledCount")
            )
          }, undefined)
        }
      }
    }).pipe(
      Effect.catchCause((cause) => Effect.logError("effect-mq: stalled sweep failed", cause)),
      Effect.forever
    )

    // Tick one due repeatable-job schedule. tickSchedule is a single atomic
    // op (CAS on nextRunAt + insert + advance), so concurrent sweepers fire
    // each slot exactly once — even when the previous slot's job row has
    // already been pruned by retention.
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
        const fired = yield* retryStore(store.tickSchedule(schedule.key, slot, next, {
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
          dedupe: undefined,
          trace: undefined,
          parent: undefined,
          delayMs: 0
        }))
        if (fired) {
          yield* Metric.update(
            Metrics.scheduleTicks.pipe(Metric.withAttributes({ name: schedule.jobName })),
            1
          )
        }
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

    // The flow reconciliation engine (see `FlowSweepWork`). Every action is
    // idempotent by construction: enqueues dedup on the deterministic child
    // id, reports dedup on the dependency row's state, cancels dedup on the
    // child's state, and the cascaded flag dedups its own write — so crashes
    // anywhere in the sweep are safe.
    const reconcileChild = (flowId: JobId, child: FlowChildSpec) =>
      Effect.gen(function*() {
        const childStore = storesByKey.get(child.storeKey)
        if (childStore === undefined) {
          // A flow registered on another worker shares this parent store;
          // that worker's sweeper holds the child store layer.
          return
        }
        const id = child.request.id
        if (id === undefined) return
        const existing = yield* retryStore(childStore.getJob(id))
        if (Option.isNone(existing)) {
          // Never landed (crash between the FanOut ack and the enqueue), or
          // pruned before reporting (see the child-retention guidance) —
          // (re-)drive it from the persisted spec. The flow may have settled
          // since this work item was snapshotted (another sweeper's cascade
          // would then have found this child missing, marked its row
          // cascaded, and moved on), so re-check the parent around the
          // enqueue: before, to skip settled flows, and after, so a settle
          // that lands inside the window cannot leave an orphan running.
          const before = yield* retryStore(store.getJob(flowId))
          if (Option.isNone(before) || before.value.state !== "waiting-children") return
          yield* retryStore(childStore.enqueue(child.request))
          const after = yield* retryStore(store.getJob(flowId))
          if (Option.isNone(after) || after.value.state !== "waiting-children") {
            yield* retryStore(
              childStore.cancel(id).pipe(
                Effect.catchTag(["JobNotFoundError", "JobNotCancellableError"], () => Effect.void)
              )
            )
          }
          return
        }
        const state = existing.value.state
        if (state !== "completed" && state !== "failed" && state !== "cancelled") {
          // Still in flight: never blindly re-drive a live child.
          return
        }
        // Terminal without a delivered report (a dropped outbox entry, or a
        // child whose store has no relaying worker): synthesize the report
        // from the child store's own record.
        const results = yield* retryStore(store.recordChildResults([{
          flowId,
          childKey: child.childKey,
          outcome: state,
          exit: existing.value.exit,
          failedReason: existing.value.failedReason
        }]))
        const result = results[0]
        if (result?.applied === true) {
          yield* Metric.update(
            Metrics.flowChildReports.pipe(
              Metric.withAttributes({
                flow: child.request.parent?.flowName ?? "unknown",
                outcome: state,
                source: "reconcile"
              })
            ),
            1
          )
          if (result?.parentSettled === true && state === "failed") {
            yield* Effect.logError(
              `effect-mq: flow ${flowId} settled on reconciled child failure "${child.childKey}"`
            )
          }
        }
      })

    const cascadeChildren = (
      flowId: JobId,
      children: ReadonlyArray<{
        readonly childKey: string
        readonly storeKey: string
        readonly childJobId: JobId
      }>
    ) =>
      Effect.gen(function*() {
        const done: Array<string> = []
        for (const child of children) {
          const childStore = storesByKey.get(child.storeKey)
          if (childStore === undefined) continue
          // Idempotent: a vanished or already-terminal child is "cancelled
          // enough".
          yield* retryStore(
            childStore.cancel(child.childJobId).pipe(
              Effect.catchTag(["JobNotFoundError", "JobNotCancellableError"], () => Effect.void)
            )
          )
          done.push(child.childKey)
        }
        if (done.length > 0) {
          yield* retryStore(store.markChildrenCascaded(flowId, done))
          yield* Metric.update(Metrics.flowCascades, done.length)
        }
      })

    const flowSweepLoop = Effect.gen(function*() {
      yield* Effect.sleep(flowSweepMs)
      const work = yield* retryStore(store.flowSweepWork({ pendingAgeMs: flowSweepMs, limit: 512 }))
      for (const group of work.reconcile) {
        for (const child of group.children) {
          // Each child in isolation: one poison row cannot starve the sweep.
          yield* reconcileChild(group.flowId, child).pipe(
            Effect.catchCause((cause) =>
              Effect.logError(
                `effect-mq: flow reconcile failed for child "${child.childKey}" of ${group.flowId}`,
                cause
              )
            )
          )
        }
      }
      for (const group of work.cascade) {
        yield* cascadeChildren(group.flowId, group.children).pipe(
          Effect.catchCause((cause) =>
            Effect.logError(`effect-mq: flow cascade failed for ${group.flowId}`, cause)
          )
        )
      }
    }).pipe(
      Effect.catchCause((cause) => Effect.logError("effect-mq: flow sweep failed", cause)),
      Effect.forever
    )

    // Started lazily by the first flow registration — plain workers never
    // pay for a sweep query.
    const ensureFlowSweeper = Effect.suspend(() => {
      if (flowSweeperStarted) return Effect.void
      flowSweeperStarted = true
      return FiberSet.run(fibers, flowSweepLoop.pipe(Effect.updateContext(() => workerContext)))
    })

    yield* FiberSet.run(fibers, renewalLoop)
    yield* FiberSet.run(fibers, stalledLoop)
    yield* FiberSet.run(fibers, scheduleLoop)
    if (flowPolicies.size > 0) {
      yield* ensureRelay
    }

    if (options?.queueMetricsInterval !== undefined) {
      const sampleMs = Duration.toMillis(options.queueMetricsInterval)
      const depthLoop = Effect.gen(function*() {
        yield* Effect.sleep(sampleMs)
        for (const queue of startedQueues) {
          const counts = yield* retryStore(store.counts(queue))
          for (const [state, depth] of Object.entries(counts)) {
            yield* Metric.update(
              Metrics.queueDepth.pipe(Metric.withAttributes({ queue, state })),
              depth
            )
          }
        }
      }).pipe(
        Effect.catchCause((cause) => Effect.logWarning("effect-mq: queue depth sampling failed", cause)),
        Effect.forever
      )
      yield* FiberSet.run(fibers, depthLoop)
    }

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
            Context.omit(Scope_.Scope, Tracer.ParentSpan, CurrentJob)
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
          const entry: HandlerEntry = {
            run: (payload, context) =>
              provideCaptured(
                decodePayload(payload).pipe(
                  Effect.orDie,
                  Effect.flatMap((decoded) => handler(decoded)),
                  // Innermost, so neither the captured registration context
                  // nor the worker's own can shadow the running attempt.
                  Effect.provideService(CurrentJob, context)
                )
              ),
            encodeExit: (exit) => provideCaptured(encodeExit(exit)),
            unrecoverableFailure: toUnrecoverableFailure(job.retryable),
            flow: undefined
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
        }) as Effect.Effect<void, never, never>,

      registerFlow: (flow, registerOptions) =>
        Effect.gen(function*() {
          const name = flow.parent._tag
          if (handlers.has(name)) {
            return yield* Effect.die(
              new Error(`effect-mq: duplicate handler registered for job "${name}"`)
            )
          }
          if (flow.parent.store.key !== storeKey.key) {
            return yield* Effect.die(
              new Error(
                `effect-mq: flow "${flow.name}" parent "${name}" is bound to store "${flow.parent.store.key}" but this worker claims from "${storeKey.key}". ` +
                  `Provide a Worker.layer({ store }) for the parent's store.`
              )
            )
          }
          const services = (yield* Effect.context<never>()).pipe(
            Context.omit(Scope_.Scope, Tracer.ParentSpan, CurrentJob)
          )
          // Resolve every child store from the registration context (declared
          // on Flow.toLayer's signature) — this worker is the one process
          // guaranteed able to reconcile and cascade across all of them.
          for (const key of flow.childStores) {
            const service = Context.getOption(services, key)
            if (Option.isNone(service)) {
              return yield* Effect.die(
                new Error(
                  `effect-mq: flow "${flow.name}" requires child store "${key.key}" — provide it to the flow's layer`
                )
              )
            }
            storesByKey.set(key.key, service.value)
          }
          flowPolicies.set(flow.name, { failFast: flow.failFast })
          const decodePayload = Schema.decodeUnknownEffect(flow.parent.payloadJsonSchema)
          const encodeExit = Schema.encodeEffect(flow.parent.exitSchema)
          // SAFETY: same contract as `register` — the captured context holds
          // everything Flow.toLayer's signature required.
          const provideCaptured = <A, E>(effect: Effect.Effect<A, E, unknown>): Effect.Effect<A, E> =>
            effect.pipe(
              Effect.updateContext((input) =>
                Context.merge(input, services) as Context.Context<unknown>
              )
            ) as Effect.Effect<A, E>
          const emptyFlow: FlowState = {
            failFast: flow.failFast,
            pending: 0,
            completed: 0,
            failed: 0,
            cancelled: 0
          }
          const entry: HandlerEntry = {
            run: (payload, context, flowState) =>
              provideCaptured(
                decodePayload(payload).pipe(
                  Effect.orDie,
                  Effect.flatMap((decoded) =>
                    // collect dispatch requires a persisted manifest, so the
                    // fallback is defensive only.
                    flow.collect(decoded, flowState ?? emptyFlow, context)
                  ),
                  Effect.provideService(CurrentJob, context)
                )
              ),
            encodeExit: (exit) => provideCaptured(encodeExit(exit)),
            unrecoverableFailure: toUnrecoverableFailure(flow.parent.retryable),
            flow: {
              flowName: flow.name,
              failFast: flow.failFast,
              fanOut: (payload, context, parentDepth) =>
                provideCaptured(
                  decodePayload(payload).pipe(
                    Effect.orDie,
                    Effect.flatMap((decoded) => flow.fanOut(decoded, context, parentDepth)),
                    Effect.provideService(CurrentJob, context)
                  )
                ),
              enqueueChildren: (children) =>
                Effect.gen(function*() {
                  // Group per child store; enqueueMany chunks internally and
                  // dedups on the deterministic ids.
                  const byStore = new Map<string, Array<EnqueueRequest>>()
                  for (const child of children) {
                    let group = byStore.get(child.storeKey)
                    if (group === undefined) {
                      group = []
                      byStore.set(child.storeKey, group)
                    }
                    group.push(child.request)
                  }
                  for (const [key, requests] of byStore) {
                    const childStore = storesByKey.get(key)
                    if (childStore === undefined) continue
                    yield* retryStore(childStore.enqueueMany(requests)).pipe(
                      Effect.catchCause((cause) =>
                        Effect.logWarning(
                          `effect-mq: flow "${flow.name}" child enqueue incomplete; the flow sweeper will reconcile`,
                          cause
                        )
                      )
                    )
                  }
                })
            }
          }
          handlers.set(name, entry)
          const queue = registerOptions?.queue !== undefined
            ? QueueName(registerOptions.queue)
            : flow.parent.queue
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
          yield* ensureFlowSweeper
          yield* ensureRelay
          yield* firePulse
          // SAFETY: like `register`, the implementation erases requirements
          // that Flow.toLayer's public signature declares and the captured
          // context (via provideCaptured) restores; only Scope remains.
        }) as Effect.Effect<void, never, Scope.Scope>
    })
  })

/**
 * Run a worker as a layer. Provide handler layers (`Job.toLayer(...)`) on top
 * of this, and a `JobStore` below it.
 *
 * @since 0.1.0
 */
export const layer = <StoreId = JobStore, const Flows extends ReadonlyArray<FlowAny> = ReadonlyArray<never>>(
  options?: (WorkerOptions<StoreId> & { readonly flows?: Flows | undefined }) | undefined
): Layer.Layer<Worker, never, StoreId | FlowParentStores<Flows[number]>> =>
  Layer.effect(Worker, make(options))
