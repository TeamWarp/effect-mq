/**
 * Schema-first background job definitions.
 *
 * A `Job` is defined once (name, payload/success/error schemas, defaults) and
 * used from both sides:
 *
 * - producers call `MyJob.enqueue(payload, options)` (requires the job's store)
 * - runners provide `MyJob.toLayer(handler)` on top of a `Worker.layer`
 *
 * ```ts
 * import { Job, JobStore } from "effect-mq"
 * import { Effect, Schema } from "effect"
 *
 * const Durable = JobStore.named("durable")
 *
 * class SendEmail extends Job.make("SendEmail", {
 *   payload: { to: Schema.String, body: Schema.String },
 *   queue: "email",
 *   store: Durable,
 *   metadata: ({ to }) => ({ to }),
 *   defaults: { attempts: 5, backoff: { type: "exponential", delay: "1 second" } }
 * }) {}
 *
 * // producer — requires the Durable store in context, enforced at compile time
 * const jobId = yield* SendEmail.enqueue({ to: "a@b.c", body: "hi" }, { delay: "5 seconds" })
 *
 * // runner
 * const SendEmailWorker = SendEmail.toLayer((payload) => Effect.log(`sending to ${payload.to}`))
 * ```
 *
 * @since 0.1.0
 */
import { Clock, type Context, DateTime, Duration, Effect, type Exit, Layer, Metric, Option, Predicate, Schedule, Schema } from "effect"
import {
  type BackoffPolicy,
  type DedupePolicy,
  type KeepStatePolicy,
  type TraceContext,
  JobCancelledError,
  JobId,
  type JobNotCancellableError,
  JobNotFoundError,
  type JobNotPromotableError,
  type JobNotRetryableError,
  type JobState,
  JobStore,
  type KeepPolicy,
  nextOccurrence,
  QueueName,
  ScheduleKey,
  type ScheduleRecord,
  type Service as StoreService,
  unrecoverable
} from "./JobStore.ts"
import * as Metrics from "./Metrics.ts"

export {
  /**
   * Mark an error value as unrecoverable: the worker skips the remaining
   * retry budget and fails the job immediately. Returns the error unchanged.
   *
   * @since 0.2.0
   */
  unrecoverable
}
import { type JobContext, type RegisterOptions, Worker } from "./Worker.ts"

const TypeId = "~effect-mq/Job" as const

/**
 * A struct schema (or anything with struct fields).
 *
 * @since 0.1.0
 */
export interface AnyStructSchema extends Schema.Top {
  readonly fields: Schema.Struct.Fields
}

/**
 * User-facing backoff configuration.
 *
 * @since 0.1.0
 */
export interface BackoffInput {
  readonly type: "fixed" | "exponential"
  readonly delay: Duration.Input
  /** Exponential growth factor (default 2). */
  readonly factor?: number | undefined
}

/**
 * The `error` option for `Job.make`: one schema, or a list of schemas the
 * definition unions for you — the tagged-error-list style of Effect's
 * `HttpApiEndpoint`:
 *
 * ```ts
 * error: PaymentDeclined
 * error: [InvoiceNotFound, PaymentDeclined, ProviderTimeout]
 * ```
 *
 * @since 0.4.2
 */
export type ErrorInput = Schema.Top | ReadonlyArray<Schema.Top>

/**
 * The schema a job actually carries for its `error` option: lists become a
 * `Schema.Union` of their members, single schemas pass through.
 *
 * @since 0.4.2
 */
export type ResolvedError<E extends ErrorInput> = E extends ReadonlyArray<Schema.Top> ? Schema.Union<E> : E

/**
 * User-facing retention configuration for terminal jobs.
 *
 * @since 0.1.0
 */
export interface KeepStateInput {
  /** Keep at most this many terminal records (per name + state). */
  readonly count?: number | undefined
  /** Remove terminal records older than this. */
  readonly age?: Duration.Input | undefined
}

/**
 * Retention input: a flat `{ count, age }` applies to all terminal states;
 * the split form sets independent rules per state (completed jobs are
 * usually noise, failed ones evidence) — an absent state keeps its records
 * until the store's `historyTtl` ceiling:
 *
 * ```ts
 * keep: { count: 100 }                                        // all states
 * keep: { completed: { age: "1 day" }, failed: { age: "30 days" } }
 * ```
 *
 * @since 0.1.0
 */
export type KeepInput = KeepStateInput | {
  readonly completed?: KeepStateInput | undefined
  readonly failed?: KeepStateInput | undefined
  readonly cancelled?: KeepStateInput | undefined
}

/**
 * Options shared between job defaults and per-enqueue overrides.
 *
 * @since 0.1.0
 */
export interface JobOptions {
  /** Do not run before this long from enqueue time. */
  readonly delay?: Duration.Input | undefined
  /** Higher runs first; ties are FIFO. Default 0. */
  readonly priority?: number | undefined
  /** Total attempts including the first run. Default 1 (no retries). */
  readonly attempts?: number | undefined
  /**
   * Delay between retry attempts:
   * `{ type: "fixed" | "exponential", delay, factor? }`. Default: retries
   * are immediate.
   */
  readonly backoff?: BackoffInput | undefined
  /** Retention for terminal records. Default: keep forever. */
  readonly keep?: KeepInput | undefined
  /**
   * Per-run execution time limit; the worker interrupts the handler fiber
   * past it and the run counts as a failed attempt (retried per backoff).
   */
  readonly timeout?: Duration.Input | undefined
}

/**
 * @since 0.1.0
 */
/**
 * Deduplication input for `Job.make({ dedupe })` and per-enqueue overrides.
 * A bare string is shorthand for `{ key }`. Dedup never changes the job id —
 * it is a separate, name-scoped key:
 *
 * - `{ key }`: dedupe while the keyed job is pending; done jobs free the key.
 * - `{ key, ttl }`: throttle — at most one job per window.
 * - `{ key, ttl, extend: true }`: debounce — dropped enqueues push the
 *   window out.
 * - `{ key, replace: true }`: while the keyed job is still delayed, the new
 *   enqueue's content replaces it (latest wins).
 *
 * @since 0.3.0
 */
export type DedupeInput = string | {
  /**
   * The dedup key, scoped to this job's name (e.g. an employee id). Never
   * changes the job id. Must be non-empty.
   */
  readonly key: string
  /**
   * Throttle window: at most one job per key per window, even after the
   * keyed job completes. Without `ttl`, dedup lasts while the keyed job is
   * pending (waiting/delayed/active).
   */
  readonly ttl?: Duration.Input | undefined
  /**
   * Debounce (requires `ttl`): every deduplicated enqueue pushes the window
   * out again.
   */
  readonly extend?: boolean | undefined
  /**
   * Latest-wins while the keyed job is still delayed: the new enqueue's
   * payload/metadata/priority/attempts/backoff/keep/timeout/delay replace
   * the existing job's (same id, ledger preserved). A landed replace
   * re-arms the `ttl` window. In any other state, normal dedup applies.
   */
  readonly replace?: boolean | undefined
}

const normalizeDedupe = (input: DedupeInput | undefined): DedupePolicy | undefined => {
  if (input === undefined) return undefined
  const config = Predicate.isString(input) ? { key: input } : input
  if (config.key === "") {
    throw new Error("effect-mq: dedupe `key` must be non-empty")
  }
  if (config.extend === true && config.ttl === undefined) {
    throw new Error("effect-mq: dedupe `extend` requires a `ttl`")
  }
  return {
    key: config.key,
    ttlMs: config.ttl !== undefined ? Duration.toMillis(config.ttl) : undefined,
    extend: config.extend === true,
    replace: config.replace === true
  }
}

/**
 * When the job becomes runnable: a relative `delay` OR an absolute `at`
 * (any `DateTime.Input` — a `DateTime` from `DateTime.makeZonedUnsafe` for
 * wall-clock-in-timezone instants, a `Date`, ISO string, epoch millis, or
 * date parts). Setting both is a compile error; an `at` in the past runs
 * immediately.
 *
 * @since 0.4.0
 */
export type RunTimeInput =
  | {
    /**
     * Run this long after enqueue (relative). Any `Duration.Input`:
     * `"5 seconds"`, `Duration.minutes(10)`, millis, ...
     *
     * Mutually exclusive with `at` (setting both is a compile error).
     */
    readonly delay?: Duration.Input | undefined
    /** Set `delay` for relative times, or `at` (alone) for absolute ones. */
    readonly at?: undefined
  }
  | {
    /**
     * Run at an absolute instant (any `DateTime.Input`) — no duration math:
     *
     * ```ts
     * at: DateTime.makeZonedUnsafe(
     *   { year: 2026, month: 8, day: 24, hours: 9 },
     *   { timeZone: "America/New_York", adjustForTimeZone: true }
     * )
     * // also: a Date, ISO string, epoch millis, or { year, month, ... } parts
     * ```
     *
     * An `at` in the past runs immediately. Mutually exclusive with `delay`
     * (setting both is a compile error).
     */
    readonly at?: DateTime.DateTime.Input | undefined
    /** Set `at` for absolute times, or `delay` (alone) for relative ones. */
    readonly delay?: undefined
  }

/**
 * @since 0.1.0
 */
export type EnqueueOptions = Omit<JobOptions, "delay"> & RunTimeInput & {
  /**
   * Explicit job id. Enqueueing an id that already exists is a no-op that
   * returns the existing id (idempotency). Overrides the definition's
   * `idempotencyKey`.
   */
  readonly jobId?: string | undefined
  /** Send to a different queue than the definition's. */
  readonly queue?: string | undefined
  /** Queryable business context, merged over the definition's `metadata`. */
  readonly metadata?: Readonly<Record<string, string>> | undefined
  /** Deduplicate by key (see `DedupeInput`); overrides the definition's `dedupe`. */
  readonly dedupe?: DedupeInput | undefined
}

/**
 * Options for `enqueueMany` — `EnqueueOptions` minus the per-job fields:
 * `jobId` (a shared id would make every item after the first a duplicate)
 * and `dedupe` (a shared key would collapse the batch into one job; per-item
 * dedup still runs via the definition's `dedupe` callback).
 *
 * Built from parts rather than `Omit<EnqueueOptions, ...>`: a mapped type
 * over the `delay`/`at` union would flatten it and lose their exclusivity.
 *
 * @since 0.4.0
 */
export type EnqueueManyOptions = Omit<JobOptions, "delay"> & RunTimeInput & {
  /** Send every item to a different queue than the definition's. */
  readonly queue?: string | undefined
  /**
   * Queryable business context, merged over each item's definition-derived
   * `metadata` (the same overrides apply to every item).
   */
  readonly metadata?: Readonly<Record<string, string>> | undefined
}

/**
 * A definition's `defaults`, normalized to store units.
 *
 * @since 0.6.0
 */
export interface ResolvedDefaults {
  readonly delayMs: number
  readonly priority: number
  readonly attempts: number
  readonly backoff: BackoffPolicy | undefined
  readonly keep: KeepPolicy | undefined
  readonly timeoutMs: number | undefined
}

/**
 * Options for `Job.schedule`. Exactly one of `cron` (with optional IANA `tz`)
 * or `every` must be set. `cron` schedules first fire at the next matching
 * occurrence; `every` schedules first fire one interval from now.
 *
 * @since 0.2.0
 */
export interface ScheduleOptions<PayloadInput> {
  /**
   * Cron expression (5-field, e.g. `"0 9 * * 1"` = 9:00 every Monday).
   * First fires at the next matching occurrence. Exactly one of `cron` or
   * `every` must be set.
   */
  readonly cron?: string | undefined
  /** IANA time zone for `cron` (e.g. `"America/New_York"`). Default UTC. */
  readonly tz?: string | undefined
  /**
   * Fixed interval; first fires one interval from now and stays on that
   * grid. Exactly one of `cron` or `every` must be set.
   */
  readonly every?: Duration.Input | undefined
  /** The payload every occurrence is enqueued with. */
  readonly payload: PayloadInput
  /** Queryable business context, merged over the definition's `metadata`. */
  readonly metadata?: Readonly<Record<string, string>> | undefined
  /** Priority for each occurrence. Higher runs first; default 0. */
  readonly priority?: number | undefined
  /** Attempt budget for each occurrence. Default 1. */
  readonly attempts?: number | undefined
  /** Retry backoff for each occurrence. */
  readonly backoff?: BackoffInput | undefined
  /** Retention for each occurrence's terminal record. */
  readonly keep?: KeepInput | undefined
  /** Per-run execution time limit for each occurrence. */
  readonly timeout?: Duration.Input | undefined
  /**
   * Ownership label for declarative reconciliation: `JobSchedules.layer`
   * prunes only schedules carrying its own group. Unlabeled schedules are
   * never pruned. Usually set by `JobSchedules`, not by hand.
   */
  readonly group?: string | undefined
}

/**
 * The status of a job as seen by `poll`. Fetch the full run ledger with
 * `Job.attempts`.
 *
 * @since 0.1.0
 */
export interface JobStatus<A, E> {
  readonly state: JobState
  readonly attemptsMade: number
  readonly metadata: Readonly<Record<string, string>>
  /** Present for completed/failed jobs (except store-side failures like stalling). */
  readonly exit: Option.Option<Exit.Exit<A, E>>
  readonly failedReason: string | undefined
}

/**
 * One decoded entry of a job's run ledger.
 *
 * @since 0.1.0
 */
export interface JobAttempt<A, E> {
  readonly attempt: number
  readonly startedAt: number | undefined
  readonly finishedAt: number
  readonly outcome: "completed" | "retried" | "failed" | "stalled" | "cancelled" | "fanned-out"
  /** Absent for `stalled`, `cancelled`, and `fanned-out` entries. */
  readonly exit: Option.Option<Exit.Exit<A, E>>
}

/**
 * @since 0.1.0
 */
export interface Job<
  Name extends string,
  Payload extends AnyStructSchema,
  Success extends Schema.Top,
  Error extends Schema.Top,
  StoreId = JobStore
> {
  new(_: never): {}

  readonly [TypeId]: typeof TypeId
  /**
   * The job's unique name. (Named `_tag` rather than `name` because a
   * `class X extends Job.make(...) {}` subclass shadows `Function.name`.)
   */
  readonly _tag: Name
  readonly queue: QueueName
  /** The store this job's runs live on. */
  readonly store: Context.Key<StoreId, StoreService>
  readonly payloadSchema: Payload
  readonly successSchema: Success
  readonly errorSchema: Error
  /** JSON codec for the payload — what is actually persisted. */
  readonly payloadJsonSchema: Schema.toCodecJson<Payload>
  /** JSON codec for handler exits — what is actually persisted. */
  readonly exitSchema: Schema.toCodecJson<
    Schema.Exit<
      Schema.toCodecJson<Success>,
      Schema.toCodecJson<Error>,
      Schema.Defect
    >
  >
  readonly idempotencyKey: ((payload: Payload["Type"]) => string) | undefined
  readonly dedupe: ((payload: Payload["Type"]) => DedupeInput) | undefined
  readonly metadata: ((payload: Payload["Type"]) => Readonly<Record<string, string>>) | undefined
  readonly retryable: ((error: Error["Type"]) => boolean) | undefined
  readonly defaults: ResolvedDefaults

  /**
   * Queue this job. Returns the job id. Duplicate ids (via `jobId` or
   * `idempotencyKey`) are a silent no-op returning the existing id.
   */
  readonly enqueue: (
    payload: Payload["~type.make.in"],
    options?: EnqueueOptions | undefined
  ) => Effect.Effect<JobId, never, StoreId | Payload["EncodingServices"]>

  /**
   * Queue many jobs in bulk — one store round trip per chunk of plain
   * items; items whose definition derives a `dedupe` key fall back to
   * individual enqueues, in order. Returns ids aligned with the payloads.
   * Per-item semantics match `enqueue`: `idempotencyKey`, `dedupe`, and
   * `metadata` callbacks run for each payload, and duplicates are silent
   * no-ops returning the existing id. Options apply to every item
   * (`jobId`/`dedupe` are excluded — a shared id or dedup key would
   * collapse the batch into one job).
   *
   * The batch is not transactional: a store failure mid-batch may leave a
   * subset (not necessarily a prefix) enqueued. Safe under at-least-once —
   * re-running the batch skips already-inserted items when ids are
   * deterministic (`idempotencyKey`); store-assigned ids may re-insert.
   */
  readonly enqueueMany: (
    payloads: ReadonlyArray<Payload["~type.make.in"]>,
    options?: EnqueueManyOptions | undefined
  ) => Effect.Effect<ReadonlyArray<JobId>, never, StoreId | Payload["EncodingServices"]>

  /** Read the current status of a previously enqueued job. */
  readonly poll: (
    jobId: JobId
  ) => Effect.Effect<
    Option.Option<JobStatus<Success["Type"], Error["Type"]>>,
    never,
    StoreId | Success["DecodingServices"] | Error["DecodingServices"]
  >

  /** The job's decoded run ledger, oldest first. */
  readonly attempts: (
    jobId: JobId
  ) => Effect.Effect<
    ReadonlyArray<JobAttempt<Success["Type"], Error["Type"]>>,
    never,
    StoreId | Success["DecodingServices"] | Error["DecodingServices"]
  >

  /**
   * Wait (by polling) until the job finishes, then return its result. Dies
   * if the job id does not exist or the job was failed by the store itself.
   */
  readonly awaitResult: (
    jobId: JobId,
    options?: { readonly pollSchedule?: Schedule.Schedule<unknown> | undefined } | undefined
  ) => Effect.Effect<
    Success["Type"],
    Error["Type"],
    StoreId | Success["DecodingServices"] | Error["DecodingServices"]
  >

  /** `enqueue` + `awaitResult` in one call. */
  readonly execute: (
    payload: Payload["~type.make.in"],
    options?: EnqueueOptions | undefined
  ) => Effect.Effect<
    Success["Type"],
    Error["Type"],
    | StoreId
    | Payload["EncodingServices"]
    | Success["DecodingServices"]
    | Error["DecodingServices"]
  >

  /**
   * Re-run a failed job with a fresh attempt budget. The run ledger is
   * preserved. (The admin op behind a dashboard's "retry" button.)
   */
  readonly retry: (
    jobId: JobId
  ) => Effect.Effect<void, JobNotFoundError | JobNotRetryableError, StoreId>

  /**
   * Cancel a job: waiting/delayed become terminal (`cancelled`) immediately;
   * a running job's handler fiber is interrupted by its worker on the next
   * heartbeat (latency ≤ `lockRenewInterval`).
   */
  readonly cancel: (
    jobId: JobId
  ) => Effect.Effect<void, JobNotFoundError | JobNotCancellableError, StoreId>

  /**
   * Cancel whatever pending job holds this dedup key (see `DedupeInput`).
   * Idempotent: returns false when nothing pending holds the key.
   */
  readonly cancelByKey: (key: string) => Effect.Effect<boolean, never, StoreId>

  /** Run a delayed job now. */
  readonly promote: (
    jobId: JobId
  ) => Effect.Effect<void, JobNotFoundError | JobNotPromotableError, StoreId>

  /**
   * Create or replace a durable repeatable schedule for this job. Each
   * occurrence is claimed and enqueued in one atomic store op, so ticks are
   * exactly-once per occurrence across all workers — regardless of history
   * retention. Missed occurrences (downtime) collapse into a single
   * catch-up run.
   *
   * Re-registering with an *unchanged* cadence (same `cron`/`tz`/`every`) is
   * a no-op for the next occurrence — deploy-time re-registration neither
   * re-anchors `every` grids nor drops a pending catch-up run. Changing the
   * cadence resets the next occurrence.
   */
  readonly schedule: (
    key: string,
    options: ScheduleOptions<Payload["~type.make.in"]>
  ) => Effect.Effect<ScheduleKey, never, StoreId | Payload["EncodingServices"]>

  /** Remove a schedule created by `schedule`. False when it did not exist. */
  readonly unschedule: (
    key: string
  ) => Effect.Effect<boolean, never, StoreId>

  /**
   * Attach the handler that processes this job, as a layer to provide on top
   * of `Worker.layer` (bound to the same store).
   */
  readonly toLayer: <R>(
    handler: (
      payload: Payload["Type"],
      context: JobContext
    ) => Effect.Effect<Success["Type"], Error["Type"], R>,
    options?: RegisterOptions | undefined
  ) => Layer.Layer<
    never,
    never,
    | Worker
    | R
    | Payload["DecodingServices"]
    | Success["EncodingServices"]
    | Error["EncodingServices"]
  >
}

/**
 * @since 0.1.0
 */
export interface Any {
  readonly [TypeId]: typeof TypeId
  readonly _tag: string
  readonly queue: QueueName
}

interface AnyWithProps extends Job<string, AnyStructSchema, Schema.Top, Schema.Top, any> {}

const defaultPollSchedule = Schedule.min([
  Schedule.exponential(10, 2),
  Schedule.spaced("1 second")
])

/**
 * Normalize a user-facing `BackoffInput` to the persisted policy. Shared
 * with the flow runtime's child-spec builder.
 *
 * @internal
 */
export const normalizeBackoff = (input: BackoffInput | undefined): BackoffPolicy | undefined =>
  input === undefined ? undefined : {
    _tag: input.type,
    delayMs: Duration.toMillis(input.delay),
    factor: input.factor
  }

const normalizeKeepState = (input: KeepStateInput): KeepStatePolicy => ({
  count: input.count,
  ageMs: input.age !== undefined ? Duration.toMillis(input.age) : undefined
})

/**
 * Normalize a user-facing `KeepInput` to the persisted policy. Shared with
 * the flow runtime's child-spec builder.
 *
 * @internal
 */
export const normalizeKeep = (input: KeepInput | undefined): KeepPolicy | undefined => {
  if (input === undefined) return undefined
  const split = "completed" in input || "failed" in input || "cancelled" in input
  const flat = "count" in input || "age" in input
  if (split && flat) {
    throw new Error("effect-mq: `keep` must be either flat { count, age } or split per state, not both")
  }
  if (split) {
    // SAFETY: the flat branch was excluded above, so the state keys carry
    // KeepStateInput values.
    const perState = input as {
      readonly completed?: KeepStateInput | undefined
      readonly failed?: KeepStateInput | undefined
      readonly cancelled?: KeepStateInput | undefined
    }
    return {
      completed: perState.completed !== undefined ? normalizeKeepState(perState.completed) : undefined,
      failed: perState.failed !== undefined ? normalizeKeepState(perState.failed) : undefined,
      cancelled: perState.cancelled !== undefined ? normalizeKeepState(perState.cancelled) : undefined
    }
  }
  // SAFETY: the split branch was excluded above, so this is the flat form.
  const policy = normalizeKeepState(input as KeepStateInput)
  return { completed: policy, failed: policy, cancelled: policy }
}

const Proto = {
  [TypeId]: TypeId,

  enqueue(this: AnyWithProps, fields: any, options?: EnqueueOptions) {
    return Effect.suspend(() => {
      const payload = this.payloadSchema.make(fields)
      const id = options?.jobId !== undefined
        ? JobId(options.jobId)
        : this.idempotencyKey !== undefined
        ? JobId(`${this._tag}/${this.idempotencyKey(payload)}`)
        : undefined
      const metadata = {
        ...this.metadata?.(payload),
        ...options?.metadata
      }
      const dedupe = normalizeDedupe(
        options?.dedupe ?? this.dedupe?.(payload)
      )
      const queue = options?.queue !== undefined ? QueueName(options.queue) : this.queue
      if (options?.at !== undefined && options.delay !== undefined) {
        // Unrepresentable in TypeScript; guard untyped callers anyway.
        throw new Error("effect-mq: set either `delay` or `at`, not both")
      }
      const delayMs = options?.at !== undefined
        ? Effect.map(
          Clock.currentTimeMillis,
          (now) => Math.max(0, DateTime.toEpochMillis(DateTime.makeUnsafe(options.at ?? now)) - now)
        )
        : Effect.succeed(
          options?.delay !== undefined
            ? Duration.toMillis(options.delay)
            : this.defaults.delayMs
        )
      // The enqueue span's context rides along on the record, so the
      // handler's span joins the producing trace across processes.
      const spanContext = Effect.currentSpan.pipe(
        Effect.map((span) => ({
          traceId: span.traceId,
          spanId: span.spanId,
          sampled: span.sampled
        })),
        Effect.catchTag("NoSuchElementError", () => Effect.succeed(undefined))
      )
      return Effect.all([Schema.encodeEffect(this.payloadJsonSchema)(payload), delayMs, spanContext]).pipe(
        Effect.orDie,
        Effect.flatMap(([encoded, resolvedDelayMs, capturedSpan]) =>
          Effect.flatMap(this.store, (store) =>
            store.enqueue({
              id,
              name: this._tag,
              queue,
              payload: encoded,
              metadata,
              priority: options?.priority ?? this.defaults.priority,
              attemptsMax: Math.max(1, options?.attempts ?? this.defaults.attempts),
              backoff: options?.backoff !== undefined
                ? normalizeBackoff(options.backoff)
                : this.defaults.backoff,
              keep: options?.keep !== undefined
                ? normalizeKeep(options.keep)
                : this.defaults.keep,
              timeoutMs: options?.timeout !== undefined
                ? Duration.toMillis(options.timeout)
                : this.defaults.timeoutMs,
              dedupe,
              // `delayed` records scheduling INTENT (not queue backlog), so
              // the worker's auto trace-linking stays deterministic.
              trace: capturedSpan === undefined
                ? undefined
                : { ...capturedSpan, delayed: resolvedDelayMs > 0 } satisfies TraceContext,
              parent: undefined,
              delayMs: resolvedDelayMs
            }))
        ),
        Effect.orDie,
        Effect.tap((result) =>
          Metric.update(
            Metrics.jobsEnqueued.pipe(
              Metric.withAttributes({
                name: this._tag,
                queue,
                duplicate: result.duplicate ? "true" : "false"
              })
            ),
            1
          )
        ),
        Effect.map((result) => result.id)
      )
    }).pipe(
      Effect.withSpan(`${this._tag}.enqueue`, {}, { captureStackTrace: false })
    )
  },

  enqueueMany(this: AnyWithProps, payloads: ReadonlyArray<any>, options?: EnqueueManyOptions) {
    return Effect.suspend(() => {
      const queue = options?.queue !== undefined ? QueueName(options.queue) : this.queue
      if (options?.at !== undefined && options.delay !== undefined) {
        // Unrepresentable in TypeScript; guard untyped callers anyway.
        throw new Error("effect-mq: set either `delay` or `at`, not both")
      }
      const delayMs = options?.at !== undefined
        ? Effect.map(
          Clock.currentTimeMillis,
          (now) => Math.max(0, DateTime.toEpochMillis(DateTime.makeUnsafe(options.at ?? now)) - now)
        )
        : Effect.succeed(
          options?.delay !== undefined
            ? Duration.toMillis(options.delay)
            : this.defaults.delayMs
        )
      const spanContext = Effect.currentSpan.pipe(
        Effect.map((span) => ({
          traceId: span.traceId,
          spanId: span.spanId,
          sampled: span.sampled
        })),
        Effect.catchTag("NoSuchElementError", () => Effect.succeed(undefined))
      )
      const encodedAll = Effect.forEach(payloads, (fields) => {
        const payload = this.payloadSchema.make(fields)
        return Effect.map(
          Schema.encodeEffect(this.payloadJsonSchema)(payload),
          (encoded) => ({ payload, encoded })
        )
      })
      return Effect.all([encodedAll, delayMs, spanContext]).pipe(
        Effect.orDie,
        Effect.flatMap(([items, resolvedDelayMs, capturedSpan]) =>
          Effect.flatMap(this.store, (store) =>
            store.enqueueMany(items.map(({ encoded, payload }) => ({
              id: this.idempotencyKey !== undefined
                ? JobId(`${this._tag}/${this.idempotencyKey(payload)}`)
                : undefined,
              name: this._tag,
              queue,
              payload: encoded,
              metadata: { ...this.metadata?.(payload), ...options?.metadata },
              priority: options?.priority ?? this.defaults.priority,
              attemptsMax: Math.max(1, options?.attempts ?? this.defaults.attempts),
              backoff: options?.backoff !== undefined
                ? normalizeBackoff(options.backoff)
                : this.defaults.backoff,
              keep: options?.keep !== undefined
                ? normalizeKeep(options.keep)
                : this.defaults.keep,
              timeoutMs: options?.timeout !== undefined
                ? Duration.toMillis(options.timeout)
                : this.defaults.timeoutMs,
              dedupe: normalizeDedupe(this.dedupe?.(payload)),
              trace: capturedSpan === undefined
                ? undefined
                : { ...capturedSpan, delayed: resolvedDelayMs > 0 } satisfies TraceContext,
              parent: undefined,
              delayMs: resolvedDelayMs
            })))
          )
        ),
        Effect.orDie,
        Effect.tap((results) => {
          const duplicates = results.filter((result) => result.duplicate).length
          const update = (duplicate: "true" | "false", count: number) =>
            count === 0 ? Effect.void : Metric.update(
              Metrics.jobsEnqueued.pipe(
                Metric.withAttributes({ name: this._tag, queue, duplicate })
              ),
              count
            )
          return Effect.andThen(
            update("false", results.length - duplicates),
            update("true", duplicates)
          )
        }),
        Effect.map((results) => results.map((result) => result.id))
      )
    }).pipe(
      Effect.withSpan(`${this._tag}.enqueueMany`, {}, { captureStackTrace: false })
    )
  },

  poll(this: AnyWithProps, jobId: JobId) {
    const self = this
    return Effect.flatMap(this.store, (store) =>
      store.getJob(jobId).pipe(
        Effect.orDie,
        Effect.flatMap(Option.match({
          onNone: () => Effect.succeedNone,
          onSome: (record) =>
            Effect.gen(function*() {
              const exit = record.exit === undefined
                ? Option.none()
                : Option.some(
                  yield* Schema.decodeUnknownEffect(self.exitSchema)(record.exit).pipe(
                    Effect.orDie
                  )
                )
              return Option.some({
                state: record.state,
                attemptsMade: record.attemptsMade,
                metadata: record.metadata,
                exit,
                failedReason: record.failedReason
              })
            })
        }))
      )).pipe(
        Effect.withSpan(`${this._tag}.poll`, { attributes: { jobId } }, { captureStackTrace: false })
      )
  },

  attempts(this: AnyWithProps, jobId: JobId) {
    const self = this
    return Effect.flatMap(this.store, (store) =>
      store.getAttempts(jobId).pipe(
        Effect.orDie,
        Effect.flatMap(Effect.forEach((attempt) =>
          Effect.map(
            attempt.exit === undefined
              ? Effect.succeedNone
              : Schema.decodeUnknownEffect(self.exitSchema)(attempt.exit).pipe(
                Effect.orDie,
                Effect.map(Option.some)
              ),
            (exit) => ({
              attempt: attempt.attempt,
              startedAt: attempt.startedAt,
              finishedAt: attempt.finishedAt,
              outcome: attempt.outcome,
              exit
            })
          )
        ))
      )).pipe(
        Effect.withSpan(`${this._tag}.attempts`, { attributes: { jobId } }, { captureStackTrace: false })
      )
  },

  awaitResult(
    this: AnyWithProps,
    jobId: JobId,
    options?: { readonly pollSchedule?: Schedule.Schedule<unknown> | undefined }
  ) {
    const self = this
    return Effect.gen(function*() {
      const schedule = options?.pollSchedule ?? defaultPollSchedule
      let sleep: Effect.Effect<unknown> | undefined
      while (true) {
        const status = yield* self.poll(jobId)
        if (Option.isNone(status)) {
          return yield* Effect.die(new JobNotFoundError({ jobId }))
        }
        const { exit, failedReason, state } = status.value
        if (state === "cancelled") {
          return yield* Effect.die(new JobCancelledError({ jobId }))
        }
        if (state === "completed" || state === "failed") {
          if (Option.isSome(exit)) {
            return yield* exit.value
          }
          return yield* Effect.die(
            new Error(
              `effect-mq: job "${jobId}" failed without a result${
                failedReason === undefined ? "" : `: ${failedReason}`
              }`
            )
          )
        }
        sleep ??= (yield* Schedule.toStepWithSleep(schedule))(void 0).pipe(
          Effect.catch(() =>
            Effect.die(`${self._tag}.awaitResult: poll schedule exhausted`)
          )
        )
        yield* sleep
      }
    }).pipe(
      Effect.withSpan(`${this._tag}.awaitResult`, { attributes: { jobId } }, { captureStackTrace: false })
    )
  },

  execute(this: AnyWithProps, fields: any, options?: EnqueueOptions) {
    return Effect.flatMap(
      this.enqueue(fields, options),
      (jobId) => this.awaitResult(jobId)
    )
  },

  retry(this: AnyWithProps, jobId: JobId) {
    return Effect.flatMap(this.store, (store) =>
      store.retry(jobId).pipe(
        Effect.catchTag("JobStoreError", (error) => Effect.die(error))
      )).pipe(
        Effect.withSpan(`${this._tag}.retry`, { attributes: { jobId } }, { captureStackTrace: false })
      )
  },

  cancel(this: AnyWithProps, jobId: JobId) {
    return Effect.flatMap(this.store, (store) =>
      store.cancel(jobId).pipe(
        Effect.catchTag("JobStoreError", (error) => Effect.die(error))
      )).pipe(
        Effect.withSpan(`${this._tag}.cancel`, { attributes: { jobId } }, { captureStackTrace: false })
      )
  },

  cancelByKey(this: AnyWithProps, key: string) {
    return Effect.flatMap(this.store, (store) =>
      store.cancelByDedupe(this._tag, key).pipe(
        Effect.catchTag("JobStoreError", (error) => Effect.die(error))
      )).pipe(
        Effect.withSpan(`${this._tag}.cancelByKey`, { attributes: { key } }, { captureStackTrace: false })
      )
  },

  promote(this: AnyWithProps, jobId: JobId) {
    return Effect.flatMap(this.store, (store) =>
      store.promote(jobId).pipe(
        Effect.catchTag("JobStoreError", (error) => Effect.die(error))
      )).pipe(
        Effect.withSpan(`${this._tag}.promote`, { attributes: { jobId } }, { captureStackTrace: false })
      )
  },

  schedule(this: AnyWithProps, key: string, options: ScheduleOptions<never>) {
    const self = this
    return Effect.gen(function*() {
      const hasCron = options.cron !== undefined
      const hasEvery = options.every !== undefined
      if (hasCron === hasEvery) {
        return yield* Effect.die(
          new Error(`effect-mq: schedule "${key}" must set exactly one of \`cron\` or \`every\``)
        )
      }
      const everyMs = options.every !== undefined ? Duration.toMillis(options.every) : undefined
      const now = yield* Clock.currentTimeMillis
      const nextRunAt = nextOccurrence(
        { cron: options.cron, tz: options.tz, everyMs },
        now,
        now
      )
      if (nextRunAt === undefined) {
        return yield* Effect.die(
          new Error(`effect-mq: schedule "${key}" has an invalid cron expression: "${options.cron}"`)
        )
      }
      const payload = self.payloadSchema.make(options.payload)
      const encoded = yield* Schema.encodeEffect(self.payloadJsonSchema)(payload).pipe(Effect.orDie)
      const scheduleKey = ScheduleKey(`${self._tag}/${key}`)
      const record: ScheduleRecord = {
        key: scheduleKey,
        jobName: self._tag,
        queue: self.queue,
        cron: options.cron,
        tz: options.tz,
        everyMs,
        payload: encoded,
        metadata: { ...self.metadata?.(payload), ...options.metadata },
        priority: options.priority ?? self.defaults.priority,
        attemptsMax: Math.max(1, options.attempts ?? self.defaults.attempts),
        backoff: options.backoff !== undefined
          ? normalizeBackoff(options.backoff)
          : self.defaults.backoff,
        keep: options.keep !== undefined ? normalizeKeep(options.keep) : self.defaults.keep,
        timeoutMs: options.timeout !== undefined
          ? Duration.toMillis(options.timeout)
          : self.defaults.timeoutMs,
        group: options.group,
        nextRunAt
      }
      const store = yield* self.store
      yield* store.upsertSchedule(record).pipe(Effect.orDie)
      return scheduleKey
    }).pipe(
      Effect.withSpan(`${this._tag}.schedule`, { attributes: { key } }, { captureStackTrace: false })
    )
  },

  unschedule(this: AnyWithProps, key: string) {
    return Effect.flatMap(this.store, (store) =>
      store.removeSchedule(ScheduleKey(`${this._tag}/${key}`)).pipe(Effect.orDie)).pipe(
        Effect.withSpan(`${this._tag}.unschedule`, { attributes: { key } }, { captureStackTrace: false })
      )
  },

  toLayer(
    this: AnyWithProps,
    handler: (payload: any, context: JobContext) => Effect.Effect<any, any, any>,
    options?: RegisterOptions
  ) {
    return Layer.effectDiscard(
      Effect.flatMap(Worker, (worker) => worker.register(this, handler, options))
    )
  }
}

const boundMethods = [
  "enqueue",
  "enqueueMany",
  "poll",
  "attempts",
  "awaitResult",
  "execute",
  "retry",
  "cancel",
  "cancelByKey",
  "promote",
  "schedule",
  "unschedule",
  "toLayer"
] as const

const makeProto = (options: {
  readonly _tag: string
  readonly queue: QueueName
  readonly store: Context.Key<any, StoreService>
  readonly payloadSchema: Schema.Top
  readonly payloadJsonSchema: Schema.Top
  readonly successSchema: Schema.Top
  readonly errorSchema: Schema.Top
  readonly exitSchema: Schema.Top
  readonly idempotencyKey: ((payload: any) => string) | undefined
  readonly dedupe: ((payload: any) => DedupeInput) | undefined
  readonly metadata: ((payload: any) => Readonly<Record<string, string>>) | undefined
  readonly retryable: ((error: any) => boolean) | undefined
  readonly defaults: ResolvedDefaults
}): any => {
  function JobDefinition() {}
  Object.setPrototypeOf(JobDefinition, Proto)
  Object.assign(JobDefinition, options)
  // Cosmetic only (job identity is `_tag`): function `name` is read-only for
  // assignment but configurable.
  Object.defineProperty(JobDefinition, "name", { value: options._tag, configurable: true })
  // Bind the API as own properties so methods survive destructuring
  // (`const { enqueue } = MyJob`) and passing as values.
  for (const key of boundMethods) {
    // SAFETY: every entry in `boundMethods` is a `this`-dependent function on
    // `Proto`; binding only fixes the receiver. The precise signatures are
    // re-declared by the public `Job` interface.
    const method = Proto[key] as (...args: ReadonlyArray<never>) => object
    Object.defineProperty(JobDefinition, key, {
      value: method.bind(JobDefinition),
      configurable: true
    })
  }
  return JobDefinition
}

/**
 * Define a job.
 *
 * @since 0.1.0
 */
export const make = <
  const Name extends string,
  Payload extends Schema.Struct.Fields | AnyStructSchema,
  Success extends Schema.Top = Schema.Void,
  Error extends ErrorInput = Schema.Never,
  StoreId = JobStore
>(
  name: Name,
  options: {
    /** The payload schema: a `Schema.Struct` or its bare fields object. */
    readonly payload: Payload
    /** Schema for the handler's success value (decodable via `awaitResult`/`attempts`). Default `Schema.Void`. */
    readonly success?: Success | undefined
    /**
     * Schema for the handler's typed failure — one schema, or a list of
     * schemas unioned for you (round-trips through storage). Default
     * `Schema.Never`.
     */
    readonly error?: Error | undefined
    /**
     * Derive a stable job id from the payload. Enqueueing the same key twice
     * while the first job still exists is a no-op (returns the existing id).
     */
    readonly idempotencyKey?:
      | ((
        payload: Payload extends Schema.Struct.Fields ? Schema.Struct.Type<Payload>
          : Payload["Type"]
      ) => string)
      | undefined
    /**
     * Derive a dedup key (and optional throttle/debounce/replace behavior)
     * from the payload. Unlike `idempotencyKey`, this never changes the job
     * id — see `DedupeInput` for the mode semantics.
     */
    readonly dedupe?:
      | ((
        payload: Payload extends Schema.Struct.Fields ? Schema.Struct.Type<Payload>
          : Payload["Type"]
      ) => DedupeInput)
      | undefined
    /**
     * Derive queryable business context from the payload (flat string map, so
     * every driver can index it). Merged with per-enqueue `metadata`.
     */
    readonly metadata?:
      | ((
        payload: Payload extends Schema.Struct.Fields ? Schema.Struct.Type<Payload>
          : Payload["Type"]
      ) => Readonly<Record<string, string>>)
      | undefined
    /**
     * When present, a typed handler failure for which this returns false
     * skips the remaining retry budget (see also `Job.unrecoverable`).
     */
    readonly retryable?:
      | ((
        error: ResolvedError<Error>["Type"]
      ) => boolean)
      | undefined
    /** The queue this job runs on. Default `"default"`. */
    readonly queue?: string | undefined
    /**
     * The store this job's runs live on (a `JobStore.named(...)` key).
     * Default: the default `JobStore`.
     */
    readonly store?: Context.Key<StoreId, StoreService> | undefined
    /** Default enqueue options (`delay`, `priority`, `attempts`, `backoff`, `keep`, `timeout`); per-enqueue options override. */
    readonly defaults?: JobOptions | undefined
  }
): Job<
  Name,
  Payload extends Schema.Struct.Fields ? Schema.Struct<Payload> : Payload,
  Success,
  ResolvedError<Error>,
  StoreId
> => {
  // SAFETY: `Schema.isSchema` discriminates the `Payload` union at runtime;
  // TypeScript cannot narrow an unresolved generic, so each branch asserts
  // the side the guard just proved.
  const payloadSchema = Schema.isSchema(options.payload)
    ? options.payload as AnyStructSchema
    : Schema.Struct(options.payload as Schema.Struct.Fields)
  const successSchema = options.success ?? Schema.Void
  // SAFETY: `Array.isArray` discriminates the `ErrorInput` union at runtime;
  // TypeScript cannot narrow an unresolved generic, so each branch asserts
  // the side the guard just proved.
  const errorSchema = options.error === undefined
    ? Schema.Never
    : Array.isArray(options.error)
    ? Schema.Union(options.error as ReadonlyArray<Schema.Top>)
    : options.error as Schema.Top
  // Wrapping the whole Exit schema in the JSON codec makes the *encoded* side
  // plain JSON (a live Exit/Cause instance would not survive serializing
  // drivers like Redis/Postgres).
  const exitSchema = Schema.toCodecJson(Schema.Exit(
    Schema.toCodecJson(successSchema),
    Schema.toCodecJson(errorSchema),
    Schema.Defect()
  ))
  return makeProto({
    _tag: name,
    queue: QueueName(options.queue ?? "default"),
    store: options.store ?? JobStore,
    payloadSchema,
    payloadJsonSchema: Schema.toCodecJson(payloadSchema),
    successSchema,
    errorSchema,
    exitSchema,
    idempotencyKey: options.idempotencyKey,
    dedupe: options.dedupe,
    metadata: options.metadata,
    retryable: options.retryable,
    defaults: {
      delayMs: options.defaults?.delay !== undefined
        ? Duration.toMillis(options.defaults.delay)
        : 0,
      priority: options.defaults?.priority ?? 0,
      attempts: Math.max(1, options.defaults?.attempts ?? 1),
      backoff: normalizeBackoff(options.defaults?.backoff),
      keep: normalizeKeep(options.defaults?.keep),
      timeoutMs: options.defaults?.timeout !== undefined
        ? Duration.toMillis(options.defaults.timeout)
        : undefined
    }
  })
}
