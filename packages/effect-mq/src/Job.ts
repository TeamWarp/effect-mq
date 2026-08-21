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
import { Clock, type Context, Duration, Effect, type Exit, Layer, Option, Predicate, Schedule, Schema } from "effect"
import {
  type BackoffPolicy,
  type DedupePolicy,
  type KeepStatePolicy,
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
  readonly key: string
  readonly ttl?: Duration.Input | undefined
  readonly extend?: boolean | undefined
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
 * @since 0.1.0
 */
export interface EnqueueOptions extends JobOptions {
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

interface ResolvedDefaults {
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
  readonly cron?: string | undefined
  readonly tz?: string | undefined
  readonly every?: Duration.Input | undefined
  readonly payload: PayloadInput
  /** Queryable business context, merged over the definition's `metadata`. */
  readonly metadata?: Readonly<Record<string, string>> | undefined
  readonly priority?: number | undefined
  readonly attempts?: number | undefined
  readonly backoff?: BackoffInput | undefined
  readonly keep?: KeepInput | undefined
  readonly timeout?: Duration.Input | undefined
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
  readonly outcome: "completed" | "retried" | "failed" | "stalled" | "cancelled"
  /** Absent for `stalled` and `cancelled` entries. */
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

  /** Run a delayed job now. */
  readonly promote: (
    jobId: JobId
  ) => Effect.Effect<void, JobNotFoundError | JobNotPromotableError, StoreId>

  /**
   * Create or replace a durable repeatable schedule for this job. Ticks
   * enqueue with a slot-deterministic id, so schedules are exactly-once per
   * occurrence across all workers (assuming history retention windows
   * comfortably exceed the sweep interval — a pruned tick job cannot dedup a
   * pathologically stale sweeper). Missed occurrences (downtime) collapse
   * into a single catch-up run.
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

const normalizeBackoff = (input: BackoffInput | undefined): BackoffPolicy | undefined =>
  input === undefined ? undefined : {
    _tag: input.type,
    delayMs: Duration.toMillis(input.delay),
    factor: input.factor
  }

const normalizeKeepState = (input: KeepStateInput): KeepStatePolicy => ({
  count: input.count,
  ageMs: input.age !== undefined ? Duration.toMillis(input.age) : undefined
})

const normalizeKeep = (input: KeepInput | undefined): KeepPolicy | undefined => {
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
      return Schema.encodeEffect(this.payloadJsonSchema)(payload).pipe(
        Effect.orDie,
        Effect.flatMap((encoded) =>
          Effect.flatMap(this.store, (store) =>
            store.enqueue({
              id,
              name: this._tag,
              queue: options?.queue !== undefined
                ? QueueName(options.queue)
                : this.queue,
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
              delayMs: options?.delay !== undefined
                ? Duration.toMillis(options.delay)
                : this.defaults.delayMs
            }))
        ),
        Effect.orDie,
        Effect.map((result) => result.id)
      )
    }).pipe(
      Effect.withSpan(`${this._tag}.enqueue`, {}, { captureStackTrace: false })
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
  "poll",
  "attempts",
  "awaitResult",
  "execute",
  "retry",
  "cancel",
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
  Error extends Schema.Top = Schema.Never,
  StoreId = JobStore
>(
  name: Name,
  options: {
    readonly payload: Payload
    readonly success?: Success | undefined
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
        error: Error["Type"]
      ) => boolean)
      | undefined
    /** The queue this job runs on. Default `"default"`. */
    readonly queue?: string | undefined
    /**
     * The store this job's runs live on (a `JobStore.named(...)` key).
     * Default: the default `JobStore`.
     */
    readonly store?: Context.Key<StoreId, StoreService> | undefined
    readonly defaults?: JobOptions | undefined
  }
): Job<
  Name,
  Payload extends Schema.Struct.Fields ? Schema.Struct<Payload> : Payload,
  Success,
  Error,
  StoreId
> => {
  // SAFETY: `Schema.isSchema` discriminates the `Payload` union at runtime;
  // TypeScript cannot narrow an unresolved generic, so each branch asserts
  // the side the guard just proved.
  const payloadSchema = Schema.isSchema(options.payload)
    ? options.payload as AnyStructSchema
    : Schema.Struct(options.payload as Schema.Struct.Fields)
  const successSchema = options.success ?? Schema.Void
  const errorSchema = options.error ?? Schema.Never
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
