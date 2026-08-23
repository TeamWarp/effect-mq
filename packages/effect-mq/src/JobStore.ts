/**
 * The storage seam of effect-mq.
 *
 * `JobStore` is the minimal, storage-agnostic interface a queue backend must
 * implement. Every method must be atomic within the driver. The reference
 * implementation is `MemoryJobStore`; Postgres (via `effect-mq/drizzle`),
 * Redis, etc. drivers implement the same service.
 *
 * The store works entirely on *encoded* (JSON-safe) payloads and exits —
 * schema encoding/decoding happens in `Job` (producer side) and `Worker`
 * (consumer side), so drivers stay dumb.
 *
 * Multiple stores can coexist in one application via named store keys (see
 * `named`): each job definition binds to a store key, so business-critical
 * jobs can live in Postgres while disposable ones live elsewhere.
 *
 * @since 0.1.0
 */
import { Brand, Context, Cron, Data, Duration, type Effect, type Option, Predicate, Result } from "effect"

/**
 * The identifier of an enqueued job. Produced by `enqueue` (either
 * store-assigned or derived from a custom id / idempotency key).
 *
 * @since 0.1.0
 */
export type JobId = Brand.Branded<string, "effect-mq/JobId">

/**
 * Brand a raw string as a `JobId`.
 *
 * @since 0.1.0
 */
export const JobId: Brand.Constructor<JobId> = Brand.nominal<JobId>()

/**
 * The name of a queue.
 *
 * @since 0.1.0
 */
export type QueueName = Brand.Branded<string, "effect-mq/QueueName">

/**
 * Brand a raw string as a `QueueName`.
 *
 * @since 0.1.0
 */
export const QueueName: Brand.Constructor<QueueName> = Brand.nominal<QueueName>()

/**
 * The identifier of a repeatable-job schedule.
 *
 * @since 0.2.0
 */
export type ScheduleKey = Brand.Branded<string, "effect-mq/ScheduleKey">

/**
 * Brand a raw string as a `ScheduleKey`.
 *
 * @since 0.2.0
 */
export const ScheduleKey: Brand.Constructor<ScheduleKey> = Brand.nominal<ScheduleKey>()

/**
 * The lifecycle states of a job.
 *
 * - `waiting`: runnable now, ordered by (priority desc, enqueue order asc)
 * - `delayed`: must not run before `runAt`
 * - `active`: claimed by a worker holding a lock token
 * - `completed` / `failed`: terminal, with an encoded `Exit` stored
 *
 * @since 0.1.0
 */
export type JobState =
  | "waiting"
  | "delayed"
  | "active"
  | "completed"
  | "failed"
  | "cancelled"

/**
 * Retry backoff policy, persisted on the job record so any worker can route
 * retries consistently. Delay for attempt `n` (1-based):
 *
 * - `fixed`: `delayMs`
 * - `exponential`: `delayMs * factor ** (n - 1)` (factor defaults to 2)
 *
 * @since 0.1.0
 */
export interface BackoffPolicy {
  readonly _tag: "fixed" | "exponential"
  readonly delayMs: number
  readonly factor?: number | undefined
}

/**
 * Retention rule for one terminal state's records (per job name).
 *
 * @since 0.3.0
 */
export interface KeepStatePolicy {
  /** Keep at most this many terminal records (per name + state). */
  readonly count?: number | undefined
  /** Remove terminal records older than this many milliseconds. */
  readonly ageMs?: number | undefined
}

/**
 * Retention policy persisted on the record, split by terminal state —
 * completed jobs are usually noise, failed ones evidence. Applied by the
 * store after terminal acks (scoped to jobs with the same name and state),
 * and honoured by the store's periodic history sweep when one is configured.
 * An absent state keeps its records until the store's `historyTtl` ceiling
 * (forever without one). Producers accept a flat `{ count, age }` shorthand
 * and normalize it to all three states before it reaches a driver.
 *
 * @since 0.1.0
 */
export interface KeepPolicy {
  readonly completed?: KeepStatePolicy | undefined
  readonly failed?: KeepStatePolicy | undefined
  readonly cancelled?: KeepStatePolicy | undefined
}

/**
 * The terminal states retention applies to.
 *
 * @since 0.3.0
 */
export type TerminalState = "completed" | "failed" | "cancelled"

/**
 * Store-level retention ceiling: one duration for all terminal states, or a
 * per-state split (an absent state is never swept by the timer).
 *
 * @since 0.3.0
 */
export type HistoryTtlInput =
  | Duration.Input
  | {
    readonly completed?: Duration.Input | undefined
    readonly failed?: Duration.Input | undefined
    readonly cancelled?: Duration.Input | undefined
  }

/**
 * `HistoryTtlInput` normalized to milliseconds per terminal state.
 *
 * @since 0.3.0
 */
export interface HistoryTtlByState {
  readonly completed?: number | undefined
  readonly failed?: number | undefined
  readonly cancelled?: number | undefined
}

/**
 * Normalize a `HistoryTtlInput` to milliseconds per terminal state.
 *
 * @internal
 */
export const normalizeHistoryTtl = (
  input: HistoryTtlInput
): HistoryTtlByState => {
  if (Predicate.isObject(input) && !Duration.isDuration(input)) {
    if (!("completed" in input || "failed" in input || "cancelled" in input)) {
      // {} or a DurationObject would silently normalize to a 0ms ceiling and
      // wipe all history — refuse loudly; use "90 days"-style inputs.
      throw new Error(
        "effect-mq: historyTtl object must set at least one of completed/failed/cancelled"
      )
    }
    // SAFETY: Duration inputs (numbers, strings, bigints, tuples, Duration
    // instances, DurationObjects) never carry terminal-state keys, so this
    // is the per-state split form.
    const split = input as {
      readonly completed?: Duration.Input | undefined
      readonly failed?: Duration.Input | undefined
      readonly cancelled?: Duration.Input | undefined
    }
    return {
      completed: split.completed !== undefined ? Duration.toMillis(split.completed) : undefined,
      failed: split.failed !== undefined ? Duration.toMillis(split.failed) : undefined,
      cancelled: split.cancelled !== undefined ? Duration.toMillis(split.cancelled) : undefined
    }
  }
  // SAFETY: not the split form (checked above), so a plain Duration input.
  const ms = Duration.toMillis(input as Duration.Input)
  return { completed: ms, failed: ms, cancelled: ms }
}

/**
 * One run of a job, persisted so the full run history is durable and
 * inspectable — the storage-level analogue of `tapError` before a rerun,
 * extended to successes. Attempt numbers are monotonic per job and survive
 * `retry` (they are decoupled from the record's `attemptsMade` budget).
 *
 * @since 0.1.0
 */
export interface AttemptRecord {
  /** 1-based, monotonic per job (= previous ledger length + 1). */
  readonly attempt: number
  /** Claim time of this run (epoch millis). */
  readonly startedAt: number | undefined
  /** Ack/recovery time of this run (epoch millis). */
  readonly finishedAt: number
  readonly outcome: "completed" | "retried" | "failed" | "stalled" | "cancelled"
  /** Schema-encoded `Exit`; undefined for `stalled`. */
  readonly exit: unknown
}

/**
 * A job as persisted by the store. `payload` and `exit` are schema-encoded
 * (JSON-safe) values — the store never inspects them. The per-run ledger is
 * fetched separately via `getAttempts` so listings stay cheap.
 *
 * @since 0.1.0
 */
export interface JobRecord {
  readonly id: JobId
  readonly name: string
  readonly queue: QueueName
  readonly payload: unknown
  /** Flat, indexable projection of business context for querying/UIs. */
  readonly metadata: Readonly<Record<string, string>>
  readonly state: JobState
  readonly priority: number
  /** Total attempts allowed (including the first run). */
  readonly attemptsMax: number
  /** Attempts consumed in the current budget (reset by `retry`). */
  readonly attemptsMade: number
  readonly stalledCount: number
  readonly backoff: BackoffPolicy | undefined
  readonly keep: KeepPolicy | undefined
  /** Per-run execution time limit; the worker interrupts the handler past it. */
  readonly timeoutMs: number | undefined
  /**
   * Set by `cancel` on an active job; the owning worker interrupts the
   * handler on its next heartbeat and acks `Cancelled`.
   */
  readonly cancelRequested: boolean
  /** The dedup key this job was enqueued under, if any (see `DedupePolicy`). */
  readonly dedupeKey: string | undefined
  /** The producer's span context, restored as the handler span's parent. */
  readonly trace: TraceContext | undefined
  /** Epoch millis before which the job must not be claimed. */
  readonly runAt: number
  readonly enqueuedAt: number
  readonly processedAt: number | undefined
  readonly finishedAt: number | undefined
  /** Schema-encoded `Exit`, present for completed/failed jobs. */
  readonly exit: unknown
  /** Set when the store itself failed the job (e.g. exceeded stall limit). */
  readonly failedReason: string | undefined
}

/**
 * Deduplication policy carried on an enqueue request, scoped per job name.
 * Dedup NEVER changes the job id — ids come from the caller, the idempotency
 * key, or the store's id generator; the dedup key is a separate value with
 * its own lifecycle:
 *
 * - `{ key }` (no ttl): dedupe while the keyed job is pending (waiting,
 *   delayed, or active); a terminal keyed job frees the key.
 * - `{ key, ttlMs }`: throttle — at most one job per key per window,
 *   regardless of the keyed job's completion.
 * - `extend` (requires `ttlMs`): each deduplicated enqueue pushes the window
 *   out (debounce).
 * - `replace`: while the keyed job is still *delayed*, the new enqueue's
 *   payload/metadata/priority/attempts/backoff/keep/timeout/delay replace the
 *   existing job's (id and ledger preserved), and a `ttlMs` window is
 *   re-armed from the replace; in every other state normal dedup applies.
 *
 * A deduplicated (or replaced) enqueue returns the keyed job's id with
 * `duplicate: true`.
 *
 * @since 0.3.0
 */
export interface DedupePolicy {
  /** Non-empty; producers validate before the request reaches a driver. */
  readonly key: string
  readonly ttlMs: number | undefined
  readonly extend: boolean
  readonly replace: boolean
}

/**
 * The producer's span context, persisted at enqueue so the handler's span
 * can join the producing trace across processes (via `Tracer.externalSpan`).
 *
 * @since 0.4.0
 */
export interface TraceContext {
  readonly traceId: string
  readonly spanId: string
  readonly sampled: boolean
}

/**
 * @since 0.1.0
 */
export interface EnqueueRequest {
  /**
   * Custom/idempotency id. When a job with this id already exists (in any
   * state), the request is a no-op and the result has `duplicate: true`.
   * When `undefined` the store assigns a unique id.
   */
  readonly id: JobId | undefined
  readonly name: string
  readonly queue: QueueName
  readonly payload: unknown
  readonly metadata: Readonly<Record<string, string>>
  readonly priority: number
  readonly attemptsMax: number
  readonly backoff: BackoffPolicy | undefined
  readonly keep: KeepPolicy | undefined
  readonly timeoutMs: number | undefined
  /** Deduplicate against other enqueues sharing `dedupe.key` (same name). */
  readonly dedupe: DedupePolicy | undefined
  /** The producer's span context, for cross-process trace propagation. */
  readonly trace: TraceContext | undefined
  readonly delayMs: number
}

/**
 * Generator for store-assigned job ids, used when `EnqueueRequest.id` is
 * undefined (custom ids and idempotency keys always win, and schedule tick
 * ids stay slot-deterministic). May be effectful (e.g. draw from Effect's
 * `Random`). The store retries a bounded number of times when a generated id
 * collides with an existing job, then fails the enqueue with
 * `JobStoreError` — generators must have enough entropy that collisions are
 * pathological, not routine.
 *
 * @example `({ name }) => \`${name}_${ulid()}\``
 *
 * @since 0.2.0
 */
export type IdGenerator = (
  request: EnqueueRequest
) => string | Effect.Effect<string>

/**
 * @since 0.1.0
 */
export interface EnqueueResult {
  readonly id: JobId
  /** True when a job with this id already existed; nothing was modified. */
  readonly duplicate: boolean
}

/**
 * @since 0.1.0
 */
export interface ClaimOptions {
  readonly queue: QueueName
  /** Only jobs with these names may be claimed (the worker's registered handlers). */
  readonly names: ReadonlyArray<string>
  /** Worker-generated lock token; all subsequent acks must present it. */
  readonly token: string
  readonly lockDurationMs: number
}

/**
 * Result of a claim attempt. `Empty.nextRunAt` is the earliest `runAt` among
 * matching delayed jobs (so the worker knows how long to sleep), and
 * `wakeToken` is an opaque cursor for `awaitWake` so wake-ups that happen
 * between the claim and the wait are not lost.
 *
 * @since 0.1.0
 */
export type ClaimResult =
  | { readonly _tag: "Claimed"; readonly job: JobRecord }
  | {
    readonly _tag: "Empty"
    readonly nextRunAt: number | undefined
    readonly wakeToken: number
  }

/**
 * How a worker acknowledges a claimed job. Retry routing (backoff delay,
 * attempts accounting) is computed by the worker; the store only applies it.
 *
 * Every outcome appends an `AttemptRecord` to the job's ledger (`Complete` →
 * completed, `Retry` → retried, `Fail` → failed).
 *
 * @since 0.1.0
 */
export type AckOutcome =
  | { readonly _tag: "Complete"; readonly exit: unknown }
  | { readonly _tag: "Retry"; readonly delayMs: number; readonly exit: unknown }
  | { readonly _tag: "Fail"; readonly exit: unknown }
  | { readonly _tag: "Cancelled" }

/**
 * Filters and pagination for `list`. Results are ordered newest-first
 * (`enqueuedAt` desc, then id desc); pass the returned `cursor` back to get
 * the next page.
 *
 * @since 0.1.0
 */
export interface ListOptions {
  readonly queue?: QueueName | undefined
  readonly name?: string | undefined
  readonly states?: ReadonlyArray<JobState> | undefined
  /** Every entry must match the record's metadata exactly (AND semantics). */
  readonly metadata?: Readonly<Record<string, string>> | undefined
  readonly cursor?: string | undefined
  /** Page size; default 50. */
  readonly limit?: number | undefined
}

/**
 * @since 0.1.0
 */
export interface ListResult {
  readonly items: ReadonlyArray<JobRecord>
  /** Present when more items may exist; pass back via `ListOptions.cursor`. */
  readonly cursor: string | undefined
}

/**
 * A repeatable-job schedule as persisted by the store. Exactly one of `cron`
 * (with optional IANA `tz`) or `everyMs` is set. The payload is stored
 * schema-encoded, like job payloads. `nextRunAt` is maintained by the worker
 * sweep via `advanceSchedule`; ticks enqueue with the deterministic id
 * `sched/<key>/<slot>`, so concurrent sweepers dedup naturally.
 *
 * @since 0.2.0
 */
export interface ScheduleRecord {
  readonly key: ScheduleKey
  readonly jobName: string
  readonly queue: QueueName
  readonly cron: string | undefined
  readonly tz: string | undefined
  readonly everyMs: number | undefined
  readonly payload: unknown
  readonly metadata: Readonly<Record<string, string>>
  readonly priority: number
  readonly attemptsMax: number
  readonly backoff: BackoffPolicy | undefined
  readonly keep: KeepPolicy | undefined
  readonly timeoutMs: number | undefined
  /** Epoch millis of the next occurrence to enqueue. */
  readonly nextRunAt: number
}

/**
 * Result of a heartbeat: locks that could not be extended (lost to stall
 * recovery or another worker) and active jobs with a pending cancel request
 * (the worker must interrupt them and ack `Cancelled`).
 *
 * @since 0.2.0
 */
export interface ExtendLocksResult {
  readonly lost: ReadonlyArray<JobId>
  readonly cancelRequested: ReadonlyArray<JobId>
}

/**
 * A transient or fatal driver error (connection loss, serialization, etc.).
 *
 * @since 0.1.0
 */
export class JobStoreError extends Data.TaggedError("JobStoreError")<{
  readonly message: string
  readonly cause?: unknown
}> {}

/**
 * Tag-based guard (safe across duplicate module copies, unlike `instanceof`).
 *
 * @since 0.1.0
 */
// oxlint-disable-next-line anti-slop/no-unknown-parameters -- a type guard IS the boundary parser; `unknown` input is its purpose
export const isJobStoreError = (u: unknown): u is JobStoreError =>
  Predicate.hasProperty(u, "_tag") && u._tag === "JobStoreError"

// Identity-based marking (WeakSet, works on frozen error instances) so the
// error keeps its declared type and schema round-trip untouched.
const unrecoverableRegistry = new WeakSet<object>()

/**
 * Mark an error value as unrecoverable: when a handler fails with it, the
 * worker skips the remaining retry budget and fails the job immediately. The
 * error itself is returned unchanged, so typed error channels and schemas
 * are unaffected. Also exported as `Job.unrecoverable`.
 *
 * Marking is identity-based, so it only works for object errors — a
 * primitive (string/number) failure is returned unmarked and retries
 * normally; use the definition-level `retryable` predicate for those.
 *
 * @since 0.2.0
 */
export const unrecoverable = <E>(error: E): E => {
  if (Object(error) === error) {
    // SAFETY: `Object(x) === x` is true exactly for object values, which is
    // what WeakSet membership requires.
    unrecoverableRegistry.add(error as object)
  }
  return error
}

/**
 * Whether `unrecoverable` marked this value.
 *
 * @internal
 */
// oxlint-disable-next-line anti-slop/no-unknown-parameters -- boundary classifier over arbitrary error values
export const isMarkedUnrecoverable = (u: unknown): boolean =>
  // SAFETY: `Object(u) === u` short-circuits to false for primitives, so the
  // assertion only ever passes object values to the WeakSet.
  Object(u) === u && unrecoverableRegistry.has(u as object)

/**
 * The next occurrence of a schedule strictly after `now`. `fromSlot` anchors
 * `everyMs` schedules (occurrences stay on the `slot + k * every` grid).
 * Returns undefined for an invalid cron expression.
 *
 * @internal
 */
export const nextOccurrence = (
  schedule: Pick<ScheduleRecord, "cron" | "tz" | "everyMs">,
  fromSlot: number,
  now: number
): number | undefined => {
  if (schedule.cron !== undefined) {
    const parsed = Cron.parse(schedule.cron, schedule.tz)
    if (Result.isFailure(parsed)) return undefined
    // Cron.next throws for parseable-but-unsatisfiable expressions (e.g.
    // "0 0 30 2 *"); treat those as invalid rather than defecting the sweep.
    try {
      return Cron.next(parsed.success, new Date(Math.max(fromSlot, now))).getTime()
    } catch {
      return undefined
    }
  }
  if (schedule.everyMs !== undefined && schedule.everyMs > 0) {
    const behind = Math.max(0, now - fromSlot)
    const steps = Math.floor(behind / schedule.everyMs) + 1
    return fromSlot + steps * schedule.everyMs
  }
  return undefined
}


/**
 * The presented lock token no longer owns the job (it stalled and was
 * recovered, or another worker claimed it).
 *
 * @since 0.1.0
 */
export class LockLostError extends Data.TaggedError("LockLostError")<{
  readonly jobId: JobId
}> {}

/**
 * @since 0.1.0
 */
export class JobNotFoundError extends Data.TaggedError("JobNotFoundError")<{
  readonly jobId: JobId
}> {}

/**
 * `retry` was called on a job that is not in the `failed` state.
 *
 * @since 0.1.0
 */
export class JobNotRetryableError extends Data.TaggedError("JobNotRetryableError")<{
  readonly jobId: JobId
  readonly state: JobState
}> {}

/**
 * `cancel` was called on a job that is already terminal.
 *
 * @since 0.2.0
 */
export class JobNotCancellableError extends Data.TaggedError("JobNotCancellableError")<{
  readonly jobId: JobId
  readonly state: JobState
}> {}

/**
 * `promote` was called on a job that is not in the `delayed` state.
 *
 * @since 0.2.0
 */
export class JobNotPromotableError extends Data.TaggedError("JobNotPromotableError")<{
  readonly jobId: JobId
  readonly state: JobState
}> {}

/**
 * Raised (as a defect) by `awaitResult` when the awaited job was cancelled.
 *
 * @since 0.2.0
 */
export class JobCancelledError extends Data.TaggedError("JobCancelledError")<{
  readonly jobId: JobId
}> {}

/**
 * The service shape every store implements.
 *
 * @since 0.1.0
 */
export interface Service {
  /**
   * Insert a job. Routing: `delayMs > 0` lands in `delayed`, otherwise
   * `waiting`. Duplicate ids are a silent no-op (see `EnqueueRequest.id`).
   */
  readonly enqueue: (
    request: EnqueueRequest
  ) => Effect.Effect<EnqueueResult, JobStoreError>

  /**
   * Atomically: promote due delayed jobs, then claim the best runnable job
   * matching `queue` + `names` (highest priority first, FIFO within a
   * priority), locking it with `token` for `lockDurationMs`.
   */
  readonly claim: (
    options: ClaimOptions
  ) => Effect.Effect<ClaimResult, JobStoreError>

  /**
   * Acknowledge a claimed job. Verifies the lock token, releases the lock,
   * increments `attemptsMade`, appends to the attempts ledger, then applies
   * the outcome (`Complete`/`Fail` are terminal and apply the record's `keep`
   * policy; `Retry` re-queues after `delayMs`).
   */
  readonly ack: (
    id: JobId,
    token: string,
    outcome: AckOutcome
  ) => Effect.Effect<void, JobStoreError | JobNotFoundError | LockLostError>

  /**
   * Return a claimed job to `waiting` without consuming an attempt or
   * recording a ledger entry (used on worker shutdown).
   */
  readonly release: (
    id: JobId,
    token: string
  ) => Effect.Effect<void, JobStoreError | JobNotFoundError | LockLostError>

  /**
   * Heartbeat: extend the given locks. Returns the ids whose lock could NOT
   * be extended (lost to stall recovery or another worker).
   */
  readonly extendLocks: (
    locks: ReadonlyArray<{ readonly id: JobId; readonly token: string }>,
    durationMs: number
  ) => Effect.Effect<ExtendLocksResult, JobStoreError>

  /**
   * Sweep active jobs whose lock has expired. Each recovered job gets
   * `stalledCount + 1` and a `stalled` ledger entry; jobs exceeding
   * `maxStalledCount` are failed (`failed: true` in the result), the rest
   * return to `waiting`.
   */
  readonly recoverStalled: (options: {
    readonly maxStalledCount: number
  }) => Effect.Effect<
    ReadonlyArray<{ readonly id: JobId; readonly failed: boolean }>,
    JobStoreError
  >

  /**
   * Resolve when new work *may* be runnable for the given queues since the
   * `wakeToken` observed by a previous `claim`. Spurious wake-ups are fine;
   * callers must combine with their own timeout. Must be interruptible.
   * Polling-only drivers may never resolve.
   */
  readonly awaitWake: (
    queues: ReadonlyArray<QueueName>,
    wakeToken: number
  ) => Effect.Effect<void, JobStoreError>

  readonly getJob: (
    id: JobId
  ) => Effect.Effect<Option.Option<JobRecord>, JobStoreError>

  /** The job's run ledger, oldest first. Empty for unknown ids. */
  readonly getAttempts: (
    id: JobId
  ) => Effect.Effect<ReadonlyArray<AttemptRecord>, JobStoreError>

  /** Query jobs (newest first) — the data layer for dashboards/UIs. */
  readonly list: (
    options: ListOptions
  ) => Effect.Effect<ListResult, JobStoreError>

  /**
   * Re-run a failed job: back to `waiting` with a fresh attempt budget
   * (`attemptsMade`/`stalledCount` reset, terminal fields cleared). The
   * attempts ledger is preserved and keeps numbering monotonically.
   */
  readonly retry: (
    id: JobId
  ) => Effect.Effect<
    void,
    JobStoreError | JobNotFoundError | JobNotRetryableError
  >

  /**
   * Cancel a job. Waiting/delayed jobs become terminal (`cancelled`)
   * immediately; active jobs get `cancelRequested` set, and the owning
   * worker interrupts the handler on its next heartbeat. Terminal jobs fail
   * with `JobNotCancellableError`.
   */
  readonly cancel: (
    id: JobId
  ) => Effect.Effect<
    void,
    JobStoreError | JobNotFoundError | JobNotCancellableError
  >

  /**
   * Cancel whatever pending job is registered under a dedup key
   * (name-scoped): pending states are cancelled exactly like `cancel`
   * (waiting/delayed become terminal, active gets the heartbeat flag).
   * Returns false when no pending job holds the key — idempotent by design,
   * so "cancel it if anything is scheduled" needs no existence check.
   *
   * @since 0.4.0
   */
  readonly cancelByDedupe: (
    name: string,
    key: string
  ) => Effect.Effect<boolean, JobStoreError>

  /** Move a delayed job to `waiting` now. */
  readonly promote: (
    id: JobId
  ) => Effect.Effect<
    void,
    JobStoreError | JobNotFoundError | JobNotPromotableError
  >

  /**
   * Durably pause a queue: claims return `Empty` until `resume` (delayed
   * jobs still promote to `waiting`, they just aren't handed out). Affects
   * every worker on the store.
   */
  readonly pause: (queue: QueueName) => Effect.Effect<void, JobStoreError>

  /** Undo `pause` and wake idle workers. */
  readonly resume: (queue: QueueName) => Effect.Effect<void, JobStoreError>

  readonly pausedQueues: () => Effect.Effect<
    ReadonlyArray<QueueName>,
    JobStoreError
  >

  /** Create or replace a repeatable-job schedule (keyed by `schedule.key`). */
  readonly upsertSchedule: (
    schedule: ScheduleRecord
  ) => Effect.Effect<void, JobStoreError>

  /** Remove a schedule. Returns false when the key does not exist. */
  readonly removeSchedule: (
    key: ScheduleKey
  ) => Effect.Effect<boolean, JobStoreError>

  readonly listSchedules: (options?: {
    readonly jobName?: string | undefined
    readonly queue?: QueueName | undefined
  }) => Effect.Effect<ReadonlyArray<ScheduleRecord>, JobStoreError>

  /** Schedules whose `nextRunAt` is due (per the Effect `Clock`). */
  readonly dueSchedules: () => Effect.Effect<
    ReadonlyArray<ScheduleRecord>,
    JobStoreError
  >

  /**
   * Advance a schedule's `nextRunAt` from `expectedRunAt` to `nextRunAt`
   * (conditional, so concurrent sweepers cannot regress it).
   */
  readonly advanceSchedule: (
    key: ScheduleKey,
    expectedRunAt: number,
    nextRunAt: number
  ) => Effect.Effect<void, JobStoreError>

  readonly counts: (
    queue?: QueueName
  ) => Effect.Effect<Record<JobState, number>, JobStoreError>

  /** Remove a job (and its ledger). Refuses (returns false) when active. */
  readonly remove: (id: JobId) => Effect.Effect<boolean, JobStoreError>
}

/**
 * The default store key. Jobs without an explicit `store` binding use this.
 *
 * @since 0.1.0
 */
export class JobStore extends Context.Service<JobStore, Service>()(
  "effect-mq/JobStore"
) {}

/**
 * Phantom identifier for a named store — appears in `R` so the type system
 * enforces that the right store layer is provided.
 *
 * @since 0.1.0
 */
export interface Named<in out Name extends string> {
  readonly "~effect-mq/JobStore/Named": Name
}

/**
 * Create a named store key. Jobs bound to it (via `Job.make`'s `store`
 * option) require it in `R` instead of the default `JobStore`, letting
 * different jobs run on different storage infrastructure:
 *
 * ```ts
 * const Durable = JobStore.named("durable")     // -> Postgres in prod
 * const Ephemeral = JobStore.named("ephemeral") // -> Redis in prod
 * ```
 *
 * Keys are identified by their name string: two `named("durable")` calls are
 * interchangeable.
 *
 * @since 0.1.0
 */
export const named = <const Name extends string>(
  name: Name
): Context.Key<Named<Name>, Service> =>
  Context.Service<Named<Name>, Service>(`effect-mq/JobStore/${name}`)

/**
 * Any store key — the default `JobStore` or a `named` one.
 *
 * @since 0.1.0
 */
export type AnyKey = Context.Key<any, Service>
