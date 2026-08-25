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
 * - `waiting-children`: a flow parent parked until its children settle (see
 *   `AckOutcome`'s `FanOut`); never claimable, not terminal
 * - `completed` / `failed`: terminal, with an encoded `Exit` stored
 *
 * @since 0.1.0
 */
export type JobState =
  | "waiting"
  | "delayed"
  | "active"
  | "waiting-children"
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
  readonly outcome: "completed" | "retried" | "failed" | "stalled" | "cancelled" | "fanned-out"
  /**
   * Schema-encoded `Exit`; undefined for `stalled`, `cancelled`, and
   * `fanned-out` entries, and for `failed` entries written by a store-side
   * settle (a fail-fast flow parent) rather than a handler run.
   */
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
  /** Present on flow children: the link back to their parent flow. */
  readonly parent: ParentEnvelope | undefined
  /** Present on flow parents once their manifest landed (see `FlowState`). */
  readonly flow: FlowState | undefined
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
  /**
   * Whether the enqueue explicitly scheduled the job for the future
   * (`delay`/`at`). Drives the worker's `traceLinking: "auto"` policy:
   * immediate work continues the producer trace, future work starts its own
   * trace with a causal link.
   */
  readonly delayed: boolean
}

/**
 * The persisted link from a flow child job to its parent flow. Attached by
 * the flow runtime at fan-out (never by producers). Its presence puts the
 * job under the outbox invariant: the child's store appends its report to
 * the outbox with every terminal transition (see `OutboxEntry`), and a
 * worker's relay delivers it into the parent's store (see
 * `Worker.layer({ flows })`).
 *
 * @since 0.6.0
 */
export interface ParentEnvelope {
  /** The flow definition's name (`Flow.make(name, ...)`). */
  readonly flowName: string
  /** The parent job's id in the parent store. */
  readonly flowId: JobId
  /** This child's key, unique within the flow (the idempotency mechanism). */
  readonly childKey: string
  /** The parent store's context-key string, for cross-store report routing. */
  readonly parentStoreKey: string
  /**
   * This child's nesting level: 1 for children of a top-level flow, one
   * more per level of nesting. Carried explicitly (never parsed out of
   * ids — user keys are arbitrary strings) so the fan-out depth cap can
   * catch cyclic definitions.
   */
  readonly depth: number
}

/**
 * Flow bookkeeping persisted on a parent job by the `FanOut` ack. Its
 * presence IS the phase marker: absent means the parent has not fanned out
 * yet (a claim dispatches `fanOut`); present means the manifest landed (a
 * claim dispatches `collect`, and a re-run can never fan out twice).
 *
 * The four counters always sum to the manifest size: applied reports move
 * one child from `pending` to its outcome bucket, and settle-time marking
 * (fail-fast, parent cancel) moves every remaining `pending` child to
 * `cancelled` in the same atomic op. `collect` reads its tallies from here
 * without touching a single dependency row.
 *
 * @since 0.6.0
 */
export interface FlowState {
  /** When true, the first failed child report settles the parent as failed. */
  readonly failFast: boolean
  /** Children whose result has not been recorded yet. */
  readonly pending: number
  readonly completed: number
  readonly failed: number
  readonly cancelled: number
}

/**
 * One child of a flow fan-out, persisted on its dependency row so the flow
 * sweeper can (re-)enqueue the child from storage alone after any crash.
 * `request.id` is the deterministic flow child id — derived from the parent
 * store key, flow id, and child key, so re-enqueues are idempotent — and
 * `request.parent` carries the envelope.
 *
 * @since 0.6.0
 */
export interface FlowChildSpec {
  readonly childKey: string
  /** The CHILD store's context-key string (children may live elsewhere). */
  readonly storeKey: string
  readonly request: EnqueueRequest
}

/**
 * A dependency row: one child's status and result as recorded in the PARENT
 * store. `exit` is the child's schema-encoded exit; `failedReason` carries
 * store-side child failures (stall exhaustion, a nested parent's fail-fast
 * settle) that never produced an exit. `cascaded` marks that a cancel no
 * longer needs to be delivered into the child's store (set by
 * `markChildrenCascaded`, or immediately when the recorded outcome came
 * FROM the child's store).
 *
 * @since 0.6.0
 */
export interface FlowChildRecord {
  readonly flowId: JobId
  readonly childKey: string
  readonly name: string
  readonly storeKey: string
  readonly childJobId: JobId
  readonly status: "pending" | "completed" | "failed" | "cancelled"
  readonly exit: unknown
  readonly failedReason: string | undefined
  readonly cascaded: boolean
}

/**
 * An idempotent child-result report delivered into the parent store — by a
 * worker's outbox relay (the push path) or synthesized by the flow sweeper
 * from child-store state (the reconcile path). Both may deliver the same
 * report; the dependency row's state dedups them.
 *
 * @since 0.6.0
 */
export interface FlowChildReport {
  readonly flowId: JobId
  readonly childKey: string
  readonly outcome: "completed" | "failed" | "cancelled"
  /** The child's schema-encoded exit; undefined for store-side failures. */
  readonly exit: unknown
  /** Present when the child was failed store-side (no exit exists). */
  readonly failedReason: string | undefined
}

/**
 * One undelivered child-result report in a CHILD store's outbox.
 *
 * The outbox invariant every driver must uphold: whenever a store operation
 * moves a job carrying a `parent` envelope INTO a terminal state — a
 * `Complete`/`Fail`/`Cancelled` ack, stall exhaustion, a direct `cancel` of
 * a waiting/delayed child, a cancel honoured during release or stall
 * recovery, or a fail-fast settle of a NESTED flow parent (its terminal
 * transition happens store-side, with no ack) — the same atomic operation
 * appends the corresponding report here. The worker's relay then drains
 * the outbox into the parent store in batches and deletes what it
 * delivered. Because dependency rows dedup redelivery, the relay needs no
 * leases: crash anywhere and the entries are simply delivered again.
 *
 * `remove` appends nothing (an operator override), and a `FanOut` ack
 * appends nothing (`waiting-children` is not terminal).
 *
 * @since 0.6.0
 */
export interface OutboxEntry {
  /** Store-assigned, opaque; pass back to `deleteOutbox` verbatim. */
  readonly id: string
  readonly flowName: string
  /** The parent store's context-key string, for relay routing. */
  readonly parentStoreKey: string
  readonly report: FlowChildReport
}

/**
 * The flow sweeper's work list, scoped by parent state so settled flows
 * never re-drive work:
 *
 * - `reconcile`: for parents still in `waiting-children`, dependency rows
 *   `pending` longer than the caller's threshold, with their stored specs —
 *   the sweeper checks the child's store and either enqueues the child
 *   (missing) or synthesizes its report (terminal).
 * - `cascade`: rows marked `cancelled` by a settle but not yet delivered as
 *   real cancels into their child store (any parent state).
 *
 * @since 0.6.0
 */
export interface FlowSweepWork {
  readonly reconcile: ReadonlyArray<{
    readonly flowId: JobId
    readonly children: ReadonlyArray<FlowChildSpec>
  }>
  readonly cascade: ReadonlyArray<{
    readonly flowId: JobId
    readonly children: ReadonlyArray<{
      readonly childKey: string
      readonly storeKey: string
      readonly childJobId: JobId
    }>
  }>
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
  /** Flow-parent link, set only by the flow runtime (opaque to the store). */
  readonly parent: ParentEnvelope | undefined
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
 * completed, `Retry` → retried, `Fail` → failed, `FanOut` → fanned-out).
 *
 * `FanOut` (flow parents only) atomically: persists `FlowState` (phase
 * marker + policy + `pending = children.length`), inserts one `pending`
 * dependency row per child (spec stored, so crash recovery needs only the
 * parent store), and parks the parent in `waiting-children` — or, for an
 * empty spec, settles it straight to `waiting`. It does NOT consume an
 * attempt (a phase transition is not a completed run), and it does NOT
 * enqueue the children (the flow runtime and sweeper own that). A parent
 * whose `flow` is already present keeps its persisted manifest untouched —
 * the new children are ignored and the state transition follows the existing
 * `pending` count, so a double fan-out cannot duplicate children.
 *
 * @since 0.1.0
 */
export type AckOutcome =
  | { readonly _tag: "Complete"; readonly exit: unknown }
  | { readonly _tag: "Retry"; readonly delayMs: number; readonly exit: unknown }
  | { readonly _tag: "Fail"; readonly exit: unknown }
  | { readonly _tag: "Cancelled" }
  | {
    readonly _tag: "FanOut"
    readonly failFast: boolean
    readonly children: ReadonlyArray<FlowChildSpec>
  }

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
 * sweep via the atomic `tickSchedule` (CAS + insert + advance in one op),
 * with tick jobs using the deterministic id `sched/<key>/<slot>`; concurrent
 * sweepers lose the CAS, so each occurrence fires exactly once.
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
  /**
   * Ownership label for declarative reconciliation (`JobSchedules.layer`):
   * a reconciler only ever prunes schedules carrying ITS group. Unlabeled
   * schedules (plain `.schedule()` calls) are never pruned.
   */
  readonly group: string | undefined
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
   * Insert many jobs in bulk: one store round trip per chunk of plain items
   * (drivers chunk large batches); items carrying a dedup key run through
   * the single-enqueue decision tree individually, in order. Results align
   * positionally with the requests; each item carries full single-enqueue
   * semantics (id dedup, dedup keys, delayed routing).
   *
   * The batch is NOT one transaction — items are independent, and a failure
   * may leave a *subset* (not necessarily a prefix) applied. Safe under
   * at-least-once: re-running a batch whose ids are deterministic skips
   * what already landed, but items with store-assigned ids may re-insert.
   * FIFO order within a priority holds except for an item whose
   * auto/generated id collides with an existing row — it re-draws and lands
   * after its batch-mates.
   *
   * @since 0.4.0
   */
  readonly enqueueMany: (
    requests: ReadonlyArray<EnqueueRequest>
  ) => Effect.Effect<ReadonlyArray<EnqueueResult>, JobStoreError>

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
   * policy; `Retry` re-queues after `delayMs`). Terminal outcomes for jobs
   * carrying a `parent` envelope also append an `OutboxEntry` atomically.
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
   * return to `waiting`. Stall-exhausting a job that carries a `parent`
   * envelope appends its failed report to the outbox atomically.
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
   * attempts ledger is preserved and keeps numbering monotonically. A flow
   * parent's `flow` field and dependency rows survive: a retried fail-fast
   * flow re-enters the `collect` phase with its recorded (mixed) results —
   * it can never fan out twice.
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
   * worker interrupts the handler on its next heartbeat. A `waiting-children`
   * flow parent settles to `cancelled` AND flips its remaining `pending`
   * dependency rows to `cancelled` (not `cascaded`) in the same atomic op —
   * the flow sweeper then delivers real cancels into the child stores, and
   * late child reports find their row terminal and drop. Cancelling a job
   * that itself carries a `parent` envelope appends its cancelled report to
   * the outbox atomically. Terminal jobs fail with `JobNotCancellableError`.
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
    /** Only schedules carrying this ownership group. */
    readonly group?: string | undefined
  }) => Effect.Effect<ReadonlyArray<ScheduleRecord>, JobStoreError>

  /** Schedules whose `nextRunAt` is due (per the Effect `Clock`). */
  readonly dueSchedules: () => Effect.Effect<
    ReadonlyArray<ScheduleRecord>,
    JobStoreError
  >

  /**
   * Atomically claim one schedule occurrence: iff the schedule's `nextRunAt`
   * still equals `expectedRunAt`, insert the tick job AND advance to
   * `nextRunAt` in the same transaction, returning true. A stale sweeper's
   * tick returns false without inserting — exactly-once per occurrence even
   * when the previous slot's job row has been pruned by retention.
   *
   * @since 0.4.0
   */
  readonly tickSchedule: (
    key: ScheduleKey,
    expectedRunAt: number,
    nextRunAt: number,
    request: EnqueueRequest
  ) => Effect.Effect<boolean, JobStoreError>

  /**
   * Advance a schedule's `nextRunAt` from `expectedRunAt` to `nextRunAt`
   * (conditional, so concurrent sweepers cannot regress it).
   */
  readonly advanceSchedule: (
    key: ScheduleKey,
    expectedRunAt: number,
    nextRunAt: number
  ) => Effect.Effect<void, JobStoreError>

  /**
   * Record a batch of child outcomes on their dependency rows — idempotent
   * and atomic, results positional. Each report applies only when its row
   * is still `pending` (`applied: false` for duplicate, late, or unknown
   * reports); an applied report moves the child from the parent's `pending`
   * counter to its outcome counter and marks the row `cascaded` (the
   * outcome came from the child's store — no cancel needs delivering). A
   * batch may span flows.
   *
   * Per flow, all row updates apply BEFORE the settle decision, and the
   * parent settles at most once per batch (`parentSettled: true` on the
   * report that decided it): when `pending` hits zero, `waiting-children` →
   * `waiting` (runnable now, phase `collect`) — or on the first applied
   * `failed` report in batch order under the fail-fast policy, which
   * instead settles the parent terminally `failed` (store-side,
   * `failedReason` set, no exit — like stall exhaustion) and flips every
   * remaining `pending` row to `cancelled`/not-`cascaded` in the same
   * atomic op. Fail-fast wins the tie when one report triggers both rules.
   *
   * Lock ordering (drivers MUST follow it): dependency rows first, the
   * parent row second — reports, fail-fast marking, and cancel marking all
   * take locks in this order, so report-vs-settle cannot deadlock.
   *
   * Drivers may process very large batches in atomic sub-batches (the
   * Redis driver chunks at 500); the apply-all-before-settle rule then
   * holds per sub-batch. Worker relays never exceed one page (500), so
   * this only shows on direct store calls with larger batches.
   *
   * @since 0.6.0
   */
  readonly recordChildResults: (
    reports: ReadonlyArray<FlowChildReport>
  ) => Effect.Effect<
    ReadonlyArray<{ readonly applied: boolean; readonly parentSettled: boolean }>,
    JobStoreError
  >

  /**
   * The oldest undelivered outbox entries, up to `limit` (see
   * `OutboxEntry` for the append invariant). The relay peeks, delivers via
   * `recordChildResults` on the parent store, then deletes — redelivery
   * after a crash is safe because dependency rows dedup.
   *
   * `after` pages past a previously-returned entry id (exclusive), whether
   * or not that entry still exists — the relay walks the whole outbox this
   * way, so entries it cannot route (their parent store is not provided
   * here) never blockade the ones behind them. Anything other than an id
   * this store issued may be treated as unset.
   *
   * @since 0.6.0
   */
  readonly peekOutbox: (options: {
    readonly limit: number
    readonly after?: string | undefined
  }) => Effect.Effect<ReadonlyArray<OutboxEntry>, JobStoreError>

  /**
   * Delete delivered outbox entries by id. Idempotent; unknown ids are
   * ignored.
   *
   * @since 0.6.0
   */
  readonly deleteOutbox: (
    ids: ReadonlyArray<string>
  ) => Effect.Effect<void, JobStoreError>

  /**
   * A flow's dependency rows, ordered by child key; feeds `collect` and
   * dashboards. Pass the returned `cursor` back for the next page.
   *
   * @since 0.6.0
   */
  readonly listChildResults: (
    flowId: JobId,
    options?: {
      readonly cursor?: string | undefined
      /** Page size; default 1000. */
      readonly limit?: number | undefined
    } | undefined
  ) => Effect.Effect<
    {
      readonly items: ReadonlyArray<FlowChildRecord>
      readonly cursor: string | undefined
    },
    JobStoreError
  >

  /**
   * The flow sweeper's work list (see `FlowSweepWork`). `pendingAgeMs`
   * scopes reconciliation to rows whose eligibility timestamp is at least
   * this old (giving the push path time); `limit` bounds the rows returned
   * per class per sweep.
   *
   * Returning a row for reconciliation re-arms its eligibility timestamp
   * (it is not returned again until another `pendingAgeMs` elapses), so a
   * full page ROTATES across sweeps: healthy in-flight children and rows
   * this sweeper cannot act on never pin the head of the page and starve
   * the rows behind them.
   *
   * @since 0.6.0
   */
  readonly flowSweepWork: (options: {
    readonly pendingAgeMs: number
    readonly limit?: number | undefined
  }) => Effect.Effect<FlowSweepWork, JobStoreError>

  /**
   * Mark dependency rows as `cascaded` after their cancels were delivered
   * into (or confirmed unnecessary by) the child's store. Idempotent.
   *
   * @since 0.6.0
   */
  readonly markChildrenCascaded: (
    flowId: JobId,
    childKeys: ReadonlyArray<string>
  ) => Effect.Effect<void, JobStoreError>

  readonly counts: (
    queue?: QueueName
  ) => Effect.Effect<Record<JobState, number>, JobStoreError>

  /**
   * Remove a job (and its ledger; a flow parent's dependency rows go with
   * it). Refuses (returns false) when active or `waiting-children`.
   *
   * Note the retention asymmetry for flows: AUTOMATIC pruning (`keep`
   * policies, the `historyTtl` sweep) must skip a settled flow parent whose
   * rows still owe cascade cancels (`cancelled` and not `cascaded`) — those
   * rows are the only record that real cancels are still due in the child
   * stores. `remove` is the explicit operator override and deletes anyway.
   */
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
