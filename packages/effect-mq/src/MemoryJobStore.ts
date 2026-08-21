/**
 * In-memory `JobStore` built purely on Effect primitives.
 *
 * The reference driver: used for tests, local development and as the template
 * for real storage adapters. Time comes from `Clock`, so everything is fully
 * `TestClock`-compatible.
 *
 * Atomicity note: every operation reads the clock once and then mutates state
 * in a single synchronous block, which is atomic on the JavaScript runtime.
 *
 * @since 0.1.0
 */
import { Clock, type Context, Deferred, Duration, Effect, Exit, Layer, Option, type Scope } from "effect"
import {
  type AttemptRecord,
  type ClaimResult,
  type EnqueueRequest,
  type ExtendLocksResult,
  type IdGenerator,
  JobId,
  JobNotCancellableError,
  JobNotFoundError,
  JobNotPromotableError,
  JobNotRetryableError,
  type JobRecord,
  type JobState,
  JobStore,
  type ListResult,
  LockLostError,
  JobStoreError,
  type QueueName,
  type ScheduleKey,
  type ScheduleRecord,
  type Service
} from "./JobStore.ts"

interface MemJob {
  readonly id: JobId
  readonly name: string
  readonly queue: QueueName
  readonly payload: unknown
  readonly metadata: Readonly<Record<string, string>>
  state: JobState
  readonly priority: number
  readonly attemptsMax: number
  attemptsMade: number
  stalledCount: number
  readonly backoff: JobRecord["backoff"]
  readonly keep: JobRecord["keep"]
  readonly timeoutMs: number | undefined
  cancelRequested: boolean
  runAt: number
  readonly enqueuedAt: number
  processedAt: number | undefined
  finishedAt: number | undefined
  exit: unknown
  failedReason: string | undefined
  attempts: Array<AttemptRecord>
  seq: number
  lockToken: string | undefined
  lockExpiresAt: number | undefined
}

const TERMINAL_STATES: ReadonlySet<JobState> = new Set(["completed", "failed", "cancelled"])

const snapshot = (job: MemJob): JobRecord => ({
  id: job.id,
  name: job.name,
  queue: job.queue,
  payload: job.payload,
  metadata: job.metadata,
  state: job.state,
  priority: job.priority,
  attemptsMax: job.attemptsMax,
  attemptsMade: job.attemptsMade,
  stalledCount: job.stalledCount,
  backoff: job.backoff,
  keep: job.keep,
  timeoutMs: job.timeoutMs,
  cancelRequested: job.cancelRequested,
  runAt: job.runAt,
  enqueuedAt: job.enqueuedAt,
  processedAt: job.processedAt,
  finishedAt: job.finishedAt,
  exit: job.exit,
  failedReason: job.failedReason
})

const metadataMatches = (
  record: Readonly<Record<string, string>>,
  filter: Readonly<Record<string, string>>
): boolean => {
  for (const [key, value] of Object.entries(filter)) {
    if (record[key] !== value) return false
  }
  return true
}

interface MemoryStore {
  readonly service: Service
  readonly sweepHistory: (now: number, ttlMs: number) => void
}

const makeStoreUnsafe = (options?: MemoryJobStoreOptions | undefined): MemoryStore => {
  const jobs = new Map<string, MemJob>()
  const schedules = new Map<ScheduleKey, ScheduleRecord>()
  const paused = new Set<QueueName>()
  let seq = 0
  let idCounter = 0
  let wakeVersion = 0
  let wake = Deferred.makeUnsafe<void>()

  // Synchronous on purpose: it is called inside the same synchronous block as
  // the state mutation, so no effect-op boundary (where an interrupt could
  // land) can separate a mutation from its wake-up signal.
  const signalWake = () => {
    wakeVersion += 1
    const current = wake
    wake = Deferred.makeUnsafe<void>()
    Deferred.doneUnsafe(current, Exit.succeed<void>(void 0))
  }

  const promoteDue = (now: number) => {
    for (const job of jobs.values()) {
      if (job.state === "delayed" && job.runAt <= now) {
        job.state = "waiting"
      }
    }
  }

  const clearLock = (job: MemJob) => {
    job.lockToken = undefined
    job.lockExpiresAt = undefined
  }

  const recordAttempt = (
    job: MemJob,
    outcome: AttemptRecord["outcome"],
    finishedAt: number,
    exit: AttemptRecord["exit"]
  ) => {
    job.attempts.push({
      attempt: job.attempts.length + 1,
      startedAt: job.processedAt,
      finishedAt,
      outcome,
      exit
    })
  }

  const markCancelled = (job: MemJob, now: number) => {
    clearLock(job)
    job.cancelRequested = false
    job.state = "cancelled"
    job.finishedAt = now
    recordAttempt(job, "cancelled", now, undefined)
    applyKeep(job, now)
  }

  // Terminal retention: keep at most `count` and drop older than `ageMs`
  // among terminal jobs sharing this job's name + state.
  const applyKeep = (job: MemJob, now: number) => {
    if (job.keep === undefined) return
    const peers = Array.from(jobs.values())
      .filter((peer) => peer.name === job.name && peer.state === job.state)
      .toSorted((a, b) => ((b.finishedAt ?? 0) - (a.finishedAt ?? 0)) || (b.seq - a.seq))
    const remove = new Set<string>()
    if (job.keep.ageMs !== undefined) {
      for (const peer of peers) {
        if (peer.finishedAt !== undefined && peer.finishedAt <= now - job.keep.ageMs) {
          remove.add(peer.id)
        }
      }
    }
    if (job.keep.count !== undefined) {
      for (const peer of peers.slice(Math.max(0, job.keep.count))) {
        remove.add(peer.id)
      }
    }
    for (const id of remove) {
      jobs.delete(id)
    }
  }

  const sweepHistory = (now: number, ttlMs: number) => {
    for (const job of jobs.values()) {
      if (
        TERMINAL_STATES.has(job.state) &&
        job.finishedAt !== undefined &&
        job.finishedAt <= now - ttlMs
      ) {
        jobs.delete(job.id)
      }
    }
  }

  const service: Service = JobStore.of({
    enqueue: (request: EnqueueRequest) =>
      Effect.gen(function*() {
        const now = yield* Clock.currentTimeMillis
        let id = request.id
        if (id === undefined) {
          const generate = options?.idGenerator
          if (generate === undefined) {
            // Store-assigned ids must never collide with user-supplied ones.
            do {
              id = JobId(`j-${++idCounter}`)
            } while (jobs.has(id))
          } else {
            // A user generator gets a bounded number of collision retries; a
            // healthy generator's entropy makes even one retry pathological.
            for (let i = 0; i < 5 && id === undefined; i++) {
              const raw = generate(request)
              const candidate = JobId(Effect.isEffect(raw) ? yield* raw : raw)
              if (!jobs.has(candidate)) {
                id = candidate
              }
            }
            if (id === undefined) {
              return yield* new JobStoreError({
                message: "enqueue failed: could not generate a unique job id"
              })
            }
          }
        } else if (jobs.has(id)) {
          return { id, duplicate: true }
        }
        jobs.set(id, {
          id,
          name: request.name,
          queue: request.queue,
          payload: request.payload,
          metadata: request.metadata,
          state: request.delayMs > 0 ? "delayed" : "waiting",
          priority: request.priority,
          attemptsMax: request.attemptsMax,
          attemptsMade: 0,
          stalledCount: 0,
          backoff: request.backoff,
          keep: request.keep,
          timeoutMs: request.timeoutMs,
          cancelRequested: false,
          runAt: now + Math.max(0, request.delayMs),
          enqueuedAt: now,
          processedAt: undefined,
          finishedAt: undefined,
          exit: undefined,
          failedReason: undefined,
          attempts: [],
          seq: ++seq,
          lockToken: undefined,
          lockExpiresAt: undefined
        })
        signalWake()
        return { id, duplicate: false }
      }),

    claim: (options) =>
      Effect.gen(function*() {
        const now = yield* Clock.currentTimeMillis
        promoteDue(now)
        const names = new Set(options.names)
        let best: MemJob | undefined
        let nextRunAt: number | undefined
        for (const job of jobs.values()) {
          if (job.queue !== options.queue || !names.has(job.name)) continue
          if (job.state === "waiting") {
            if (
              best === undefined ||
              job.priority > best.priority ||
              (job.priority === best.priority && job.seq < best.seq)
            ) {
              best = job
            }
          } else if (job.state === "delayed") {
            if (nextRunAt === undefined || job.runAt < nextRunAt) {
              nextRunAt = job.runAt
            }
          }
        }
        if (best === undefined || paused.has(options.queue)) {
          const empty: ClaimResult = { _tag: "Empty", nextRunAt, wakeToken: wakeVersion }
          return empty
        }
        best.state = "active"
        best.lockToken = options.token
        best.lockExpiresAt = now + options.lockDurationMs
        best.processedAt = now
        const claimed: ClaimResult = { _tag: "Claimed", job: snapshot(best) }
        return claimed
      }),

    ack: (id, token, outcome) =>
      Effect.gen(function*() {
        const now = yield* Clock.currentTimeMillis
        const job = jobs.get(id)
        if (job === undefined) {
          return yield* new JobNotFoundError({ jobId: id })
        }
        if (job.state !== "active" || job.lockToken !== token) {
          return yield* new LockLostError({ jobId: id })
        }
        clearLock(job)
        job.attemptsMade += 1
        switch (outcome._tag) {
          case "Complete": {
            job.cancelRequested = false
            job.state = "completed"
            job.exit = outcome.exit
            job.finishedAt = now
            recordAttempt(job, "completed", now, outcome.exit)
            applyKeep(job, now)
            break
          }
          case "Fail": {
            job.cancelRequested = false
            job.state = "failed"
            job.exit = outcome.exit
            job.finishedAt = now
            recordAttempt(job, "failed", now, outcome.exit)
            applyKeep(job, now)
            break
          }
          case "Cancelled": {
            job.cancelRequested = false
            job.state = "cancelled"
            job.finishedAt = now
            recordAttempt(job, "cancelled", now, undefined)
            applyKeep(job, now)
            break
          }
          case "Retry": {
            if (job.cancelRequested) {
              // A cancel raced a natural failure before the heartbeat could
              // interrupt the run: cancellation wins over revival (mirrors
              // release/recoverStalled).
              markCancelled(job, now)
              break
            }
            recordAttempt(job, "retried", now, outcome.exit)
            job.runAt = now + Math.max(0, outcome.delayMs)
            job.state = outcome.delayMs > 0 ? "delayed" : "waiting"
            job.seq = ++seq
            signalWake()
            break
          }
        }
      }),

    release: (id, token) =>
      Effect.gen(function*() {
        const now = yield* Clock.currentTimeMillis
        const job = jobs.get(id)
        if (job === undefined) {
          return yield* new JobNotFoundError({ jobId: id })
        }
        if (job.state !== "active" || job.lockToken !== token) {
          return yield* new LockLostError({ jobId: id })
        }
        if (job.cancelRequested) {
          // A cancel arrived while the worker was shutting down: honour it
          // instead of reviving the job.
          markCancelled(job, now)
          return
        }
        clearLock(job)
        job.state = "waiting"
        signalWake()
      }),

    extendLocks: (locks, durationMs) =>
      Effect.gen(function*() {
        const now = yield* Clock.currentTimeMillis
        const lost: Array<JobId> = []
        const cancelRequested: Array<JobId> = []
        for (const lock of locks) {
          const job = jobs.get(lock.id)
          if (
            job === undefined ||
            job.state !== "active" ||
            job.lockToken !== lock.token
          ) {
            lost.push(lock.id)
          } else if (job.cancelRequested) {
            cancelRequested.push(lock.id)
          } else {
            job.lockExpiresAt = now + durationMs
          }
        }
        const result: ExtendLocksResult = { lost, cancelRequested }
        return result
      }),

    recoverStalled: (options) =>
      Effect.gen(function*() {
        const now = yield* Clock.currentTimeMillis
        const recovered: Array<{ id: JobId; failed: boolean }> = []
        for (const job of jobs.values()) {
          if (
            job.state !== "active" ||
            job.lockExpiresAt === undefined ||
            job.lockExpiresAt > now
          ) {
            continue
          }
          if (job.cancelRequested) {
            // The owning worker died before honouring the cancel: finish it.
            markCancelled(job, now)
            continue
          }
          clearLock(job)
          job.stalledCount += 1
          recordAttempt(job, "stalled", now, undefined)
          if (job.stalledCount > options.maxStalledCount) {
            job.state = "failed"
            job.finishedAt = now
            job.failedReason = "job stalled more than allowable limit"
            recovered.push({ id: job.id, failed: true })
          } else {
            job.state = "waiting"
            recovered.push({ id: job.id, failed: false })
          }
        }
        if (recovered.some((entry) => !entry.failed)) {
          signalWake()
        }
        return recovered
      }),

    awaitWake: (_queues, wakeToken) =>
      Effect.suspend(() => {
        if (wakeVersion > wakeToken) return Effect.void
        return Deferred.await(wake)
      }),

    getJob: (id) =>
      Effect.sync(() => {
        const job = jobs.get(id)
        return job === undefined ? Option.none() : Option.some(snapshot(job))
      }),

    getAttempts: (id) =>
      Effect.sync(() => {
        const job = jobs.get(id)
        return job === undefined ? [] : [...job.attempts]
      }),

    list: (options) =>
      Effect.sync(() => {
        const limit = Math.max(1, options.limit ?? 50)
        const states = options.states === undefined ? undefined : new Set(options.states)
        // Newest first, stable across retries: (enqueuedAt desc, id desc).
        let cursor: { readonly at: number; readonly id: string } | undefined
        if (options.cursor !== undefined) {
          const split = options.cursor.indexOf(":")
          cursor = {
            at: Number(options.cursor.slice(0, split)),
            id: options.cursor.slice(split + 1)
          }
        }
        const matches = Array.from(jobs.values())
          .filter((job) =>
            (options.queue === undefined || job.queue === options.queue) &&
            (options.name === undefined || job.name === options.name) &&
            (states === undefined || states.has(job.state)) &&
            (options.metadata === undefined || metadataMatches(job.metadata, options.metadata))
          )
          .toSorted((a, b) =>
            b.enqueuedAt !== a.enqueuedAt
              ? b.enqueuedAt - a.enqueuedAt
              : b.id < a.id
              ? -1
              : b.id > a.id
              ? 1
              : 0
          )
          .filter((job) =>
            cursor === undefined ||
            job.enqueuedAt < cursor.at ||
            (job.enqueuedAt === cursor.at && job.id < cursor.id)
          )
        const items = matches.slice(0, limit).map(snapshot)
        const last = items[items.length - 1]
        const result: ListResult = {
          items,
          cursor: matches.length > limit && last !== undefined
            ? `${last.enqueuedAt}:${last.id}`
            : undefined
        }
        return result
      }),

    retry: (id) =>
      Effect.gen(function*() {
        const job = jobs.get(id)
        if (job === undefined) {
          return yield* new JobNotFoundError({ jobId: id })
        }
        if (job.state !== "failed") {
          return yield* new JobNotRetryableError({ jobId: id, state: job.state })
        }
        const now = yield* Clock.currentTimeMillis
        job.state = "waiting"
        job.attemptsMade = 0
        job.stalledCount = 0
        job.cancelRequested = false
        job.exit = undefined
        job.failedReason = undefined
        job.finishedAt = undefined
        job.processedAt = undefined
        job.runAt = now
        job.seq = ++seq
        signalWake()
      }),

    cancel: (id) =>
      Effect.gen(function*() {
        const now = yield* Clock.currentTimeMillis
        const job = jobs.get(id)
        if (job === undefined) {
          return yield* new JobNotFoundError({ jobId: id })
        }
        switch (job.state) {
          case "waiting":
          case "delayed": {
            markCancelled(job, now)
            return
          }
          case "active": {
            job.cancelRequested = true
            return
          }
          default: {
            return yield* new JobNotCancellableError({ jobId: id, state: job.state })
          }
        }
      }),

    promote: (id) =>
      Effect.gen(function*() {
        const now = yield* Clock.currentTimeMillis
        const job = jobs.get(id)
        if (job === undefined) {
          return yield* new JobNotFoundError({ jobId: id })
        }
        if (job.state !== "delayed") {
          return yield* new JobNotPromotableError({ jobId: id, state: job.state })
        }
        job.state = "waiting"
        job.runAt = now
        signalWake()
      }),

    pause: (queue) =>
      Effect.sync(() => {
        paused.add(queue)
      }),

    resume: (queue) =>
      Effect.sync(() => {
        if (paused.delete(queue)) {
          signalWake()
        }
      }),

    pausedQueues: () => Effect.sync(() => Array.from(paused)),

    upsertSchedule: (schedule) =>
      Effect.sync(() => {
        // An unchanged cadence keeps its next occurrence: deploy-time
        // re-registration must not re-anchor `every` grids or drop a pending
        // catch-up run. A changed cadence takes the caller's fresh nextRunAt.
        const existing = schedules.get(schedule.key)
        const sameCadence = existing !== undefined &&
          existing.cron === schedule.cron &&
          existing.tz === schedule.tz &&
          existing.everyMs === schedule.everyMs
        schedules.set(
          schedule.key,
          sameCadence ? { ...schedule, nextRunAt: existing.nextRunAt } : schedule
        )
        signalWake()
      }),

    removeSchedule: (key) => Effect.sync(() => schedules.delete(key)),

    listSchedules: (options) =>
      Effect.sync(() =>
        Array.from(schedules.values()).filter((schedule) =>
          (options?.jobName === undefined || schedule.jobName === options.jobName) &&
          (options?.queue === undefined || schedule.queue === options.queue)
        )
      ),

    dueSchedules: () =>
      Effect.gen(function*() {
        const now = yield* Clock.currentTimeMillis
        return Array.from(schedules.values())
          .filter((schedule) => schedule.nextRunAt <= now)
          .toSorted((a, b) => a.nextRunAt - b.nextRunAt)
      }),

    advanceSchedule: (key, expectedRunAt, nextRunAt) =>
      Effect.sync(() => {
        const schedule = schedules.get(key)
        if (schedule !== undefined && schedule.nextRunAt === expectedRunAt) {
          schedules.set(key, { ...schedule, nextRunAt })
        }
      }),

    counts: (queue) =>
      Effect.sync(() => {
        const counts = {
          waiting: 0,
          delayed: 0,
          active: 0,
          completed: 0,
          failed: 0,
          cancelled: 0
        } satisfies Record<JobState, number>
        for (const job of jobs.values()) {
          if (queue !== undefined && job.queue !== queue) continue
          counts[job.state] += 1
        }
        return counts
      }),

    remove: (id) =>
      Effect.sync(() => {
        const job = jobs.get(id)
        if (job === undefined || job.state === "active") return false
        jobs.delete(id)
        return true
      })
  })

  return { service, sweepHistory }
}

/**
 * @since 0.2.0
 */
export interface MemoryJobStoreOptions {
  /**
   * Store-level retention ceiling: terminal records (completed, failed,
   * cancelled) older than this are removed by a periodic sweep. Per-job
   * `keep` policies may only be stricter.
   */
  readonly historyTtl?: Duration.Input | undefined
  /** Sweep cadence (default 1 minute). */
  readonly historySweepInterval?: Duration.Input | undefined
  /**
   * Generator for store-assigned job ids (e.g. `() => \`job_${ulid()}\``).
   * Default: a `j-<n>` counter. See `JobStore.IdGenerator`.
   */
  readonly idGenerator?: IdGenerator | undefined
}

/**
 * Build a fresh in-memory `JobStore` implementation (no history sweeper —
 * use `makeWith` for `historyTtl` support).
 *
 * @since 0.1.0
 */
export const make: Effect.Effect<Service> = Effect.sync(() => makeStoreUnsafe().service)

/**
 * Build a fresh in-memory `JobStore` with options; the history sweeper (when
 * configured) lives in the surrounding `Scope`.
 *
 * @since 0.2.0
 */
export const makeWith = (
  options?: MemoryJobStoreOptions | undefined
): Effect.Effect<Service, never, Scope.Scope> =>
  Effect.gen(function*() {
    const { service, sweepHistory } = makeStoreUnsafe(options)
    if (options?.historyTtl !== undefined) {
      const ttlMs = Duration.toMillis(options.historyTtl)
      const intervalMs = Duration.toMillis(options.historySweepInterval ?? "1 minute")
      yield* Effect.gen(function*() {
        yield* Effect.sleep(intervalMs)
        const now = yield* Clock.currentTimeMillis
        sweepHistory(now, ttlMs)
      }).pipe(Effect.forever, Effect.forkScoped)
    }
    return service
  })

/**
 * A fresh in-memory `JobStore` layer.
 *
 * @since 0.1.0
 */
export const layer: Layer.Layer<JobStore> = Layer.effect(JobStore, makeWith())

/**
 * An in-memory layer with options (e.g. `historyTtl`).
 *
 * @since 0.2.0
 */
export const layerWith = (
  options?: MemoryJobStoreOptions | undefined
): Layer.Layer<JobStore> => Layer.effect(JobStore, makeWith(options))

/**
 * An in-memory layer for a specific store key.
 *
 * @since 0.1.0
 */
export const layerFor = <Id>(
  store: Context.Key<Id, Service>,
  options?: MemoryJobStoreOptions | undefined
): Layer.Layer<Id> => Layer.effect(store, makeWith(options))
