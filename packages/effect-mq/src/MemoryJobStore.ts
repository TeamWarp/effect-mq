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
import { Clock, type Context, Deferred, Effect, Exit, Layer, Option } from "effect"
import {
  type AttemptRecord,
  type ClaimResult,
  type EnqueueRequest,
  JobId,
  JobNotFoundError,
  JobNotRetryableError,
  type JobRecord,
  type JobState,
  JobStore,
  type ListResult,
  LockLostError,
  type QueueName,
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

/**
 * Build a fresh in-memory `JobStore` implementation.
 *
 * @since 0.1.0
 */
export const make: Effect.Effect<Service> = Effect.sync(() => {
  const jobs = new Map<string, MemJob>()
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

  return JobStore.of({
    enqueue: (request: EnqueueRequest) =>
      Effect.gen(function*() {
        const now = yield* Clock.currentTimeMillis
        let id = request.id
        if (id === undefined) {
          // Store-assigned ids must never collide with user-supplied ones.
          do {
            id = JobId(`j-${++idCounter}`)
          } while (jobs.has(id))
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
        if (best === undefined) {
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
            job.state = "completed"
            job.exit = outcome.exit
            job.finishedAt = now
            recordAttempt(job, "completed", now, outcome.exit)
            applyKeep(job, now)
            break
          }
          case "Fail": {
            job.state = "failed"
            job.exit = outcome.exit
            job.finishedAt = now
            recordAttempt(job, "failed", now, outcome.exit)
            applyKeep(job, now)
            break
          }
          case "Retry": {
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
        const job = jobs.get(id)
        if (job === undefined) {
          return yield* new JobNotFoundError({ jobId: id })
        }
        if (job.state !== "active" || job.lockToken !== token) {
          return yield* new LockLostError({ jobId: id })
        }
        clearLock(job)
        job.state = "waiting"
        signalWake()
      }),

    extendLocks: (locks, durationMs) =>
      Effect.gen(function*() {
        const now = yield* Clock.currentTimeMillis
        const lost: Array<JobId> = []
        for (const lock of locks) {
          const job = jobs.get(lock.id)
          if (
            job !== undefined &&
            job.state === "active" &&
            job.lockToken === lock.token
          ) {
            job.lockExpiresAt = now + durationMs
          } else {
            lost.push(lock.id)
          }
        }
        return lost
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
        job.exit = undefined
        job.failedReason = undefined
        job.finishedAt = undefined
        job.processedAt = undefined
        job.runAt = now
        job.seq = ++seq
        signalWake()
      }),

    counts: (queue) =>
      Effect.sync(() => {
        const counts = {
          waiting: 0,
          delayed: 0,
          active: 0,
          completed: 0,
          failed: 0
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
})

/**
 * A fresh in-memory `JobStore` layer. Pass a named store key to provide a
 * `JobStore.named(...)` slot instead of the default.
 *
 * @since 0.1.0
 */
export const layer: Layer.Layer<JobStore> = Layer.effect(JobStore, make)

/**
 * An in-memory layer for a specific store key.
 *
 * @since 0.1.0
 */
export const layerFor = <Id>(
  store: Context.Key<Id, Service>
): Layer.Layer<Id> => Layer.effect(store, make)
