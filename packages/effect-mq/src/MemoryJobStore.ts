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
  type FlowChildRecord,
  type FlowChildSpec,
  type FlowSweepWork,
  type HistoryTtlByState,
  type HistoryTtlInput,
  type IdGenerator,
  JobId,
  JobNotCancellableError,
  JobNotFoundError,
  JobNotPromotableError,
  JobNotRetryableError,
  type JobRecord,
  type JobState,
  type KeepStatePolicy,
  JobStore,
  type ListResult,
  LockLostError,
  JobStoreError,
  normalizeHistoryTtl,
  type TerminalState,
  type QueueName,
  type ScheduleKey,
  type ScheduleRecord,
  type Service
} from "./JobStore.ts"

interface MemJob {
  readonly id: JobId
  readonly name: string
  readonly queue: QueueName
  payload: unknown
  metadata: Readonly<Record<string, string>>
  state: JobState
  priority: number
  attemptsMax: number
  attemptsMade: number
  stalledCount: number
  backoff: JobRecord["backoff"]
  keep: JobRecord["keep"]
  timeoutMs: number | undefined
  cancelRequested: boolean
  readonly dedupeKey: string | undefined
  trace: JobRecord["trace"]
  readonly parent: JobRecord["parent"]
  flow: JobRecord["flow"]
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
  dedupeKey: job.dedupeKey,
  trace: job.trace,
  parent: job.parent,
  flow: job.flow,
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
  readonly sweepHistory: (now: number, ttlByState: HistoryTtlByState) => void
}

interface MemFlowChild {
  readonly flowId: JobId
  readonly childKey: string
  readonly storeKey: string
  readonly spec: EnqueueRequest
  status: FlowChildRecord["status"]
  exit: unknown
  failedReason: string | undefined
  cascaded: boolean
  readonly pendingSince: number
}

const makeStoreUnsafe = (options?: MemoryJobStoreOptions | undefined): MemoryStore => {
  const jobs = new Map<string, MemJob>()
  const schedules = new Map<ScheduleKey, ScheduleRecord>()
  const paused = new Set<QueueName>()
  // Flow dependency rows, keyed by parent job id then child key. Insertion
  // order is FanOut spec order; listChildResults sorts by child key.
  const flowChildren = new Map<JobId, Map<string, MemFlowChild>>()
  // Dedup registry: one entry per (name, key). `expiresAt` is set for
  // ttl/throttle windows; pending-mode entries live as long as their job.
  const dedupes = new Map<string, { jobId: JobId; expiresAt: number | undefined }>()
  const dedupeMapKey = (name: string, key: string) => `${name}\u0000${key}`
  let seq = 0
  let idCounter = 0
  let wakeVersion = 0
  let lastBroadcast = 0
  const lastWake = new Map<QueueName, number>()
  interface Waiter {
    readonly queues: ReadonlySet<QueueName>
    readonly deferred: Deferred.Deferred<void>
  }
  const waiters = new Set<Waiter>()
  const lastWakeFor = (queue: QueueName) => Math.max(lastWake.get(queue) ?? 0, lastBroadcast)

  // Synchronous on purpose: it is called inside the same synchronous block as
  // the state mutation, so no effect-op boundary (where an interrupt could
  // land) can separate a mutation from its wake-up signal. A queue targets
  // only waiters watching it; no queue broadcasts (rare maintenance verbs).
  const signalWake = (queue?: QueueName) => {
    wakeVersion += 1
    if (queue === undefined) {
      lastBroadcast = wakeVersion
    } else {
      lastWake.set(queue, wakeVersion)
    }
    // Snapshot-and-clear BEFORE resolving: doneUnsafe resumes waiting fibers
    // synchronously, and a woken taker that re-parks registers a NEW waiter —
    // resolving inside the live Set iteration would visit it and livelock.
    const toWake: Array<Waiter> = []
    for (const waiter of waiters) {
      if (queue === undefined || waiter.queues.has(queue)) {
        waiters.delete(waiter)
        toWake.push(waiter)
      }
    }
    for (const waiter of toWake) {
      Deferred.doneUnsafe(waiter.deferred, Exit.succeed<void>(void 0))
    }
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

  // A job leaving the pending states frees its pending-mode dedup entry;
  // live throttle windows deliberately outlast the job.
  const releaseDedupe = (job: MemJob, now: number) => {
    if (job.dedupeKey === undefined) return
    const key = dedupeMapKey(job.name, job.dedupeKey)
    const entry = dedupes.get(key)
    if (
      entry !== undefined && entry.jobId === job.id &&
      (entry.expiresAt === undefined || entry.expiresAt <= now)
    ) {
      dedupes.delete(key)
    }
  }

  // A pruned/removed flow parent takes its dependency rows with it.
  const deleteJob = (id: string) => {
    jobs.delete(id)
    // SAFETY: flowChildren keys are JobIds; a plain string that is not one
    // simply misses.
    flowChildren.delete(id as JobId)
  }

  // Settle-time marking: remaining pending rows flip to cancelled (NOT
  // cascaded — the sweeper still owes the child stores real cancels), so
  // late reports find their row terminal and drop, and `listChildResults`
  // stays truthful. Lock order note: in this driver everything is one
  // synchronous block, but the row-then-parent order is still observed.
  const markPendingRowsCancelled = (flowId: JobId) => {
    const rows = flowChildren.get(flowId)
    if (rows === undefined) return
    for (const row of rows.values()) {
      if (row.status === "pending") {
        row.status = "cancelled"
        row.cascaded = false
      }
    }
  }

  const markCancelled = (job: MemJob, now: number) => {
    clearLock(job)
    job.cancelRequested = false
    if (job.state === "waiting-children") {
      markPendingRowsCancelled(job.id)
      if (job.flow !== undefined) {
        job.flow = { ...job.flow, pending: 0 }
      }
    }
    job.state = "cancelled"
    job.finishedAt = now
    recordAttempt(job, "cancelled", now, undefined)
    releaseDedupe(job, now)
    applyKeep(job, now)
  }

  const keepPolicyFor = (keep: JobRecord["keep"], state: JobState) => {
    if (keep === undefined) return undefined
    const policy = state === "completed"
      ? keep.completed
      : state === "failed"
      ? keep.failed
      : state === "cancelled"
      ? keep.cancelled
      : undefined
    if (policy !== undefined) return policy
    // Records persisted by 0.2.x carry the flat {count, ageMs} shape — honour
    // it as an all-states policy so upgrades keep pruning.
    if (
      keep.completed === undefined && keep.failed === undefined && keep.cancelled === undefined &&
      ("count" in keep || "ageMs" in keep)
    ) {
      // SAFETY: the flat legacy shape carries KeepStatePolicy fields.
      return keep as KeepStatePolicy
    }
    return undefined
  }

  // Terminal retention: keep at most `count` and drop older than `ageMs`
  // among terminal jobs sharing this job's name + state (policies are split
  // per terminal state).
  const applyKeep = (job: MemJob, now: number) => {
    const policy = keepPolicyFor(job.keep, job.state)
    if (policy === undefined) return
    const peers = Array.from(jobs.values())
      .filter((peer) => peer.name === job.name && peer.state === job.state)
      .toSorted((a, b) => ((b.finishedAt ?? 0) - (a.finishedAt ?? 0)) || (b.seq - a.seq))
    const remove = new Set<string>()
    if (policy.ageMs !== undefined) {
      for (const peer of peers) {
        if (peer.finishedAt !== undefined && peer.finishedAt <= now - policy.ageMs) {
          remove.add(peer.id)
        }
      }
    }
    if (policy.count !== undefined) {
      for (const peer of peers.slice(Math.max(0, policy.count))) {
        remove.add(peer.id)
      }
    }
    for (const id of remove) {
      deleteJob(id)
    }
  }

  const sweepHistory = (now: number, ttlByState: HistoryTtlByState) => {
    for (const job of jobs.values()) {
      if (!TERMINAL_STATES.has(job.state) || job.finishedAt === undefined) continue
      // SAFETY: TERMINAL_STATES membership was checked above.
      const state = job.state as TerminalState
      const ttl = ttlByState[state]
      const keepAge = keepPolicyFor(job.keep, state)?.ageMs
      // The sweep honours min(per-row keep age, store ceiling) — a quiet job
      // name is pruned on the timer, not only when its group is acked.
      const effective = keepAge !== undefined && (ttl === undefined || keepAge < ttl) ? keepAge : ttl
      if (effective !== undefined && job.finishedAt <= now - effective) {
        deleteJob(job.id)
      }
    }
    // Dead dedup entries: expired window, or a pointer at a vanished job.
    for (const [key, entry] of dedupes) {
      const alive = entry.expiresAt !== undefined
        ? entry.expiresAt > now
        : jobs.has(entry.jobId) && !TERMINAL_STATES.has(jobs.get(entry.jobId)?.state ?? "completed")
      if (!alive) dedupes.delete(key)
    }
  }

  const cancelJob = (id: JobId) =>
    Effect.gen(function*() {
      const now = yield* Clock.currentTimeMillis
      const job = jobs.get(id)
      if (job === undefined) {
        return yield* new JobNotFoundError({ jobId: id })
      }
      switch (job.state) {
        case "waiting":
        case "delayed":
        case "waiting-children": {
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
    })

  const insertJobRecord = (id: JobId, request: EnqueueRequest, now: number) => {
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
      dedupeKey: request.dedupe?.key,
      trace: request.trace,
      parent: request.parent,
      flow: undefined,
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
    signalWake(request.queue)
  }

  const enqueueOne = (request: EnqueueRequest) =>
    Effect.gen(function*() {
        const now = yield* Clock.currentTimeMillis
        if (request.id !== undefined && jobs.has(request.id)) {
          return { id: request.id, duplicate: true }
        }
        // The dedup decision tree runs BEFORE id generation, so a
        // deduplicated enqueue never consults the id generator.
        if (request.dedupe !== undefined) {
          const mapKey = dedupeMapKey(request.name, request.dedupe.key)
          const entry = dedupes.get(mapKey)
          if (entry !== undefined) {
            const keyed = jobs.get(entry.jobId)
            const windowLive = entry.expiresAt !== undefined && now < entry.expiresAt
            // Latest-wins while the keyed job is still delayed.
            if (request.dedupe.replace && keyed !== undefined && keyed.state === "delayed") {
              keyed.payload = request.payload
              keyed.metadata = request.metadata
              keyed.priority = request.priority
              keyed.attemptsMax = request.attemptsMax
              keyed.backoff = request.backoff
              keyed.keep = request.keep
              keyed.timeoutMs = request.timeoutMs
              keyed.trace = request.trace
              keyed.runAt = now + Math.max(0, request.delayMs)
              // A landed replace re-arms the ttl window.
              if (request.dedupe.ttlMs !== undefined) {
                entry.expiresAt = now + request.dedupe.ttlMs
              }
              signalWake(keyed.queue)
              return { id: keyed.id, duplicate: true }
            }
            if (windowLive) {
              if (request.dedupe.extend && request.dedupe.ttlMs !== undefined) {
                entry.expiresAt = now + request.dedupe.ttlMs
              }
              return { id: entry.jobId, duplicate: true }
            }
            const pending = keyed !== undefined && !TERMINAL_STATES.has(keyed.state)
            if (entry.expiresAt === undefined && pending) {
              return { id: entry.jobId, duplicate: true }
            }
            // Dead entry (expired window / finished job): fall through and
            // let the new job take over the key below.
          }
        }
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
        }
        insertJobRecord(id, request, now)
        if (request.dedupe !== undefined) {
          dedupes.set(dedupeMapKey(request.name, request.dedupe.key), {
            jobId: id,
            expiresAt: request.dedupe.ttlMs !== undefined ? now + request.dedupe.ttlMs : undefined
          })
        }
        return { id, duplicate: false }
      })

  const service: Service = JobStore.of({
    enqueue: enqueueOne,

    enqueueMany: (requests) => Effect.forEach(requests, enqueueOne),

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
        if (
          outcome._tag === "FanOut" &&
          outcome.children.some((child) => child.request.id === undefined)
        ) {
          // Validate BEFORE any mutation, so a bad spec cannot leave the job
          // half-acked (lock cleared, ledger written, still active).
          return yield* new JobStoreError({
            message: "FanOut child specs require an explicit request.id"
          })
        }
        clearLock(job)
        // A fan-out is a phase transition, not a completed run — the attempt
        // budget spans both phases.
        if (outcome._tag !== "FanOut") {
          job.attemptsMade += 1
        }
        switch (outcome._tag) {
          case "FanOut": {
            recordAttempt(job, "fanned-out", now, undefined)
            if (job.flow === undefined) {
              job.flow = { failFast: outcome.failFast, pending: outcome.children.length }
              const rows = new Map<string, MemFlowChild>()
              for (const child of outcome.children) {
                rows.set(child.childKey, {
                  flowId: job.id,
                  childKey: child.childKey,
                  storeKey: child.storeKey,
                  spec: child.request,
                  status: "pending",
                  exit: undefined,
                  failedReason: undefined,
                  cascaded: false,
                  pendingSince: now
                })
              }
              flowChildren.set(job.id, rows)
            }
            // A manifest that was already present is kept untouched (double
            // fan-out converges on the persisted children); the state
            // transition follows the persisted pending count either way.
            if (job.cancelRequested) {
              // A cancel raced the fan-out: cancellation wins. The rows exist
              // and get marked, so the sweeper cascades (mostly no-op cancels
              // for never-enqueued children).
              markCancelled(job, now)
              break
            }
            if (job.flow.pending > 0) {
              job.state = "waiting-children"
            } else {
              // Empty spec: settle straight to runnable `collect`.
              job.state = "waiting"
              job.runAt = now
              job.seq = ++seq
              signalWake(job.queue)
            }
            break
          }
          case "Complete": {
            job.cancelRequested = false
            job.state = "completed"
            job.exit = outcome.exit
            job.finishedAt = now
            recordAttempt(job, "completed", now, outcome.exit)
            releaseDedupe(job, now)
            applyKeep(job, now)
            break
          }
          case "Fail": {
            job.cancelRequested = false
            job.state = "failed"
            job.exit = outcome.exit
            job.finishedAt = now
            recordAttempt(job, "failed", now, outcome.exit)
            releaseDedupe(job, now)
            applyKeep(job, now)
            break
          }
          case "Cancelled": {
            job.cancelRequested = false
            job.state = "cancelled"
            job.finishedAt = now
            recordAttempt(job, "cancelled", now, undefined)
            releaseDedupe(job, now)
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
            signalWake(job.queue)
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
        signalWake(job.queue)
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
            releaseDedupe(job, now)
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

    awaitWake: (queues, wakeToken) =>
      Effect.suspend(() => {
        if (queues.some((queue) => lastWakeFor(queue) > wakeToken)) return Effect.void
        const waiter: Waiter = { queues: new Set(queues), deferred: Deferred.makeUnsafe<void>() }
        waiters.add(waiter)
        return Deferred.await(waiter.deferred).pipe(
          Effect.ensuring(Effect.sync(() => waiters.delete(waiter)))
        )
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
        signalWake(job.queue)
      }),

    cancel: cancelJob,

    cancelByDedupe: (name, key) =>
      Effect.suspend(() => {
        const entry = dedupes.get(dedupeMapKey(name, key))
        if (entry === undefined) return Effect.succeed(false)
        return cancelJob(entry.jobId).pipe(
          Effect.as(true),
          // Idempotent: a vanished or already-terminal keyed job is "nothing
          // pending", not an error.
          Effect.catchTag(["JobNotFoundError", "JobNotCancellableError"], () => Effect.succeed(false))
        )
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
        signalWake(job.queue)
      }),

    pause: (queue) =>
      Effect.sync(() => {
        paused.add(queue)
      }),

    resume: (queue) =>
      Effect.sync(() => {
        if (paused.delete(queue)) {
          signalWake(queue)
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
        signalWake(schedule.queue)
      }),

    removeSchedule: (key) => Effect.sync(() => schedules.delete(key)),

    listSchedules: (options) =>
      Effect.sync(() =>
        Array.from(schedules.values()).filter((schedule) =>
          (options?.jobName === undefined || schedule.jobName === options.jobName) &&
          (options?.queue === undefined || schedule.queue === options.queue) &&
          (options?.group === undefined || schedule.group === options.group)
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

    tickSchedule: (key, expectedRunAt, nextRunAt, request) =>
      Effect.gen(function*() {
        const id = request.id
        if (id === undefined) {
          return yield* new JobStoreError({
            message: "tickSchedule requires an explicit request.id"
          })
        }
        const now = yield* Clock.currentTimeMillis
        // One synchronous block: CAS, insert, and advance commit together —
        // no yield point can interleave another sweeper between them.
        const schedule = schedules.get(key)
        if (schedule === undefined || schedule.nextRunAt !== expectedRunAt) {
          return false
        }
        schedules.set(key, { ...schedule, nextRunAt })
        // Slot already materialized (pre-0.4 crash between enqueue and
        // advance): the schedule still advances, but nothing new fired.
        if (jobs.has(id)) return false
        insertJobRecord(id, request, now)
        return true
      }),

    recordChildResult: (report) =>
      Effect.gen(function*() {
        const now = yield* Clock.currentTimeMillis
        // Lock order: dependency row first, parent row second.
        const row = flowChildren.get(report.flowId)?.get(report.childKey)
        if (row === undefined || row.status !== "pending") {
          return { applied: false, parentSettled: false }
        }
        row.status = report.outcome
        row.exit = report.exit
        row.failedReason = report.failedReason
        // The outcome came from the child's store: no cancel to deliver.
        row.cascaded = true
        const parent = jobs.get(report.flowId)
        if (parent === undefined || parent.flow === undefined) {
          return { applied: true, parentSettled: false }
        }
        parent.flow = { ...parent.flow, pending: Math.max(0, parent.flow.pending - 1) }
        if (parent.state !== "waiting-children") {
          return { applied: true, parentSettled: false }
        }
        if (parent.flow.failFast && report.outcome === "failed") {
          // First failure settles the parent terminally (store-side, like
          // stall exhaustion) and marks the remaining rows in the same op.
          markPendingRowsCancelled(parent.id)
          parent.flow = { ...parent.flow, pending: 0 }
          parent.cancelRequested = false
          parent.state = "failed"
          parent.finishedAt = now
          parent.failedReason = `effect-mq: flow child "${report.childKey}" failed`
          recordAttempt(parent, "failed", now, undefined)
          releaseDedupe(parent, now)
          applyKeep(parent, now)
          return { applied: true, parentSettled: true }
        }
        if (parent.flow.pending === 0) {
          // All children settled: the parent resumes runnable, phase collect.
          parent.state = "waiting"
          parent.runAt = now
          parent.seq = ++seq
          signalWake(parent.queue)
          return { applied: true, parentSettled: true }
        }
        return { applied: true, parentSettled: false }
      }),

    listChildResults: (flowId, options) =>
      Effect.sync(() => {
        const limit = Math.max(1, options?.limit ?? 1000)
        const rows = Array.from(flowChildren.get(flowId)?.values() ?? [])
          .toSorted((a, b) => (a.childKey < b.childKey ? -1 : a.childKey > b.childKey ? 1 : 0))
          .filter((row) => options?.cursor === undefined || row.childKey > options.cursor)
        const page = rows.slice(0, limit)
        const items: Array<FlowChildRecord> = page.map((row) => ({
          flowId: row.flowId,
          childKey: row.childKey,
          name: row.spec.name,
          storeKey: row.storeKey,
          // SAFETY: FanOut validated every spec id at ack time.
          childJobId: row.spec.id as JobId,
          status: row.status,
          exit: row.exit,
          failedReason: row.failedReason,
          cascaded: row.cascaded
        }))
        const last = page[page.length - 1]
        return {
          items,
          cursor: rows.length > limit && last !== undefined ? last.childKey : undefined
        }
      }),

    flowSweepWork: (options) =>
      Effect.gen(function*() {
        const now = yield* Clock.currentTimeMillis
        const limit = Math.max(1, options.limit ?? 1000)
        const threshold = now - options.pendingAgeMs
        const reconcile: Array<{ flowId: JobId; children: Array<FlowChildSpec> }> = []
        const cascade: Array<
          { flowId: JobId; children: Array<{ childKey: string; storeKey: string; childJobId: JobId }> }
        > = []
        let reconcileCount = 0
        let cascadeCount = 0
        for (const [flowId, rows] of flowChildren) {
          const parent = jobs.get(flowId)
          const reconciling = parent !== undefined && parent.state === "waiting-children"
          let reconcileGroup: Array<FlowChildSpec> | undefined
          let cascadeGroup:
            | Array<{ childKey: string; storeKey: string; childJobId: JobId }>
            | undefined
          for (const row of rows.values()) {
            if (
              reconciling && row.status === "pending" &&
              row.pendingSince <= threshold && reconcileCount < limit
            ) {
              reconcileGroup ??= []
              reconcileGroup.push({ childKey: row.childKey, storeKey: row.storeKey, request: row.spec })
              reconcileCount += 1
            }
            if (row.status === "cancelled" && !row.cascaded && cascadeCount < limit) {
              cascadeGroup ??= []
              cascadeGroup.push({
                childKey: row.childKey,
                storeKey: row.storeKey,
                // SAFETY: FanOut validated every spec id at ack time.
                childJobId: row.spec.id as JobId
              })
              cascadeCount += 1
            }
          }
          if (reconcileGroup !== undefined) reconcile.push({ flowId, children: reconcileGroup })
          if (cascadeGroup !== undefined) cascade.push({ flowId, children: cascadeGroup })
        }
        const work: FlowSweepWork = { reconcile, cascade }
        return work
      }),

    markChildrenCascaded: (flowId, childKeys) =>
      Effect.sync(() => {
        const rows = flowChildren.get(flowId)
        if (rows === undefined) return
        for (const key of childKeys) {
          const row = rows.get(key)
          if (row !== undefined) {
            row.cascaded = true
          }
        }
      }),

    counts: (queue) =>
      Effect.sync(() => {
        const counts = {
          waiting: 0,
          delayed: 0,
          active: 0,
          "waiting-children": 0,
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
        if (job === undefined || job.state === "active" || job.state === "waiting-children") {
          return false
        }
        deleteJob(id)
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
   * Store-level retention ceiling: terminal records older than this are
   * removed by a periodic sweep — one duration for all terminal states or a
   * per-state split (`{ completed: "1 day", failed: "30 days" }`). The sweep
   * also honours stricter per-job `keep.age` rules.
   */
  readonly historyTtl?: HistoryTtlInput | undefined
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
      const ttlByState = normalizeHistoryTtl(options.historyTtl)
      const intervalMs = Duration.toMillis(options.historySweepInterval ?? "1 minute")
      yield* Effect.gen(function*() {
        yield* Effect.sleep(intervalMs)
        const now = yield* Clock.currentTimeMillis
        sweepHistory(now, ttlByState)
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
