/**
 * A Redis-backed `JobStore` built on `effect/unstable/persistence`'s `Redis`
 * service. Every mutation is a single Lua script (see `scripts.ts`), so all
 * `JobStore` operations are atomic on the server and safe across processes.
 *
 * Provide the `Redis` service from your platform package — `NodeRedis.layer`
 * (`@effect/platform-node`, node-redis), `BunRedis.layer`
 * (`@effect/platform-bun`, `Bun.redis`) — or `Redis.make` over any client.
 *
 * Wake-ups ride the client's pub/sub channel, so workers in other processes
 * pick jobs up promptly; the worker's `pollInterval` is the fallback.
 *
 * @since 0.2.0
 */
import { Clock, type Context, Deferred, Duration, Effect, Exit, Layer, Option, Queue, Schedule, type Scope } from "effect"
import { Redis } from "effect/unstable/persistence"
import * as JobStore from "../JobStore.ts"
import * as scripts from "./scripts.ts"

/**
 * @since 0.2.0
 */
export interface RedisJobStoreOptions {
  /** Key prefix for everything this store writes (default `effect-mq`). */
  readonly prefix?: string | undefined
  /**
   * Store-level retention ceiling: terminal records older than this are
   * removed by a periodic sweep. Per-job `keep` may only be stricter.
   */
  readonly historyTtl?: Duration.Input | undefined
  /** History sweep cadence (default 1 minute). */
  readonly historySweepInterval?: Duration.Input | undefined
  /**
   * Generator for store-assigned job ids (e.g. `() => \`job_${ulid()}\``).
   * Default: `j-<n>` from the store's counter. See `JobStore.IdGenerator`.
   */
  readonly idGenerator?: JobStore.IdGenerator | undefined
}

const storeError = (message: string) => (cause: unknown) => new JobStore.JobStoreError({ message, cause })

/** Fold a Lua `HGETALL` reply (flat `[field, value, ...]`) into a map. */
const foldPairs = (flat: ReadonlyArray<string>): ReadonlyMap<string, string> => {
  const out = new Map<string, string>()
  for (let i = 0; i + 1 < flat.length; i += 2) {
    const field = flat[i]
    const value = flat[i + 1]
    if (field !== undefined && value !== undefined) {
      out.set(field, value)
    }
  }
  return out
}

// Hash fields use "" for absent optional values.
const optionalString = (value: string | undefined): string | undefined =>
  value === undefined || value === "" ? undefined : value

const optionalNumber = (value: string | undefined): number | undefined =>
  value === undefined || value === "" ? undefined : Number(value)

const optionalJson = <A = unknown>(value: string | undefined): A | undefined => {
  if (value === undefined || value === "") return undefined
  // SAFETY: the value round-trips JSON this driver itself wrote for the field.
  return JSON.parse(value) as A
}

const toRecord = (hash: ReadonlyMap<string, string>): JobStore.JobRecord => ({
  id: JobStore.JobId(hash.get("id") ?? ""),
  name: hash.get("name") ?? "",
  queue: JobStore.QueueName(hash.get("queue") ?? ""),
  payload: optionalJson(hash.get("payload")) ?? null,
  metadata: optionalJson<Readonly<Record<string, string>>>(hash.get("metadata")) ?? {},
  // SAFETY: the state field is only ever written with JobState members.
  state: (hash.get("state") ?? "waiting") as JobStore.JobState,
  priority: Number(hash.get("priority") ?? 0),
  attemptsMax: Number(hash.get("attemptsMax") ?? 1),
  attemptsMade: Number(hash.get("attemptsMade") ?? 0),
  stalledCount: Number(hash.get("stalledCount") ?? 0),
  backoff: optionalJson<JobStore.BackoffPolicy>(hash.get("backoff")),
  keep: optionalJson<JobStore.KeepPolicy>(hash.get("keep")),
  timeoutMs: optionalNumber(hash.get("timeoutMs")),
  cancelRequested: hash.get("cancelRequested") === "1",
  runAt: Number(hash.get("runAt") ?? 0),
  enqueuedAt: Number(hash.get("enqueuedAt") ?? 0),
  processedAt: optionalNumber(hash.get("processedAt")),
  finishedAt: optionalNumber(hash.get("finishedAt")),
  exit: optionalJson(hash.get("exit")),
  failedReason: optionalString(hash.get("failedReason"))
})

const toSchedule = (hash: ReadonlyMap<string, string>): JobStore.ScheduleRecord => ({
  key: JobStore.ScheduleKey(hash.get("key") ?? ""),
  jobName: hash.get("jobName") ?? "",
  queue: JobStore.QueueName(hash.get("queue") ?? ""),
  cron: optionalString(hash.get("cron")),
  tz: optionalString(hash.get("tz")),
  everyMs: optionalNumber(hash.get("everyMs")),
  payload: optionalJson(hash.get("payload")),
  metadata: optionalJson<Readonly<Record<string, string>>>(hash.get("metadata")) ?? {},
  priority: Number(hash.get("priority") ?? 0),
  attemptsMax: Number(hash.get("attemptsMax") ?? 1),
  backoff: optionalJson<JobStore.BackoffPolicy>(hash.get("backoff")),
  keep: optionalJson<JobStore.KeepPolicy>(hash.get("keep")),
  timeoutMs: optionalNumber(hash.get("timeoutMs")),
  nextRunAt: Number(hash.get("nextRunAt") ?? 0)
})

// cjson encodes an empty Lua table as {}, not [] — normalize.
const asArray = <A>(value: ReadonlyArray<A> | Record<string, never>): ReadonlyArray<A> =>
  Array.isArray(value) ? value : []

const JOB_STATES: ReadonlyArray<JobStore.JobState> = [
  "waiting",
  "delayed",
  "active",
  "completed",
  "failed",
  "cancelled"
]

/**
 * Build a `RedisJobStore` service. Needs the `Redis` service and a `Scope`
 * (the wake-up subscription and the optional history sweeper live in it).
 *
 * @since 0.2.0
 */
export const make = (
  options?: RedisJobStoreOptions | undefined
): Effect.Effect<JobStore.Service, never, Redis.Redis | Scope.Scope> =>
  Effect.gen(function*() {
    const redis = yield* Redis.Redis
    const prefix = options?.prefix ?? "effect-mq"
    const wakeChannel = `${prefix}:wake`

    const evalEnqueue = redis.eval(scripts.enqueue)
    const evalClaim = redis.eval(scripts.claim)
    const evalAck = redis.eval(scripts.ack)
    const evalRelease = redis.eval(scripts.release)
    const evalExtendLocks = redis.eval(scripts.extendLocks)
    const evalRecoverStalled = redis.eval(scripts.recoverStalled)
    const evalGetJob = redis.eval(scripts.getJob)
    const evalList = redis.eval(scripts.list)
    const evalCounts = redis.eval(scripts.counts)
    const evalRemove = redis.eval(scripts.remove)
    const evalRetry = redis.eval(scripts.retry)
    const evalCancel = redis.eval(scripts.cancel)
    const evalPromote = redis.eval(scripts.promote)
    const evalUpsertSchedule = redis.eval(scripts.upsertSchedule)
    const evalRemoveSchedule = redis.eval(scripts.removeSchedule)
    const evalListSchedules = redis.eval(scripts.listSchedules)
    const evalDueSchedules = redis.eval(scripts.dueSchedules)
    const evalAdvanceSchedule = redis.eval(scripts.advanceSchedule)
    const evalSweepHistory = redis.eval(scripts.sweepHistory)

    // Wake protocol: a local version + Deferred chain (same-process wake-ups
    // never depend on the pub/sub round trip), with the channel carrying
    // cross-process wake-ups. Same shape as the Postgres driver's NOTIFY.
    let wakeVersion = 0
    let wake = Deferred.makeUnsafe<void>()
    const signalWakeLocal = () => {
      wakeVersion += 1
      const current = wake
      wake = Deferred.makeUnsafe<void>()
      Deferred.doneUnsafe(current, Exit.succeed<void>(void 0))
    }
    const wakeUp = Effect.suspend(() => {
      signalWakeLocal()
      return redis.send("PUBLISH", wakeChannel, "1").pipe(Effect.ignore)
    })

    // Cross-process wake-ups. The pump resubscribes on connection loss (Bun
    // subscribers do not auto-reconnect); the retry delay only ever runs
    // after a real failure, so TestClock runs are unaffected.
    yield* Effect.scoped(
      Effect.gen(function*() {
        const messages = yield* redis.subscribe(wakeChannel)
        while (true) {
          yield* Queue.take(messages)
          signalWakeLocal()
        }
      })
    ).pipe(
      Effect.retry(Schedule.spaced("1 second")),
      Effect.catchCause((cause) => Effect.logWarning("effect-mq: redis wake subscription failed", cause)),
      Effect.forkScoped
    )

    if (options?.historyTtl !== undefined) {
      const ttlMs = Duration.toMillis(options.historyTtl)
      const intervalMs = Duration.toMillis(options.historySweepInterval ?? "1 minute")
      yield* Effect.gen(function*() {
        yield* Effect.sleep(intervalMs)
        const now = yield* Clock.currentTimeMillis
        while ((yield* evalSweepHistory(prefix, now - ttlMs, 500)) !== "0") {
          // bounded batches until the window is clean
        }
      }).pipe(
        Effect.catchCause((cause) => Effect.logError("effect-mq: redis history sweep failed", cause)),
        Effect.forever,
        Effect.forkScoped
      )
    }

    const generateCandidate = (request: JobStore.EnqueueRequest, generate: JobStore.IdGenerator) =>
      Effect.suspend(() => {
        const raw = generate(request)
        return Effect.isEffect(raw) ? raw : Effect.succeed(raw)
      })

    const enqueueOnce = (
      request: JobStore.EnqueueRequest,
      idMode: "user" | "generated" | "auto",
      id: string,
      now: number
    ) =>
      evalEnqueue(
        prefix,
        idMode,
        id,
        request.name,
        request.queue,
        JSON.stringify(request.payload ?? null),
        JSON.stringify(request.metadata),
        request.priority,
        request.attemptsMax,
        request.backoff === undefined ? "" : JSON.stringify(request.backoff),
        request.keep === undefined ? "" : JSON.stringify(request.keep),
        request.timeoutMs === undefined ? "" : String(request.timeoutMs),
        Math.max(0, request.delayMs),
        now
      )

    const store: JobStore.Service = {
      enqueue: (request) =>
        Effect.gen(function*() {
          const now = yield* Clock.currentTimeMillis
          const generate = options?.idGenerator
          for (let i = 0; i < 5; i++) {
            const mode = request.id !== undefined
              ? "user" as const
              : generate !== undefined
              ? "generated" as const
              : "auto" as const
            const candidate = mode === "user"
              ? request.id ?? ""
              : mode === "generated" && generate !== undefined
              ? yield* generateCandidate(request, generate)
              : ""
            const reply: {
              id?: string
              duplicate?: boolean
              wake?: boolean
              collision?: boolean
              error?: string
            } = JSON.parse(yield* enqueueOnce(request, mode, candidate, now))
            if (reply.collision === true) continue
            if (reply.error !== undefined || reply.id === undefined) {
              return yield* new JobStore.JobStoreError({
                message: "enqueue failed: could not generate a unique job id"
              })
            }
            if (reply.wake === true) {
              yield* wakeUp
            }
            return { id: JobStore.JobId(reply.id), duplicate: reply.duplicate === true }
          }
          return yield* new JobStore.JobStoreError({
            message: "enqueue failed: could not generate a unique job id"
          })
        }).pipe(
          Effect.mapError((error) =>
            error instanceof JobStore.JobStoreError ? error : storeError("enqueue failed")(error)
          )
        ),

      claim: (claimOptions) =>
        // Snapshot BEFORE the script runs: a wake that fires while the claim
        // executes must make awaitWake(token) return immediately.
        Effect.suspend(() => {
          const observedWake = wakeVersion
          return Effect.gen(function*() {
            const now = yield* Clock.currentTimeMillis
            const reply: { job?: ReadonlyArray<string>; empty?: boolean; nextRunAt?: number } = JSON.parse(
              yield* evalClaim(
                prefix,
                claimOptions.queue,
                JSON.stringify(claimOptions.names),
                claimOptions.token,
                claimOptions.lockDurationMs,
                now
              )
            )
            if (reply.job !== undefined) {
              const claimed: JobStore.ClaimResult = { _tag: "Claimed", job: toRecord(foldPairs(reply.job)) }
              return claimed
            }
            const empty: JobStore.ClaimResult = {
              _tag: "Empty",
              nextRunAt: reply.nextRunAt,
              wakeToken: observedWake
            }
            return empty
          })
        }).pipe(Effect.mapError(storeError("claim failed"))),

      ack: (id, token, outcome) =>
        Effect.gen(function*() {
          const now = yield* Clock.currentTimeMillis
          const exitJson = outcome._tag === "Cancelled" || outcome.exit === undefined
            ? ""
            : JSON.stringify(outcome.exit)
          const delayMs = outcome._tag === "Retry" ? Math.max(0, outcome.delayMs) : 0
          const reply: { error?: string; wake?: boolean } = JSON.parse(
            yield* evalAck(prefix, id, token, outcome._tag, exitJson, delayMs, now).pipe(
              Effect.mapError(storeError("ack failed"))
            )
          )
          if (reply.error === "notfound") return yield* new JobStore.JobNotFoundError({ jobId: id })
          if (reply.error === "locklost") return yield* new JobStore.LockLostError({ jobId: id })
          if (reply.wake === true) {
            yield* wakeUp
          }
        }),

      release: (id, token) =>
        Effect.gen(function*() {
          const now = yield* Clock.currentTimeMillis
          const reply: { error?: string; wake?: boolean } = JSON.parse(
            yield* evalRelease(prefix, id, token, now).pipe(Effect.mapError(storeError("release failed")))
          )
          if (reply.error === "notfound") return yield* new JobStore.JobNotFoundError({ jobId: id })
          if (reply.error === "locklost") return yield* new JobStore.LockLostError({ jobId: id })
          if (reply.wake === true) {
            yield* wakeUp
          }
        }),

      extendLocks: (locks, durationMs) =>
        Effect.gen(function*() {
          if (locks.length === 0) {
            const empty: JobStore.ExtendLocksResult = { lost: [], cancelRequested: [] }
            return empty
          }
          const now = yield* Clock.currentTimeMillis
          const reply: {
            lost: ReadonlyArray<string> | Record<string, never>
            cancel: ReadonlyArray<string> | Record<string, never>
          } = JSON.parse(yield* evalExtendLocks(prefix, JSON.stringify(locks), durationMs, now))
          const result: JobStore.ExtendLocksResult = {
            lost: asArray(reply.lost).map(JobStore.JobId),
            cancelRequested: asArray(reply.cancel).map(JobStore.JobId)
          }
          return result
        }).pipe(Effect.mapError(storeError("extendLocks failed"))),

      recoverStalled: (recoverOptions) =>
        Effect.gen(function*() {
          const now = yield* Clock.currentTimeMillis
          const recovered: ReadonlyArray<{ id: string; failed: boolean }> = JSON.parse(
            yield* evalRecoverStalled(prefix, recoverOptions.maxStalledCount, now)
          )
          const result = recovered.map((entry) => ({ id: JobStore.JobId(entry.id), failed: entry.failed }))
          if (result.some((entry) => !entry.failed)) {
            yield* wakeUp
          }
          return result
        }).pipe(Effect.mapError(storeError("recoverStalled failed"))),

      awaitWake: (_queues, wakeToken) =>
        Effect.suspend(() => {
          if (wakeVersion > wakeToken) return Effect.void
          return Deferred.await(wake)
        }),

      getJob: (id) =>
        evalGetJob(prefix, id).pipe(
          Effect.mapError(storeError("getJob failed")),
          Effect.map((raw) => {
            const flat: ReadonlyArray<string> = JSON.parse(raw)
            return flat.length === 0 ? Option.none() : Option.some(toRecord(foldPairs(flat)))
          })
        ),

      getAttempts: (id) =>
        redis.send("LRANGE", `${prefix}:attempts:${id}`, "0", "-1").pipe(
          Effect.mapError(storeError("getAttempts failed")),
          Effect.map((raw) => {
            // SAFETY: LRANGE always replies with an array of bulk strings.
            const entries = raw as ReadonlyArray<string>
            return entries.map((entry) => {
              const parsed: {
                attempt: number
                startedAt: number | null
                finishedAt: number
                outcome: JobStore.AttemptRecord["outcome"]
                exit?: unknown
              } = JSON.parse(entry)
              const record: JobStore.AttemptRecord = {
                attempt: parsed.attempt,
                startedAt: parsed.startedAt ?? undefined,
                finishedAt: parsed.finishedAt,
                outcome: parsed.outcome,
                // The ledger omits the key entirely for absent exits, so a
                // legitimate encoded null exit survives the round trip.
                exit: Object.hasOwn(parsed, "exit") ? parsed.exit : undefined
              }
              return record
            })
          })
        ),

      list: (listOptions) =>
        Effect.gen(function*() {
          const limit = Math.max(1, listOptions.limit ?? 50)
          const filters = {
            queue: listOptions.queue,
            name: listOptions.name,
            states: listOptions.states,
            metadata: listOptions.metadata
          }
          const reply: { items: ReadonlyArray<ReadonlyArray<string>> | Record<string, never>; more: boolean } = JSON
            .parse(
              yield* evalList(prefix, JSON.stringify(filters), listOptions.cursor ?? "", limit)
            )
          const items = asArray(reply.items).map((flat) => toRecord(foldPairs(flat)))
          const last = items[items.length - 1]
          const result: JobStore.ListResult = {
            items,
            cursor: reply.more && last !== undefined ? `${last.enqueuedAt}:${last.id}` : undefined
          }
          return result
        }).pipe(Effect.mapError(storeError("list failed"))),

      retry: (id) =>
        Effect.gen(function*() {
          const now = yield* Clock.currentTimeMillis
          const reply: { error?: string; state?: JobStore.JobState } = JSON.parse(
            yield* evalRetry(prefix, id, now).pipe(Effect.mapError(storeError("retry failed")))
          )
          if (reply.error === "notfound") return yield* new JobStore.JobNotFoundError({ jobId: id })
          if (reply.error === "state") {
            return yield* new JobStore.JobNotRetryableError({ jobId: id, state: reply.state ?? "failed" })
          }
          yield* wakeUp
        }),

      counts: (queue) =>
        evalCounts(prefix).pipe(
          Effect.mapError(storeError("counts failed")),
          Effect.map((raw) => {
            const flat: ReadonlyArray<string> = JSON.parse(raw)
            const byField = foldPairs(flat)
            // SAFETY: fromEntries over the exhaustive JOB_STATES list yields
            // exactly one zeroed entry per JobState member.
            const totals = Object.fromEntries(JOB_STATES.map((state) => [state, 0])) as Record<
              JobStore.JobState,
              number
            >
            for (const [field, count] of byField) {
              const split = field.lastIndexOf("|")
              const fieldQueue = field.slice(0, split)
              // SAFETY: counts fields are written as `<queue>|<JobState>`.
              const state = field.slice(split + 1) as JobStore.JobState
              if (queue === undefined || fieldQueue === queue) {
                totals[state] += Number(count)
              }
            }
            return totals
          })
        ),

      remove: (id) =>
        evalRemove(prefix, id).pipe(
          Effect.mapError(storeError("remove failed")),
          Effect.map((raw) => raw !== "0")
        ),

      cancel: (id) =>
        Effect.gen(function*() {
          const now = yield* Clock.currentTimeMillis
          const reply: { error?: string; state?: JobStore.JobState } = JSON.parse(
            yield* evalCancel(prefix, id, now).pipe(Effect.mapError(storeError("cancel failed")))
          )
          if (reply.error === "notfound") return yield* new JobStore.JobNotFoundError({ jobId: id })
          if (reply.error === "state") {
            return yield* new JobStore.JobNotCancellableError({ jobId: id, state: reply.state ?? "completed" })
          }
        }),

      promote: (id) =>
        Effect.gen(function*() {
          const now = yield* Clock.currentTimeMillis
          const reply: { error?: string; state?: JobStore.JobState } = JSON.parse(
            yield* evalPromote(prefix, id, now).pipe(Effect.mapError(storeError("promote failed")))
          )
          if (reply.error === "notfound") return yield* new JobStore.JobNotFoundError({ jobId: id })
          if (reply.error === "state") {
            return yield* new JobStore.JobNotPromotableError({ jobId: id, state: reply.state ?? "completed" })
          }
          yield* wakeUp
        }),

      pause: (queue) =>
        redis.send("SADD", `${prefix}:paused`, queue).pipe(
          Effect.mapError(storeError("pause failed")),
          Effect.asVoid
        ),

      resume: (queue) =>
        redis.send("SREM", `${prefix}:paused`, queue).pipe(
          Effect.mapError(storeError("resume failed")),
          Effect.flatMap((removed) => Number(removed) > 0 ? wakeUp : Effect.void)
        ),

      pausedQueues: () =>
        redis.send("SMEMBERS", `${prefix}:paused`).pipe(
          Effect.mapError(storeError("pausedQueues failed")),
          Effect.map((raw) => {
            // SAFETY: SMEMBERS always replies with an array of bulk strings.
            const members = raw as ReadonlyArray<string>
            return members.map(JobStore.QueueName)
          })
        ),

      upsertSchedule: (schedule) =>
        evalUpsertSchedule(
          prefix,
          schedule.key,
          schedule.jobName,
          schedule.queue,
          schedule.cron ?? "",
          schedule.tz ?? "",
          schedule.everyMs === undefined ? "" : String(schedule.everyMs),
          schedule.payload === undefined ? "" : JSON.stringify(schedule.payload),
          JSON.stringify(schedule.metadata),
          String(schedule.priority),
          String(schedule.attemptsMax),
          schedule.backoff === undefined ? "" : JSON.stringify(schedule.backoff),
          schedule.keep === undefined ? "" : JSON.stringify(schedule.keep),
          schedule.timeoutMs === undefined ? "" : String(schedule.timeoutMs),
          schedule.nextRunAt
        ).pipe(
          Effect.mapError(storeError("upsertSchedule failed")),
          Effect.andThen(wakeUp)
        ),

      removeSchedule: (key) =>
        evalRemoveSchedule(prefix, key).pipe(
          Effect.mapError(storeError("removeSchedule failed")),
          Effect.map((raw) => raw !== "0")
        ),

      listSchedules: (listOptions) =>
        evalListSchedules(prefix, JSON.stringify(listOptions ?? {})).pipe(
          Effect.mapError(storeError("listSchedules failed")),
          Effect.map((raw) => {
            const flat: ReadonlyArray<ReadonlyArray<string>> = JSON.parse(raw)
            return flat.map((pairs) => toSchedule(foldPairs(pairs)))
          })
        ),

      dueSchedules: () =>
        Effect.gen(function*() {
          const now = yield* Clock.currentTimeMillis
          const flat: ReadonlyArray<ReadonlyArray<string>> = JSON.parse(yield* evalDueSchedules(prefix, now))
          return flat.map((pairs) => toSchedule(foldPairs(pairs)))
        }).pipe(Effect.mapError(storeError("dueSchedules failed"))),

      advanceSchedule: (key, expectedRunAt, nextRunAt) =>
        evalAdvanceSchedule(prefix, key, expectedRunAt, nextRunAt).pipe(
          Effect.mapError(storeError("advanceSchedule failed")),
          Effect.asVoid
        )
    }

    return store
  })

/**
 * A Redis-backed layer for the default `JobStore`.
 *
 * @since 0.2.0
 */
export const layer = (
  options?: RedisJobStoreOptions | undefined
): Layer.Layer<JobStore.JobStore, never, Redis.Redis> => Layer.effect(JobStore.JobStore, make(options))

/**
 * A Redis-backed layer for a specific named store key.
 *
 * @since 0.2.0
 */
export const layerFor = <StoreId>(
  store: Context.Key<StoreId, JobStore.Service>,
  options?: RedisJobStoreOptions | undefined
): Layer.Layer<StoreId, never, Redis.Redis> => Layer.effect(store, make(options))
