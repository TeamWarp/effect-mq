/**
 * A Postgres `JobStore` running through drizzle's Effect driver
 * (`drizzle-orm/effect-postgres`, which is built on `@effect/sql-pg` —
 * Node and Bun compatible).
 *
 * - Claims use `FOR UPDATE SKIP LOCKED`; acks are lock-token guarded.
 * - ALL time comes from the Effect `Clock` as bind parameters (never SQL
 *   `now()`), so the conformance suite runs against real Postgres under
 *   `TestClock`.
 * - Wake-ups use LISTEN/NOTIFY through the shared `PgClient` (with the
 *   worker's `pollInterval` as the fallback), so cross-process workers wake
 *   promptly.
 *
 * TODO: a standalone non-drizzle Postgres driver on plain `@effect/sql-pg`
 * (same table layout), and an adapter for promise-based drizzle databases.
 *
 * @since 0.1.0
 */
import * as JobStore from "../JobStore.ts"
import type { PgClient } from "@effect/sql-pg"
import { asc, eq, getTableColumns, sql } from "drizzle-orm"
import * as PgDrizzle from "drizzle-orm/effect-postgres"
import { getTableConfig } from "drizzle-orm/pg-core"
import { Clock, type Context, Deferred, Duration, Effect, Layer, Option, type Scope, Stream } from "effect"
import type { MqDedupeTable, MqJobAttemptsTable, MqJobsTable, MqQueueControlTable, MqSchedulesTable } from "./schema.ts"

const { JobId } = JobStore

/**
 * @since 0.1.0
 */
export interface DrizzleJobStoreOptions<StoreId = JobStore.JobStore> {
  /** The jobs table instance (from `mqJobs`). */
  readonly jobs: MqJobsTable
  /** The run-ledger table instance (from `mqJobAttempts`). */
  readonly attempts: MqJobAttemptsTable
  /** The schedules table instance (from `mqSchedules`). */
  readonly schedules: MqSchedulesTable
  /** The queue pause/resume flags table (from `mqQueueControl`). */
  readonly queues: MqQueueControlTable
  /** The dedup-key registry table (from `mqDedupe`). */
  readonly dedupe: MqDedupeTable
  /**
   * Values for columns added via `mqJobs({ extend })`, evaluated at enqueue
   * (and on dedupe `replace`). Keys are the extended columns' TS names.
   * Default: each extended column fills from `request.metadata[<TS name>]`,
   * NULL when absent.
   */
  readonly extraValues?:
    | ((request: JobStore.EnqueueRequest) => Readonly<Record<string, ExtraColumnValue>>)
    | undefined
  /** Bind to a `JobStore.named(...)` key; default: the default `JobStore`. */
  readonly store?: Context.Key<StoreId, JobStore.Service> | undefined
  /**
   * Store-level retention ceiling: terminal records older than this are
   * removed by a periodic sweep. Per-job `keep` may only be stricter.
   */
  readonly historyTtl?: Duration.Input | undefined
  /** History sweep cadence (default 1 minute). */
  readonly historySweepInterval?: Duration.Input | undefined
  /**
   * Generator for store-assigned job ids (e.g. `() => \`job_${ulid()}\``).
   * Default: `j-<seq>` from the jobs sequence. See `JobStore.IdGenerator`.
   */
  readonly idGenerator?: JobStore.IdGenerator | undefined
  /**
   * Probe the tables at startup and fail fast when the schema is missing
   * (default true). Migrations are owned by your drizzle-kit pipeline.
   */
  readonly validate?: boolean | undefined
}

type Db = PgDrizzle.EffectPgDatabase & { readonly $client: PgClient.PgClient }

/**
 * A value the driver can bind directly into an extended column.
 *
 * @since 0.3.0
 */
export type ExtraColumnValue = string | number | boolean | Date | null

const storeError = (message: string) => (cause: unknown) =>
  new JobStore.JobStoreError({ message, cause })

/**
 * drizzle's Effect driver types raw `execute` results as row arrays, but at
 * runtime the raw path yields the node-postgres `QueryResult` envelope
 * (`{ rows }`) while query-builder paths really do yield arrays. Accept both.
 */
interface RowEnvelope<T> {
  readonly rows: ReadonlyArray<T>
}

const rowsOf = <T>(result: ReadonlyArray<T> | RowEnvelope<T>): ReadonlyArray<T> =>
  "rows" in result ? result.rows : result

type JobRow = {
  readonly id: string
  readonly name: string
  readonly queue: string
  readonly state: JobStore.JobState
  readonly priority: number
  readonly seq: number | string
  readonly payload: unknown
  readonly metadata: Record<string, string>
  readonly attemptsMax: number
  readonly attemptsMade: number
  readonly stalledCount: number
  readonly backoff: JobStore.BackoffPolicy | null
  readonly keep: JobStore.KeepPolicy | null
  readonly timeoutMs: number | string | null
  readonly cancelRequested: boolean
  readonly dedupeKey: string | null
  readonly runAt: Date
  readonly enqueuedAt: Date
  readonly processedAt: Date | null
  readonly finishedAt: Date | null
  readonly exit: unknown
  readonly failedReason: string | null
}

type ScheduleRow = {
  readonly key: string
  readonly jobName: string
  readonly queue: string
  readonly cron: string | null
  readonly tz: string | null
  readonly everyMs: number | string | null
  readonly payload: unknown
  readonly metadata: Record<string, string>
  readonly priority: number
  readonly attemptsMax: number
  readonly backoff: JobStore.BackoffPolicy | null
  readonly keep: JobStore.KeepPolicy | null
  readonly timeoutMs: number | string | null
  readonly nextRunAt: Date
}

const toSchedule = (row: ScheduleRow): JobStore.ScheduleRecord => ({
  key: JobStore.ScheduleKey(row.key),
  jobName: row.jobName,
  queue: JobStore.QueueName(row.queue),
  cron: row.cron ?? undefined,
  tz: row.tz ?? undefined,
  everyMs: row.everyMs === null ? undefined : Number(row.everyMs),
  payload: row.payload,
  metadata: row.metadata ?? {},
  priority: row.priority,
  attemptsMax: row.attemptsMax,
  backoff: row.backoff ?? undefined,
  keep: row.keep ?? undefined,
  timeoutMs: row.timeoutMs === null ? undefined : Number(row.timeoutMs),
  nextRunAt: row.nextRunAt.getTime()
})

const toRecord = (row: JobRow): JobStore.JobRecord => ({
  id: JobId(row.id),
  name: row.name,
  queue: JobStore.QueueName(row.queue),
  payload: row.payload,
  metadata: row.metadata ?? {},
  state: row.state,
  priority: row.priority,
  attemptsMax: row.attemptsMax,
  attemptsMade: row.attemptsMade,
  stalledCount: row.stalledCount,
  backoff: row.backoff ?? undefined,
  keep: row.keep ?? undefined,
  timeoutMs: row.timeoutMs === null || row.timeoutMs === undefined ? undefined : Number(row.timeoutMs),
  cancelRequested: row.cancelRequested,
  dedupeKey: row.dedupeKey ?? undefined,
  runAt: row.runAt.getTime(),
  enqueuedAt: row.enqueuedAt.getTime(),
  processedAt: row.processedAt?.getTime(),
  finishedAt: row.finishedAt?.getTime(),
  exit: row.exit ?? undefined,
  failedReason: row.failedReason ?? undefined
})

/**
 * Build the store implementation. Requires `PgClient` and a `Scope` (for the
 * LISTEN subscription).
 *
 * @since 0.1.0
 */
export const make = (
  options: DrizzleJobStoreOptions<any>
): Effect.Effect<JobStore.Service, JobStore.JobStoreError, PgClient.PgClient | Scope.Scope> =>
  Effect.gen(function*() {
    const db: Db = yield* PgDrizzle.makeWithDefaults()
    const client = db.$client
    const jobs = options.jobs
    const attempts = options.attempts
    const schedules = options.schedules
    const queues = options.queues
    const dedupe = options.dedupe
    const jobsName = getTableConfig(jobs).name
    const attemptsName = getTableConfig(attempts).name
    const wakeChannel = `effect_mq_wake_${jobsName}`

    // Columns the user added via `mqJobs({ extend })`: everything beyond the
    // factory's own set. They are written at enqueue (and on dedupe replace)
    // from `extraValues` or the metadata entry with the same TS key.
    const BASE_JOB_COLUMNS = new Set([
      "id",
      "name",
      "queue",
      "state",
      "priority",
      "seq",
      "payload",
      "metadata",
      "attemptsMax",
      "attemptsMade",
      "stalledCount",
      "backoff",
      "keep",
      "timeoutMs",
      "cancelRequested",
      "dedupeKey",
      "runAt",
      "enqueuedAt",
      "processedAt",
      "finishedAt",
      "exit",
      "failedReason",
      "lockToken",
      "lockExpiresAt"
    ])
    const extendedColumns = Object.entries(getTableColumns(jobs))
      .filter(([key]) => !BASE_JOB_COLUMNS.has(key))
      .map(([key, column]) => ({ key, name: column.name }))
    const extraColumnNames = extendedColumns.length === 0
      ? sql``
      : sql.join(extendedColumns.map((column) => sql`, ${sql.identifier(column.name)}`))
    const extraColumnValues = (request: JobStore.EnqueueRequest) => {
      if (extendedColumns.length === 0) return sql``
      const mapped = options.extraValues?.(request) ?? {}
      return sql.join(extendedColumns.map((column) =>
        sql`, ${Object.hasOwn(mapped, column.key) ? mapped[column.key] : request.metadata[column.key] ?? null}`
      ))
    }
    const extraColumnAssignments = (request: JobStore.EnqueueRequest) => {
      if (extendedColumns.length === 0) return sql``
      const mapped = options.extraValues?.(request) ?? {}
      return sql.join(extendedColumns.map((column) =>
        sql`, ${sql.identifier(column.name)} = ${
          Object.hasOwn(mapped, column.key) ? mapped[column.key] : request.metadata[column.key] ?? null
        }`
      ))
    }

    if (options.validate ?? true) {
      yield* Effect.all([
        db.select({ id: jobs.id }).from(jobs).limit(0),
        db.select({ jobId: attempts.jobId }).from(attempts).limit(0),
        db.select({ key: schedules.key }).from(schedules).limit(0),
        db.select({ queue: queues.queue }).from(queues).limit(0),
        db.select({ key: dedupe.key }).from(dedupe).limit(0)
      ]).pipe(
        Effect.mapError(storeError(
          `effect-mq: tables "${jobsName}"/"${attemptsName}" are missing or mismatched — ` +
            `re-export the effect-mq/drizzle schema factories from your drizzle schema and run your migrations (drizzle-kit generate)`
        ))
      )
    }

    // Wake plumbing: a local version + deferred chain (same protocol as the
    // memory driver), fed by (a) local store operations and (b) cross-process
    // LISTEN notifications.
    let wakeVersion = 0
    let wake = Deferred.makeUnsafe<void>()
    const signalWake = () => {
      wakeVersion += 1
      const current = wake
      wake = Deferred.makeUnsafe<void>()
      Deferred.doneUnsafe(current, Effect.void)
    }
    // Resubscribe forever: if the LISTEN stream ends or fails, wake-ups
    // degrade to the worker's pollInterval until the next attempt succeeds.
    yield* client.listen(wakeChannel).pipe(
      Stream.runForEach(() => Effect.sync(signalWake)),
      Effect.catchCause((cause) =>
        Effect.logWarning(
          "effect-mq: LISTEN subscription failed; wake-ups degraded to polling until resubscribe",
          cause
        )
      ),
      Effect.andThen(Effect.sleep("1 second")),
      Effect.forever,
      Effect.forkScoped
    )
    if (options.historyTtl !== undefined) {
      const ttlMs = Duration.toMillis(options.historyTtl)
      const sweepMs = Duration.toMillis(options.historySweepInterval ?? "1 minute")
      yield* Effect.gen(function*() {
        yield* Effect.sleep(sweepMs)
        const now = yield* nowDate
        yield* db.execute(sql`
          DELETE FROM ${jobs}
          WHERE ${jobs.state} IN ('completed', 'failed', 'cancelled')
            AND ${jobs.finishedAt} <= ${new Date(now.getTime() - ttlMs)}
        `)
        // Dead dedup rows: expired windows, or pointers at vanished jobs.
        yield* db.execute(sql`
          DELETE FROM ${dedupe}
          WHERE (${dedupe.windowExpiresAt} IS NOT NULL AND ${dedupe.windowExpiresAt} <= ${now})
             OR (${dedupe.windowExpiresAt} IS NULL AND NOT EXISTS (
               SELECT 1 FROM ${jobs} WHERE ${jobs.id} = ${dedupe.jobId}
                 AND ${jobs.state} IN ('waiting', 'delayed', 'active')
             ))
        `)
      }).pipe(
        Effect.catchCause((cause) => Effect.logWarning("effect-mq: history sweep failed", cause)),
        Effect.forever,
        Effect.forkScoped
      )
    }

    // Local mutation wake-up: bump synchronously, then best-effort NOTIFY so
    // workers in other processes wake promptly too.
    const wakeUp: Effect.Effect<void> = Effect.suspend(() => {
      signalWake()
      // The payload must be non-empty: @effect/sql-pg drops falsy payloads.
      return client.notify(wakeChannel, "1").pipe(Effect.ignore)
    })

    const nowDate = Effect.map(Clock.currentTimeMillis, (ms) => new Date(ms))

    // quote_ident handles quoted/mixed-case table names safely.
    const seqExpr = sql`nextval(pg_get_serial_sequence(quote_ident(${jobsName}), 'seq'))`

    const insertAttempt = (
      tx: Pick<Db, "execute">,
      jobId: string,
      outcome: JobStore.AttemptRecord["outcome"],
      startedAt: Date | null,
      finishedAt: Date,
      exit: JobStore.AttemptRecord["exit"]
    ) =>
      tx.execute(sql`
        INSERT INTO ${attempts} (job_id, attempt, outcome, started_at, finished_at, exit)
        SELECT ${jobId}, COALESCE(MAX(${attempts.attempt}), 0) + 1, ${outcome}, ${startedAt}, ${finishedAt},
               ${exit === undefined ? null : JSON.stringify(exit)}::jsonb
        FROM ${attempts} WHERE ${attempts.jobId} = ${jobId}
      `)

    // Retention: drop terminal peers (same name + state) beyond count/age.
    const applyKeep = (
      tx: Pick<Db, "execute">,
      row: { name: string; state: string; keep: JobStore.KeepPolicy | null },
      now: Date
    ) =>
      Effect.gen(function*() {
        const keep = row.keep
        if (keep === null || keep === undefined) return
        if (keep.ageMs !== undefined) {
          yield* tx.execute(sql`
            DELETE FROM ${jobs}
            WHERE ${jobs.name} = ${row.name} AND ${jobs.state} = ${row.state}
              AND ${jobs.finishedAt} <= ${new Date(now.getTime() - keep.ageMs)}
          `)
        }
        if (keep.count !== undefined) {
          yield* tx.execute(sql`
            DELETE FROM ${jobs}
            WHERE ${jobs.name} = ${row.name} AND ${jobs.state} = ${row.state}
              AND ${jobs.id} NOT IN (
                SELECT ${jobs.id} FROM ${jobs}
                WHERE ${jobs.name} = ${row.name} AND ${jobs.state} = ${row.state}
                ORDER BY ${jobs.finishedAt} DESC, ${jobs.seq} DESC
                LIMIT ${keep.count}
              )
          `)
        }
      })

    // Distinguish JobNotFound vs LockLost after a guarded UPDATE hit 0 rows.
    const explainMiss = (
      id: JobStore.JobId
    ): Effect.Effect<never, JobStore.JobStoreError | JobStore.JobNotFoundError | JobStore.LockLostError> =>
      db.select({ id: jobs.id }).from(jobs).where(eq(jobs.id, id)).pipe(
        Effect.mapError(storeError("failed to inspect job")),
        Effect.flatMap((rows) =>
          Effect.fail<JobStore.JobNotFoundError | JobStore.LockLostError>(
            rows.length === 0
              ? new JobStore.JobNotFoundError({ jobId: id })
              : new JobStore.LockLostError({ jobId: id })
          )
        )
      )

    // The shared INSERT: store-assigned ids come from the configured
    // generator (or the seq sequence); loop on the (unlikely) collision with
    // an existing id — ON CONFLICT DO NOTHING makes the retry safe. Returns
    // the result or undefined when the caller-supplied id already exists.
    const insertJob = (
      exec: Pick<Db, "execute">,
      request: JobStore.EnqueueRequest,
      now: Date
    ) =>
      Effect.gen(function*() {
        const runAt = new Date(now.getTime() + Math.max(0, request.delayMs))
        const state = request.delayMs > 0 ? "delayed" : "waiting"
        const generate = options.idGenerator
        for (let i = 0; i < 5; i++) {
          const generated = request.id === undefined && generate !== undefined
            ? yield* Effect.suspend(() => {
              const raw = generate(request)
              return Effect.isEffect(raw) ? raw : Effect.succeed(raw)
            })
            : undefined
          const idExpr = request.id !== undefined
            ? sql`${request.id}`
            : generated !== undefined
            ? sql`${generated}`
            : sql`'j-' || ${seqExpr}::text`
          const rows = rowsOf(yield* exec.execute<{ id: string }>(sql`
            INSERT INTO ${jobs} (id, name, queue, state, priority, payload, metadata,
              attempts_max, backoff, keep, timeout_ms, dedupe_key, run_at, enqueued_at${extraColumnNames})
            VALUES (${idExpr}, ${request.name}, ${request.queue}, ${state}, ${request.priority},
              ${JSON.stringify(request.payload ?? null)}::jsonb, ${JSON.stringify(request.metadata)}::jsonb,
              ${request.attemptsMax},
              ${request.backoff === undefined ? null : JSON.stringify(request.backoff)}::jsonb,
              ${request.keep === undefined ? null : JSON.stringify(request.keep)}::jsonb,
              ${request.timeoutMs ?? null}, ${request.dedupe?.key ?? null}, ${runAt}, ${now}${extraColumnValues(request)})
            ON CONFLICT (id) DO NOTHING
            RETURNING ${jobs.id} AS id
          `).pipe(Effect.mapError(storeError("enqueue failed"))))
          const inserted = rows[0]
          if (inserted !== undefined) {
            return { id: JobId(inserted.id), duplicate: false }
          }
          if (request.id !== undefined) {
            return { id: request.id, duplicate: true }
          }
          // generated id collided with an existing user id; try again
        }
        return yield* new JobStore.JobStoreError({
          message: "enqueue failed: could not generate a unique job id"
        })
      })

    // Enqueue with a dedup policy: one transaction locks the (name, key) row
    // and applies the decision tree (replace-while-delayed, throttle window,
    // pending dedup) before falling through to a fresh insert.
    const enqueueDeduped = (request: JobStore.EnqueueRequest, policy: JobStore.DedupePolicy) =>
      db.transaction((tx) =>
        Effect.gen(function*() {
          const now = yield* nowDate
          // The explicit-id duplicate check precedes the dedup tree, matching
          // the memory and redis drivers.
          if (request.id !== undefined) {
            const existing = rowsOf(yield* tx.execute<{ id: string }>(sql`
              SELECT ${jobs.id} AS id FROM ${jobs} WHERE ${jobs.id} = ${request.id}
            `))
            if (existing.length > 0) {
              return { id: request.id, duplicate: true, wake: false }
            }
          }
          // A SELECT FOR UPDATE on a missing row locks nothing, so two
          // concurrent first-enqueues would both insert. The no-op upsert
          // always takes the row lock: a fresh placeholder (job_id = '')
          // reads as "no entry" and falls through to the insert below.
          const rows = rowsOf(yield* tx.execute<{ jobId: string; windowExpiresAt: Date | null }>(sql`
            INSERT INTO ${dedupe} (name, key, job_id, window_expires_at)
            VALUES (${request.name}, ${policy.key}, '', NULL)
            ON CONFLICT (name, key) DO UPDATE SET name = EXCLUDED.name
            RETURNING ${dedupe.jobId} AS "jobId", ${dedupe.windowExpiresAt} AS "windowExpiresAt"
          `))
          const entry = rows[0]
          if (entry !== undefined && entry.jobId !== "") {
            // Plain read (no FOR UPDATE): locking the job row here would
            // invert the jobs-then-dedupe lock order every terminal
            // transition uses and deadlock under load. The replace branch
            // compensates with a state-conditional UPDATE.
            const keyed = rowsOf(yield* tx.execute<{ state: JobStore.JobState }>(sql`
              SELECT ${jobs.state} AS state FROM ${jobs} WHERE ${jobs.id} = ${entry.jobId}
            `))
            const keyedState = keyed[0]?.state
            const windowLive = entry.windowExpiresAt !== null &&
              entry.windowExpiresAt.getTime() > now.getTime()
            const bumpWindow = policy.extend && policy.ttlMs !== undefined
              ? tx.execute(sql`
                UPDATE ${dedupe} SET window_expires_at = ${new Date(now.getTime() + policy.ttlMs)}
                WHERE ${dedupe.name} = ${request.name} AND ${dedupe.key} = ${policy.key}
              `).pipe(Effect.asVoid)
              : Effect.void
            // Latest-wins while the keyed job is still delayed. The UPDATE
            // re-checks the state so a concurrent claim degrades this to a
            // plain dedup instead of rewriting an active job.
            if (policy.replace && keyedState === "delayed") {
              const replaced = rowsOf(yield* tx.execute<{ id: string }>(sql`
                UPDATE ${jobs} SET
                  payload = ${JSON.stringify(request.payload ?? null)}::jsonb,
                  metadata = ${JSON.stringify(request.metadata)}::jsonb,
                  priority = ${request.priority},
                  attempts_max = ${request.attemptsMax},
                  backoff = ${request.backoff === undefined ? null : JSON.stringify(request.backoff)}::jsonb,
                  keep = ${request.keep === undefined ? null : JSON.stringify(request.keep)}::jsonb,
                  timeout_ms = ${request.timeoutMs ?? null},
                  run_at = ${new Date(now.getTime() + Math.max(0, request.delayMs))}${extraColumnAssignments(request)}
                WHERE ${jobs.id} = ${entry.jobId} AND ${jobs.state} = 'delayed'
                RETURNING ${jobs.id} AS id
              `))
              if (replaced.length > 0) {
                // A landed replace re-arms the ttl window (the entry must
                // outlive the chain it is deduplicating).
                if (policy.ttlMs !== undefined) {
                  yield* tx.execute(sql`
                    UPDATE ${dedupe} SET window_expires_at = ${new Date(now.getTime() + policy.ttlMs)}
                    WHERE ${dedupe.name} = ${request.name} AND ${dedupe.key} = ${policy.key}
                  `)
                }
                return { id: JobId(entry.jobId), duplicate: true, wake: true }
              }
              return { id: JobId(entry.jobId), duplicate: true, wake: false }
            }
            if (windowLive) {
              yield* bumpWindow
              return { id: JobId(entry.jobId), duplicate: true, wake: false }
            }
            const pending = keyedState !== undefined && keyedState !== "completed" &&
              keyedState !== "failed" && keyedState !== "cancelled"
            if (entry.windowExpiresAt === null && pending) {
              return { id: JobId(entry.jobId), duplicate: true, wake: false }
            }
            // Dead entry: the new job takes over the key below.
          }
          const result = yield* insertJob(tx, request, now)
          if (!result.duplicate) {
            yield* tx.execute(sql`
              INSERT INTO ${dedupe} (name, key, job_id, window_expires_at)
              VALUES (${request.name}, ${policy.key}, ${result.id},
                ${policy.ttlMs === undefined ? null : new Date(now.getTime() + policy.ttlMs)})
              ON CONFLICT (name, key) DO UPDATE SET
                job_id = EXCLUDED.job_id, window_expires_at = EXCLUDED.window_expires_at
            `)
          }
          return { ...result, wake: !result.duplicate }
        })
      ).pipe(
        // Residual lock-order inversions (replace vs cancel of the same
        // delayed job) surface as Postgres deadlocks (40P01); one side is
        // killed and safe to retry.
        Effect.retry({
          times: 3,
          while: (error) => String(error).includes("40P01") || String(error).includes("deadlock detected")
        }),
        Effect.mapError((error) =>
          error instanceof JobStore.JobStoreError ? error : storeError("enqueue failed")(error)
        )
      )

    // A job leaving the pending states frees its pending-mode dedup row; live
    // throttle windows deliberately outlast the job.
    const releaseDedupe = (
      exec: Pick<Db, "execute">,
      name: string,
      dedupeKey: string | null,
      jobId: string,
      now: Date
    ) =>
      dedupeKey === null
        ? Effect.void
        : exec.execute(sql`
          DELETE FROM ${dedupe}
          WHERE ${dedupe.name} = ${name} AND ${dedupe.key} = ${dedupeKey}
            AND ${dedupe.jobId} = ${jobId}
            AND (${dedupe.windowExpiresAt} IS NULL OR ${dedupe.windowExpiresAt} <= ${now})
        `).pipe(Effect.asVoid)

    const store: JobStore.Service = {
      enqueue: (request) =>
        Effect.gen(function*() {
          if (request.dedupe !== undefined) {
            const result = yield* enqueueDeduped(request, request.dedupe)
            if (result.wake) {
              yield* wakeUp
            }
            return { id: result.id, duplicate: result.duplicate }
          }
          const now = yield* nowDate
          const result = yield* insertJob(db, request, now)
          if (!result.duplicate) {
            yield* wakeUp
          }
          return result
        }),

      claim: (claimOptions) =>
        Effect.suspend(() => {
          // Snapshot BEFORE the transaction: any wake that fires while the
          // claim's statements run must make awaitWake(token) return
          // immediately (spurious wake-ups are allowed; lost ones are not).
          const observedWake = wakeVersion
          return db.transaction((tx) =>
          Effect.gen(function*() {
            const now = yield* nowDate
            // Promote due delayed jobs first (separate statement: CTEs share
            // a snapshot, so an UPDATE CTE would be invisible to the claim).
            yield* tx.execute(sql`
              UPDATE ${jobs} SET state = 'waiting'
              WHERE ${jobs.queue} = ${claimOptions.queue} AND ${jobs.state} = 'delayed'
                AND ${jobs.runAt} <= ${now}
            `)
            const pausedRows = rowsOf(yield* tx.execute<{ paused: boolean }>(sql`
              SELECT ${queues.paused} AS "paused" FROM ${queues}
              WHERE ${queues.queue} = ${claimOptions.queue}
            `))
            const isPaused = pausedRows[0]?.paused === true
            const claimed = isPaused ? [] : rowsOf(yield* tx.execute<JobRow>(sql`
              WITH candidate AS (
                SELECT ${jobs.id} AS id FROM ${jobs}
                WHERE ${jobs.queue} = ${claimOptions.queue} AND ${jobs.state} = 'waiting'
                  AND ${jobs.name} = ANY(${sql.param([...claimOptions.names])})
                ORDER BY ${jobs.priority} DESC, ${jobs.seq} ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
              )
              UPDATE ${jobs} SET state = 'active', lock_token = ${claimOptions.token},
                lock_expires_at = ${new Date(now.getTime() + claimOptions.lockDurationMs)},
                processed_at = ${now}
              FROM candidate WHERE ${jobs.id} = candidate.id
              RETURNING ${jobs.id} AS "id", ${jobs.name} AS "name", ${jobs.queue} AS "queue",
                ${jobs.state} AS "state", ${jobs.priority} AS "priority", ${jobs.seq} AS "seq",
                ${jobs.payload} AS "payload", ${jobs.metadata} AS "metadata",
                ${jobs.attemptsMax} AS "attemptsMax", ${jobs.attemptsMade} AS "attemptsMade",
                ${jobs.stalledCount} AS "stalledCount", ${jobs.backoff} AS "backoff",
                ${jobs.keep} AS "keep", ${jobs.timeoutMs} AS "timeoutMs",
                ${jobs.cancelRequested} AS "cancelRequested", ${jobs.dedupeKey} AS "dedupeKey",
                ${jobs.runAt} AS "runAt", ${jobs.enqueuedAt} AS "enqueuedAt",
                ${jobs.processedAt} AS "processedAt", ${jobs.finishedAt} AS "finishedAt",
                ${jobs.exit} AS "exit", ${jobs.failedReason} AS "failedReason"
            `))
            const row = claimed[0]
            if (row !== undefined) {
              const result: JobStore.ClaimResult = { _tag: "Claimed", job: toRecord(row) }
              return result
            }
            const next = rowsOf(yield* tx.execute<{ next: Date | null }>(sql`
              SELECT MIN(${jobs.runAt}) AS next FROM ${jobs}
              WHERE ${jobs.queue} = ${claimOptions.queue} AND ${jobs.state} = 'delayed'
                AND ${jobs.name} = ANY(${sql.param([...claimOptions.names])})
            `))
            const empty: JobStore.ClaimResult = {
              _tag: "Empty",
              nextRunAt: next[0]?.next?.getTime(),
              wakeToken: observedWake
            }
            return empty
          })
          )
        }).pipe(Effect.mapError((error) =>
          error instanceof JobStore.JobStoreError ? error : storeError("claim failed")(error)
        )),

      ack: (id, token, outcome) =>
        db.transaction((tx) =>
          Effect.gen(function*() {
            const now = yield* nowDate
            const update = outcome._tag === "Complete"
              ? sql`state = 'completed', cancel_requested = FALSE, exit = ${JSON.stringify(outcome.exit ?? null)}::jsonb, finished_at = ${now}`
              : outcome._tag === "Fail"
              ? sql`state = 'failed', cancel_requested = FALSE, exit = ${JSON.stringify(outcome.exit ?? null)}::jsonb, finished_at = ${now}`
              : outcome._tag === "Cancelled"
              ? sql`state = 'cancelled', cancel_requested = FALSE, finished_at = ${now}`
              // A cancel that raced this natural failure wins over revival
              // (mirrors release/recoverStalled).
              : sql`state = CASE WHEN ${jobs.cancelRequested} THEN 'cancelled'
                    ELSE ${outcome.delayMs > 0 ? "delayed" : "waiting"} END,
                  finished_at = CASE WHEN ${jobs.cancelRequested} THEN ${now}::timestamptz ELSE NULL END,
                  run_at = CASE WHEN ${jobs.cancelRequested} THEN ${jobs.runAt}
                    ELSE ${new Date(now.getTime() + Math.max(0, outcome.delayMs))}::timestamptz END,
                  seq = CASE WHEN ${jobs.cancelRequested} THEN ${jobs.seq} ELSE ${seqExpr} END,
                  cancel_requested = FALSE`
            const rows = rowsOf(yield* tx.execute<
              {
                processedAt: Date | null
                name: string
                state: string
                keep: JobStore.KeepPolicy | null
                dedupeKey: string | null
              }
            >(sql`
              UPDATE ${jobs} SET ${update},
                attempts_made = ${jobs.attemptsMade} + 1, lock_token = NULL, lock_expires_at = NULL
              WHERE ${jobs.id} = ${id} AND ${jobs.state} = 'active' AND ${jobs.lockToken} = ${token}
              RETURNING ${jobs.processedAt} AS "processedAt", ${jobs.name} AS "name",
                ${jobs.state} AS "state", ${jobs.keep} AS "keep", ${jobs.dedupeKey} AS "dedupeKey"
            `))
            const row = rows[0]
            if (row === undefined) {
              return yield* explainMiss(id)
            }
            const cancelledRetry = outcome._tag === "Retry" && row.state === "cancelled"
            const ledgerOutcome = outcome._tag === "Complete"
              ? "completed" as const
              : outcome._tag === "Fail"
              ? "failed" as const
              : outcome._tag === "Cancelled" || cancelledRetry
              ? "cancelled" as const
              : "retried" as const
            yield* insertAttempt(
              tx,
              id,
              ledgerOutcome,
              row.processedAt,
              now,
              outcome._tag === "Cancelled" || cancelledRetry ? undefined : outcome.exit
            )
            if (outcome._tag !== "Retry" || cancelledRetry) {
              yield* releaseDedupe(tx, row.name, row.dedupeKey, id, now)
              yield* applyKeep(tx, row, now)
            }
          })
        ).pipe(
          Effect.mapError((error) =>
            error instanceof JobStore.JobNotFoundError || error instanceof JobStore.LockLostError ||
              error instanceof JobStore.JobStoreError
              ? error
              : storeError("ack failed")(error)
          ),
          Effect.tap(() => outcome._tag === "Retry" ? wakeUp : Effect.void)
        ),

      release: (id, token) =>
        Effect.gen(function*() {
          const cancelled = yield* db.transaction((tx) =>
            Effect.gen(function*() {
              const now = yield* nowDate
              // A cancel that arrived while the worker was shutting down is
              // honoured instead of reviving the job.
              const rows = rowsOf(yield* tx.execute<
                {
                  id: string
                  cancelled: boolean
                  processedAt: Date | null
                  name: string
                  keep: JobStore.KeepPolicy | null
                  dedupeKey: string | null
                }
              >(sql`
                UPDATE ${jobs} SET
                  state = CASE WHEN ${jobs.cancelRequested} THEN 'cancelled' ELSE 'waiting' END,
                  finished_at = CASE WHEN ${jobs.cancelRequested} THEN ${now}::timestamptz ELSE NULL END,
                  cancel_requested = FALSE,
                  lock_token = NULL, lock_expires_at = NULL
                WHERE ${jobs.id} = ${id} AND ${jobs.state} = 'active' AND ${jobs.lockToken} = ${token}
                RETURNING ${jobs.id} AS id, (${jobs.state} = 'cancelled') AS cancelled,
                  ${jobs.processedAt} AS "processedAt", ${jobs.name} AS "name", ${jobs.keep} AS "keep",
                  ${jobs.dedupeKey} AS "dedupeKey"
              `))
              const row = rows[0]
              if (row === undefined) return undefined
              if (row.cancelled) {
                yield* insertAttempt(tx, id, "cancelled", row.processedAt, now, undefined)
                yield* releaseDedupe(tx, row.name, row.dedupeKey, id, now)
                yield* applyKeep(tx, { name: row.name, state: "cancelled", keep: row.keep }, now)
              }
              return row.cancelled
            })
          ).pipe(Effect.mapError((error) =>
            error instanceof JobStore.JobStoreError ? error : storeError("release failed")(error)
          ))
          if (cancelled === undefined) {
            return yield* explainMiss(id)
          }
          if (!cancelled) {
            yield* wakeUp
          }
        }),

      extendLocks: (locks, durationMs) =>
        Effect.gen(function*() {
          if (locks.length === 0) {
            const empty: JobStore.ExtendLocksResult = { lost: [], cancelRequested: [] }
            return empty
          }
          const now = yield* nowDate
          const rows = rowsOf(yield* db.execute<{ id: string; status: "lost" | "cancel" }>(sql`
            WITH input AS (
              SELECT ids.job_id, toks.token
              FROM unnest(${sql.param(locks.map((lock) => lock.id))}::text[]) WITH ORDINALITY AS ids(job_id, ord)
              JOIN unnest(${sql.param(locks.map((lock) => lock.token))}::text[]) WITH ORDINALITY AS toks(token, ord) USING (ord)
            ),
            updated AS (
              UPDATE ${jobs} SET lock_expires_at = ${new Date(now.getTime() + durationMs)}
              FROM input
              WHERE ${jobs.id} = input.job_id AND ${jobs.state} = 'active'
                AND ${jobs.lockToken} = input.token AND ${jobs.cancelRequested} = FALSE
              RETURNING ${jobs.id} AS id
            )
            SELECT input.job_id AS id,
              CASE WHEN ${jobs.id} IS NOT NULL AND ${jobs.state} = 'active'
                AND ${jobs.lockToken} = input.token AND ${jobs.cancelRequested} THEN 'cancel'
                ELSE 'lost' END AS status
            FROM input
            LEFT JOIN updated ON updated.id = input.job_id
            LEFT JOIN ${jobs} ON ${jobs.id} = input.job_id
            WHERE updated.id IS NULL
          `).pipe(Effect.mapError(storeError("extendLocks failed"))))
          const result: JobStore.ExtendLocksResult = {
            lost: rows.filter((row) => row.status === "lost").map((row) => JobId(row.id)),
            cancelRequested: rows.filter((row) => row.status === "cancel").map((row) => JobId(row.id))
          }
          return result
        }),

      recoverStalled: (recoverOptions) =>
        db.transaction((tx) =>
          Effect.gen(function*() {
            const now = yield* nowDate
            // A stalled job whose worker died before honouring a cancel
            // request is finished as cancelled rather than revived.
            const rows = rowsOf(yield* tx.execute<
              {
                id: string
                state: string
                processedAt: Date | null
                name: string
                keep: JobStore.KeepPolicy | null
                dedupeKey: string | null
              }
            >(sql`
              UPDATE ${jobs} SET
                stalled_count = CASE WHEN ${jobs.cancelRequested} THEN ${jobs.stalledCount}
                  ELSE ${jobs.stalledCount} + 1 END,
                lock_token = NULL, lock_expires_at = NULL,
                state = CASE
                  WHEN ${jobs.cancelRequested} THEN 'cancelled'
                  WHEN ${jobs.stalledCount} + 1 > ${recoverOptions.maxStalledCount}::int THEN 'failed'
                  ELSE 'waiting' END,
                finished_at = CASE
                  WHEN ${jobs.cancelRequested} OR ${jobs.stalledCount} + 1 > ${recoverOptions.maxStalledCount}::int
                  THEN ${now}::timestamptz ELSE NULL END,
                failed_reason = CASE
                  WHEN ${jobs.cancelRequested} THEN NULL
                  WHEN ${jobs.stalledCount} + 1 > ${recoverOptions.maxStalledCount}::int
                  THEN 'job stalled more than allowable limit' ELSE NULL END,
                cancel_requested = FALSE
              WHERE ${jobs.state} = 'active' AND ${jobs.lockExpiresAt} <= ${now}::timestamptz
              RETURNING ${jobs.id} AS "id", ${jobs.state} AS "state", ${jobs.processedAt} AS "processedAt",
                ${jobs.name} AS "name", ${jobs.keep} AS "keep", ${jobs.dedupeKey} AS "dedupeKey"
            `))
            const recovered: Array<{ id: JobStore.JobId; failed: boolean }> = []
            for (const row of rows) {
              yield* insertAttempt(
                tx,
                row.id,
                row.state === "cancelled" ? "cancelled" : "stalled",
                row.processedAt,
                now,
                undefined
              )
              if (row.state === "cancelled" || row.state === "failed") {
                yield* releaseDedupe(tx, row.name, row.dedupeKey, row.id, now)
              }
              if (row.state === "cancelled") {
                yield* applyKeep(tx, { name: row.name, state: "cancelled", keep: row.keep }, now)
              } else {
                recovered.push({ id: JobId(row.id), failed: row.state === "failed" })
              }
            }
            return recovered
          })
        ).pipe(
          Effect.mapError((error) =>
            error instanceof JobStore.JobStoreError ? error : storeError("recoverStalled failed")(error)
          ),
          Effect.tap((recovered) =>
            recovered.some((entry) => !entry.failed) ? wakeUp : Effect.void
          )
        ),

      awaitWake: (_queues, wakeToken) =>
        Effect.suspend(() => {
          if (wakeVersion > wakeToken) return Effect.void
          return Deferred.await(wake)
        }),

      getJob: (id) =>
        db.select().from(jobs).where(eq(jobs.id, id)).pipe(
          Effect.mapError(storeError("getJob failed")),
          Effect.map((rows) => {
            const row = rows[0]
            return row === undefined ? Option.none() : Option.some(toRecord(row))
          })
        ),

      getAttempts: (id) =>
        db.select().from(attempts).where(eq(attempts.jobId, id)).orderBy(asc(attempts.attempt)).pipe(
          Effect.mapError(storeError("getAttempts failed")),
          Effect.map((rows) =>
            rows.map((row): JobStore.AttemptRecord => ({
              attempt: row.attempt,
              startedAt: row.startedAt?.getTime(),
              finishedAt: row.finishedAt.getTime(),
              outcome: row.outcome,
              exit: row.exit ?? undefined
            }))
          )
        ),

      list: (listOptions) =>
        Effect.gen(function*() {
          const limit = Math.max(1, listOptions.limit ?? 50)
          const conditions = [sql`TRUE`]
          if (listOptions.queue !== undefined) conditions.push(sql`${jobs.queue} = ${listOptions.queue}`)
          if (listOptions.name !== undefined) conditions.push(sql`${jobs.name} = ${listOptions.name}`)
          if (listOptions.states !== undefined) {
            // NB: an empty array matches nothing (ANY('{}') is false), same
            // as the memory driver.
            conditions.push(sql`${jobs.state} = ANY(${sql.param([...listOptions.states])})`)
          }
          if (listOptions.metadata !== undefined && Object.keys(listOptions.metadata).length > 0) {
            conditions.push(sql`${jobs.metadata} @> ${JSON.stringify(listOptions.metadata)}::jsonb`)
          }
          if (listOptions.cursor !== undefined) {
            const split = listOptions.cursor.indexOf(":")
            const cursorAt = new Date(Number(listOptions.cursor.slice(0, split)))
            const cursorId = listOptions.cursor.slice(split + 1)
            conditions.push(
              sql`(${jobs.enqueuedAt}, ${jobs.id}) < (${cursorAt}, ${cursorId})`
            )
          }
          const rows = rowsOf(yield* db.execute<JobRow>(sql`
            SELECT ${jobs.id} AS "id", ${jobs.name} AS "name", ${jobs.queue} AS "queue",
              ${jobs.state} AS "state", ${jobs.priority} AS "priority", ${jobs.seq} AS "seq",
              ${jobs.payload} AS "payload", ${jobs.metadata} AS "metadata",
              ${jobs.attemptsMax} AS "attemptsMax", ${jobs.attemptsMade} AS "attemptsMade",
              ${jobs.stalledCount} AS "stalledCount", ${jobs.backoff} AS "backoff",
              ${jobs.keep} AS "keep", ${jobs.timeoutMs} AS "timeoutMs",
              ${jobs.cancelRequested} AS "cancelRequested",
              ${jobs.runAt} AS "runAt", ${jobs.enqueuedAt} AS "enqueuedAt",
              ${jobs.processedAt} AS "processedAt", ${jobs.finishedAt} AS "finishedAt",
              ${jobs.exit} AS "exit", ${jobs.failedReason} AS "failedReason"
            FROM ${jobs}
            WHERE ${sql.join(conditions, sql` AND `)}
            ORDER BY ${jobs.enqueuedAt} DESC, ${jobs.id} DESC
            LIMIT ${limit + 1}
          `).pipe(Effect.mapError(storeError("list failed"))))
          const items = rows.slice(0, limit).map(toRecord)
          const last = items[items.length - 1]
          return {
            items,
            cursor: rows.length > limit && last !== undefined
              ? `${last.enqueuedAt}:${last.id}`
              : undefined
          }
        }),

      retry: (id) =>
        Effect.gen(function*() {
          const now = yield* nowDate
          const rows = rowsOf(yield* db.execute<{ id: string }>(sql`
            UPDATE ${jobs} SET state = 'waiting', attempts_made = 0, stalled_count = 0,
              cancel_requested = FALSE,
              exit = NULL, failed_reason = NULL, finished_at = NULL, processed_at = NULL,
              run_at = ${now}, seq = ${seqExpr}
            WHERE ${jobs.id} = ${id} AND ${jobs.state} = 'failed'
            RETURNING ${jobs.id} AS id
          `).pipe(Effect.mapError(storeError("retry failed"))))
          if (rows.length === 0) {
            const existing = yield* db.select({ state: jobs.state }).from(jobs)
              .where(eq(jobs.id, id)).pipe(Effect.mapError(storeError("retry failed")))
            const found = existing[0]
            if (found === undefined) {
              return yield* new JobStore.JobNotFoundError({ jobId: id })
            }
            return yield* new JobStore.JobNotRetryableError({ jobId: id, state: found.state })
          }
          yield* wakeUp
        }),

      cancel: (id) =>
        db.transaction((tx) =>
          Effect.gen(function*() {
            const now = yield* nowDate
            // One guarded statement: waiting/delayed become terminal, active
            // gets the cancel-request flag; anything else is reported by state.
            const rows = rowsOf(yield* tx.execute<
              {
                id: string
                state: string
                processedAt: Date | null
                name: string
                keep: JobStore.KeepPolicy | null
                dedupeKey: string | null
              }
            >(sql`
              UPDATE ${jobs} SET
                state = CASE WHEN ${jobs.state} IN ('waiting', 'delayed') THEN 'cancelled' ELSE ${jobs.state} END,
                finished_at = CASE WHEN ${jobs.state} IN ('waiting', 'delayed') THEN ${now}::timestamptz ELSE ${jobs.finishedAt} END,
                cancel_requested = CASE WHEN ${jobs.state} = 'active' THEN TRUE ELSE ${jobs.cancelRequested} END
              WHERE ${jobs.id} = ${id} AND ${jobs.state} IN ('waiting', 'delayed', 'active')
              RETURNING ${jobs.id} AS id, ${jobs.state} AS state,
                ${jobs.processedAt} AS "processedAt", ${jobs.name} AS "name", ${jobs.keep} AS "keep",
                ${jobs.dedupeKey} AS "dedupeKey"
            `))
            const row = rows[0]
            if (row === undefined) {
              const existing = rowsOf(yield* tx.execute<{ state: JobStore.JobState }>(sql`
                SELECT ${jobs.state} AS state FROM ${jobs} WHERE ${jobs.id} = ${id}
              `))
              const found = existing[0]
              if (found === undefined) {
                return yield* new JobStore.JobNotFoundError({ jobId: id })
              }
              return yield* new JobStore.JobNotCancellableError({ jobId: id, state: found.state })
            }
            if (row.state === "cancelled") {
              yield* insertAttempt(tx, id, "cancelled", row.processedAt, now, undefined)
              yield* releaseDedupe(tx, row.name, row.dedupeKey, id, now)
              yield* applyKeep(tx, { name: row.name, state: "cancelled", keep: row.keep }, now)
            }
          })
        ).pipe(
          Effect.mapError((error) =>
            error instanceof JobStore.JobNotFoundError ||
              error instanceof JobStore.JobNotCancellableError ||
              error instanceof JobStore.JobStoreError
              ? error
              : storeError("cancel failed")(error)
          ),
          Effect.asVoid
        ),

      promote: (id) =>
        Effect.gen(function*() {
          const now = yield* nowDate
          const rows = rowsOf(yield* db.execute<{ id: string }>(sql`
            UPDATE ${jobs} SET state = 'waiting', run_at = ${now}
            WHERE ${jobs.id} = ${id} AND ${jobs.state} = 'delayed'
            RETURNING ${jobs.id} AS id
          `).pipe(Effect.mapError(storeError("promote failed"))))
          if (rows.length === 0) {
            const existing = rowsOf(yield* db.execute<{ state: JobStore.JobState }>(sql`
              SELECT ${jobs.state} AS state FROM ${jobs} WHERE ${jobs.id} = ${id}
            `).pipe(Effect.mapError(storeError("promote failed"))))
            const found = existing[0]
            if (found === undefined) {
              return yield* new JobStore.JobNotFoundError({ jobId: id })
            }
            return yield* new JobStore.JobNotPromotableError({ jobId: id, state: found.state })
          }
          yield* wakeUp
        }),

      pause: (queue) =>
        db.execute(sql`
          INSERT INTO ${queues} (queue, paused) VALUES (${queue}, TRUE)
          ON CONFLICT (queue) DO UPDATE SET paused = TRUE
        `).pipe(
          Effect.mapError(storeError("pause failed")),
          Effect.asVoid
        ),

      resume: (queue) =>
        db.execute(sql`
          UPDATE ${queues} SET paused = FALSE WHERE ${queues.queue} = ${queue}
        `).pipe(
          Effect.mapError(storeError("resume failed")),
          Effect.andThen(wakeUp)
        ),

      pausedQueues: () =>
        db.execute<{ queue: string }>(sql`
          SELECT ${queues.queue} AS queue FROM ${queues} WHERE ${queues.paused} = TRUE
        `).pipe(
          Effect.mapError(storeError("pausedQueues failed")),
          Effect.map((result) => rowsOf(result).map((row) => JobStore.QueueName(row.queue)))
        ),

      upsertSchedule: (schedule) =>
        db.execute(sql`
          INSERT INTO ${schedules} (key, job_name, queue, cron, tz, every_ms, payload, metadata,
            priority, attempts_max, backoff, keep, timeout_ms, next_run_at)
          VALUES (${schedule.key}, ${schedule.jobName}, ${schedule.queue},
            ${schedule.cron ?? null}, ${schedule.tz ?? null}, ${schedule.everyMs ?? null},
            ${JSON.stringify(schedule.payload ?? null)}::jsonb, ${JSON.stringify(schedule.metadata)}::jsonb,
            ${schedule.priority}, ${schedule.attemptsMax},
            ${schedule.backoff === undefined ? null : JSON.stringify(schedule.backoff)}::jsonb,
            ${schedule.keep === undefined ? null : JSON.stringify(schedule.keep)}::jsonb,
            ${schedule.timeoutMs ?? null}, ${new Date(schedule.nextRunAt)})
          ON CONFLICT (key) DO UPDATE SET
            job_name = EXCLUDED.job_name, queue = EXCLUDED.queue, cron = EXCLUDED.cron,
            tz = EXCLUDED.tz, every_ms = EXCLUDED.every_ms, payload = EXCLUDED.payload,
            metadata = EXCLUDED.metadata, priority = EXCLUDED.priority,
            attempts_max = EXCLUDED.attempts_max, backoff = EXCLUDED.backoff,
            keep = EXCLUDED.keep, timeout_ms = EXCLUDED.timeout_ms,
            next_run_at = CASE
              WHEN ${schedules.cron} IS NOT DISTINCT FROM EXCLUDED.cron
                AND ${schedules.tz} IS NOT DISTINCT FROM EXCLUDED.tz
                AND ${schedules.everyMs} IS NOT DISTINCT FROM EXCLUDED.every_ms
              THEN ${schedules.nextRunAt}
              ELSE EXCLUDED.next_run_at END
        `).pipe(
          Effect.mapError(storeError("upsertSchedule failed")),
          Effect.asVoid
        ),

      removeSchedule: (key) =>
        db.execute<{ key: string }>(sql`
          DELETE FROM ${schedules} WHERE ${schedules.key} = ${key}
          RETURNING ${schedules.key} AS key
        `).pipe(
          Effect.mapError(storeError("removeSchedule failed")),
          Effect.map((result) => rowsOf(result).length > 0)
        ),

      listSchedules: (listOptions) =>
        Effect.gen(function*() {
          const conditions = [sql`TRUE`]
          if (listOptions?.jobName !== undefined) {
            conditions.push(sql`${schedules.jobName} = ${listOptions.jobName}`)
          }
          if (listOptions?.queue !== undefined) {
            conditions.push(sql`${schedules.queue} = ${listOptions.queue}`)
          }
          const rows = rowsOf(yield* db.execute<ScheduleRow>(sql`
            SELECT ${schedules.key} AS "key", ${schedules.jobName} AS "jobName",
              ${schedules.queue} AS "queue", ${schedules.cron} AS "cron", ${schedules.tz} AS "tz",
              ${schedules.everyMs} AS "everyMs", ${schedules.payload} AS "payload",
              ${schedules.metadata} AS "metadata", ${schedules.priority} AS "priority",
              ${schedules.attemptsMax} AS "attemptsMax", ${schedules.backoff} AS "backoff",
              ${schedules.keep} AS "keep", ${schedules.timeoutMs} AS "timeoutMs",
              ${schedules.nextRunAt} AS "nextRunAt"
            FROM ${schedules}
            WHERE ${sql.join(conditions, sql` AND `)}
            ORDER BY ${schedules.key}
          `).pipe(Effect.mapError(storeError("listSchedules failed"))))
          return rows.map(toSchedule)
        }),

      dueSchedules: () =>
        Effect.gen(function*() {
          const now = yield* nowDate
          const rows = rowsOf(yield* db.execute<ScheduleRow>(sql`
            SELECT ${schedules.key} AS "key", ${schedules.jobName} AS "jobName",
              ${schedules.queue} AS "queue", ${schedules.cron} AS "cron", ${schedules.tz} AS "tz",
              ${schedules.everyMs} AS "everyMs", ${schedules.payload} AS "payload",
              ${schedules.metadata} AS "metadata", ${schedules.priority} AS "priority",
              ${schedules.attemptsMax} AS "attemptsMax", ${schedules.backoff} AS "backoff",
              ${schedules.keep} AS "keep", ${schedules.timeoutMs} AS "timeoutMs",
              ${schedules.nextRunAt} AS "nextRunAt"
            FROM ${schedules}
            WHERE ${schedules.nextRunAt} <= ${now}
            ORDER BY ${schedules.nextRunAt} ASC
          `).pipe(Effect.mapError(storeError("dueSchedules failed"))))
          return rows.map(toSchedule)
        }),

      advanceSchedule: (key, expectedRunAt, nextRunAt) =>
        db.execute(sql`
          UPDATE ${schedules} SET next_run_at = ${new Date(nextRunAt)}
          WHERE ${schedules.key} = ${key} AND ${schedules.nextRunAt} = ${new Date(expectedRunAt)}
        `).pipe(
          Effect.mapError(storeError("advanceSchedule failed")),
          Effect.asVoid
        ),

      counts: (queue) =>
        db.execute<{ state: JobStore.JobState; count: number }>(sql`
          SELECT ${jobs.state} AS "state", count(*)::int AS "count" FROM ${jobs}
          ${queue === undefined ? sql`` : sql`WHERE ${jobs.queue} = ${queue}`}
          GROUP BY ${jobs.state}
        `).pipe(
          Effect.mapError(storeError("counts failed")),
          Effect.map((result) => {
            const rows = rowsOf(result)
            const counts = {
              waiting: 0,
              delayed: 0,
              active: 0,
              completed: 0,
              failed: 0,
              cancelled: 0
            } satisfies Record<JobStore.JobState, number>
            for (const row of rows) counts[row.state] = row.count
            return counts
          })
        ),

      remove: (id) =>
        db.execute(sql`
          DELETE FROM ${jobs}
          WHERE ${jobs.id} = ${id} AND ${jobs.state} <> 'active'
          RETURNING ${jobs.id} AS id
        `).pipe(
          Effect.mapError(storeError("remove failed")),
          Effect.map((result) => rowsOf(result).length > 0)
        )
    }

    return store
  })

/**
 * A Postgres-backed `JobStore` layer over your drizzle tables. Requires
 * `PgClient` (from `@effect/sql-pg`).
 *
 * ```ts
 * const StoreLive = DrizzleJobStore.layer({ jobs, attempts, store: Durable }).pipe(
 *   Layer.provide(PgClient.layer({ url: Redacted.make(DATABASE_URL) }))
 * )
 * ```
 *
 * @since 0.1.0
 */
export const layer = <StoreId = JobStore.JobStore>(
  options: DrizzleJobStoreOptions<StoreId>
): Layer.Layer<StoreId, JobStore.JobStoreError, PgClient.PgClient> =>
  Layer.effect(
    // SAFETY: when `options.store` is omitted the public signature fixes
    // `StoreId` to its default `JobStore.JobStore`, so the default key is the
    // right `Context.Key<StoreId>`; when it is present the cast is an identity.
    (options.store ?? JobStore.JobStore) as Context.Key<StoreId, JobStore.Service>,
    make(options)
  )
