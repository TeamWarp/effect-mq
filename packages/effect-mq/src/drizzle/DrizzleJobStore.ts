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
import { asc, eq, sql } from "drizzle-orm"
import * as PgDrizzle from "drizzle-orm/effect-postgres"
import { getTableConfig } from "drizzle-orm/pg-core"
import { Clock, type Context, Deferred, Effect, Layer, Option, type Scope, Stream } from "effect"
import type { MqJobAttemptsTable, MqJobsTable } from "./schema.ts"

const { JobId } = JobStore

/**
 * @since 0.1.0
 */
export interface DrizzleJobStoreOptions<StoreId = JobStore.JobStore> {
  /** The jobs table instance (from `mqJobs`). */
  readonly jobs: MqJobsTable
  /** The run-ledger table instance (from `mqJobAttempts`). */
  readonly attempts: MqJobAttemptsTable
  /** Bind to a `JobStore.named(...)` key; default: the default `JobStore`. */
  readonly store?: Context.Key<StoreId, JobStore.Service> | undefined
  /**
   * Probe the tables at startup and fail fast when the schema is missing
   * (default true). Migrations are owned by your drizzle-kit pipeline.
   */
  readonly validate?: boolean | undefined
}

type Db = PgDrizzle.EffectPgDatabase & { readonly $client: PgClient.PgClient }

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
  readonly runAt: Date
  readonly enqueuedAt: Date
  readonly processedAt: Date | null
  readonly finishedAt: Date | null
  readonly exit: unknown
  readonly failedReason: string | null
}

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
    const jobsName = getTableConfig(jobs).name
    const attemptsName = getTableConfig(attempts).name
    const wakeChannel = `effect_mq_wake_${jobsName}`

    if (options.validate ?? true) {
      yield* Effect.all([
        db.select({ id: jobs.id }).from(jobs).limit(0),
        db.select({ jobId: attempts.jobId }).from(attempts).limit(0)
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

    const store: JobStore.Service = {
      enqueue: (request) =>
        Effect.gen(function*() {
          const now = yield* nowDate
          const runAt = new Date(now.getTime() + Math.max(0, request.delayMs))
          const state = request.delayMs > 0 ? "delayed" : "waiting"
          // Store-assigned ids come from the seq sequence; loop on the
          // (unlikely) collision with a user-supplied id.
          for (let i = 0; i < 5; i++) {
            const idExpr = request.id !== undefined
              ? sql`${request.id}`
              : sql`'j-' || ${seqExpr}::text`
            const rows = rowsOf(yield* db.execute<{ id: string }>(sql`
              INSERT INTO ${jobs} (id, name, queue, state, priority, payload, metadata,
                attempts_max, backoff, keep, run_at, enqueued_at)
              VALUES (${idExpr}, ${request.name}, ${request.queue}, ${state}, ${request.priority},
                ${JSON.stringify(request.payload ?? null)}::jsonb, ${JSON.stringify(request.metadata)}::jsonb,
                ${request.attemptsMax},
                ${request.backoff === undefined ? null : JSON.stringify(request.backoff)}::jsonb,
                ${request.keep === undefined ? null : JSON.stringify(request.keep)}::jsonb,
                ${runAt}, ${now})
              ON CONFLICT (id) DO NOTHING
              RETURNING ${jobs.id} AS id
            `).pipe(Effect.mapError(storeError("enqueue failed"))))
            const inserted = rows[0]
            if (inserted !== undefined) {
              yield* wakeUp
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
            const claimed = rowsOf(yield* tx.execute<JobRow>(sql`
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
                ${jobs.keep} AS "keep", ${jobs.runAt} AS "runAt", ${jobs.enqueuedAt} AS "enqueuedAt",
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
              ? sql`state = 'completed', exit = ${JSON.stringify(outcome.exit ?? null)}::jsonb, finished_at = ${now}`
              : outcome._tag === "Fail"
              ? sql`state = 'failed', exit = ${JSON.stringify(outcome.exit ?? null)}::jsonb, finished_at = ${now}`
              : sql`state = ${outcome.delayMs > 0 ? "delayed" : "waiting"},
                  run_at = ${new Date(now.getTime() + Math.max(0, outcome.delayMs))},
                  seq = ${seqExpr}`
            const rows = rowsOf(yield* tx.execute<
              { processedAt: Date | null; name: string; state: string; keep: JobStore.KeepPolicy | null }
            >(sql`
              UPDATE ${jobs} SET ${update},
                attempts_made = ${jobs.attemptsMade} + 1, lock_token = NULL, lock_expires_at = NULL
              WHERE ${jobs.id} = ${id} AND ${jobs.state} = 'active' AND ${jobs.lockToken} = ${token}
              RETURNING ${jobs.processedAt} AS "processedAt", ${jobs.name} AS "name",
                ${jobs.state} AS "state", ${jobs.keep} AS "keep"
            `))
            const row = rows[0]
            if (row === undefined) {
              return yield* explainMiss(id)
            }
            const ledgerOutcome = outcome._tag === "Complete"
              ? "completed" as const
              : outcome._tag === "Fail"
              ? "failed" as const
              : "retried" as const
            yield* insertAttempt(tx, id, ledgerOutcome, row.processedAt, now, outcome.exit)
            if (outcome._tag !== "Retry") {
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
          const rows = rowsOf(yield* db.execute<{ id: string }>(sql`
            UPDATE ${jobs} SET state = 'waiting', lock_token = NULL, lock_expires_at = NULL
            WHERE ${jobs.id} = ${id} AND ${jobs.state} = 'active' AND ${jobs.lockToken} = ${token}
            RETURNING ${jobs.id} AS id
          `).pipe(Effect.mapError(storeError("release failed"))))
          if (rows.length === 0) {
            return yield* explainMiss(id)
          }
          yield* wakeUp
        }),

      extendLocks: (locks, durationMs) =>
        Effect.gen(function*() {
          if (locks.length === 0) return []
          const now = yield* nowDate
          const rows = rowsOf(yield* db.execute<{ id: string }>(sql`
            WITH input AS (
              SELECT ids.job_id, toks.token
              FROM unnest(${sql.param(locks.map((lock) => lock.id))}::text[]) WITH ORDINALITY AS ids(job_id, ord)
              JOIN unnest(${sql.param(locks.map((lock) => lock.token))}::text[]) WITH ORDINALITY AS toks(token, ord) USING (ord)
            ),
            updated AS (
              UPDATE ${jobs} SET lock_expires_at = ${new Date(now.getTime() + durationMs)}
              FROM input
              WHERE ${jobs.id} = input.job_id AND ${jobs.state} = 'active' AND ${jobs.lockToken} = input.token
              RETURNING ${jobs.id} AS id
            )
            SELECT input.job_id AS id FROM input
            LEFT JOIN updated ON updated.id = input.job_id
            WHERE updated.id IS NULL
          `).pipe(Effect.mapError(storeError("extendLocks failed"))))
          return rows.map((row) => JobId(row.id))
        }),

      recoverStalled: (recoverOptions) =>
        db.transaction((tx) =>
          Effect.gen(function*() {
            const now = yield* nowDate
            const rows = rowsOf(yield* tx.execute<
              { id: string; state: string; processedAt: Date | null }
            >(sql`
              UPDATE ${jobs} SET
                stalled_count = ${jobs.stalledCount} + 1,
                lock_token = NULL, lock_expires_at = NULL,
                state = CASE WHEN ${jobs.stalledCount} + 1 > ${recoverOptions.maxStalledCount}::int
                  THEN 'failed' ELSE 'waiting' END,
                finished_at = CASE WHEN ${jobs.stalledCount} + 1 > ${recoverOptions.maxStalledCount}::int
                  THEN ${now}::timestamptz ELSE NULL END,
                failed_reason = CASE WHEN ${jobs.stalledCount} + 1 > ${recoverOptions.maxStalledCount}::int
                  THEN 'job stalled more than allowable limit' ELSE NULL END
              WHERE ${jobs.state} = 'active' AND ${jobs.lockExpiresAt} <= ${now}::timestamptz
              RETURNING ${jobs.id} AS "id", ${jobs.state} AS "state", ${jobs.processedAt} AS "processedAt"
            `))
            for (const row of rows) {
              yield* insertAttempt(tx, row.id, "stalled", row.processedAt, now, undefined)
            }
            return rows.map((row) => ({ id: JobId(row.id), failed: row.state === "failed" }))
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
              ${jobs.keep} AS "keep", ${jobs.runAt} AS "runAt", ${jobs.enqueuedAt} AS "enqueuedAt",
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
              failed: 0
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
