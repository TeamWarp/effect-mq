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
import { resubscribeForever } from "./resubscribe.ts"
import type { PgClient } from "@effect/sql-pg"
import { asc, eq, getTableColumns, type SQL, sql } from "drizzle-orm"
import * as PgDrizzle from "drizzle-orm/effect-postgres"
import { getTableConfig } from "drizzle-orm/pg-core"
import { Cause, Clock, type Context, Deferred, Duration, Effect, Layer, Option, Predicate, type Scope, Stream } from "effect"
import type {
  MqDedupeTable,
  MqFlowChildrenTable,
  MqFlowOutboxTable,
  MqJobAttemptsTable,
  MqJobsTable,
  MqQueueControlTable,
  MqSchedulesTable
} from "./schema.ts"

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
  /** The flow dependency-rows table (from `mqFlowChildren`). */
  readonly flowChildren: MqFlowChildrenTable
  /** The child-report outbox table (from `mqFlowOutbox`). */
  readonly flowOutbox: MqFlowOutboxTable
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
   * removed by a periodic sweep — one duration for all terminal states or a
   * per-state split (`{ completed: "1 day", failed: "30 days" }`). The sweep
   * also honours stricter per-job `keep.age` rules.
   */
  readonly historyTtl?: JobStore.HistoryTtlInput | undefined
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

// The exact spellings a bigserial ever produces: no leading zeros, no signs,
// no whitespace — what outbox ids look like and nothing Postgres would
// silently normalize into one.
const CANONICAL_BIGSERIAL = /^[1-9]\d*$/

/**
 * Whether an error surfaced by drizzle's Effect driver is a Postgres
 * deadlock (40P01), i.e. safe to retry.
 *
 * The deadlock travels wrapped: drizzle's session fails with an
 * `EffectDrizzleQueryError` whose `cause` field is `Cause.fail(SqlError)`
 * (drizzle-orm `pg-core/effect/session.ts`), and `@effect/sql-pg` classifies
 * pg code 40P01 into the `SqlError`'s `reason: DeadlockError` (PgClient.ts
 * `classifyError`). Neither layer's `toString` renders the pg code, so this
 * unwraps structurally instead of string-matching the rendered error.
 *
 * @internal
 */
// oxlint-disable-next-line anti-slop/no-unknown-parameters -- a retry-predicate classifier over whatever the driver threw IS the boundary parser
export const isDeadlockError = (error: unknown): boolean => {
  // Unwrap drizzle's envelope: its `cause` is an Effect Cause holding the
  // original failure. A bare SqlError (non-drizzle path) passes through.
  const unwrapped = Predicate.hasProperty(error, "cause") && Cause.isCause(error.cause)
    ? Option.getOrUndefined(Cause.findErrorOption(error.cause))
    : error
  if (
    !Predicate.hasProperty(unwrapped, "_tag") || unwrapped._tag !== "SqlError" ||
    !Predicate.hasProperty(unwrapped, "reason")
  ) {
    return false
  }
  const reason = unwrapped.reason
  if (Predicate.hasProperty(reason, "_tag") && reason._tag === "DeadlockError") {
    return true
  }
  // Fallback for classifiers that missed the code: the reason keeps the raw
  // pg error, whose message for 40P01 is "deadlock detected".
  if (Predicate.hasProperty(reason, "cause")) {
    const raw = String(reason.cause)
    return raw.includes("40P01") || raw.includes("deadlock detected")
  }
  return false
}

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
  readonly trace: JobStore.TraceContext | null
  readonly parent: JobStore.ParentEnvelope | null
  readonly flowFailFast: boolean | null
  readonly flowPending: number | null
  readonly flowCompleted: number | null
  readonly flowFailed: number | null
  readonly flowCancelled: number | null
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
  readonly group: string | null
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
  group: row.group ?? undefined,
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
  trace: row.trace ?? undefined,
  parent: row.parent ?? undefined,
  // The flow columns are NULL together; `flowPending` is the presence marker.
  flow: row.flowPending === null || row.flowPending === undefined
    ? undefined
    : {
      failFast: row.flowFailFast === true,
      pending: Number(row.flowPending),
      completed: Number(row.flowCompleted ?? 0),
      failed: Number(row.flowFailed ?? 0),
      cancelled: Number(row.flowCancelled ?? 0)
    },
  runAt: row.runAt.getTime(),
  enqueuedAt: row.enqueuedAt.getTime(),
  processedAt: row.processedAt?.getTime(),
  finishedAt: row.finishedAt?.getTime(),
  exit: row.exit ?? undefined,
  failedReason: row.failedReason ?? undefined
})

type FlowChildRow = {
  readonly flowId: string
  readonly childKey: string
  readonly name: string
  readonly storeKey: string
  /** Projected from `spec->>'id'` (never NULL: the FanOut ack validated it). */
  readonly childJobId: string
  readonly status: JobStore.FlowChildRecord["status"]
  readonly exit: unknown
  readonly failedReason: string | null
  readonly cascaded: boolean
}

const toFlowChildRecord = (row: FlowChildRow): JobStore.FlowChildRecord => ({
  flowId: JobId(row.flowId),
  childKey: row.childKey,
  name: row.name,
  storeKey: row.storeKey,
  childJobId: JobId(row.childJobId),
  status: row.status,
  exit: row.exit ?? undefined,
  failedReason: row.failedReason ?? undefined,
  cascaded: row.cascaded
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
    const flowChildren = options.flowChildren
    const flowOutbox = options.flowOutbox
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
      "trace",
      "parent",
      "flowFailFast",
      "flowPending",
      "flowCompleted",
      "flowFailed",
      "flowCancelled",
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
        db.select({ key: dedupe.key }).from(dedupe).limit(0),
        db.select({ flowId: flowChildren.flowId }).from(flowChildren).limit(0),
        db.select({ id: flowOutbox.id }).from(flowOutbox).limit(0)
      ]).pipe(
        Effect.mapError(storeError(
          `effect-mq: tables "${jobsName}"/"${attemptsName}" are missing or mismatched — ` +
            `re-export the effect-mq/drizzle schema factories from your drizzle schema and run your migrations (drizzle-kit generate)`
        ))
      )
    }

    // Wake plumbing: a queue-filtered waiter registry (same protocol as the
    // memory driver), fed by (a) local store operations and (b) cross-process
    // LISTEN notifications whose payload names the queue ("*" broadcasts).
    // Filtering matters at scale: without it every enqueue wakes every idle
    // taker of every queue on the store.
    let wakeVersion = 0
    let lastBroadcast = 0
    const lastWake = new Map<JobStore.QueueName, number>()
    interface Waiter {
      readonly queues: ReadonlySet<JobStore.QueueName>
      readonly deferred: Deferred.Deferred<void>
    }
    const waiters = new Set<Waiter>()
    const lastWakeFor = (queue: JobStore.QueueName) => Math.max(lastWake.get(queue) ?? 0, lastBroadcast)
    const signalWake = (queue?: JobStore.QueueName) => {
      wakeVersion += 1
      if (queue === undefined) {
        lastBroadcast = wakeVersion
      } else {
        lastWake.set(queue, wakeVersion)
      }
      // Snapshot-and-clear BEFORE resolving: doneUnsafe resumes waiting
      // fibers synchronously, and a woken taker that re-parks registers a
      // NEW waiter — resolving inside the live Set iteration would visit it
      // and livelock.
      const toWake: Array<Waiter> = []
      for (const waiter of waiters) {
        if (queue === undefined || waiter.queues.has(queue)) {
          waiters.delete(waiter)
          toWake.push(waiter)
        }
      }
      for (const waiter of toWake) {
        Deferred.doneUnsafe(waiter.deferred, Effect.void)
      }
    }
    // Wake-ups degrade to the worker's pollInterval whenever this is not
    // subscribed; see `resubscribeForever` for the retry and log policy.
    yield* resubscribeForever(
      client.listen(wakeChannel).pipe(
        Stream.runForEach((payload) =>
          Effect.sync(() => signalWake(payload === "*" ? undefined : JobStore.QueueName(payload)))
        )
      )
    ).pipe(Effect.forkScoped)
    // Automatic retention (the store-level sweep and per-job `keep`) never
    // prunes a flow parent that still owes cascade cancels: its dependency
    // rows marked `cancelled` and not `cascaded` are the only record that
    // real cancels are still due in the child stores. The explicit `remove`
    // verb is not exempted.
    const owesCascades = sql`EXISTS (
      SELECT 1 FROM ${flowChildren}
      WHERE ${flowChildren.flowId} = ${jobs.id}
        AND ${flowChildren.status} = 'cancelled' AND ${flowChildren.cascaded} = FALSE
    )`
    // A pruned flow parent's dependency rows go with it in the same statement.
    const purgeJobsWhere = (predicate: SQL) =>
      sql`
        WITH deleted AS (
          DELETE FROM ${jobs}
          WHERE ${predicate}
          RETURNING ${jobs.id} AS id
        )
        DELETE FROM ${flowChildren}
        WHERE ${flowChildren.flowId} IN (SELECT id FROM deleted)
      `

    if (options.historyTtl !== undefined) {
      const ttlByState = JobStore.normalizeHistoryTtl(options.historyTtl)
      const sweepMs = Duration.toMillis(options.historySweepInterval ?? "1 minute")
      yield* Effect.gen(function*() {
        yield* Effect.sleep(sweepMs)
        const now = yield* nowDate
        // Per-state ceilings, refined by stricter per-row keep ages — a quiet
        // job name is pruned on the timer, not only when its group is acked.
        for (const state of ["completed", "failed", "cancelled"] as const) {
          const ttl = ttlByState[state]
          yield* db.execute(purgeJobsWhere(sql`
            ${jobs.state} = ${state} AND NOT ${owesCascades} AND (
              ${ttl !== undefined ? sql`${jobs.finishedAt} <= ${new Date(now.getTime() - ttl)}` : sql`FALSE`}
              OR (
                COALESCE(
                  ${jobs.keep}->${state}->>'ageMs',
                  CASE WHEN ${jobs.keep} ?| array['completed', 'failed', 'cancelled'] THEN NULL
                    ELSE ${jobs.keep}->>'ageMs' END
                ) IS NOT NULL
                AND ${jobs.finishedAt} <= ${now}::timestamptz
                  - make_interval(secs => (COALESCE(
                      ${jobs.keep}->${state}->>'ageMs',
                      CASE WHEN ${jobs.keep} ?| array['completed', 'failed', 'cancelled'] THEN NULL
                        ELSE ${jobs.keep}->>'ageMs' END
                    )::double precision) / 1000.0))
            )
          `))
        }
        // Dead dedup rows: expired windows, or pointers at vanished jobs.
        yield* db.execute(sql`
          DELETE FROM ${dedupe}
          WHERE (${dedupe.windowExpiresAt} IS NOT NULL AND ${dedupe.windowExpiresAt} <= ${now})
             OR (${dedupe.windowExpiresAt} IS NULL AND NOT EXISTS (
               SELECT 1 FROM ${jobs} WHERE ${jobs.id} = ${dedupe.jobId}
                 AND ${jobs.state} IN ('waiting', 'delayed', 'active', 'waiting-children')
             ))
        `)
      }).pipe(
        Effect.catchCause((cause) => Effect.logWarning("effect-mq: history sweep failed", cause)),
        Effect.forever,
        Effect.forkScoped
      )
    }

    // Local mutation wake-up: bump synchronously, then best-effort NOTIFY so
    // workers in other processes wake promptly too. The payload names the
    // queue (and must be non-empty: @effect/sql-pg drops falsy payloads).
    const wakeUp = (queue?: JobStore.QueueName): Effect.Effect<void> =>
      Effect.suspend(() => {
        signalWake(queue)
        return client.notify(wakeChannel, queue !== undefined && queue.length > 0 ? queue : "*").pipe(Effect.ignore)
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
    const keepPolicyFor = (
      keep: JobStore.KeepPolicy | null | undefined,
      state: string
    ): JobStore.KeepStatePolicy | undefined => {
      if (keep === null || keep === undefined) return undefined
      const policy = state === "completed"
        ? keep.completed
        : state === "failed"
        ? keep.failed
        : state === "cancelled"
        ? keep.cancelled
        : undefined
      if (policy !== undefined) return policy
      // Rows persisted by 0.2.x carry the flat {count, ageMs} shape — honour
      // it as an all-states policy so upgrades keep pruning.
      if (
        keep.completed === undefined && keep.failed === undefined && keep.cancelled === undefined &&
        ("count" in keep || "ageMs" in keep)
      ) {
        // SAFETY: the flat legacy shape carries KeepStatePolicy fields.
        return keep as JobStore.KeepStatePolicy
      }
      return undefined
    }

    const applyKeep = (
      tx: Pick<Db, "execute">,
      row: { name: string; state: string; keep: JobStore.KeepPolicy | null },
      now: Date
    ) =>
      Effect.gen(function*() {
        const keep = keepPolicyFor(row.keep, row.state)
        if (keep === undefined) return
        if (keep.ageMs !== undefined) {
          yield* tx.execute(purgeJobsWhere(sql`
            ${jobs.name} = ${row.name} AND ${jobs.state} = ${row.state}
              AND ${jobs.finishedAt} <= ${new Date(now.getTime() - keep.ageMs)}
              AND NOT ${owesCascades}
          `))
        }
        if (keep.count !== undefined) {
          yield* tx.execute(purgeJobsWhere(sql`
            ${jobs.name} = ${row.name} AND ${jobs.state} = ${row.state}
              AND ${jobs.id} NOT IN (
                SELECT ${jobs.id} FROM ${jobs}
                WHERE ${jobs.name} = ${row.name} AND ${jobs.state} = ${row.state}
                ORDER BY ${jobs.finishedAt} DESC, ${jobs.seq} DESC
                LIMIT ${keep.count}
              )
              AND NOT ${owesCascades}
          `))
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
              attempts_max, backoff, keep, timeout_ms, dedupe_key, trace, parent, run_at, enqueued_at${extraColumnNames})
            VALUES (${idExpr}, ${request.name}, ${request.queue}, ${state}, ${request.priority},
              ${JSON.stringify(request.payload ?? null)}::jsonb, ${JSON.stringify(request.metadata)}::jsonb,
              ${request.attemptsMax},
              ${request.backoff === undefined ? null : JSON.stringify(request.backoff)}::jsonb,
              ${request.keep === undefined ? null : JSON.stringify(request.keep)}::jsonb,
              ${request.timeoutMs ?? null}, ${request.dedupe?.key ?? null},
              ${request.trace === undefined ? null : JSON.stringify(request.trace)}::jsonb,
              ${request.parent === undefined ? null : JSON.stringify(request.parent)}::jsonb,
              ${runAt}, ${now}${extraColumnValues(request)})
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
              const replaced = rowsOf(yield* tx.execute<{ id: string; queue: string }>(sql`
                UPDATE ${jobs} SET
                  payload = ${JSON.stringify(request.payload ?? null)}::jsonb,
                  metadata = ${JSON.stringify(request.metadata)}::jsonb,
                  priority = ${request.priority},
                  attempts_max = ${request.attemptsMax},
                  backoff = ${request.backoff === undefined ? null : JSON.stringify(request.backoff)}::jsonb,
                  keep = ${request.keep === undefined ? null : JSON.stringify(request.keep)}::jsonb,
                  timeout_ms = ${request.timeoutMs ?? null},
                  trace = ${request.trace === undefined ? null : JSON.stringify(request.trace)}::jsonb,
                  run_at = ${new Date(now.getTime() + Math.max(0, request.delayMs))}${extraColumnAssignments(request)}
                WHERE ${jobs.id} = ${entry.jobId} AND ${jobs.state} = 'delayed'
                RETURNING ${jobs.id} AS id, ${jobs.queue} AS "queue"
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
                // The replace does not move the job between queues — wake
                // the queue that actually holds the now-rescheduled job.
                return {
                  id: JobId(entry.jobId),
                  duplicate: true,
                  wake: true,
                  wakeQueue: JobStore.QueueName(replaced[0]?.queue ?? request.queue)
                }
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
          while: isDeadlockError
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

    // The outbox invariant: every operation that moves a job carrying a
    // `parent` envelope INTO a terminal state appends its report here in the
    // same transaction (see `JobStore.OutboxEntry`). `exit`/`failedReason`
    // mirror the job row AFTER the transition; JSON.stringify drops
    // undefined fields so absent values read back as undefined.
    const appendOutbox = (
      exec: Pick<Db, "execute">,
      parent: JobStore.ParentEnvelope | null | undefined,
      outcome: JobStore.FlowChildReport["outcome"],
      exit: JobStore.FlowChildReport["exit"],
      failedReason: string | null | undefined
    ) =>
      parent === null || parent === undefined
        ? Effect.void
        : exec.execute(sql`
          INSERT INTO ${flowOutbox} (flow_name, parent_store_key, report)
          VALUES (${parent.flowName}, ${parent.parentStoreKey}, ${
          JSON.stringify({
            flowId: parent.flowId,
            childKey: parent.childKey,
            outcome,
            exit,
            failedReason: failedReason ?? undefined
          })
        }::jsonb)
        `).pipe(Effect.asVoid)

    // Flip a flow's still-pending dependency rows to `cancelled` and NOT
    // `cascaded` (the sweeper still owes the child stores real cancels).
    // Embedded as a CTE body so the caller can count the marked rows into
    // the parent's `cancelled` counter in the same statement.
    const cancelPendingChildren = (flowId: string) =>
      sql`
        UPDATE ${flowChildren} SET status = 'cancelled', cascaded = FALSE
        WHERE ${flowChildren.flowId} = ${flowId} AND ${flowChildren.status} = 'pending'
        RETURNING 1
      `

    // Shared by cancel and cancelByDedupe.
    const cancelJob = (id: JobStore.JobId) =>
      db.transaction((tx) =>
        Effect.gen(function*() {
          const now = yield* nowDate
          // Contract lock order (dependency rows first, parent second): mark
          // a waiting-children parent's remaining pending rows cancelled.
          // Pending rows exist only while the parent is `waiting-children`,
          // so this is a no-op for every other state; a non-cancellable
          // parent rolls the transaction back anyway.
          const firstPass = rowsOf(yield* tx.execute<{ marked: number }>(sql`
            WITH marked AS (${cancelPendingChildren(id)})
            SELECT count(*)::int AS marked FROM marked
          `))
          const preMarked = firstPass[0]?.marked ?? 0
          // One guarded statement: waiting/delayed/waiting-children become
          // terminal, active gets the cancel-request flag; anything else is
          // reported by state.
          const rows = rowsOf(yield* tx.execute<
            {
              id: string
              state: string
              processedAt: Date | null
              name: string
              keep: JobStore.KeepPolicy | null
              dedupeKey: string | null
              hasFlow: boolean
              parent: JobStore.ParentEnvelope | null
              exit: unknown
              failedReason: string | null
            }
          >(sql`
            UPDATE ${jobs} SET
              state = CASE WHEN ${jobs.state} IN ('waiting', 'delayed', 'waiting-children') THEN 'cancelled' ELSE ${jobs.state} END,
              finished_at = CASE WHEN ${jobs.state} IN ('waiting', 'delayed', 'waiting-children') THEN ${now}::timestamptz ELSE ${jobs.finishedAt} END,
              flow_pending = CASE WHEN ${jobs.state} = 'waiting-children' THEN 0 ELSE ${jobs.flowPending} END,
              cancel_requested = CASE WHEN ${jobs.state} = 'active' THEN TRUE ELSE ${jobs.cancelRequested} END
            WHERE ${jobs.id} = ${id} AND ${jobs.state} IN ('waiting', 'delayed', 'active', 'waiting-children')
            RETURNING ${jobs.id} AS id, ${jobs.state} AS state,
              ${jobs.processedAt} AS "processedAt", ${jobs.name} AS "name", ${jobs.keep} AS "keep",
              ${jobs.dedupeKey} AS "dedupeKey", (${jobs.flowPending} IS NOT NULL) AS "hasFlow",
              ${jobs.parent} AS "parent", ${jobs.exit} AS "exit", ${jobs.failedReason} AS "failedReason"
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
            if (row.hasFlow) {
              // Re-run the marking now that the parent lock is held: the
              // first UPDATE above races a concurrent FanOut — its
              // uncommitted dependency-row INSERTs are invisible, while
              // this UPDATE's own EPQ re-check can still see the parent as
              // 'waiting-children' after the FanOut commits. Without this
              // pass those rows would stay 'pending' forever, invisible to
              // both sweep classes. Every row either pass marked lands in
              // the `cancelled` counter.
              yield* tx.execute(sql`
                WITH marked AS (${cancelPendingChildren(id)})
                UPDATE ${jobs} SET flow_cancelled = ${jobs.flowCancelled} + ${preMarked}::int
                  + (SELECT count(*)::int FROM marked)
                WHERE ${jobs.id} = ${id}
              `)
            }
            yield* insertAttempt(tx, id, "cancelled", row.processedAt, now, undefined)
            // A cancelled child reports upward through the outbox.
            yield* appendOutbox(tx, row.parent, "cancelled", row.exit ?? undefined, row.failedReason)
            yield* releaseDedupe(tx, row.name, row.dedupeKey, id, now)
            yield* applyKeep(tx, { name: row.name, state: "cancelled", keep: row.keep }, now)
          }
        })
      ).pipe(
        // Rare lock-order inversion (this row-then-parent flip vs a
        // fail-fast settle's parent-then-rows marking) surfaces as a
        // Postgres deadlock (40P01); one side is killed and safe to retry.
        Effect.retry({
          times: 3,
          while: isDeadlockError
        }),
        Effect.mapError((error) =>
          error instanceof JobStore.JobNotFoundError ||
            error instanceof JobStore.JobNotCancellableError ||
            error instanceof JobStore.JobStoreError
            ? error
            : storeError("cancel failed")(error)
        ),
        Effect.asVoid
      )

    const enqueueOne = (request: JobStore.EnqueueRequest) =>
      Effect.gen(function*() {
        if (request.dedupe !== undefined) {
          const result = yield* enqueueDeduped(request, request.dedupe)
          if (result.wake) {
            yield* wakeUp("wakeQueue" in result && result.wakeQueue !== undefined ? result.wakeQueue : request.queue)
          }
          return { id: result.id, duplicate: result.duplicate }
        }
        const now = yield* nowDate
        const result = yield* insertJob(db, request, now)
        if (!result.duplicate) {
          yield* wakeUp(request.queue)
        }
        return result
      })

    // Multi-row insert with every id resolved client-side, so the ON
    // CONFLICT outcome maps back to items unambiguously (RETURNING only
    // yields rows that actually inserted, in no guaranteed order). Auto ids
    // draw from the seq sequence one statement per round; a conflicted auto
    // id (user squatting on "j-<n>", or a colliding generator) re-draws.
    const insertBatch = (requests: ReadonlyArray<JobStore.EnqueueRequest>) =>
      Effect.gen(function*() {
        const now = yield* nowDate
        const generate = options.idGenerator
        const results: Array<JobStore.EnqueueResult | undefined> = requests.map(() => undefined)
        interface PendingItem {
          readonly request: JobStore.EnqueueRequest
          readonly index: number
          id: JobStore.JobId | undefined
        }
        let pending: Array<PendingItem> = requests.map((request, index) => ({ request, index, id: request.id }))
        for (let round = 0; round < 5 && pending.length > 0; round++) {
          const needIds = pending.filter((item) => item.id === undefined)
          if (needIds.length > 0) {
            if (generate !== undefined) {
              for (const item of needIds) {
                const raw = generate(item.request)
                item.id = JobId(Effect.isEffect(raw) ? yield* raw : raw)
              }
            } else {
              const rows = rowsOf(yield* db.execute<{ id: string }>(sql`
                SELECT 'j-' || ${seqExpr}::text AS id FROM generate_series(1, ${needIds.length})
              `).pipe(Effect.mapError(storeError("enqueueMany failed"))))
              for (let i = 0; i < needIds.length; i++) {
                const row = rows[i]
                const item = needIds[i]
                if (row !== undefined && item !== undefined) {
                  item.id = JobId(row.id)
                }
              }
            }
          }
          // Intra-batch repeats: only an id's first occurrence inserts. A
          // later explicit repeat is a duplicate; an auto collision re-draws.
          const seen = new Set<string>()
          const toInsert: Array<{ request: JobStore.EnqueueRequest; index: number; id: JobStore.JobId }> = []
          const stillPending: Array<PendingItem> = []
          for (const item of pending) {
            const id = item.id
            if (id === undefined) continue
            if (seen.has(id)) {
              if (item.request.id !== undefined) {
                results[item.index] = { id, duplicate: true }
              } else {
                item.id = undefined
                stillPending.push(item)
              }
              continue
            }
            seen.add(id)
            toInsert.push({ request: item.request, index: item.index, id })
          }
          pending = stillPending
          // One INSERT per chunk keeps the bind-parameter count bounded.
          for (let start = 0; start < toInsert.length; start += 500) {
            const chunk = toInsert.slice(start, start + 500)
            const values = chunk.map(({ id, request }) =>
              sql`(${id}, ${request.name}, ${request.queue}, ${
                request.delayMs > 0 ? "delayed" : "waiting"
              }, ${request.priority},
                ${JSON.stringify(request.payload ?? null)}::jsonb, ${JSON.stringify(request.metadata)}::jsonb,
                ${request.attemptsMax},
                ${request.backoff === undefined ? null : JSON.stringify(request.backoff)}::jsonb,
                ${request.keep === undefined ? null : JSON.stringify(request.keep)}::jsonb,
                ${request.timeoutMs ?? null}, ${request.dedupe?.key ?? null},
                ${request.trace === undefined ? null : JSON.stringify(request.trace)}::jsonb,
                ${request.parent === undefined ? null : JSON.stringify(request.parent)}::jsonb,
                ${new Date(now.getTime() + Math.max(0, request.delayMs))}, ${now}${extraColumnValues(request)})`
            )
            const rows = rowsOf(yield* db.execute<{ id: string }>(sql`
              INSERT INTO ${jobs} (id, name, queue, state, priority, payload, metadata,
                attempts_max, backoff, keep, timeout_ms, dedupe_key, trace, parent, run_at, enqueued_at${extraColumnNames})
              VALUES ${sql.join(values, sql`, `)}
              ON CONFLICT (id) DO NOTHING
              RETURNING ${jobs.id} AS id
            `).pipe(Effect.mapError(storeError("enqueueMany failed"))))
            const inserted = new Set(rows.map((row) => row.id))
            // Wake per committed chunk, immediately: a later chunk's failure
            // (or id exhaustion below) must not strand these durable rows
            // unwoken until the poll interval.
            const freshQueues = new Set<JobStore.QueueName>()
            for (const item of chunk) {
              if (inserted.has(item.id)) {
                results[item.index] = { id: item.id, duplicate: false }
                freshQueues.add(item.request.queue)
              } else if (item.request.id !== undefined) {
                results[item.index] = { id: item.id, duplicate: true }
              } else {
                // Auto/generated id squatted by an existing row: re-draw and
                // re-insert next round. NOTE this lands the item after its
                // batch-mates in seq (FIFO) order — acceptable for a
                // pathological collision, and documented on the contract.
                pending.push({ request: item.request, index: item.index, id: undefined })
              }
            }
            for (const queue of freshQueues) {
              yield* wakeUp(queue)
            }
          }
        }
        const resolved: Array<JobStore.EnqueueResult> = []
        for (const result of results) {
          if (result === undefined) {
            return yield* new JobStore.JobStoreError({
              message: "enqueueMany failed: could not generate unique job ids"
            })
          }
          resolved.push(result)
        }
        return resolved
      })

    // The FanOut ack: land the manifest (flow columns + dependency rows) and
    // park the parent, all lock-token-guarded in one transaction. A fan-out
    // is a phase transition, not a completed run — `attempts_made` is not
    // incremented; the ledger records `fanned-out` with no exit.
    const ackFanOut = (
      id: JobStore.JobId,
      token: string,
      outcome: Extract<JobStore.AckOutcome, { _tag: "FanOut" }>
    ): Effect.Effect<void, JobStore.JobStoreError | JobStore.JobNotFoundError | JobStore.LockLostError> =>
      Effect.gen(function*() {
        // Validate BEFORE any mutation, so a bad spec cannot leave the job
        // half-acked (lock cleared, ledger written, still active).
        if (outcome.children.some((child) => child.request.id === undefined)) {
          return yield* new JobStore.JobStoreError({
            message: "FanOut child specs require an explicit request.id"
          })
        }
        const wakeQueue = yield* db.transaction((tx) =>
          Effect.gen(function*() {
            const now = yield* nowDate
            const rows = rowsOf(yield* tx.execute<
              {
                processedAt: Date | null
                name: string
                keep: JobStore.KeepPolicy | null
                dedupeKey: string | null
                queue: string
                cancelRequested: boolean
                flowPending: number | null
                parent: JobStore.ParentEnvelope | null
                exit: unknown
                failedReason: string | null
              }
            >(sql`
              UPDATE ${jobs} SET lock_token = NULL, lock_expires_at = NULL
              WHERE ${jobs.id} = ${id} AND ${jobs.state} = 'active' AND ${jobs.lockToken} = ${token}
              RETURNING ${jobs.processedAt} AS "processedAt", ${jobs.name} AS "name",
                ${jobs.keep} AS "keep", ${jobs.dedupeKey} AS "dedupeKey", ${jobs.queue} AS "queue",
                ${jobs.cancelRequested} AS "cancelRequested", ${jobs.flowPending} AS "flowPending",
                ${jobs.parent} AS "parent", ${jobs.exit} AS "exit", ${jobs.failedReason} AS "failedReason"
            `))
            const row = rows[0]
            if (row === undefined) {
              return yield* explainMiss(id)
            }
            yield* insertAttempt(tx, id, "fanned-out", row.processedAt, now, undefined)
            let pending: number
            if (row.flowPending === null || row.flowPending === undefined) {
              pending = outcome.children.length
              yield* tx.execute(sql`
                UPDATE ${jobs} SET flow_fail_fast = ${outcome.failFast}, flow_pending = ${pending},
                  flow_completed = 0, flow_failed = 0, flow_cancelled = 0
                WHERE ${jobs.id} = ${id}
              `)
              // Chunked multi-row VALUES, like enqueueMany's insertBatch.
              for (let start = 0; start < outcome.children.length; start += 500) {
                const chunk = outcome.children.slice(start, start + 500)
                const values = chunk.map((child) =>
                  sql`(${id}, ${child.childKey}, ${child.request.name}, ${child.storeKey},
                    ${JSON.stringify(child.request)}::jsonb, 'pending', NULL, NULL, FALSE, ${now})`
                )
                yield* tx.execute(sql`
                  INSERT INTO ${flowChildren} (flow_id, child_key, name, store_key, spec,
                    status, exit, failed_reason, cascaded, pending_since)
                  VALUES ${sql.join(values, sql`, `)}
                `)
              }
            } else {
              // A manifest that was already present is kept untouched (double
              // fan-out converges on the persisted children); the state
              // transition follows the persisted pending count either way.
              pending = Number(row.flowPending)
            }
            if (row.cancelRequested) {
              // A cancel raced the fan-out: cancellation wins. The rows exist
              // and get marked (into the `cancelled` counter), so the sweeper
              // cascades (mostly no-op cancels for never-enqueued children).
              yield* tx.execute(sql`
                WITH marked AS (${cancelPendingChildren(id)})
                UPDATE ${jobs} SET state = 'cancelled', finished_at = ${now},
                  cancel_requested = FALSE, flow_pending = 0,
                  flow_cancelled = ${jobs.flowCancelled} + (SELECT count(*)::int FROM marked)
                WHERE ${jobs.id} = ${id}
              `)
              yield* insertAttempt(tx, id, "cancelled", row.processedAt, now, undefined)
              // A cancelled NESTED parent reports upward through the outbox.
              yield* appendOutbox(tx, row.parent, "cancelled", row.exit ?? undefined, row.failedReason)
              yield* releaseDedupe(tx, row.name, row.dedupeKey, id, now)
              yield* applyKeep(tx, { name: row.name, state: "cancelled", keep: row.keep }, now)
              return undefined
            }
            if (pending > 0) {
              yield* tx.execute(sql`
                UPDATE ${jobs} SET state = 'waiting-children' WHERE ${jobs.id} = ${id}
              `)
              return undefined
            }
            // Empty (or fully recorded) manifest: straight to runnable collect.
            yield* tx.execute(sql`
              UPDATE ${jobs} SET state = 'waiting', run_at = ${now}, seq = ${seqExpr}
              WHERE ${jobs.id} = ${id}
            `)
            return JobStore.QueueName(row.queue)
          })
        ).pipe(
          Effect.mapError((error) =>
            error instanceof JobStore.JobNotFoundError || error instanceof JobStore.LockLostError ||
              error instanceof JobStore.JobStoreError
              ? error
              : storeError("ack failed")(error)
          )
        )
        if (wakeQueue !== undefined) {
          yield* wakeUp(wakeQueue)
        }
      })

    const store: JobStore.Service = {
      enqueue: enqueueOne,

      enqueueMany: (requests) =>
        Effect.gen(function*() {
          const results: Array<JobStore.EnqueueResult> = []
          let batch: Array<JobStore.EnqueueRequest> = []
          const flush = () =>
            Effect.gen(function*() {
              if (batch.length === 0) return
              const items = batch
              batch = []
              // No spread: a six-figure batch would blow the engine's
              // argument-count limit after the rows already committed.
              for (const result of yield* insertBatch(items)) {
                results.push(result)
              }
            })
          for (const request of requests) {
            // Dedup items run through the transactional single-enqueue path
            // in order; runs of plain items between them batch into one
            // INSERT with multi-row VALUES.
            if (request.dedupe !== undefined) {
              yield* flush()
              results.push(yield* enqueueOne(request))
            } else {
              batch.push(request)
            }
          }
          yield* flush()
          return results
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
                ${jobs.trace} AS "trace", ${jobs.parent} AS "parent",
                ${jobs.flowFailFast} AS "flowFailFast", ${jobs.flowPending} AS "flowPending",
                ${jobs.flowCompleted} AS "flowCompleted", ${jobs.flowFailed} AS "flowFailed",
                ${jobs.flowCancelled} AS "flowCancelled",
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

      ack: (id, token, outcome) => {
        if (outcome._tag === "FanOut") {
          return ackFanOut(id, token, outcome)
        }
        return db.transaction((tx) =>
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
                queue: string
                parent: JobStore.ParentEnvelope | null
                exit: unknown
                failedReason: string | null
              }
            >(sql`
              UPDATE ${jobs} SET ${update},
                attempts_made = ${jobs.attemptsMade} + 1, lock_token = NULL, lock_expires_at = NULL
              WHERE ${jobs.id} = ${id} AND ${jobs.state} = 'active' AND ${jobs.lockToken} = ${token}
              RETURNING ${jobs.processedAt} AS "processedAt", ${jobs.name} AS "name",
                ${jobs.state} AS "state", ${jobs.keep} AS "keep", ${jobs.dedupeKey} AS "dedupeKey",
                ${jobs.queue} AS "queue", ${jobs.parent} AS "parent", ${jobs.exit} AS "exit",
                ${jobs.failedReason} AS "failedReason"
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
              // A terminal transition of an envelope-carrying child reports
              // upward through the outbox (exit/failedReason as persisted).
              yield* appendOutbox(
                tx,
                row.parent,
                ledgerOutcome === "retried" ? "cancelled" : ledgerOutcome,
                row.exit ?? undefined,
                row.failedReason
              )
              yield* releaseDedupe(tx, row.name, row.dedupeKey, id, now)
              yield* applyKeep(tx, row, now)
            }
            return outcome._tag === "Retry" && !cancelledRetry
              ? JobStore.QueueName(row.queue)
              : undefined
          })
        ).pipe(
          Effect.mapError((error) =>
            error instanceof JobStore.JobNotFoundError || error instanceof JobStore.LockLostError ||
              error instanceof JobStore.JobStoreError
              ? error
              : storeError("ack failed")(error)
          ),
          Effect.tap((queue) => queue !== undefined ? wakeUp(queue) : Effect.void),
          Effect.asVoid
        )
      },

      release: (id, token) =>
        Effect.gen(function*() {
          const released = yield* db.transaction((tx) =>
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
                  queue: string
                  parent: JobStore.ParentEnvelope | null
                  exit: unknown
                  failedReason: string | null
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
                  ${jobs.dedupeKey} AS "dedupeKey", ${jobs.queue} AS "queue",
                  ${jobs.parent} AS "parent", ${jobs.exit} AS "exit", ${jobs.failedReason} AS "failedReason"
              `))
              const row = rows[0]
              if (row === undefined) return undefined
              if (row.cancelled) {
                yield* insertAttempt(tx, id, "cancelled", row.processedAt, now, undefined)
                // A cancel honoured at release is a terminal transition too.
                yield* appendOutbox(tx, row.parent, "cancelled", row.exit ?? undefined, row.failedReason)
                yield* releaseDedupe(tx, row.name, row.dedupeKey, id, now)
                yield* applyKeep(tx, { name: row.name, state: "cancelled", keep: row.keep }, now)
              }
              return { cancelled: row.cancelled, queue: JobStore.QueueName(row.queue) }
            })
          ).pipe(Effect.mapError((error) =>
            error instanceof JobStore.JobStoreError ? error : storeError("release failed")(error)
          ))
          if (released === undefined) {
            return yield* explainMiss(id)
          }
          if (!released.cancelled) {
            yield* wakeUp(released.queue)
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
                parent: JobStore.ParentEnvelope | null
                exit: unknown
                failedReason: string | null
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
                ${jobs.name} AS "name", ${jobs.keep} AS "keep", ${jobs.dedupeKey} AS "dedupeKey",
                ${jobs.parent} AS "parent", ${jobs.exit} AS "exit", ${jobs.failedReason} AS "failedReason"
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
                // Honoured cancels and stall exhaustion are terminal
                // transitions: envelope-carrying children report upward.
                yield* appendOutbox(
                  tx,
                  row.parent,
                  row.state === "cancelled" ? "cancelled" : "failed",
                  row.exit ?? undefined,
                  row.failedReason
                )
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
            recovered.some((entry) => !entry.failed) ? wakeUp() : Effect.void
          )
        ),

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
          const orderBy = listOptions.orderBy ?? "enqueuedAt"
          const descending = (listOptions.order ?? "desc") === "desc"
          // Order on the exact value the record reports: enqueued_at/run_at
          // are NOT NULL columns; finished_at is absent on non-terminal rows
          // and sorts as 0 — COALESCE to the epoch, which is what the
          // `<orderValueMillis>:<id>` cursor round-trips through `new
          // Date(0)`. All stored timestamps were bound from the Clock at
          // millisecond precision, so cursor equality comparisons are exact.
          const orderExpr = orderBy === "enqueuedAt"
            ? sql`${jobs.enqueuedAt}`
            : orderBy === "runAt"
            ? sql`${jobs.runAt}`
            : sql`COALESCE(${jobs.finishedAt}, 'epoch'::timestamptz)`
          const direction = descending ? sql`DESC` : sql`ASC`
          if (listOptions.cursor !== undefined) {
            // Exclusive keyset: `<orderValueMillis>:<id>`, strictly past the
            // cursor row in the requested direction (id tiebreak included).
            const split = listOptions.cursor.indexOf(":")
            const cursorAt = new Date(Number(listOptions.cursor.slice(0, split)))
            const cursorId = listOptions.cursor.slice(split + 1)
            conditions.push(
              descending
                ? sql`(${orderExpr}, ${jobs.id}) < (${cursorAt}, ${cursorId})`
                : sql`(${orderExpr}, ${jobs.id}) > (${cursorAt}, ${cursorId})`
            )
          }
          const rows = rowsOf(yield* db.execute<JobRow>(sql`
            SELECT ${jobs.id} AS "id", ${jobs.name} AS "name", ${jobs.queue} AS "queue",
              ${jobs.state} AS "state", ${jobs.priority} AS "priority", ${jobs.seq} AS "seq",
              ${jobs.payload} AS "payload", ${jobs.metadata} AS "metadata",
              ${jobs.attemptsMax} AS "attemptsMax", ${jobs.attemptsMade} AS "attemptsMade",
              ${jobs.stalledCount} AS "stalledCount", ${jobs.backoff} AS "backoff",
              ${jobs.keep} AS "keep", ${jobs.timeoutMs} AS "timeoutMs",
              ${jobs.cancelRequested} AS "cancelRequested", ${jobs.dedupeKey} AS "dedupeKey",
              ${jobs.trace} AS "trace", ${jobs.parent} AS "parent",
              ${jobs.flowFailFast} AS "flowFailFast", ${jobs.flowPending} AS "flowPending",
              ${jobs.flowCompleted} AS "flowCompleted", ${jobs.flowFailed} AS "flowFailed",
              ${jobs.flowCancelled} AS "flowCancelled",
              ${jobs.runAt} AS "runAt", ${jobs.enqueuedAt} AS "enqueuedAt",
              ${jobs.processedAt} AS "processedAt", ${jobs.finishedAt} AS "finishedAt",
              ${jobs.exit} AS "exit", ${jobs.failedReason} AS "failedReason"
            FROM ${jobs}
            WHERE ${sql.join(conditions, sql` AND `)}
            ORDER BY ${orderExpr} ${direction}, ${jobs.id} ${direction}
            LIMIT ${limit + 1}
          `).pipe(Effect.mapError(storeError("list failed"))))
          const items = rows.slice(0, limit).map(toRecord)
          const last = items[items.length - 1]
          return {
            items,
            cursor: rows.length > limit && last !== undefined
              ? `${
                orderBy === "enqueuedAt"
                  ? last.enqueuedAt
                  : orderBy === "runAt"
                  ? last.runAt
                  : last.finishedAt ?? 0
              }:${last.id}`
              : undefined
          }
        }),

      retry: (id) =>
        Effect.gen(function*() {
          const now = yield* nowDate
          const rows = rowsOf(yield* db.execute<{ id: string; queue: string }>(sql`
            UPDATE ${jobs} SET state = 'waiting', attempts_made = 0, stalled_count = 0,
              cancel_requested = FALSE,
              exit = NULL, failed_reason = NULL, finished_at = NULL, processed_at = NULL,
              run_at = ${now}, seq = ${seqExpr}
            WHERE ${jobs.id} = ${id} AND ${jobs.state} = 'failed'
            RETURNING ${jobs.id} AS id, ${jobs.queue} AS "queue"
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
          yield* wakeUp(JobStore.QueueName(rows[0]?.queue ?? ""))
        }),

      cancel: (id) => cancelJob(id),

      cancelByDedupe: (name, key) =>
        Effect.gen(function*() {
          const rows = rowsOf(yield* db.execute<{ jobId: string }>(sql`
            SELECT ${dedupe.jobId} AS "jobId" FROM ${dedupe}
            WHERE ${dedupe.name} = ${name} AND ${dedupe.key} = ${key}
          `).pipe(Effect.mapError(storeError("cancelByDedupe failed"))))
          const jobId = rows[0]?.jobId
          // "" is the in-transaction placeholder; treat like no entry.
          if (jobId === undefined || jobId === "") return false
          return yield* cancelJob(JobId(jobId)).pipe(
            Effect.as(true),
            // Idempotent: a vanished or already-terminal keyed job is
            // "nothing pending", not an error.
            Effect.catchTag(["JobNotFoundError", "JobNotCancellableError"], () => Effect.succeed(false))
          )
        }),
      promote: (id) =>
        Effect.gen(function*() {
          const now = yield* nowDate
          const rows = rowsOf(yield* db.execute<{ id: string; queue: string }>(sql`
            UPDATE ${jobs} SET state = 'waiting', run_at = ${now}
            WHERE ${jobs.id} = ${id} AND ${jobs.state} = 'delayed'
            RETURNING ${jobs.id} AS id, ${jobs.queue} AS "queue"
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
          yield* wakeUp(JobStore.QueueName(rows[0]?.queue ?? ""))
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
          Effect.andThen(wakeUp(queue))
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
            priority, attempts_max, backoff, keep, timeout_ms, group_name, next_run_at)
          VALUES (${schedule.key}, ${schedule.jobName}, ${schedule.queue},
            ${schedule.cron ?? null}, ${schedule.tz ?? null}, ${schedule.everyMs ?? null},
            ${JSON.stringify(schedule.payload ?? null)}::jsonb, ${JSON.stringify(schedule.metadata)}::jsonb,
            ${schedule.priority}, ${schedule.attemptsMax},
            ${schedule.backoff === undefined ? null : JSON.stringify(schedule.backoff)}::jsonb,
            ${schedule.keep === undefined ? null : JSON.stringify(schedule.keep)}::jsonb,
            ${schedule.timeoutMs ?? null}, ${schedule.group ?? null}, ${new Date(schedule.nextRunAt)})
          ON CONFLICT (key) DO UPDATE SET
            job_name = EXCLUDED.job_name, queue = EXCLUDED.queue, cron = EXCLUDED.cron,
            tz = EXCLUDED.tz, every_ms = EXCLUDED.every_ms, payload = EXCLUDED.payload,
            metadata = EXCLUDED.metadata, priority = EXCLUDED.priority,
            attempts_max = EXCLUDED.attempts_max, backoff = EXCLUDED.backoff,
            keep = EXCLUDED.keep, timeout_ms = EXCLUDED.timeout_ms,
            group_name = EXCLUDED.group_name,
            next_run_at = CASE
              WHEN ${schedules.cron} IS NOT DISTINCT FROM EXCLUDED.cron
                AND ${schedules.tz} IS NOT DISTINCT FROM EXCLUDED.tz
                AND ${schedules.everyMs} IS NOT DISTINCT FROM EXCLUDED.every_ms
              THEN ${schedules.nextRunAt}
              ELSE EXCLUDED.next_run_at END
        `).pipe(
          Effect.mapError(storeError("upsertSchedule failed")),
          Effect.andThen(wakeUp(schedule.queue))
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
          if (listOptions?.group !== undefined) {
            conditions.push(sql`${schedules.group} = ${listOptions.group}`)
          }
          const rows = rowsOf(yield* db.execute<ScheduleRow>(sql`
            SELECT ${schedules.key} AS "key", ${schedules.jobName} AS "jobName",
              ${schedules.queue} AS "queue", ${schedules.cron} AS "cron", ${schedules.tz} AS "tz",
              ${schedules.everyMs} AS "everyMs", ${schedules.payload} AS "payload",
              ${schedules.metadata} AS "metadata", ${schedules.priority} AS "priority",
              ${schedules.attemptsMax} AS "attemptsMax", ${schedules.backoff} AS "backoff",
              ${schedules.keep} AS "keep", ${schedules.timeoutMs} AS "timeoutMs",
              ${schedules.group} AS "group", ${schedules.nextRunAt} AS "nextRunAt"
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
              ${schedules.group} AS "group", ${schedules.nextRunAt} AS "nextRunAt"
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

      tickSchedule: (key, expectedRunAt, nextRunAt, request) =>
        Effect.gen(function*() {
          if (request.id === undefined) {
            return yield* new JobStore.JobStoreError({
              message: "tickSchedule requires an explicit request.id"
            })
          }
          // One transaction: the nextRunAt CAS claims the slot and the job
          // INSERT commits with it, so a stale sweeper can never re-fire an
          // occurrence — even after retention pruned the previous slot's job.
          const fired = yield* db.transaction((tx) =>
            Effect.gen(function*() {
              const claimed = rowsOf(yield* tx.execute<{ key: string }>(sql`
                UPDATE ${schedules} SET next_run_at = ${new Date(nextRunAt)}
                WHERE ${schedules.key} = ${key} AND ${schedules.nextRunAt} = ${new Date(expectedRunAt)}
                RETURNING ${schedules.key} AS key
              `))
              if (claimed.length === 0) return false
              const now = yield* nowDate
              const result = yield* insertJob(tx, request, now)
              // A pre-existing slot row (pre-0.4 crash between enqueue and
              // advance) still advances the schedule but fires nothing new.
              return !result.duplicate
            })
          ).pipe(Effect.mapError((error) =>
            error instanceof JobStore.JobStoreError ? error : storeError("tickSchedule failed")(error)
          ))
          if (fired) {
            yield* wakeUp(request.queue)
          }
          return fired
        }),

      recordChildResults: (reports) =>
        Effect.gen(function*() {
          if (reports.length === 0) {
            const none: Array<{ applied: boolean; parentSettled: boolean }> = []
            return none
          }
          const batch = yield* db.transaction((tx) =>
            Effect.gen(function*() {
              const now = yield* nowDate
              const results = reports.map(() => ({ applied: false, parentSettled: false }))

              // Phase 1a — apply every row update (contract lock order:
              // dependency rows FIRST, parents second). Only a (flow, key)'s
              // first occurrence in the batch can apply — later duplicates
              // would find the row non-pending anyway, and UPDATE ... FROM
              // must never see two source rows for one target.
              const candidates: Array<{ readonly index: number; readonly report: JobStore.FlowChildReport }> = []
              const seen = new Set<string>()
              for (const [index, report] of reports.entries()) {
                const key = `${report.flowId}\u0000${report.childKey}`
                if (seen.has(key)) continue
                seen.add(key)
                candidates.push({ index, report })
              }
              for (let start = 0; start < candidates.length; start += 200) {
                const chunk = candidates.slice(start, start + 200)
                const values = chunk.map(({ index, report }) =>
                  sql`(${index}::int, ${report.flowId}::text, ${report.childKey}::text,
                    ${report.outcome}::text,
                    ${report.exit === undefined ? null : JSON.stringify(report.exit)}::jsonb,
                    ${report.failedReason ?? null}::text)`
                )
                const appliedRows = rowsOf(yield* tx.execute<{ ord: number }>(sql`
                  UPDATE ${flowChildren} SET status = v.outcome, exit = v.exit,
                    failed_reason = v.failed_reason, cascaded = TRUE
                  FROM (VALUES ${sql.join(values, sql`, `)})
                    AS v(ord, flow_id, child_key, outcome, exit, failed_reason)
                  WHERE ${flowChildren.flowId} = v.flow_id AND ${flowChildren.childKey} = v.child_key
                    AND ${flowChildren.status} = 'pending'
                  RETURNING v.ord AS "ord"
                `))
                for (const row of appliedRows) {
                  const result = results[Number(row.ord)]
                  if (result !== undefined) {
                    result.applied = true
                  }
                }
              }

              // Tally the applied reports per touched flow, in batch order.
              interface Touch {
                appliedCount: number
                completed: number
                failed: number
                cancelled: number
                firstAppliedFailed: number | undefined
                lastApplied: number
              }
              const touched = new Map<string, Touch>()
              for (const [index, report] of reports.entries()) {
                if (results[index]?.applied !== true) continue
                const touch = touched.get(report.flowId) ?? {
                  appliedCount: 0,
                  completed: 0,
                  failed: 0,
                  cancelled: 0,
                  firstAppliedFailed: undefined,
                  lastApplied: index
                }
                touch.appliedCount += 1
                touch.lastApplied = index
                if (report.outcome === "completed") touch.completed += 1
                if (report.outcome === "cancelled") touch.cancelled += 1
                if (report.outcome === "failed") {
                  touch.failed += 1
                  touch.firstAppliedFailed ??= index
                }
                touched.set(report.flowId, touch)
              }

              // Phase 1b + 2 — per touched flow (sorted, so concurrent
              // batches take parent locks in one order): move the applied
              // children from `pending` to their outcome counters, then make
              // at most one settle decision. Fail-fast wins the tie.
              const wakeQueues: Array<JobStore.QueueName> = []
              for (const flowId of [...touched.keys()].toSorted()) {
                const touch = touched.get(flowId)
                if (touch === undefined) continue
                const parents = rowsOf(yield* tx.execute<
                  {
                    state: JobStore.JobState
                    flowPending: number
                    flowFailFast: boolean | null
                    processedAt: Date | null
                    name: string
                    keep: JobStore.KeepPolicy | null
                    dedupeKey: string | null
                    queue: string
                    parent: JobStore.ParentEnvelope | null
                  }
                >(sql`
                  UPDATE ${jobs} SET
                    flow_pending = GREATEST(${jobs.flowPending} - ${touch.appliedCount}::int, 0),
                    flow_completed = ${jobs.flowCompleted} + ${touch.completed}::int,
                    flow_failed = ${jobs.flowFailed} + ${touch.failed}::int,
                    flow_cancelled = ${jobs.flowCancelled} + ${touch.cancelled}::int
                  WHERE ${jobs.id} = ${flowId} AND ${jobs.flowPending} IS NOT NULL
                  RETURNING ${jobs.state} AS "state", ${jobs.flowPending} AS "flowPending",
                    ${jobs.flowFailFast} AS "flowFailFast", ${jobs.processedAt} AS "processedAt",
                    ${jobs.name} AS "name", ${jobs.keep} AS "keep",
                    ${jobs.dedupeKey} AS "dedupeKey", ${jobs.queue} AS "queue",
                    ${jobs.parent} AS "parent"
                `))
                const parent = parents[0]
                if (parent === undefined || parent.state !== "waiting-children") continue
                const failedIndex = parent.flowFailFast === true ? touch.firstAppliedFailed : undefined
                const failedReport = failedIndex !== undefined ? reports[failedIndex] : undefined
                if (failedIndex !== undefined && failedReport !== undefined) {
                  // The first applied failure settles the parent terminally
                  // (store-side, like stall exhaustion: failedReason, no
                  // exit) and marks the remaining rows in the same
                  // transaction. A nested parent's own report goes to the
                  // outbox here — this settle IS its terminal transition.
                  const reason = `effect-mq: flow child "${failedReport.childKey}" failed`
                  yield* tx.execute(sql`
                    WITH marked AS (${cancelPendingChildren(flowId)})
                    UPDATE ${jobs} SET state = 'failed', finished_at = ${now},
                      failed_reason = ${reason}, cancel_requested = FALSE, flow_pending = 0,
                      flow_cancelled = ${jobs.flowCancelled} + (SELECT count(*)::int FROM marked)
                    WHERE ${jobs.id} = ${flowId}
                  `)
                  yield* insertAttempt(tx, flowId, "failed", parent.processedAt, now, undefined)
                  yield* appendOutbox(tx, parent.parent, "failed", undefined, reason)
                  yield* releaseDedupe(tx, parent.name, parent.dedupeKey, flowId, now)
                  yield* applyKeep(tx, { name: parent.name, state: "failed", keep: parent.keep }, now)
                  const decided = results[failedIndex]
                  if (decided !== undefined) {
                    decided.parentSettled = true
                  }
                  continue
                }
                if (Number(parent.flowPending) === 0) {
                  // All children settled: the parent resumes runnable, phase
                  // collect, settled at the flow's last applied report.
                  yield* tx.execute(sql`
                    UPDATE ${jobs} SET state = 'waiting', run_at = ${now}, seq = ${seqExpr}
                    WHERE ${jobs.id} = ${flowId}
                  `)
                  wakeQueues.push(JobStore.QueueName(parent.queue))
                  const decided = results[touch.lastApplied]
                  if (decided !== undefined) {
                    decided.parentSettled = true
                  }
                }
              }
              return { results, wakeQueues }
            })
          ).pipe(
            // Rare lock-order inversion (a concurrent cancel/settle marking
            // rows) surfaces as a Postgres deadlock (40P01); the killed side
            // is safe to retry — the row-state guard keeps it idempotent.
            Effect.retry({
              times: 3,
              while: isDeadlockError
            }),
            Effect.mapError((error) =>
              error instanceof JobStore.JobStoreError ? error : storeError("recordChildResults failed")(error)
            )
          )
          for (const queue of batch.wakeQueues) {
            yield* wakeUp(queue)
          }
          return batch.results
        }),

      peekOutbox: (peekOptions) =>
        Effect.gen(function*() {
          const limit = Math.max(0, peekOptions.limit)
          if (limit === 0) {
            const none: Array<JobStore.OutboxEntry> = []
            return none
          }
          // `after` pages past a previously returned id (exclusive), whether
          // or not that entry still exists. Anything that is not a canonical
          // id this store issued is treated as unset.
          const after = peekOptions.after !== undefined && CANONICAL_BIGSERIAL.test(peekOptions.after)
            ? peekOptions.after
            : undefined
          const rows = rowsOf(yield* db.execute<
            {
              id: string
              flowName: string
              parentStoreKey: string
              report: JobStore.FlowChildReport
            }
          >(sql`
            SELECT ${flowOutbox.id}::text AS "id", ${flowOutbox.flowName} AS "flowName",
              ${flowOutbox.parentStoreKey} AS "parentStoreKey", ${flowOutbox.report} AS "report"
            FROM ${flowOutbox}
            ${after === undefined ? sql`` : sql`WHERE ${flowOutbox.id} > ${after}::bigint`}
            ORDER BY ${flowOutbox.id} ASC
            LIMIT ${limit}
          `).pipe(Effect.mapError(storeError("peekOutbox failed"))))
          return rows.map((row): JobStore.OutboxEntry => ({
            id: row.id,
            flowName: row.flowName,
            parentStoreKey: row.parentStoreKey,
            report: row.report
          }))
        }),

      deleteOutbox: (ids) =>
        Effect.suspend(() => {
          // Ids are opaque strings to callers; only CANONICAL ids this store
          // could have issued can match. The strictness matters twice: a
          // foreign/garbled id must not blow up the ::bigint cast, and a
          // non-canonical spelling ("007") must stay an unknown-id no-op
          // rather than cast to 7 and delete a live entry.
          const numeric = ids.filter((id) => CANONICAL_BIGSERIAL.test(id))
          if (numeric.length === 0) return Effect.void
          return db.execute(sql`
            DELETE FROM ${flowOutbox}
            WHERE ${flowOutbox.id} = ANY(${sql.param(numeric)}::bigint[])
          `).pipe(
            Effect.mapError(storeError("deleteOutbox failed")),
            Effect.asVoid
          )
        }),

      listChildResults: (flowId, listOptions) =>
        Effect.gen(function*() {
          const limit = Math.max(1, listOptions?.limit ?? 1000)
          const cursor = listOptions?.cursor
          // `spec->>'id'` instead of the whole spec: at 10k children the full
          // payloads would transfer on every collect.
          const rows = rowsOf(yield* db.execute<FlowChildRow>(sql`
            SELECT ${flowChildren.flowId} AS "flowId", ${flowChildren.childKey} AS "childKey",
              ${flowChildren.name} AS "name", ${flowChildren.storeKey} AS "storeKey",
              ${flowChildren.spec}->>'id' AS "childJobId", ${flowChildren.status} AS "status",
              ${flowChildren.exit} AS "exit", ${flowChildren.failedReason} AS "failedReason",
              ${flowChildren.cascaded} AS "cascaded"
            FROM ${flowChildren}
            WHERE ${flowChildren.flowId} = ${flowId}
              ${cursor === undefined ? sql`` : sql`AND ${flowChildren.childKey} > ${cursor}`}
            ORDER BY ${flowChildren.childKey} ASC
            LIMIT ${limit + 1}
          `).pipe(Effect.mapError(storeError("listChildResults failed"))))
          const page = rows.slice(0, limit)
          const items = page.map(toFlowChildRecord)
          const last = items[items.length - 1]
          return {
            items,
            cursor: rows.length > limit && last !== undefined ? last.childKey : undefined
          }
        }),

      flowSweepWork: (sweepOptions) =>
        Effect.gen(function*() {
          const now = yield* nowDate
          const limit = Math.max(1, sweepOptions.limit ?? 1000)
          const threshold = new Date(now.getTime() - sweepOptions.pendingAgeMs)
          type SweepRow = {
            readonly flowId: string
            readonly childKey: string
            readonly storeKey: string
            readonly spec: JobStore.EnqueueRequest
          }
          // Reconcile: pending rows past the eligibility threshold whose
          // parent is still parked (a settled flow never re-drives work).
          // Returning a row re-arms `pending_since` in the same statement, so
          // a full page rotates across sweeps instead of pinning its head.
          // SKIP LOCKED: rows a concurrent report/settle holds are its
          // business, and never waiting means this statement cannot deadlock.
          // The raw column names are safe: only table names vary across
          // factory instances. RETURNING order is unspecified — sorted below.
          const reconcileRows = rowsOf(yield* db.execute<SweepRow>(sql`
            WITH due AS (
              SELECT c.flow_id, c.child_key
              FROM ${flowChildren} c
              JOIN ${jobs} j ON j.id = c.flow_id
              WHERE c.status = 'pending' AND c.pending_since <= ${threshold}
                AND j.state = 'waiting-children'
              ORDER BY c.flow_id, c.child_key
              LIMIT ${limit}
              FOR UPDATE OF c SKIP LOCKED
            )
            UPDATE ${flowChildren} SET pending_since = ${now}
            FROM due
            WHERE ${flowChildren.flowId} = due.flow_id AND ${flowChildren.childKey} = due.child_key
            RETURNING ${flowChildren.flowId} AS "flowId", ${flowChildren.childKey} AS "childKey",
              ${flowChildren.storeKey} AS "storeKey", ${flowChildren.spec} AS "spec"
          `).pipe(Effect.mapError(storeError("flowSweepWork failed"))))
            .toSorted((a, b) =>
              a.flowId !== b.flowId
                ? (a.flowId < b.flowId ? -1 : 1)
                : a.childKey < b.childKey
                ? -1
                : a.childKey > b.childKey
                ? 1
                : 0
            )
          // Cascade: cancelled rows whose cancel has not been delivered into
          // the child's store yet (any parent state).
          const cascadeRows = rowsOf(yield* db.execute<SweepRow>(sql`
            SELECT ${flowChildren.flowId} AS "flowId", ${flowChildren.childKey} AS "childKey",
              ${flowChildren.storeKey} AS "storeKey", ${flowChildren.spec} AS "spec"
            FROM ${flowChildren}
            WHERE ${flowChildren.status} = 'cancelled' AND ${flowChildren.cascaded} = FALSE
            ORDER BY ${flowChildren.flowId}, ${flowChildren.childKey}
            LIMIT ${limit}
          `).pipe(Effect.mapError(storeError("flowSweepWork failed"))))
          const reconcile: Array<{
            flowId: JobStore.JobId
            children: Array<JobStore.FlowChildSpec>
          }> = []
          for (const row of reconcileRows) {
            const flowId = JobId(row.flowId)
            let group = reconcile[reconcile.length - 1]
            if (group === undefined || group.flowId !== flowId) {
              group = { flowId, children: [] }
              reconcile.push(group)
            }
            group.children.push({ childKey: row.childKey, storeKey: row.storeKey, request: row.spec })
          }
          const cascade: Array<{
            flowId: JobStore.JobId
            children: Array<{ childKey: string; storeKey: string; childJobId: JobStore.JobId }>
          }> = []
          for (const row of cascadeRows) {
            const flowId = JobId(row.flowId)
            let group = cascade[cascade.length - 1]
            if (group === undefined || group.flowId !== flowId) {
              group = { flowId, children: [] }
              cascade.push(group)
            }
            group.children.push({
              childKey: row.childKey,
              storeKey: row.storeKey,
              // SAFETY: the FanOut ack validated every spec id before
              // persisting it.
              childJobId: row.spec.id as JobStore.JobId
            })
          }
          const work: JobStore.FlowSweepWork = { reconcile, cascade }
          return work
        }),

      markChildrenCascaded: (flowId, childKeys) =>
        childKeys.length === 0
          ? Effect.void
          : db.execute(sql`
            UPDATE ${flowChildren} SET cascaded = TRUE
            WHERE ${flowChildren.flowId} = ${flowId}
              AND ${flowChildren.childKey} = ANY(${sql.param([...childKeys])})
          `).pipe(
            Effect.mapError(storeError("markChildrenCascaded failed")),
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
              "waiting-children": 0,
              completed: 0,
              failed: 0,
              cancelled: 0
            } satisfies Record<JobStore.JobState, number>
            for (const row of rows) counts[row.state] = row.count
            return counts
          })
        ),

      remove: (id) =>
        // The purge CTE takes a removed flow parent's dependency rows with it
        // in the same statement.
        db.execute(sql`
          WITH deleted AS (
            DELETE FROM ${jobs}
            WHERE ${jobs.id} = ${id} AND ${jobs.state} NOT IN ('active', 'waiting-children')
            RETURNING ${jobs.id} AS id
          ), purged AS (
            DELETE FROM ${flowChildren}
            WHERE ${flowChildren.flowId} IN (SELECT id FROM deleted)
          )
          SELECT id FROM deleted
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
