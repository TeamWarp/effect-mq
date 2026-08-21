/**
 * Drizzle schema factories for effect-mq's Postgres tables.
 *
 * Re-export these from your own drizzle schema so drizzle-kit owns the
 * migrations and your queries are fully typed — including the job `name`
 * column typed to the union of your job tags:
 *
 * ```ts
 * // schema.ts
 * import { mqJobAttempts, mqJobs } from "effect-mq/drizzle"
 *
 * type DurableJobs = typeof SyncBenefits._tag | typeof GenerateReport._tag
 * export const jobs = mqJobs<DurableJobs>()
 * export const jobAttempts = mqJobAttempts(jobs)
 * ```
 *
 * Then `drizzle-kit generate` emits the CREATE TABLE migrations into your
 * pipeline like any other table. Reads through drizzle are encouraged;
 * writes must go through the `JobStore` (e.g. `store.retry`) so locking and
 * wake-up invariants hold.
 *
 * @since 0.1.0
 */
import type * as JobStore from "../JobStore.ts"
import { sql } from "drizzle-orm"
import { bigint, boolean, index, integer, jsonb, pgTable, primaryKey, text, timestamp } from "drizzle-orm/pg-core"

type JobId = JobStore.JobId
type QueueName = JobStore.QueueName
type ScheduleKey = JobStore.ScheduleKey
type JobState = JobStore.JobState
type BackoffPolicy = JobStore.BackoffPolicy
type KeepPolicy = JobStore.KeepPolicy
type AttemptOutcome = JobStore.AttemptRecord["outcome"]
/**
 * The jobs table factory. `JobName` types the `name` column — derive it from
 * your job definitions: `mqJobs<typeof SyncBenefits._tag | typeof Report._tag>()`.
 *
 * @since 0.1.0
 */
export const mqJobs = <JobName extends string = string>(
  tableName = "effect_mq_jobs"
) =>
  pgTable(tableName, {
    id: text("id").primaryKey().$type<JobId>(),
    name: text("name").notNull().$type<JobName>(),
    queue: text("queue").notNull().$type<QueueName>(),
    state: text("state").notNull().$type<JobState>(),
    priority: integer("priority").notNull().default(0),
    /** FIFO order within a priority; bumped on retry so retries go to the tail. */
    seq: bigint("seq", { mode: "number" }).notNull().generatedByDefaultAsIdentity(),
    payload: jsonb("payload"),
    metadata: jsonb("metadata").notNull().default({}).$type<Record<string, string>>(),
    attemptsMax: integer("attempts_max").notNull(),
    attemptsMade: integer("attempts_made").notNull().default(0),
    stalledCount: integer("stalled_count").notNull().default(0),
    backoff: jsonb("backoff").$type<BackoffPolicy>(),
    keep: jsonb("keep").$type<KeepPolicy>(),
    timeoutMs: bigint("timeout_ms", { mode: "number" }),
    cancelRequested: boolean("cancel_requested").notNull().default(false),
    runAt: timestamp("run_at", { withTimezone: true, mode: "date" }).notNull(),
    enqueuedAt: timestamp("enqueued_at", { withTimezone: true, mode: "date" }).notNull(),
    processedAt: timestamp("processed_at", { withTimezone: true, mode: "date" }),
    finishedAt: timestamp("finished_at", { withTimezone: true, mode: "date" }),
    exit: jsonb("exit"),
    failedReason: text("failed_reason"),
    lockToken: text("lock_token"),
    lockExpiresAt: timestamp("lock_expires_at", { withTimezone: true, mode: "date" })
  }, (table) => [
    // Claim path: pop highest priority, FIFO within it.
    index(`${tableName}_ready_idx`)
      .on(table.queue, table.priority.desc(), table.seq.asc())
      .where(sql`${table.state} = 'waiting'`),
    // Delayed promotion + nextRunAt.
    index(`${tableName}_delayed_idx`)
      .on(table.queue, table.runAt)
      .where(sql`${table.state} = 'delayed'`),
    // Stalled sweep.
    index(`${tableName}_active_idx`)
      .on(table.lockExpiresAt)
      .where(sql`${table.state} = 'active'`),
    // History/retention queries.
    index(`${tableName}_history_idx`).on(table.name, table.state, table.finishedAt),
    // Listing (newest first, keyset pagination).
    index(`${tableName}_listing_idx`).on(table.enqueuedAt.desc(), table.id.desc()),
    // Metadata containment queries.
    index(`${tableName}_metadata_idx`).using("gin", table.metadata.op("jsonb_path_ops"))
  ])

/**
 * The job run-ledger table factory (one row per attempt, including
 * successes and stall recoveries).
 *
 * @since 0.1.0
 */
export const mqJobAttempts = (
  jobs: ReturnType<typeof mqJobs<any>>,
  tableName = "effect_mq_job_attempts"
) =>
  pgTable(tableName, {
    jobId: text("job_id").notNull().references(() => jobs.id, { onDelete: "cascade" }).$type<JobId>(),
    attempt: integer("attempt").notNull(),
    outcome: text("outcome").notNull().$type<AttemptOutcome>(),
    startedAt: timestamp("started_at", { withTimezone: true, mode: "date" }),
    finishedAt: timestamp("finished_at", { withTimezone: true, mode: "date" }).notNull(),
    exit: jsonb("exit")
  }, (table) => [
    primaryKey({ columns: [table.jobId, table.attempt] })
  ])

/**
 * Repeatable-job schedules (one row per `Job.schedule` key).
 *
 * @since 0.2.0
 */
export const mqSchedules = (tableName = "effect_mq_schedules") =>
  pgTable(tableName, {
    key: text("key").primaryKey().$type<ScheduleKey>(),
    jobName: text("job_name").notNull(),
    queue: text("queue").notNull().$type<QueueName>(),
    cron: text("cron"),
    tz: text("tz"),
    everyMs: bigint("every_ms", { mode: "number" }),
    payload: jsonb("payload"),
    metadata: jsonb("metadata").notNull().default({}).$type<Record<string, string>>(),
    priority: integer("priority").notNull().default(0),
    attemptsMax: integer("attempts_max").notNull(),
    backoff: jsonb("backoff").$type<BackoffPolicy>(),
    keep: jsonb("keep").$type<KeepPolicy>(),
    timeoutMs: bigint("timeout_ms", { mode: "number" }),
    nextRunAt: timestamp("next_run_at", { withTimezone: true, mode: "date" }).notNull()
  }, (table) => [
    index(`${tableName}_due_idx`).on(table.nextRunAt)
  ])

/**
 * Durable queue control flags (pause/resume).
 *
 * @since 0.2.0
 */
export const mqQueueControl = (tableName = "effect_mq_queue_control") =>
  pgTable(tableName, {
    queue: text("queue").primaryKey().$type<QueueName>(),
    paused: boolean("paused").notNull().default(false)
  })

/**
 * @since 0.1.0
 */
export type MqJobsTable = ReturnType<typeof mqJobs<any>>

/**
 * @since 0.1.0
 */
export type MqJobAttemptsTable = ReturnType<typeof mqJobAttempts>

/**
 * @since 0.2.0
 */
export type MqSchedulesTable = ReturnType<typeof mqSchedules>

/**
 * @since 0.2.0
 */
export type MqQueueControlTable = ReturnType<typeof mqQueueControl>
