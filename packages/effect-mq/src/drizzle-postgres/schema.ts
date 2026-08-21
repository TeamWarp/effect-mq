/**
 * Drizzle schema factories for effect-mq's Postgres tables.
 *
 * Re-export these from your own drizzle schema so drizzle-kit owns the
 * migrations and your queries are fully typed — including the job `name`
 * column typed to the union of your job tags:
 *
 * ```ts
 * // schema.ts
 * import { mqJobAttempts, mqJobs } from "effect-mq/drizzle-postgres"
 *
 * type DurableJobs = typeof GenerateInvoice._tag | typeof GenerateReport._tag
 * export const jobs = mqJobs<DurableJobs>()
 * export const jobAttempts = mqJobAttempts(jobs)
 * ```
 *
 * Every factory accepts an `extraConfig` callback — the same shape as
 * drizzle's own third `pgTable` argument — to add your own indexes (or
 * checks/policies) on top of the built-in ones:
 *
 * ```ts
 * export const jobs = mqJobs<DurableJobs>("effect_mq_jobs", {
 *   extraConfig: (t) => [index("jobs_name_recent_idx").on(t.name, t.enqueuedAt.desc())]
 * })
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
import {
  type AnyPgColumnBuilder,
  bigint,
  boolean,
  index,
  integer,
  jsonb,
  type PgBuildExtraConfigColumns,
  pgTable,
  type PgTableExtraConfigValue,
  primaryKey,
  text,
  timestamp
} from "drizzle-orm/pg-core"

type JobId = JobStore.JobId
type QueueName = JobStore.QueueName
type ScheduleKey = JobStore.ScheduleKey
type JobState = JobStore.JobState
type BackoffPolicy = JobStore.BackoffPolicy
type KeepPolicy = JobStore.KeepPolicy
type AttemptOutcome = JobStore.AttemptRecord["outcome"]

/**
 * Table-factory options: `extraConfig` receives the table's columns (exactly
 * like drizzle's third `pgTable` argument) and returns additional indexes,
 * checks, or policies, appended after the built-in ones.
 *
 * @since 0.2.1
 */
export interface MqTableOptions<Columns extends Record<string, AnyPgColumnBuilder>> {
  readonly extraConfig?:
    | ((table: PgBuildExtraConfigColumns<Columns>) => Array<PgTableExtraConfigValue>)
    | undefined
}

const jobsColumns = <JobName extends string>() => ({
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
})

/**
 * The jobs table factory. `JobName` types the `name` column — derive it from
 * your job definitions: `mqJobs<typeof GenerateInvoice._tag | typeof Report._tag>()`.
 *
 * @since 0.1.0
 */
export const mqJobs = <JobName extends string = string>(
  tableName = "effect_mq_jobs",
  options?: MqTableOptions<ReturnType<typeof jobsColumns<JobName>>>
) =>
  pgTable(tableName, jobsColumns<JobName>(), (table) => [
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
    // History/retention queries (leading `name` also serves name-only filters).
    index(`${tableName}_history_idx`).on(table.name, table.state, table.finishedAt),
    // Listing (newest first, keyset pagination).
    index(`${tableName}_listing_idx`).on(table.enqueuedAt.desc(), table.id.desc()),
    // Metadata containment queries.
    index(`${tableName}_metadata_idx`).using("gin", table.metadata.op("jsonb_path_ops")),
    ...options?.extraConfig?.(table) ?? []
  ])

const attemptsColumns = (jobs: MqJobsTable) => ({
  jobId: text("job_id").notNull().references(() => jobs.id, { onDelete: "cascade" }).$type<JobId>(),
  attempt: integer("attempt").notNull(),
  outcome: text("outcome").notNull().$type<AttemptOutcome>(),
  startedAt: timestamp("started_at", { withTimezone: true, mode: "date" }),
  finishedAt: timestamp("finished_at", { withTimezone: true, mode: "date" }).notNull(),
  exit: jsonb("exit")
})

/**
 * The job run-ledger table factory (one row per attempt, including
 * successes and stall recoveries).
 *
 * @since 0.1.0
 */
export const mqJobAttempts = (
  jobs: ReturnType<typeof mqJobs<any>>,
  tableName = "effect_mq_job_attempts",
  options?: MqTableOptions<ReturnType<typeof attemptsColumns>>
) =>
  pgTable(tableName, attemptsColumns(jobs), (table) => [
    primaryKey({ columns: [table.jobId, table.attempt] }),
    ...options?.extraConfig?.(table) ?? []
  ])

const schedulesColumns = () => ({
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
})

/**
 * Repeatable-job schedules (one row per `Job.schedule` key).
 *
 * @since 0.2.0
 */
export const mqSchedules = (
  tableName = "effect_mq_schedules",
  options?: MqTableOptions<ReturnType<typeof schedulesColumns>>
) =>
  pgTable(tableName, schedulesColumns(), (table) => [
    index(`${tableName}_due_idx`).on(table.nextRunAt),
    ...options?.extraConfig?.(table) ?? []
  ])

const queueControlColumns = () => ({
  queue: text("queue").primaryKey().$type<QueueName>(),
  paused: boolean("paused").notNull().default(false)
})

/**
 * Durable queue control flags (pause/resume).
 *
 * @since 0.2.0
 */
export const mqQueueControl = (
  tableName = "effect_mq_queue_control",
  options?: MqTableOptions<ReturnType<typeof queueControlColumns>>
) =>
  pgTable(tableName, queueControlColumns(), (table) => [
    ...options?.extraConfig?.(table) ?? []
  ])

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
