/**
 * The two stores this demo runs against, plus the demo-only Postgres
 * bootstrap. Real applications re-export the schema factories from their
 * drizzle schema and let drizzle-kit own the DDL; a demo wants a clean slate
 * on every run, so it borrows the repo's canonical DDL (kept honest by a
 * schema-drift test) and drop/creates the default-named tables at startup.
 */
import { NodeRedis } from "@effect/platform-node"
import { Redis } from "effect/unstable/persistence"
import { PgClient } from "@effect/sql-pg"
import { Effect, Layer, Redacted } from "effect"
import { JobStore } from "effect-mq"
import {
  DrizzleJobStore,
  mqDedupe,
  mqFlowChildren,
  mqFlowOutbox,
  mqJobAttempts,
  mqJobs,
  mqQueueControl,
  mqSchedules
} from "effect-mq/drizzle-postgres"
import { RedisJobStore } from "effect-mq/redis"
import { createTablesSql, dropTablesSql } from "../../../packages/effect-mq/test/drizzle-postgres/support.ts"

// docker compose up -d (repo root): postgres on 5433, redis on 6380.
const pgUrl = process.env.EFFECT_MQ_PG_URL ??
  "postgres://postgres:postgres@localhost:5433/effect_mq_test"
const redisUrl = process.env.EFFECT_MQ_REDIS_URL ?? "redis://localhost:6380"

export const PgLive: Layer.Layer<PgClient.PgClient> = PgClient.layer({
  url: Redacted.make(pgUrl),
  maxConnections: 6
}).pipe(Layer.orDie)

export const RedisLive = NodeRedis.layer({ url: redisUrl }).pipe(Layer.orDie)

/** Disposable email jobs live in Redis under a named store key. */
export const EmailStore = JobStore.named("emails")

const tableNames = {
  jobs: "effect_mq_jobs",
  attempts: "effect_mq_job_attempts",
  schedules: "effect_mq_schedules",
  queues: "effect_mq_queue_control",
  dedupe: "effect_mq_dedupe",
  flowChildren: "effect_mq_flow_children",
  flowOutbox: "effect_mq_flow_outbox"
}

const jobs = mqJobs(tableNames.jobs)
const attempts = mqJobAttempts(jobs, tableNames.attempts)
const schedules = mqSchedules(tableNames.schedules)
const queues = mqQueueControl(tableNames.queues)
const dedupe = mqDedupe(tableNames.dedupe)
const flowChildren = mqFlowChildren(tableNames.flowChildren)
const flowOutbox = mqFlowOutbox(tableNames.flowOutbox)

/** Demo-only: a clean slate on every run. */
export const resetTables = Effect.gen(function*() {
  const client = yield* PgClient.PgClient
  yield* client.unsafe(dropTablesSql(tableNames)).pipe(Effect.ignore)
  for (const statement of createTablesSql(tableNames)) {
    yield* client.unsafe(statement)
  }
}).pipe(Effect.orDie)

/** Business-critical jobs: Postgres, as the DEFAULT JobStore. */
export const PgStoreLive: Layer.Layer<JobStore.JobStore, never, PgClient.PgClient> = Layer.effect(
  JobStore.JobStore,
  DrizzleJobStore.make({ jobs, attempts, schedules, queues, dedupe, flowChildren, flowOutbox }).pipe(
    Effect.orDie
  )
)

/**
 * Disposable email jobs: Redis, under the named store. The prefix is fixed
 * so the dashboard process sees the same keys; `resetRedis` clears it at
 * the start of every demo run.
 */
const demoPrefix = "podcast-demo"

export const RedisStoreLive = RedisJobStore.layerFor(EmailStore, {
  prefix: demoPrefix
}).pipe(Layer.provide(RedisLive))

/** Demo-only: wipe the previous run's Redis keys. */
export const resetRedis = Effect.gen(function*() {
  const redis = yield* Redis.Redis
  let cursor = "0"
  do {
    const [next, keys] = yield* redis.send<[string, Array<string>]>(
      "SCAN",
      cursor,
      "MATCH",
      `${demoPrefix}:*`,
      "COUNT",
      "500"
    )
    if (keys.length > 0) {
      yield* redis.send("DEL", ...keys)
    }
    cursor = next
  } while (cursor !== "0")
}).pipe(Effect.orDie)
