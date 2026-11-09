import { PgClient } from "@effect/sql-pg"
import { assert, describe, expect, it } from "@effect/vitest"
import { Deferred, Effect, Exit, Fiber, Layer, Option, Schedule, Schema, Scope } from "effect"
import { SqlError, UnknownError } from "effect/sql/SqlError"
import { DrizzleJobStore } from "../../src/drizzle-postgres/index.ts"
import { Job, JobStore, Worker } from "../../src/index.ts"
import { freshStoreEffect, pgAvailable, pgClientLive, pgUrl } from "./support.ts"

const available = await pgAvailable()
const queue = JobStore.QueueName("default")

const listenerPids = (client: PgClient.PgClient, channel: string) =>
  client.unsafe<{ pid: number }>(
    "SELECT pid FROM pg_stat_activity WHERE datname = current_database() AND state = 'idle' AND query = $1",
    [`LISTEN "${channel}"`]
  ).pipe(Effect.map((rows) => rows.map((row) => row.pid)))

const awaitListener = (client: PgClient.PgClient, channel: string, previous?: number) =>
  listenerPids(client, channel).pipe(
    Effect.repeat({ schedule: Schedule.spaced("20 millis"), until: (pids) => pids.length === 1 && pids[0] !== previous }),
    Effect.timeout("5 seconds"),
    Effect.map((pids) => {
      const pid = pids[0]
      assert(pid !== undefined)
      return pid
    })
  )

const park = (store: JobStore.Service, target = queue) =>
  Effect.gen(function*() {
    const claim = yield* store.claim({ queue: target, names: ["Notification"], token: "waiter", lockDurationMs: 5_000 })
    assert(claim._tag === "Empty")
    return yield* Effect.forkChild(store.awaitWake([target], claim.wakeToken))
  })

const withListen = (client: PgClient.PgClient, listen: PgClient.PgClient["listen"]): PgClient.PgClient =>
  Object.assign(function(...args: Parameters<PgClient.PgClient>) {
    return client(...args)
  }, client, { listen })

const unavailableListen = new SqlError({
  reason: new UnknownError({ message: "LISTEN unavailable", cause: new Error("notification connection unavailable") })
})

if (!available) {
  describe("Postgres notifications", () => {
    it.skip(`skipped: no Postgres at ${pgUrl}`, () => {})
  })
} else {
  describe("Postgres notifications", () => {
    it.live("retries a failed acquisition and closes each attempt's scope", () =>
      Effect.gen(function*() {
        const client = yield* PgClient.PgClient
        let attempts = 0
        let released = 0
        const recovering = withListen(client, (channel) => Effect.gen(function*() {
          attempts++
          yield* Effect.addFinalizer(() => Effect.sync(() => { released++ }))
          if (attempts === 1) return yield* unavailableListen
          return yield* client.listen(channel)
        }))
        const scope = yield* Scope.fork(yield* Scope.Scope)
        const { store, names } = yield* Scope.provide(
          freshStoreEffect.pipe(Effect.provideService(PgClient.PgClient, recovering)), scope
        )
        const channel = `effect_mq_wake_${names.jobs}`
        yield* awaitListener(client, channel)
        expect(attempts).toBe(2)
        expect(released).toBe(1)
        const waiter = yield* park(store)
        yield* client.notify(channel, queue)
        yield* Fiber.join(waiter).pipe(Effect.timeout("2 seconds"))
        yield* Scope.close(scope, Exit.void).pipe(Effect.timeout("2 seconds"))
        expect(released).toBe(2)
        expect(yield* listenerPids(client, channel)).toHaveLength(0)
      }).pipe(Effect.scoped, Effect.provide(pgClientLive())))

    it.live("routes queue payloads and broadcasts without waking unrelated queues", () =>
      Effect.gen(function*() {
        const client = yield* PgClient.PgClient
        const { names, store } = yield* freshStoreEffect
        const channel = `effect_mq_wake_${names.jobs}`
        yield* awaitListener(client, channel)
        const first = yield* park(store)
        const other = yield* park(store, JobStore.QueueName("other"))
        yield* client.notify(channel, queue)
        yield* Fiber.join(first).pipe(Effect.timeout("2 seconds"))
        expect(other.pollUnsafe()).toBeUndefined()
        yield* client.notify(channel, "*")
        yield* Fiber.join(other).pipe(Effect.timeout("2 seconds"))
      }).pipe(Effect.scoped, Effect.provide(pgClientLive())))

    it.live("releases failed listeners and receives notifications after repeated reconnects", () =>
      Effect.gen(function*() {
        const client = yield* PgClient.PgClient
        const { names, store } = yield* freshStoreEffect
        const channel = `effect_mq_wake_${names.jobs}`
        let pid = yield* awaitListener(client, channel)
        for (let attempt = 0; attempt < 3; attempt++) {
          const waiter = yield* park(store)
          const killed = yield* client.unsafe<{ terminated: boolean }>(
            "SELECT pg_terminate_backend($1) AS terminated", [pid]
          )
          expect(killed[0]?.terminated).toBe(true)
          pid = yield* awaitListener(client, channel, pid)
          yield* client.notify(channel, queue)
          yield* Fiber.join(waiter).pipe(Effect.timeout("2 seconds"))
        }
      }).pipe(Effect.scoped, Effect.provide(pgClientLive())), 15_000)

    it.live("closing a store scope unregisters LISTEN and releases its connection", () =>
      Effect.gen(function*() {
        const client = yield* PgClient.PgClient
        const { store: _store, ...tables } = yield* freshStoreEffect
        const channel = `effect_mq_wake_${tables.names.jobs}`
        yield* awaitListener(client, channel)
        for (let attempt = 0; attempt < 5; attempt++) {
          const scope = yield* Scope.fork(yield* Scope.Scope)
          yield* Scope.provide(DrizzleJobStore.make(tables), scope)
          yield* listenerPids(client, channel).pipe(
            Effect.repeat({ schedule: Schedule.spaced("20 millis"), until: (pids) => pids.length === 2 }),
            Effect.timeout("2 seconds")
          )
          yield* Scope.close(scope, Exit.void).pipe(Effect.timeout("2 seconds"))
          expect(yield* listenerPids(client, channel)).toHaveLength(1)
        }
      }).pipe(Effect.scoped, Effect.provide(pgClientLive())))

    it.live("a worker polls durable jobs while LISTEN acquisition keeps failing", () =>
      Effect.gen(function*() {
        const client = yield* PgClient.PgClient
        let attempts = 0
        const unavailable = withListen(client, () => Effect.suspend(() => {
          attempts++
          return Effect.fail(unavailableListen)
        }))
        const { store, ...tables } = yield* freshStoreEffect.pipe(Effect.provideService(PgClient.PgClient, unavailable))
        const producer = yield* DrizzleJobStore.make(tables)
        const parked = yield* Deferred.make<void>()
        const completed = yield* Deferred.make<number>()
        const observed: JobStore.Service = {
          ...store,
          awaitWake: (queues, token) => Deferred.succeed(parked, undefined).pipe(Effect.andThen(store.awaitWake(queues, token)))
        }
        const Poll = Job.make("PollWithoutListen", { payload: { n: Schema.Number } })
        const handlers = Poll.toLayer(({ n }) => Deferred.succeed(completed, n))
        yield* Effect.gen(function*() {
          yield* Deferred.await(parked).pipe(Effect.timeout("2 seconds"))
          yield* Poll.enqueue({ n: 42 }).pipe(Effect.provideService(JobStore.JobStore, producer))
          expect(yield* Deferred.await(completed).pipe(Effect.timeout("3 seconds"))).toBe(42)
          expect(attempts).toBeGreaterThan(0)
        }).pipe(Effect.provide(handlers.pipe(
          Layer.provideMerge(Worker.layer({ pollInterval: "100 millis" })),
          Layer.provideMerge(Layer.succeed(JobStore.JobStore, observed))
        )))
      }).pipe(Effect.scoped, Effect.provide(pgClientLive())))

    it.live("empty claim names preserve jobs for a later matching claim", () =>
      Effect.gen(function*() {
        const { store } = yield* freshStoreEffect
        const Pending = Job.make("Notification", { payload: { n: Schema.Number } })
        const id = yield* Pending.enqueue({ n: 1 }).pipe(Effect.provideService(JobStore.JobStore, store))
        const empty = yield* store.claim({ queue, names: [], token: "empty", lockDurationMs: 5_000 })
        expect(empty._tag).toBe("Empty")
        expect(Option.getOrThrow(yield* store.getJob(id)).state).toBe("waiting")
        const claimed = yield* store.claim({ queue, names: ["Notification"], token: "matching", lockDurationMs: 5_000 })
        assert(claimed._tag === "Claimed")
        expect(claimed.job.id).toBe(id)
      }).pipe(Effect.scoped, Effect.provide(pgClientLive())))
  })
}
