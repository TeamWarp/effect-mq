/**
 * A test harness for asserting what your services enqueue, without running
 * a worker.
 *
 * Provide `TestJobStore.layer` in a unit test and jobs enqueued by the code
 * under test simply accumulate in `waiting`/`delayed` (nothing claims them).
 * `enqueuedOf` returns them with payloads **decoded through the job's own
 * schema** — you assert against the typed values your service produced, not
 * the encoded JSON the store persists:
 *
 * ```ts
 * import { TestJobStore } from "effect-mq/testing"
 *
 * it.effect("signup enqueues a welcome email", () =>
 *   Effect.gen(function*() {
 *     yield* SignupService.register({ email: "ada@example.com" })
 *
 *     const emails = yield* TestJobStore.enqueuedOf(SendEmail)
 *     expect(emails).toHaveLength(1)
 *     expect(emails[0]?.payload.to).toBe("ada@example.com")
 *     expect(emails[0]?.state).toBe("waiting")
 *   }).pipe(Effect.provide(TestJobStore.layer)))
 * ```
 *
 * Jobs bound to named stores use `TestJobStore.layerFor(Durable)` instead.
 * The raw `JobStore` service is also exposed (as `.store`) for advanced
 * scenarios — simulating claims/acks, reading `counts()`, and so on.
 *
 * @since 0.3.2
 */
import * as JobStore from "../JobStore.ts"
import * as MemoryJobStore from "../MemoryJobStore.ts"
import { Context, Effect, Layer, Schema } from "effect"

/**
 * The minimal structural view of a `Job.make` class that `enqueuedOf`
 * needs: its tag and its JSON payload codec.
 *
 * @since 0.3.2
 */
export interface AnyJobDefinition {
  readonly _tag: string
  readonly payloadJsonSchema: Schema.Top & { readonly DecodingServices: never }
}

/**
 * A stored job with its payload decoded back to the definition's payload
 * type.
 *
 * @since 0.3.2
 */
export interface EnqueuedJob<Payload> extends Omit<JobStore.JobRecord, "payload"> {
  readonly payload: Payload
}

const drainList = (store: JobStore.Service, name?: string) =>
  Effect.gen(function*() {
    const all: Array<JobStore.JobRecord> = []
    let cursor: string | undefined = undefined
    while (true) {
      const page: JobStore.ListResult = yield* store.list({ name, cursor, limit: 200 }).pipe(Effect.orDie)
      all.push(...page.items)
      if (page.cursor === undefined) break
      cursor = page.cursor
    }
    // list() is newest-first; flip to oldest-first for natural reading.
    // Ties (same-instant enqueues) order by id, not submission order.
    return all.toReversed()
  })

const makeApi = (store: JobStore.Service) => ({
  store,
  enqueued: (name?: string) => drainList(store, name),
  enqueuedOf: <J extends AnyJobDefinition>(job: J) =>
    drainList(store, job._tag).pipe(
      Effect.flatMap(Effect.forEach((record) =>
        Schema.decodeUnknownEffect(job.payloadJsonSchema)(record.payload).pipe(
          Effect.orDie,
          Effect.map((payload): EnqueuedJob<J["payloadJsonSchema"]["Type"]> => ({ ...record, payload }))
        )
      ))
    )
})

/**
 * Inspection API over the test store. `enqueued(name?)` returns raw records
 * oldest-first; `enqueuedOf(JobClass)` additionally decodes payloads through
 * the definition's schema.
 *
 * @since 0.3.2
 */
export class TestJobStore extends Context.Service<TestJobStore, ReturnType<typeof makeApi>>()(
  "effect-mq/testing/TestJobStore"
) {}

/**
 * A fresh in-memory store provided as BOTH the default `JobStore` (for the
 * code under test) and the `TestJobStore` inspection service (for the
 * assertions).
 *
 * @since 0.3.2
 */
export const layer: Layer.Layer<JobStore.JobStore | TestJobStore> = Layer.effectContext(
  Effect.map(MemoryJobStore.makeWith(), (store) =>
    Context.make(JobStore.JobStore, store).pipe(
      Context.add(TestJobStore, TestJobStore.of(makeApi(store)))
    ))
)

/**
 * Like `layer`, for jobs bound to a `JobStore.named(...)` key.
 *
 * @since 0.3.2
 */
export const layerFor = <Id>(
  store: Context.Key<Id, JobStore.Service>
): Layer.Layer<Id | TestJobStore> =>
  Layer.effectContext(
    Effect.map(MemoryJobStore.makeWith(), (memory) =>
      Context.make(store, memory).pipe(
        Context.add(TestJobStore, TestJobStore.of(makeApi(memory)))
      ))
  )

/**
 * Convenience accessors so tests don't have to `yield* TestJobStore` first.
 *
 * @since 0.3.2
 */
export const enqueuedOf = <J extends AnyJobDefinition>(
  job: J
): Effect.Effect<Array<EnqueuedJob<J["payloadJsonSchema"]["Type"]>>, never, TestJobStore> =>
  Effect.flatMap(TestJobStore, (api) => api.enqueuedOf(job))

/**
 * Raw records (optionally filtered by job name), oldest-first.
 *
 * @since 0.3.2
 */
export const enqueued = (
  name?: string
): Effect.Effect<Array<JobStore.JobRecord>, never, TestJobStore> =>
  Effect.flatMap(TestJobStore, (api) => api.enqueued(name))
