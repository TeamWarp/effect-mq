import { describe, expect, it } from "@effect/vitest"
import { DateTime, Effect, Redacted, Schema } from "effect"
import { Job, JobStore } from "../src/index.ts"
import { TestJobStore } from "../src/testing/index.ts"

class SendEmail extends Job.make("SendEmail", {
  payload: {
    to: Schema.String,
    apiKey: Schema.Redacted(Schema.String),
    at: Schema.DateTimeUtc
  }
}) {}

class OtherJob extends Job.make("OtherJob", { payload: { n: Schema.Number } }) {}

describe("TestJobStore", () => {
  it.effect("asserts enqueued jobs with DECODED, typed payloads", () =>
    Effect.gen(function*() {
      // The "service under test": some business code that enqueues.
      yield* SendEmail.enqueue({
        to: "ada@example.com",
        apiKey: Redacted.make("secret"),
        at: DateTime.makeUnsafe(0)
      })
      yield* OtherJob.enqueue({ n: 1 })

      const emails = yield* TestJobStore.enqueuedOf(SendEmail)
      expect(emails).toHaveLength(1)
      const email = emails[0]
      expect(email?.payload.to).toBe("ada@example.com")
      // Schema types survive the round trip: Redacted and DateTime come back
      // as real instances, not raw JSON — the whole point vs list().
      expect(Redacted.value(email?.payload.apiKey ?? Redacted.make(""))).toBe("secret")
      expect(DateTime.toEpochMillis(email?.payload.at ?? DateTime.makeUnsafe(1))).toBe(0)
      expect(email?.state).toBe("waiting")

      // Filtering by definition never leaks other jobs.
      const others = yield* TestJobStore.enqueuedOf(OtherJob)
      expect(others).toHaveLength(1)
      expect(others[0]?.payload.n).toBe(1)
    }).pipe(Effect.provide(TestJobStore.layer)))

  it.effect("preserves enqueue order and surfaces scheduling details", () =>
    Effect.gen(function*() {
      yield* OtherJob.enqueue({ n: 1 })
      yield* OtherJob.enqueue({ n: 2 }, { delay: "1 hour", priority: 5 })

      const jobs = yield* TestJobStore.enqueuedOf(OtherJob)
      expect(jobs.map((job) => job.payload.n)).toEqual([1, 2])
      expect(jobs[1]?.state).toBe("delayed")
      expect(jobs[1]?.priority).toBe(5)

      // The raw store is exposed for anything richer.
      const api = yield* TestJobStore.TestJobStore
      expect((yield* api.store.counts()).waiting).toBe(1)
    }).pipe(Effect.provide(TestJobStore.layer)))

  it.effect("layerFor covers jobs bound to named stores", () =>
    Effect.gen(function*() {
      const Durable = JobStore.named("test-durable")
      class Critical extends Job.make("Critical", {
        payload: { id: Schema.String },
        store: Durable
      }) {}

      yield* Critical.enqueue({ id: "x" }).pipe(
        Effect.provide(TestJobStore.layerFor(Durable))
      )
      // Note: assertions need the same layer instance as the enqueue —
      // provide once around both in real tests:
      yield* Effect.gen(function*() {
        yield* Critical.enqueue({ id: "y" })
        const jobs = yield* TestJobStore.enqueuedOf(Critical)
        expect(jobs.map((job) => job.payload.id)).toEqual(["y"])
      }).pipe(Effect.provide(TestJobStore.layerFor(Durable)))
    }))

  it.effect("drains past the list page size", () =>
    Effect.gen(function*() {
      for (let i = 0; i < 250; i++) {
        yield* OtherJob.enqueue({ n: i })
      }
      const jobs = yield* TestJobStore.enqueuedOf(OtherJob)
      expect(jobs).toHaveLength(250)
      // Same-instant enqueues tie-break by id, so assert the full set.
      expect(new Set(jobs.map((job) => job.payload.n)).size).toBe(250)
    }).pipe(Effect.provide(TestJobStore.layer)))
})
