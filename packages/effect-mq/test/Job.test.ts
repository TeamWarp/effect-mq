import { assert, describe, expect, it } from "@effect/vitest"
import { DateTime, Effect, Exit, Option, Redacted, Schema } from "effect"
import { Job, JobStore, MemoryJobStore } from "../src/index.ts"

const Resize = Job.make("Resize", {
  payload: { file: Schema.String, width: Schema.Number },
  success: Schema.String
})

// The class binding name differs from the job tag on purpose: job identity
// must come from `_tag`, not the (shadowable) `Function.name`.
class Notify extends Job.make("NotifyUser", {
  payload: { userId: Schema.String },
  idempotencyKey: ({ userId }) => userId,
  queue: "notifications",
  defaults: {
    attempts: 4,
    priority: 2,
    delay: "3 seconds",
    backoff: { type: "exponential", delay: 500, factor: 3 }
  }
}) {}

describe("Job", () => {
  it("make applies defaults", () => {
    expect(Resize._tag).toBe("Resize")
    expect(Resize.queue).toBe("default")
    expect(Resize.defaults).toEqual({
      delayMs: 0,
      priority: 0,
      attempts: 1,
      backoff: undefined
    })
  })

  it("make supports class extension and rich defaults", () => {
    // Class extension shadows Function.name ("Notify") — the job identity is
    // the declared tag.
    expect(Notify._tag).toBe("NotifyUser")
    expect(Notify.queue).toBe("notifications")
    expect(Notify.defaults).toEqual({
      delayMs: 3000,
      priority: 2,
      attempts: 4,
      backoff: { _tag: "exponential", delayMs: 500, factor: 3 }
    })
  })

  it.effect("enqueue stores the encoded payload with merged options", () =>
    Effect.gen(function*() {
      const store = yield* JobStore.JobStore
      const id = yield* Resize.enqueue(
        { file: "a.png", width: 100 },
        { priority: 7, attempts: 2, delay: "1 second" }
      )

      const record = yield* store.getJob(id)
      assert(Option.isSome(record))
      expect(record.value.name).toBe("Resize")
      expect(record.value.queue).toBe("default")
      expect(record.value.payload).toEqual({ file: "a.png", width: 100 })
      expect(record.value.priority).toBe(7)
      expect(record.value.attemptsMax).toBe(2)
      expect(record.value.state).toBe("delayed")
    }).pipe(Effect.provide(MemoryJobStore.layer)))

  it.effect("idempotencyKey produces a deterministic id and deduplicates", () =>
    Effect.gen(function*() {
      const store = yield* JobStore.JobStore
      const first = yield* Notify.enqueue({ userId: "u-1" })
      const second = yield* Notify.enqueue({ userId: "u-1" })
      const other = yield* Notify.enqueue({ userId: "u-2" })

      expect(first).toBe("NotifyUser/u-1")
      expect(second).toBe(first)
      expect(other).toBe("NotifyUser/u-2")
      expect((yield* store.counts()).delayed).toBe(2)
    }).pipe(Effect.provide(MemoryJobStore.layer)))

  it.effect("explicit jobId overrides the idempotency key", () =>
    Effect.gen(function*() {
      const id = yield* Notify.enqueue({ userId: "u-1" }, { jobId: "custom" })
      expect(id).toBe("custom")
    }).pipe(Effect.provide(MemoryJobStore.layer)))

  it.effect("enqueue with an invalid payload dies", () =>
    Effect.gen(function*() {
      // Malformed boundary input (a string where a number belongs), parsed
      // rather than asserted, to prove the schema rejects it at runtime.
      const exit = yield* Effect.exit(
        Resize.enqueue({ file: "a.png", width: JSON.parse('"wide"') })
      )
      assert(Exit.isFailure(exit))
      expect(Exit.hasDies(exit)).toBe(true)
    }).pipe(Effect.provide(MemoryJobStore.layer)))

  it.effect("poll returns none for unknown ids and pending status before running", () =>
    Effect.gen(function*() {
      expect(yield* Resize.poll(JobStore.JobId("nope"))).toEqual(Option.none())

      const id = yield* Resize.enqueue({ file: "a.png", width: 1 })
      const status = yield* Resize.poll(id)
      assert(Option.isSome(status))
      expect(status.value.state).toBe("waiting")
      expect(status.value.attemptsMade).toBe(0)
      expect(Option.isNone(status.value.exit)).toBe(true)
    }).pipe(Effect.provide(MemoryJobStore.layer)))
})

describe("enqueueMany", () => {
  it.effect("batches typed payloads with shared options and positional ids", () =>
    Effect.gen(function*() {
      const store = yield* JobStore.JobStore
      const ids = yield* Resize.enqueueMany(
        [{ file: "a.png", width: 1 }, { file: "b.png", width: 2 }],
        { priority: 3, delay: "1 second" }
      )
      expect(ids).toHaveLength(2)
      expect(new Set(ids).size).toBe(2)
      const first = yield* store.getJob(ids[0] ?? JobStore.JobId(""))
      assert(Option.isSome(first))
      expect(first.value.payload).toEqual({ file: "a.png", width: 1 })
      expect(first.value.priority).toBe(3)
      expect(first.value.state).toBe("delayed")
      expect((yield* store.counts()).delayed).toBe(2)
    }).pipe(Effect.provide(MemoryJobStore.layer)))

  it.effect("derives idempotency keys per item", () =>
    Effect.gen(function*() {
      const store = yield* JobStore.JobStore
      const ids = yield* Notify.enqueueMany([
        { userId: "u-1" },
        { userId: "u-1" },
        { userId: "u-2" }
      ])
      expect(ids).toEqual(["NotifyUser/u-1", "NotifyUser/u-1", "NotifyUser/u-2"])
      // Notify's default 3s delay applies to every item.
      expect((yield* store.counts()).delayed).toBe(2)
    }).pipe(Effect.provide(MemoryJobStore.layer)))

  it.effect("runs the definition's dedupe callback per item", () =>
    Effect.gen(function*() {
      const Grouped = Job.make("GroupedBatch", {
        payload: { group: Schema.String, n: Schema.Number },
        dedupe: ({ group }) => group
      })
      const store = yield* JobStore.JobStore
      const ids = yield* Grouped.enqueueMany([
        { group: "g1", n: 1 },
        { group: "g1", n: 2 },
        { group: "g2", n: 3 }
      ])
      expect(ids[1]).toBe(ids[0])
      expect(ids[2]).not.toBe(ids[0])
      expect((yield* store.counts()).waiting).toBe(2)
    }).pipe(Effect.provide(MemoryJobStore.layer)))

  it.effect("empty input is a no-op returning no ids", () =>
    Effect.gen(function*() {
      expect(yield* Resize.enqueueMany([])).toEqual([])
    }).pipe(Effect.provide(MemoryJobStore.layer)))

  it("per-job and conflicting options are type errors", () => {
    const payloads = [{ file: "a.png", width: 1 }]
    // @ts-expect-error jobId is per-job, not a batch option
    void (() => Resize.enqueueMany(payloads, { jobId: "x" }))
    // @ts-expect-error dedupe is per-job, not a batch option
    void (() => Resize.enqueueMany(payloads, { dedupe: "k" }))
    // @ts-expect-error delay and at stay mutually exclusive in batches
    void (() => Resize.enqueueMany(payloads, { delay: "1 second", at: 0 }))
    expect(true).toBe(true)
  })
})

describe("Job hardening", () => {
  it.effect("class extension persists records under the declared tag, not the class name", () =>
    Effect.gen(function*() {
      const store = yield* JobStore.JobStore
      const id = yield* Notify.enqueue({ userId: "u-9" })
      const record = yield* store.getJob(id)
      assert(Option.isSome(record))
      expect(record.value.name).toBe("NotifyUser")
    }).pipe(Effect.provide(MemoryJobStore.layer)))

  it.effect("methods survive destructuring", () =>
    Effect.gen(function*() {
      const { enqueue, enqueueMany, poll } = Resize
      const id = yield* enqueue({ file: "x.png", width: 2 })
      const status = yield* poll(id)
      assert(Option.isSome(status))
      expect(status.value.state).toBe("waiting")
      const ids = yield* enqueueMany([{ file: "y.png", width: 3 }])
      expect(ids).toHaveLength(1)
    }).pipe(Effect.provide(MemoryJobStore.layer)))

  it("attempts below 1 are clamped in defaults", () => {
    const Clamped = Job.make("Clamped", {
      payload: {},
      defaults: { attempts: 0 }
    })
    expect(Clamped.defaults.attempts).toBe(1)
  })

  it.effect("attempts below 1 are clamped per enqueue", () =>
    Effect.gen(function*() {
      const store = yield* JobStore.JobStore
      const id = yield* Resize.enqueue({ file: "a.png", width: 1 }, { attempts: -3 })
      const record = yield* store.getJob(id)
      assert(Option.isSome(record))
      expect(record.value.attemptsMax).toBe(1)
    }).pipe(Effect.provide(MemoryJobStore.layer)))

  it.effect("stored payloads are JSON-safe even for declaration schemas", () =>
    Effect.gen(function*() {
      const Timed = Job.make("Timed", {
        payload: { at: Schema.DateTimeUtc, note: Schema.String }
      })
      const store = yield* JobStore.JobStore
      const id = yield* Timed.enqueue({
        at: DateTime.makeUnsafe("2026-08-20T12:00:00Z"),
        note: "hi"
      })
      const record = yield* store.getJob(id)
      assert(Option.isSome(record))
      // Survives a real serialization boundary (what a Redis driver would do)...
      const roundTripped: unknown = JSON.parse(JSON.stringify(record.value.payload))
      expect(roundTripped).toEqual(record.value.payload)
      // ...and still decodes back to the domain type.
      const decoded = yield* Schema.decodeUnknownEffect(Timed.payloadJsonSchema)(roundTripped)
      expect(DateTime.isDateTime(decoded.at)).toBe(true)
      expect(decoded.note).toBe("hi")
    }).pipe(Effect.provide(MemoryJobStore.layer)))
})

describe("redaction", () => {
  it.effect("default Redacted payload fields round-trip (the raw value IS persisted)", () =>
    Effect.gen(function*() {
      const Notify2 = Job.make("RedactedRoundTrip", {
        payload: { apiKey: Schema.Redacted(Schema.String), to: Schema.String }
      })
      const store = yield* JobStore.JobStore
      const id = yield* Notify2.enqueue({
        apiKey: Redacted.make("secret-123"),
        to: "a@b.c"
      })
      // Effect's default Schema.Redacted has transport semantics: the encoded
      // form contains the underlying value so consumers can decode it back.
      // The redaction protects logs/inspection, NOT the database at rest.
      const record = yield* store.getJob(id)
      assert(Option.isSome(record))
      expect(record.value.payload).toEqual({ apiKey: "secret-123", to: "a@b.c" })

      // ...and the worker-side decode re-wraps it, so handlers receive a
      // Redacted value (which stringifies as <redacted> in logs).
      const decoded = yield* Schema.decodeUnknownEffect(Notify2.payloadJsonSchema)(
        record.value.payload
      )
      expect(Redacted.isRedacted(decoded.apiKey)).toBe(true)
      expect(Redacted.value(decoded.apiKey)).toBe("secret-123")
      expect(String(decoded.apiKey)).not.toContain("secret-123")
    }).pipe(Effect.provide(MemoryJobStore.layer)))

  it.effect("disallowJsonEncode Redacted fields refuse to be persisted at all", () =>
    Effect.gen(function*() {
      const NeverStored = Job.make("RedactedForbidden", {
        payload: {
          apiKey: Schema.Redacted(Schema.String, { disallowJsonEncode: true })
        }
      })
      const store = yield* JobStore.JobStore
      const exit = yield* Effect.exit(
        NeverStored.enqueue({ apiKey: Redacted.make("secret-123") })
      )
      // Enqueue dies (a programming error: this payload is marked
      // never-serializable) and nothing reaches the store.
      assert(Exit.isFailure(exit))
      expect(Exit.hasDies(exit)).toBe(true)
      expect(JSON.stringify(exit.cause)).toContain("Cannot serialize Redacted")
      expect(JSON.stringify(exit.cause)).not.toContain("secret-123")
      expect((yield* store.counts()).waiting).toBe(0)
    }).pipe(Effect.provide(MemoryJobStore.layer)))
})
