# Testing your app

Most of what you want to test is producer-side: *did my service enqueue the right job with the right payload?* You should not need a worker, a database, or sleeps for that. `effect-mq/testing` ships `TestJobStore` for those assertions, and `MemoryJobStore` covers the integration-style tests that do run a worker.

## Assert what your services enqueue

`TestJobStore.layer` provides a fresh in-memory store as **both** the default `JobStore` (for the code under test) and the `TestJobStore` inspection service (for your assertions). Nothing claims jobs, so they accumulate in `waiting`/`delayed`:

```ts
import { TestJobStore } from "effect-mq/testing"
import { Effect } from "effect"

it.effect("signup enqueues a welcome email", () =>
  Effect.gen(function*() {
    yield* SignupService.register({ email: "ada@example.com" })

    const emails = yield* TestJobStore.enqueuedOf(SendEmail)
    expect(emails).toHaveLength(1)
    expect(emails[0]?.payload.to).toBe("ada@example.com")
    expect(emails[0]?.state).toBe("waiting")
  }).pipe(Effect.provide(TestJobStore.layer)))
```

Each `Effect.provide(TestJobStore.layer)` builds an isolated store, so tests never share state.

## Decoded, typed payloads

`enqueuedOf(JobClass)` filters by the job's tag and decodes each payload back through the definition's own schema. You assert against the typed values your service produced: `Redacted` and `DateTime` fields come back as real instances, decoded from the JSON the store persists:

```ts
class SendEmail extends Job.make("SendEmail", {
  payload: {
    to: Schema.String,
    apiKey: Schema.Redacted(Schema.String),
    at: Schema.DateTimeUtc
  }
}) {}

const emails = yield* TestJobStore.enqueuedOf(SendEmail)
expect(Redacted.value(emails[0]!.payload.apiKey)).toBe("secret")
expect(DateTime.toEpochMillis(emails[0]!.payload.at)).toBe(0)
```

Results come back **oldest-first**, the natural reading order for "it enqueued A, then B". Same-instant enqueues tie-break by id, so when many jobs land in one tick, assert on the set rather than the sequence.

Beyond the payload, each record surfaces its scheduling detail: `state`, `priority`, `runAt`, `metadata`, and `dedupeKey`, enough to assert that a job was delayed, prioritized, or [deduplicated](/guide/deduplication).

`enqueued(name?)` covers tests where you don't have (or don't want) the job class: it returns the raw `JobRecord`s with encoded payloads, same ordering.

## Named stores

Jobs bound to a `JobStore.named(...)` key don't use the default `JobStore`, so `TestJobStore.layer` won't satisfy them. Use `layerFor` with the same key:

```ts
const Durable = JobStore.named("durable")

class Critical extends Job.make("Critical", {
  payload: { id: Schema.String },
  store: Durable
}) {}

it.effect("billing enqueues to the durable store", () =>
  Effect.gen(function*() {
    yield* Critical.enqueue({ id: "x" })
    const jobs = yield* TestJobStore.enqueuedOf(Critical)
    expect(jobs).toHaveLength(1)
  }).pipe(Effect.provide(TestJobStore.layerFor(Durable))))
```

::: warning
Each `layerFor(...)` call builds a fresh store. Provide **one** layer instance around both the enqueue and the assertion; separate instances give the assertion an empty store.
:::

## The raw store escape hatch

The inspection API also exposes the underlying `JobStore` service as `.store`, for scenarios richer than "what got enqueued", such as simulating claims and acks or reading `counts()`:

```ts
const api = yield* TestJobStore.TestJobStore
expect((yield* api.store.counts()).waiting).toBe(1)
```

`TestJobStore`'s API is plain Effect; nothing couples it to a test runner. The `effect-mq/testing` entrypoint also ships the driver conformance suite, which imports `@effect/vitest`, so that (otherwise optional) peer must be installed for the import to resolve.

## Integration tests: a real worker under TestClock

For end-to-end behavior (retries, delays, timeouts), run an actual worker against `MemoryJobStore.layer`. Everything in effect-mq takes time from the Effect `Clock`, so under `@effect/vitest`'s `it.effect` the `TestClock` drives the whole pipeline: a one-hour delay is a virtual-time adjustment with zero real waiting.

```ts
import { expect, it } from "@effect/vitest"
import { Effect, Layer, Schema } from "effect"
import { TestClock } from "effect/testing"
import { Job, MemoryJobStore, Worker } from "effect-mq"

/** Let all currently runnable fibers make progress. */
const settle = Effect.gen(function*() {
  for (let i = 0; i < 10; i++) yield* Effect.yieldNow
})

it.effect("delayed jobs only run once the delay elapses", () =>
  Effect.gen(function*() {
    const Later = Job.make("Later", { payload: { n: Schema.Number } })
    let ran = 0
    const handlers = Later.toLayer(() => Effect.sync(() => void ran++))

    yield* Effect.gen(function*() {
      yield* Later.enqueue({ n: 1 }, { delay: "5 seconds" })
      yield* settle
      yield* TestClock.adjust("4 seconds")
      yield* settle
      expect(ran).toBe(0)                 // still delayed

      yield* TestClock.adjust("1 second")
      yield* settle
      expect(ran).toBe(1)                 // claimed and run, zero real waiting
    }).pipe(Effect.provide(
      handlers.pipe(
        Layer.provideMerge(Worker.layer()),
        Layer.provideMerge(MemoryJobStore.layer)
      )
    ))
  }))
```

The `settle` helper yields the scheduler so worker fibers can react between clock adjustments, the same pattern effect-mq's own test suite uses.

This also works against real storage: the Postgres and Redis drivers take all time as bind parameters from the Clock, so the same TestClock-driven tests run against a real database. The [conformance suite](/storage/writing-a-driver) enforces that property on every driver.

## Where to next

- [Enqueueing](/guide/enqueueing): the options (`delay`, `priority`, `dedupe`) your assertions inspect.
- [Workers & handlers](/guide/workers): what the real worker does with those accumulated jobs.
- [Writing a driver](/storage/writing-a-driver): the conformance suite behind TestClock-on-real-storage.
