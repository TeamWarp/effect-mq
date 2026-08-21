import { describe, expect, it } from "@effect/vitest"
import { Effect, Layer, Metric, Schema } from "effect"
import { TestClock } from "effect/testing"
import { Job, JobStore, MemoryJobStore, Metrics, Worker } from "../src/index.ts"

const { QueueName } = JobStore

class Boom extends Schema.TaggedError<Boom>()("Boom", { reason: Schema.String }) {}

/** Let all currently runnable fibers make progress. */
const settle = Effect.gen(function*() {
  for (let i = 0; i < 10; i++) {
    yield* Effect.yieldNow
  }
})

// The metric registry is process-global, so every test isolates its series
// with a unique job name + queue and reads values through those tags.

describe("Metrics", () => {
  it.effect("a completed run emits enqueue, claim, wait, run, and duration metrics", () =>
    Effect.gen(function*() {
      const queue = "mq-metrics-happy"
      class Happy extends Job.make("MetricsHappy", { payload: {}, queue }) {}
      const handlers = Happy.toLayer(() => Effect.sleep("250 millis"))

      yield* Effect.gen(function*() {
        yield* Happy.enqueue({})
        yield* settle
        yield* TestClock.adjust("250 millis")
        yield* settle
      }).pipe(
        Effect.provide(
          handlers.pipe(
            Layer.provideMerge(Worker.layer()),
            Layer.provideMerge(MemoryJobStore.layer)
          )
        )
      )

      const enqueued = yield* Metric.value(Metrics.jobsEnqueued.pipe(
        Metric.withAttributes({ name: "MetricsHappy", queue, duplicate: "false" })
      ))
      expect(enqueued.count).toBe(1)

      const claimed = yield* Metric.value(Metrics.claims.pipe(
        Metric.withAttributes({ queue, result: "claimed" })
      ))
      expect(claimed.count).toBe(1)

      const waited = yield* Metric.value(Metrics.jobWaitDuration.pipe(
        Metric.withAttributes({ name: "MetricsHappy", queue })
      ))
      expect(waited.count).toBe(1)

      const runs = yield* Metric.value(Metrics.jobRuns.pipe(
        Metric.withAttributes({ name: "MetricsHappy", queue, outcome: "completed" })
      ))
      expect(runs.count).toBe(1)

      // The handler slept 250 virtual ms — the duration histogram saw it.
      const duration = yield* Metric.value(Metrics.jobRunDuration.pipe(
        Metric.withAttributes({ name: "MetricsHappy", queue, outcome: "completed" })
      ))
      expect(duration.count).toBe(1)
      expect(duration.sum).toBe(250)
    }))

  it.effect("retried and failed outcomes tag separately; duplicates tag the enqueue counter", () =>
    Effect.gen(function*() {
      const queue = "mq-metrics-flaky"
      class Flaky extends Job.make("MetricsFlaky", {
        payload: {},
        error: Boom,
        idempotencyKey: () => "only-one",
        queue,
        defaults: { attempts: 2 }
      }) {}
      const handlers = Flaky.toLayer(() => Effect.fail(new Boom({ reason: "nope" })))

      yield* Effect.gen(function*() {
        yield* Flaky.enqueue({})
        yield* Flaky.enqueue({}) // idempotency-key duplicate
        yield* settle
      }).pipe(
        Effect.provide(
          handlers.pipe(
            Layer.provideMerge(Worker.layer()),
            Layer.provideMerge(MemoryJobStore.layer)
          )
        )
      )

      const duplicates = yield* Metric.value(Metrics.jobsEnqueued.pipe(
        Metric.withAttributes({ name: "MetricsFlaky", queue, duplicate: "true" })
      ))
      expect(duplicates.count).toBe(1)

      const retried = yield* Metric.value(Metrics.jobRuns.pipe(
        Metric.withAttributes({ name: "MetricsFlaky", queue, outcome: "retried" })
      ))
      expect(retried.count).toBe(1)
      const failed = yield* Metric.value(Metrics.jobRuns.pipe(
        Metric.withAttributes({ name: "MetricsFlaky", queue, outcome: "failed" })
      ))
      expect(failed.count).toBe(1)
    }))

  it.effect("queueMetricsInterval samples store.counts into the depth gauge", () =>
    Effect.gen(function*() {
      const queue = "mq-metrics-depth"
      class Deep extends Job.make("MetricsDeep", { payload: {}, queue }) {}
      const handlers = Deep.toLayer(() => Effect.never)

      yield* Effect.gen(function*() {
        yield* Deep.enqueue({})
        yield* Deep.enqueue({}, { delay: "1 hour" })
        yield* settle
        yield* TestClock.adjust("5 seconds")
        yield* settle

        const active = yield* Metric.value(Metrics.queueDepth.pipe(
          Metric.withAttributes({ queue, state: "active" })
        ))
        expect(active.value).toBe(1)
        const delayed = yield* Metric.value(Metrics.queueDepth.pipe(
          Metric.withAttributes({ queue, state: "delayed" })
        ))
        expect(delayed.value).toBe(1)

        // In-flight gauge tracks the running handler too.
        const busy = yield* Metric.value(Metrics.jobsInFlight.pipe(
          Metric.withAttributes({ queue: QueueName(queue) })
        ))
        expect(busy.value).toBe(1)
      }).pipe(
        Effect.provide(
          handlers.pipe(
            Layer.provideMerge(Worker.layer({ queueMetricsInterval: "5 seconds" })),
            Layer.provideMerge(MemoryJobStore.layer)
          )
        )
      )
    }))

  it.effect("cancel interrupts and stall recoveries are counted", () =>
    Effect.gen(function*() {
      const queue = "mq-metrics-admin"
      class Stuck extends Job.make("MetricsStuck", { payload: {}, queue }) {}
      const handlers = Stuck.toLayer(() => Effect.never)
      const store = yield* MemoryJobStore.make
      const storeLayer = Layer.succeed(JobStore.JobStore, store)

      const before = yield* Metric.value(Metrics.cancelInterrupts)
      yield* Effect.gen(function*() {
        const id = yield* Stuck.enqueue({})
        yield* settle
        yield* Stuck.cancel(id)
        yield* TestClock.adjust("1 second")
        yield* settle
      }).pipe(
        Effect.provide(
          handlers.pipe(
            Layer.provideMerge(Worker.layer({ lockRenewInterval: "1 second" })),
            Layer.provideMerge(storeLayer)
          )
        )
      )
      const after = yield* Metric.value(Metrics.cancelInterrupts)
      expect(after.count - before.count).toBe(1)

      // A cancelled handler with a slow finalizer keeps getting re-reported
      // by the store every heartbeat until the ack — count it ONCE.
      class SlowExit extends Job.make("MetricsSlowExit", { payload: {}, queue }) {}
      const slowHandlers = SlowExit.toLayer(() =>
        Effect.never.pipe(Effect.onInterrupt(() => Effect.sleep("3 seconds")))
      )
      const slowBefore = yield* Metric.value(Metrics.cancelInterrupts)
      yield* Effect.gen(function*() {
        const id = yield* SlowExit.enqueue({})
        yield* settle
        yield* SlowExit.cancel(id)
        for (let i = 0; i < 4; i++) {
          yield* TestClock.adjust("1 second")
        }
        yield* settle
      }).pipe(
        Effect.provide(
          slowHandlers.pipe(
            Layer.provideMerge(Worker.layer({ lockRenewInterval: "1 second" })),
            Layer.provideMerge(Layer.succeed(JobStore.JobStore, yield* MemoryJobStore.make))
          )
        )
      )
      const slowAfter = yield* Metric.value(Metrics.cancelInterrupts)
      expect(slowAfter.count - slowBefore.count).toBe(1)

      const cancelled = yield* Metric.value(Metrics.jobRuns.pipe(
        Metric.withAttributes({ name: "MetricsStuck", queue, outcome: "cancelled" })
      ))
      expect(cancelled.count).toBe(1)
    }))
})
