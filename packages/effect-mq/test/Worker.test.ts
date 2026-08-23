import { assert, describe, expect, it } from "@effect/vitest"
import { Cause, Context, Deferred, Effect, Exit, Fiber, Layer, Option, Redacted, Schedule, Schema } from "effect"
import { TestClock } from "effect/testing"
import { Job, JobStore, MemoryJobStore, Worker } from "../src/index.ts"

const { QueueName } = JobStore

class SendFailure extends Schema.TaggedError<SendFailure>()("SendFailure", {
  reason: Schema.String
}) {}

/** Let all currently runnable fibers make progress. */
const settle = Effect.gen(function*() {
  for (let i = 0; i < 10; i++) {
    yield* Effect.yieldNow
  }
})

const harness = (
  handlers: Layer.Layer<never, never, Worker.Worker>,
  options?: Worker.WorkerOptions
) =>
  handlers.pipe(
    Layer.provideMerge(Worker.layer(options)),
    Layer.provideMerge(MemoryJobStore.layer)
  )

describe("Worker", () => {
  it.effect("queues a job and runs it in the background", () =>
    Effect.gen(function*() {
      const Greet = Job.make("Greet", {
        payload: { name: Schema.String },
        success: Schema.String
      })
      const seen: Array<Worker.JobContext> = []
      const handlers = Greet.toLayer((payload, context) =>
        Effect.sync(() => {
          seen.push(context)
          return `hello ${payload.name}`
        })
      )

      const result = yield* Effect.gen(function*() {
        const fiber = yield* Effect.forkChild(Greet.execute({ name: "world" }))
        yield* settle
        yield* TestClock.adjust("1 second")
        return yield* Fiber.join(fiber)
      }).pipe(Effect.provide(harness(handlers)))

      expect(result).toBe("hello world")
      expect(seen).toHaveLength(1)
      expect(seen[0]?.attempt).toBe(1)
      expect(seen[0]?.queue).toBe("default")
      expect(seen[0]?.name).toBe("Greet")
    }))

  it.effect("delayed jobs only run once the delay elapses", () =>
    Effect.gen(function*() {
      const Later = Job.make("Later", { payload: { n: Schema.Number } })
      let ran = 0
      const handlers = Later.toLayer(() => Effect.sync(() => void ran++))

      yield* Effect.gen(function*() {
        const id = yield* Later.enqueue({ n: 1 }, { delay: "5 seconds" })
        yield* settle
        yield* TestClock.adjust("4 seconds")
        yield* settle
        expect(ran).toBe(0)
        const before = yield* Later.poll(id)
        assert(Option.isSome(before))
        expect(before.value.state).toBe("delayed")

        yield* TestClock.adjust("1 second")
        yield* settle
        expect(ran).toBe(1)
        const after = yield* Later.poll(id)
        assert(Option.isSome(after))
        expect(after.value.state).toBe("completed")
      }).pipe(Effect.provide(harness(handlers)))
    }))

  it.effect("typed errors round-trip through storage to the caller", () =>
    Effect.gen(function*() {
      const Send = Job.make("Send", {
        payload: { to: Schema.String },
        error: SendFailure
      })
      const handlers = Send.toLayer(({ to }) => new SendFailure({ reason: `no route to ${to}` }))

      const error = yield* Effect.gen(function*() {
        const fiber = yield* Effect.forkChild(Effect.flip(Send.execute({ to: "mars" })))
        yield* settle
        yield* TestClock.adjust("1 second")
        return yield* Fiber.join(fiber)
      }).pipe(Effect.provide(harness(handlers)))

      expect(error._tag).toBe("SendFailure")
      expect(error.reason).toBe("no route to mars")
    }))

  it.effect("retries with backoff until success, tracking attempts", () =>
    Effect.gen(function*() {
      const Flaky = Job.make("Flaky", {
        payload: { id: Schema.String },
        success: Schema.Number,
        error: SendFailure,
        defaults: {
          attempts: 3,
          backoff: { type: "exponential", delay: "1 second", factor: 2 }
        }
      })
      const attempts: Array<number> = []
      const handlers = Flaky.toLayer((_, context) =>
        Effect.suspend(() => {
          attempts.push(context.attempt)
          return context.attempt < 3
            ? new SendFailure({ reason: "flake" })
            : Effect.succeed(context.attempt)
        })
      )

      yield* Effect.gen(function*() {
        const id = yield* Flaky.enqueue({ id: "x" })
        yield* settle
        expect(attempts).toEqual([1])

        // First retry backs off 1s (exponential base), second 2s.
        const afterFirst = yield* Flaky.poll(id)
        assert(Option.isSome(afterFirst))
        expect(afterFirst.value.state).toBe("delayed")

        yield* TestClock.adjust("1 second")
        yield* settle
        expect(attempts).toEqual([1, 2])

        yield* TestClock.adjust("1 second")
        yield* settle
        expect(attempts).toEqual([1, 2])

        yield* TestClock.adjust("1 second")
        yield* settle
        expect(attempts).toEqual([1, 2, 3])

        const final = yield* Flaky.poll(id)
        assert(Option.isSome(final))
        expect(final.value.state).toBe("completed")
        expect(final.value.attemptsMade).toBe(3)
        assert(Option.isSome(final.value.exit))

        // Every run was persisted with its attempt number, and the typed
        // errors decode back from storage.
        const ledger = yield* Flaky.attempts(id)
        expect(ledger.map((entry) => [entry.attempt, entry.outcome])).toEqual([
          [1, "retried"],
          [2, "retried"],
          [3, "completed"]
        ])
        for (const entry of ledger.slice(0, 2)) {
          assert(Option.isSome(entry.exit))
          assert(Exit.isFailure(entry.exit.value))
          const error = Cause.findErrorOption(entry.exit.value.cause)
          assert(Option.isSome(error))
          expect(error.value).toBeInstanceOf(SendFailure)
        }
      }).pipe(Effect.provide(harness(handlers)))
    }))

  it.effect("fails permanently once attempts are exhausted", () =>
    Effect.gen(function*() {
      const Doomed = Job.make("Doomed", {
        payload: { id: Schema.String },
        error: SendFailure,
        defaults: { attempts: 2, backoff: { type: "fixed", delay: "1 second" } }
      })
      const handlers = Doomed.toLayer(() => new SendFailure({ reason: "always" }))

      yield* Effect.gen(function*() {
        const id = yield* Doomed.enqueue({ id: "d" })
        const fiber = yield* Effect.forkChild(Effect.flip(Doomed.awaitResult(id)))
        yield* settle
        yield* TestClock.adjust("1 second")
        yield* settle
        yield* TestClock.adjust("1 second")
        const error = yield* Fiber.join(fiber)
        expect(error._tag).toBe("SendFailure")

        const store = yield* JobStore.JobStore
        const counts = yield* store.counts()
        expect(counts.failed).toBe(1)

        // The terminal exit decodes through poll, and the ledger through attempts.
        const status = yield* Doomed.poll(id)
        assert(Option.isSome(status))
        expect(status.value.state).toBe("failed")
        expect(status.value.attemptsMade).toBe(2)
        assert(Option.isSome(status.value.exit))
        assert(Exit.isFailure(status.value.exit.value))
        const finalError = Cause.findErrorOption(status.value.exit.value.cause)
        assert(Option.isSome(finalError))
        expect(finalError.value).toBeInstanceOf(SendFailure)

        const ledger = yield* Doomed.attempts(id)
        expect(ledger.map((entry) => [entry.attempt, entry.outcome])).toEqual([
          [1, "retried"],
          [2, "failed"]
        ])
      }).pipe(Effect.provide(harness(handlers)))
    }))

  it.effect("runs at most `concurrency` handlers per queue at once", () =>
    Effect.gen(function*() {
      const Work = Job.make("Work", { payload: { n: Schema.Number } })
      const gate = yield* Deferred.make<void>()
      let running = 0
      let maxRunning = 0
      let completed = 0
      const handlers = Work.toLayer(() =>
        Effect.gen(function*() {
          running++
          maxRunning = Math.max(maxRunning, running)
          yield* Deferred.await(gate)
          running--
          completed++
        }), { concurrency: 2 })

      yield* Effect.gen(function*() {
        for (const n of [1, 2, 3, 4]) {
          yield* Work.enqueue({ n })
        }
        yield* settle
        expect(running).toBe(2)
        expect(maxRunning).toBe(2)

        yield* Deferred.succeed(gate, void 0)
        yield* settle
        expect(completed).toBe(4)
        expect(maxRunning).toBe(2)
      }).pipe(Effect.provide(harness(handlers)))
    }))

  it.effect("higher priority jobs run first", () =>
    Effect.gen(function*() {
      const Prio = Job.make("Prio", { payload: { label: Schema.String } })
      const order: Array<string> = []
      const handlers = Prio.toLayer(({ label }) => Effect.sync(() => void order.push(label)))

      yield* Effect.gen(function*() {
        // Enqueue as delayed so both are pending before the worker can claim.
        yield* Prio.enqueue({ label: "low" }, { delay: "1 second" })
        yield* Prio.enqueue({ label: "high" }, { delay: "1 second", priority: 10 })
        yield* settle
        yield* TestClock.adjust("1 second")
        yield* settle
        expect(order).toEqual(["high", "low"])
      }).pipe(Effect.provide(harness(handlers)))
    }))

  it.effect("jobs on queues without registered handlers stay waiting", () =>
    Effect.gen(function*() {
      const Main = Job.make("Main", { payload: {} })
      const Orphan = Job.make("Orphan", { payload: {}, queue: "elsewhere" })
      const handlers = Main.toLayer(() => Effect.void)

      yield* Effect.gen(function*() {
        const id = yield* Orphan.enqueue({})
        yield* settle
        yield* TestClock.adjust("30 seconds")
        yield* settle
        const status = yield* Orphan.poll(id)
        assert(Option.isSome(status))
        expect(status.value.state).toBe("waiting")
      }).pipe(Effect.provide(harness(handlers)))
    }))

  it.effect("shutdown releases in-flight jobs back to waiting without consuming attempts", () =>
    Effect.gen(function*() {
      const store = yield* MemoryJobStore.make
      const storeLayer = Layer.succeed(JobStore.JobStore, store)
      const Stuck = Job.make("Stuck", { payload: {} })
      const started = yield* Deferred.make<void>()
      const handlers = Stuck.toLayer(() =>
        Deferred.succeed(started, void 0).pipe(Effect.andThen(Effect.never))
      )

      yield* Effect.gen(function*() {
        yield* Stuck.enqueue({})
        yield* Deferred.await(started)
      }).pipe(
        Effect.provide(
          handlers.pipe(
            Layer.provideMerge(Worker.layer()),
            Layer.provideMerge(storeLayer)
          )
        )
      )

      // Worker layer is torn down now; the interrupted job must be back to waiting.
      const counts = yield* store.counts()
      expect(counts.active).toBe(0)
      expect(counts.waiting).toBe(1)
      const jobs = yield* store.claim({
        queue: QueueName("default"),
        names: ["Stuck"],
        token: "inspect",
        lockDurationMs: 1000
      })
      assert(jobs._tag === "Claimed")
      expect(jobs.job.attemptsMade).toBe(0)
    }))

  it.effect("recovers stalled jobs from crashed workers and reprocesses them", () =>
    Effect.gen(function*() {
      const store = yield* MemoryJobStore.make
      const storeLayer = Layer.succeed(JobStore.JobStore, store)
      const Stall = Job.make("Stall", { payload: {} })
      let ran = 0
      const done = yield* Deferred.make<void>()
      const handlers = Stall.toLayer(() =>
        Effect.sync(() => void ran++).pipe(
          Effect.andThen(Deferred.succeed(done, void 0))
        )
      )

      yield* Effect.gen(function*() {
        const id = yield* Stall.enqueue({})
        // Simulate a crashed worker: claim with a short lock and never ack.
        const crashed = yield* store.claim({
          queue: QueueName("default"),
          names: ["Stall"],
          token: "crashed-worker",
          lockDurationMs: 1000
        })
        assert(crashed._tag === "Claimed")

        yield* settle
        yield* TestClock.adjust("2 seconds")
        yield* Deferred.await(done)
        expect(ran).toBe(1)

        // The handler runs in a child fiber; give the run loop a beat to ack.
        yield* settle
        const status = yield* store.getJob(id)
        assert(Option.isSome(status))
        expect(status.value.state).toBe("completed")
        expect(status.value.stalledCount).toBe(1)
      }).pipe(
        Effect.provide(
          handlers.pipe(
            Layer.provideMerge(
              Worker.layer({ stalledInterval: "2 seconds" })
            ),
            Layer.provideMerge(storeLayer)
          )
        )
      )
    }))

  it.effect("idempotent enqueues run the handler once", () =>
    Effect.gen(function*() {
      const Once = Job.make("Once", {
        payload: { key: Schema.String },
        idempotencyKey: ({ key }) => key
      })
      let ran = 0
      const handlers = Once.toLayer(() => Effect.sync(() => void ran++))

      yield* Effect.gen(function*() {
        const a = yield* Once.enqueue({ key: "k" })
        const b = yield* Once.enqueue({ key: "k" })
        expect(a).toBe(b)
        yield* settle
        yield* TestClock.adjust("1 second")
        yield* settle
        expect(ran).toBe(1)
      }).pipe(Effect.provide(harness(handlers)))
    }))

  it.effect("two job types on the same queue share its takers", () =>
    Effect.gen(function*() {
      const A = Job.make("A", { payload: {}, queue: "shared" })
      const B = Job.make("B", { payload: {}, queue: "shared" })
      const order: Array<string> = []
      const handlers = Layer.mergeAll(
        A.toLayer(() => Effect.sync(() => void order.push("A"))),
        B.toLayer(() => Effect.sync(() => void order.push("B")))
      )

      yield* Effect.gen(function*() {
        yield* A.enqueue({})
        yield* B.enqueue({})
        yield* settle
        yield* TestClock.adjust("1 second")
        yield* settle
        expect(order.toSorted()).toEqual(["A", "B"])
      }).pipe(Effect.provide(harness(handlers)))
    }))
})

describe("producer/runner separation", () => {
  it.effect("jobs are queued without any Worker layer; a separate worker scope drains them", () =>
    Effect.gen(function*() {
      // A store standing in for shared storage (e.g. Redis) between processes.
      const store = yield* MemoryJobStore.make
      const storeLayer = Layer.succeed(JobStore.JobStore, store)
      const Report = Job.make("Report", {
        payload: { month: Schema.String },
        success: Schema.String
      })

      // "Producer process": only the store is in context — no Worker anywhere.
      const id = yield* Effect.gen(function*() {
        return yield* Report.enqueue({ month: "2026-08" })
      }).pipe(Effect.provide(storeLayer))
      expect((yield* store.counts()).waiting).toBe(1)

      // "Worker process": started later, elsewhere, against the same storage.
      const result = yield* Effect.gen(function*() {
        const fiber = yield* Effect.forkChild(Report.awaitResult(id))
        yield* settle
        yield* TestClock.adjust("1 second")
        return yield* Fiber.join(fiber)
      }).pipe(
        Effect.provide(
          Report.toLayer(({ month }) => Effect.succeed(`report for ${month}`)).pipe(
            Layer.provideMerge(Worker.layer()),
            Layer.provideMerge(storeLayer)
          )
        )
      )

      expect(result).toBe("report for 2026-08")
    }))

  it("the producer API never requires the Worker service (type-level)", () => {
    const Typed = Job.make("Typed", { payload: { n: Schema.Number } })
    type EnqueueContext = Effect.Services<ReturnType<typeof Typed.enqueue>>
    type PollContext = Effect.Services<ReturnType<typeof Typed.poll>>
    // If enqueue/poll ever gained a Worker requirement these assignments break.
    const enqueueNeedsOnlyStore: [EnqueueContext] extends [JobStore.JobStore] ? true : false = true
    const pollNeedsOnlyStore: [PollContext] extends [JobStore.JobStore] ? true : false = true
    expect(enqueueNeedsOnlyStore).toBe(true)
    expect(pollNeedsOnlyStore).toBe(true)
  })
})

describe("Worker hardening", () => {
  it("backoffDelayMs computes retry delays", () => {
    expect(Worker.backoffDelayMs(undefined, 1)).toBe(0)
    expect(Worker.backoffDelayMs(undefined, 7)).toBe(0)
    expect(Worker.backoffDelayMs({ _tag: "fixed", delayMs: 500 }, 1)).toBe(500)
    expect(Worker.backoffDelayMs({ _tag: "fixed", delayMs: 500 }, 9)).toBe(500)
    expect(Worker.backoffDelayMs({ _tag: "exponential", delayMs: 1000 }, 1)).toBe(1000)
    expect(Worker.backoffDelayMs({ _tag: "exponential", delayMs: 1000 }, 2)).toBe(2000)
    expect(Worker.backoffDelayMs({ _tag: "exponential", delayMs: 1000 }, 3)).toBe(4000)
    expect(Worker.backoffDelayMs({ _tag: "exponential", delayMs: 1000, factor: 3 }, 3)).toBe(9000)
    expect(Worker.backoffDelayMs({ _tag: "exponential", delayMs: 333, factor: 1.5 }, 2)).toBe(500)
  })

  it.effect("a handler that interrupts itself is a failed attempt, not a hot loop", () =>
    Effect.gen(function*() {
      const SelfInterrupt = Job.make("SelfInterrupt", {
        payload: {},
        defaults: { attempts: 2, backoff: { type: "fixed", delay: "1 second" } }
      })
      let runs = 0
      const handlers = SelfInterrupt.toLayer(() =>
        Effect.suspend(() => {
          runs++
          return Effect.interrupt
        })
      )

      yield* Effect.gen(function*() {
        const id = yield* SelfInterrupt.enqueue({})
        yield* settle
        expect(runs).toBe(1)
        // Attempt was consumed and routed through backoff, not released.
        const afterFirst = yield* SelfInterrupt.poll(id)
        assert(Option.isSome(afterFirst))
        expect(afterFirst.value.state).toBe("delayed")
        expect(afterFirst.value.attemptsMade).toBe(1)

        yield* TestClock.adjust("1 second")
        yield* settle
        expect(runs).toBe(2)
        const final = yield* SelfInterrupt.poll(id)
        assert(Option.isSome(final))
        expect(final.value.state).toBe("failed")
        expect(final.value.attemptsMade).toBe(2)

        // No further reprocessing, ever.
        yield* TestClock.adjust("30 seconds")
        yield* settle
        expect(runs).toBe(2)
      }).pipe(Effect.provide(harness(handlers)))
    }))

  it.effect("a success value that violates the success schema fails the job instead of stranding it", () =>
    Effect.gen(function*() {
      const Strict = Job.make("Strict", {
        payload: {},
        success: Schema.Number
      })
      let runs = 0
      // The handler "succeeds" with malformed data (parsed, not asserted) to
      // prove the worker routes an unencodable result to a failed job.
      const handlers = Strict.toLayer(() =>
        Effect.sync(() => {
          runs++
          return JSON.parse('"not a number"')
        })
      )

      yield* Effect.gen(function*() {
        const id = yield* Strict.enqueue({})
        yield* settle
        // Must NOT re-run (side effects already happened) and must not limbo.
        expect(runs).toBe(1)
        const status = yield* Strict.poll(id)
        assert(Option.isSome(status))
        expect(status.value.state).toBe("failed")

        const result = yield* Effect.exit(Strict.awaitResult(id))
        assert(Exit.isFailure(result))
        expect(Exit.hasDies(result)).toBe(true)

        yield* TestClock.adjust("2 minutes")
        yield* settle
        expect(runs).toBe(1)
      }).pipe(Effect.provide(harness(handlers)))
    }))

  it.effect("lock renewal keeps long-running handlers alive past lockDuration", () =>
    Effect.gen(function*() {
      const Slow = Job.make("Slow", { payload: {} })
      let runs = 0
      const handlers = Slow.toLayer(() =>
        Effect.suspend(() => {
          runs++
          return Effect.sleep("3 seconds")
        })
      )

      yield* Effect.gen(function*() {
        const id = yield* Slow.enqueue({})
        yield* settle
        expect(runs).toBe(1)
        // Walk the clock in small steps so renewal (500ms) fires well before
        // the lock (1s) or the stalled sweep (1s) would take the job away.
        for (let i = 0; i < 8; i++) {
          yield* TestClock.adjust("500 millis")
          yield* settle
        }
        const status = yield* Slow.poll(id)
        assert(Option.isSome(status))
        expect(status.value.state).toBe("completed")
        expect(runs).toBe(1)

        const store = yield* JobStore.JobStore
        const record = yield* store.getJob(id)
        assert(Option.isSome(record))
        expect(record.value.stalledCount).toBe(0)
      }).pipe(Effect.provide(harness(handlers, {
        lockDuration: "1 second",
        lockRenewInterval: "500 millis",
        stalledInterval: "1 second",
        maxStalledCount: 1
      })))
    }))

  it.effect("transient store errors are retried, not fatal", () =>
    Effect.gen(function*() {
      const store = yield* MemoryJobStore.make
      let claimFailures = 0
      const flaky = JobStore.JobStore.of({
        ...store,
        claim: (options) =>
          Effect.suspend(() => {
            if (claimFailures < 2) {
              claimFailures++
              return Effect.fail(new JobStore.JobStoreError({ message: "flaky" }))
            }
            return store.claim(options)
          })
      })
      const Sturdy = Job.make("Sturdy", { payload: {} })
      let ran = 0
      const handlers = Sturdy.toLayer(() => Effect.sync(() => void ran++))

      yield* Effect.gen(function*() {
        yield* Sturdy.enqueue({})
        yield* settle
        // retryStore backs off 200ms then 300ms before the third attempt.
        yield* TestClock.adjust("200 millis")
        yield* settle
        yield* TestClock.adjust("300 millis")
        yield* settle
        expect(claimFailures).toBe(2)
        expect(ran).toBe(1)
      }).pipe(
        Effect.provide(
          handlers.pipe(
            Layer.provideMerge(Worker.layer()),
            Layer.provideMerge(Layer.succeed(JobStore.JobStore, flaky))
          )
        )
      )
    }))

  it.effect("the worker's stalled sweep fails repeat offenders and awaitResult reports it", () =>
    Effect.gen(function*() {
      const store = yield* MemoryJobStore.make
      const storeLayer = Layer.succeed(JobStore.JobStore, store)
      const Ghost = Job.make("Ghost", { payload: {} })
      const handlers = Ghost.toLayer(() => Effect.void)

      yield* Effect.gen(function*() {
        const id = yield* Ghost.enqueue({})
        // A "crashed worker" holds the job so our worker can never claim it.
        const crashed = yield* store.claim({
          queue: QueueName("default"),
          names: ["Ghost"],
          token: "crashed",
          lockDurationMs: 500
        })
        assert(crashed._tag === "Claimed")

        const waiter = yield* Effect.forkChild(Effect.exit(Ghost.awaitResult(id)))
        yield* settle
        // maxStalledCount 0: the first expired lock fails the job outright.
        yield* TestClock.adjust("1 second")
        yield* settle
        yield* TestClock.adjust("1 second")
        const result = yield* Fiber.join(waiter)
        assert(Exit.isFailure(result))
        expect(Exit.hasDies(result)).toBe(true)

        const record = yield* store.getJob(id)
        assert(Option.isSome(record))
        expect(record.value.state).toBe("failed")
        expect(record.value.failedReason).toBeDefined()
      }).pipe(
        Effect.provide(
          handlers.pipe(
            Layer.provideMerge(Worker.layer({
              stalledInterval: "1 second",
              maxStalledCount: 0
            })),
            Layer.provideMerge(storeLayer)
          )
        )
      )
    }))

  it.effect("two workers sharing a store never double-process a job", () =>
    Effect.gen(function*() {
      const store = yield* MemoryJobStore.make
      const storeLayer = Layer.succeed(JobStore.JobStore, store)
      const Shared = Job.make("Shared", { payload: { n: Schema.Number } })
      const runsPerJob = new Map<number, number>()
      const handlerFor = () =>
        Shared.toLayer(({ n }) =>
          Effect.sync(() => {
            runsPerJob.set(n, (runsPerJob.get(n) ?? 0) + 1)
          }), { concurrency: 2 })
      const workerStack = (id: string) =>
        handlerFor().pipe(
          Layer.provideMerge(Worker.layer({ id })),
          Layer.provideMerge(storeLayer)
        )

      yield* Effect.gen(function*() {
        for (let n = 0; n < 10; n++) {
          yield* Shared.enqueue({ n })
        }
        yield* settle
        yield* TestClock.adjust("1 second")
        yield* settle
        const counts = yield* store.counts()
        expect(counts.completed).toBe(10)
        expect(runsPerJob.size).toBe(10)
        for (const [, count] of runsPerJob) {
          expect(count).toBe(1)
        }
      }).pipe(Effect.provide(Layer.merge(workerStack("w1"), workerStack("w2"))))
    }))

  it.effect("handler defects consume attempts, are retried, and round-trip as dies", () =>
    Effect.gen(function*() {
      const Bomb = Job.make("Bomb", {
        payload: {},
        success: Schema.String,
        defaults: { attempts: 2, backoff: { type: "fixed", delay: "1 second" } }
      })
      let attempt = 0
      const handlers = Bomb.toLayer(() =>
        Effect.suspend(() => {
          attempt++
          return attempt === 1 ? Effect.die("boom") : Effect.succeed("recovered")
        })
      )

      yield* Effect.gen(function*() {
        const id = yield* Bomb.enqueue({})
        const fiber = yield* Effect.forkChild(Bomb.awaitResult(id))
        yield* settle
        yield* TestClock.adjust("1 second")
        yield* settle
        yield* TestClock.adjust("1 second")
        const result = yield* Fiber.join(fiber)
        expect(result).toBe("recovered")

        const ledger = yield* Bomb.attempts(id)
        expect(ledger.map((entry) => entry.outcome)).toEqual(["retried", "completed"])
        const firstRun = ledger[0]
        assert(firstRun !== undefined)
        assert(Option.isSome(firstRun.exit))
        assert(Exit.isFailure(firstRun.exit.value))
        expect(Cause.hasDies(firstRun.exit.value.cause)).toBe(true)
      }).pipe(Effect.provide(harness(handlers)))
    }))

  it.effect("a terminal defect surfaces as a die in awaitResult", () =>
    Effect.gen(function*() {
      const Fatal = Job.make("Fatal", { payload: {} })
      const handlers = Fatal.toLayer(() => Effect.die("kaput"))

      yield* Effect.gen(function*() {
        const id = yield* Fatal.enqueue({})
        const fiber = yield* Effect.forkChild(Effect.exit(Fatal.awaitResult(id)))
        yield* settle
        yield* TestClock.adjust("1 second")
        const result = yield* Fiber.join(fiber)
        assert(Exit.isFailure(result))
        expect(Exit.hasDies(result)).toBe(true)
      }).pipe(Effect.provide(harness(handlers)))
    }))

  it.effect("queue overrides on registration and enqueue route jobs correctly", () =>
    Effect.gen(function*() {
      const Routed = Job.make("Routed", { payload: {}, queue: "a" })
      let ran = 0
      // The handler consumes queue "b", not the definition's "a".
      const handlers = Routed.toLayer(() => Effect.sync(() => void ran++), { queue: "b" })

      yield* Effect.gen(function*() {
        const onA = yield* Routed.enqueue({})
        const onB = yield* Routed.enqueue({}, { queue: "b" })
        yield* settle
        yield* TestClock.adjust("30 seconds")
        yield* settle
        expect(ran).toBe(1)
        const statusA = yield* Routed.poll(onA)
        const statusB = yield* Routed.poll(onB)
        assert(Option.isSome(statusA) && Option.isSome(statusB))
        expect(statusA.value.state).toBe("waiting")
        expect(statusB.value.state).toBe("completed")
      }).pipe(Effect.provide(harness(handlers)))
    }))

  it.effect("WorkerOptions.queues concurrency beats toLayer concurrency", () =>
    Effect.gen(function*() {
      const Busy = Job.make("Busy", { payload: { n: Schema.Number }, queue: "q" })
      const gate = yield* Deferred.make<void>()
      let running = 0
      let maxRunning = 0
      const handlers = Busy.toLayer(() =>
        Effect.gen(function*() {
          running++
          maxRunning = Math.max(maxRunning, running)
          yield* Deferred.await(gate)
          running--
        }), { concurrency: 5 })

      yield* Effect.gen(function*() {
        for (const n of [1, 2, 3, 4, 5]) {
          yield* Busy.enqueue({ n })
        }
        yield* settle
        expect(maxRunning).toBe(2)
        yield* Deferred.succeed(gate, void 0)
        yield* settle
      }).pipe(Effect.provide(harness(handlers, {
        concurrency: 3,
        queues: { q: { concurrency: 2 } }
      })))
    }))

  it.effect("duplicate handler registration dies at layer build", () =>
    Effect.gen(function*() {
      const Dup = Job.make("Dup", { payload: {} })
      const bad = Layer.mergeAll(
        Dup.toLayer(() => Effect.void),
        Dup.toLayer(() => Effect.void)
      ).pipe(
        Layer.provideMerge(Worker.layer()),
        Layer.provideMerge(MemoryJobStore.layer)
      )
      const result = yield* Effect.exit(Effect.provide(Effect.void, bad))
      assert(Exit.isFailure(result))
      expect(Exit.hasDies(result)).toBe(true)
    }))

  it.effect("undecodable stored payloads fail the job and the taker survives", () =>
    Effect.gen(function*() {
      const store = yield* MemoryJobStore.make
      const storeLayer = Layer.succeed(JobStore.JobStore, store)
      const Typednum = Job.make("Typednum", { payload: { n: Schema.Number } })
      let ran = 0
      const handlers = Typednum.toLayer(() => Effect.sync(() => void ran++))

      yield* Effect.gen(function*() {
        // Inject a corrupt payload directly, bypassing the typed producer API.
        const junk = yield* store.enqueue({
          id: undefined,
          name: "Typednum",
          queue: QueueName("default"),
          payload: { n: "not-a-number" },
          metadata: {},
          priority: 0,
          attemptsMax: 1,
          backoff: undefined,
          keep: undefined,
          timeoutMs: undefined,
          dedupe: undefined,
          trace: undefined,
          delayMs: 0
        })
        yield* settle
        const junkRecord = yield* store.getJob(junk.id)
        assert(Option.isSome(junkRecord))
        expect(junkRecord.value.state).toBe("failed")

        // The taker is still alive and processes valid jobs.
        yield* Typednum.enqueue({ n: 42 })
        yield* settle
        expect(ran).toBe(1)
      }).pipe(
        Effect.provide(
          handlers.pipe(
            Layer.provideMerge(Worker.layer()),
            Layer.provideMerge(storeLayer)
          )
        )
      )
    }))

  it.effect("awaitResult dies when the job is removed mid-wait", () =>
    Effect.gen(function*() {
      const store = yield* MemoryJobStore.make
      const storeLayer = Layer.succeed(JobStore.JobStore, store)
      const Vanishing = Job.make("Vanishing", { payload: {} })

      yield* Effect.gen(function*() {
        const id = yield* Vanishing.enqueue({})
        const fiber = yield* Effect.forkChild(
          Effect.exit(Vanishing.awaitResult(id, { pollSchedule: Schedule.spaced("1 second") }))
        )
        yield* settle
        expect(yield* store.remove(id)).toBe(true)
        yield* TestClock.adjust("1 second")
        const result = yield* Fiber.join(fiber)
        assert(Exit.isFailure(result))
        expect(Exit.hasDies(result)).toBe(true)
      }).pipe(Effect.provide(storeLayer))
    }))

  it.effect("awaitResult dies when its poll schedule is exhausted", () =>
    Effect.gen(function*() {
      const Stalling = Job.make("Stalling", { payload: {} })
      yield* Effect.gen(function*() {
        const id = yield* Stalling.enqueue({})
        // No worker: the job never finishes; the schedule gives up after 2 steps.
        const result = yield* Effect.exit(
          Stalling.awaitResult(id, { pollSchedule: Schedule.recurs(2) })
        )
        assert(Exit.isFailure(result))
        expect(Exit.hasDies(result)).toBe(true)
      }).pipe(Effect.provide(MemoryJobStore.layer))
    }))

  it.effect("stored exits survive a JSON serialization boundary", () =>
    Effect.gen(function*() {
      const Greets = Job.make("Greets", {
        payload: { name: Schema.String },
        success: Schema.String
      })
      const handlers = Greets.toLayer(({ name }) => Effect.succeed(`hi ${name}`))

      yield* Effect.gen(function*() {
        const id = yield* Greets.enqueue({ name: "io" })
        yield* settle
        const store = yield* JobStore.JobStore
        const record = yield* store.getJob(id)
        assert(Option.isSome(record))
        expect(record.value.state).toBe("completed")
        // What a serializing driver (Redis/Postgres) would do:
        const roundTripped: unknown = JSON.parse(JSON.stringify(record.value.exit))
        const decoded = yield* Schema.decodeUnknownEffect(Greets.exitSchema)(roundTripped)
        assert(Exit.isSuccess(decoded))
        expect(decoded.value).toBe("hi io")
      }).pipe(Effect.provide(harness(handlers)))
    }))
})

describe("named stores (multi-store routing)", () => {
  it.effect("a job bound to a named store runs on it, isolated from the default store", () =>
    Effect.gen(function*() {
      const Durable = JobStore.named("durable")
      const durableStore = yield* MemoryJobStore.make
      const defaultStore = yield* MemoryJobStore.make

      const GenerateInvoice = Job.make("GenerateInvoice", {
        payload: { invoiceId: Schema.String },
        success: Schema.String,
        store: Durable,
        metadata: ({ invoiceId }) => ({ invoiceId })
      })
      const handlers = GenerateInvoice.toLayer(({ invoiceId }) => Effect.succeed(`generated ${invoiceId}`))

      const result = yield* Effect.gen(function*() {
        const fiber = yield* Effect.forkChild(GenerateInvoice.execute({ invoiceId: "inv-1" }))
        yield* settle
        yield* TestClock.adjust("1 second")
        return yield* Fiber.join(fiber)
      }).pipe(
        Effect.provide(
          handlers.pipe(
            Layer.provideMerge(Worker.layer({ store: Durable })),
            Layer.provideMerge(Layer.succeed(Durable, durableStore)),
            Layer.provideMerge(Layer.succeed(JobStore.JobStore, defaultStore))
          )
        )
      )

      expect(result).toBe("generated inv-1")
      // The run lives in the durable store only, with its metadata projection.
      expect((yield* durableStore.counts()).completed).toBe(1)
      expect((yield* defaultStore.counts()).completed).toBe(0)
      const listed = yield* durableStore.list({ metadata: { invoiceId: "inv-1" } })
      expect(listed.items).toHaveLength(1)
    }))

  it("enqueue of a store-bound job requires that store in R (type-level)", () => {
    const Durable = JobStore.named("durable")
    const Bound = Job.make("Bound", { payload: {}, store: Durable })
    type R = Effect.Services<ReturnType<typeof Bound.enqueue>>
    const requiresNamed: [JobStore.Named<"durable">] extends [R] ? true : false = true
    const notDefault: [R] extends [JobStore.JobStore] ? true : false = false
    expect(requiresNamed).toBe(true)
    expect(notDefault).toBe(false)
  })

  it.effect("registering a job on a worker bound to a different store dies", () =>
    Effect.gen(function*() {
      const Durable = JobStore.named("durable")
      const Misrouted = Job.make("Misrouted", { payload: {}, store: Durable })
      const bad = Misrouted.toLayer(() => Effect.void).pipe(
        Layer.provideMerge(Worker.layer()), // default-store worker
        Layer.provideMerge(MemoryJobStore.layer)
      )
      const result = yield* Effect.exit(Effect.provide(Effect.void, bad))
      assert(Exit.isFailure(result))
      expect(Exit.hasDies(result)).toBe(true)
    }))

  it.effect("two workers for two stores coexist in one layer graph via local provide", () =>
    Effect.gen(function*() {
      const Durable = JobStore.named("durable")
      const Ephemeral = JobStore.named("ephemeral")
      const durableStore = yield* MemoryJobStore.make
      const ephemeralStore = yield* MemoryJobStore.make

      const Sync = Job.make("Sync", { payload: {}, store: Durable })
      const Email = Job.make("Email", { payload: {}, store: Ephemeral })
      const ran: Array<string> = []

      // Worker layers provided *locally* so the two `Worker` services don't collide.
      const durableWorkers = Sync.toLayer(() => Effect.sync(() => void ran.push("sync"))).pipe(
        Layer.provide(Worker.layer({ store: Durable }))
      )
      const ephemeralWorkers = Email.toLayer(() => Effect.sync(() => void ran.push("email"))).pipe(
        Layer.provide(Worker.layer({ store: Ephemeral }))
      )

      yield* Effect.gen(function*() {
        yield* Sync.enqueue({})
        yield* Email.enqueue({})
        yield* settle
        yield* TestClock.adjust("1 second")
        yield* settle
        expect(ran.toSorted()).toEqual(["email", "sync"])
      }).pipe(
        Effect.provide(
          Layer.mergeAll(durableWorkers, ephemeralWorkers).pipe(
            Layer.provideMerge(Layer.succeed(Durable, durableStore)),
            Layer.provideMerge(Layer.succeed(Ephemeral, ephemeralStore))
          )
        )
      )

      expect((yield* durableStore.counts()).completed).toBe(1)
      expect((yield* ephemeralStore.counts()).completed).toBe(1)
    }))

  it.effect("Job.retry re-runs a failed job end-to-end", () =>
    Effect.gen(function*() {
      const Fragile = Job.make("Fragile", { payload: {}, success: Schema.String })
      let attempt = 0
      const handlers = Fragile.toLayer(() =>
        Effect.suspend(() => {
          attempt++
          return attempt === 1 ? Effect.die("first run dies") : Effect.succeed("second run ok")
        })
      )

      yield* Effect.gen(function*() {
        const id = yield* Fragile.enqueue({})
        yield* settle
        const failed = yield* Fragile.poll(id)
        assert(Option.isSome(failed))
        expect(failed.value.state).toBe("failed")

        // The dashboard "retry" button.
        yield* Fragile.retry(id)
        yield* settle
        const result = yield* Fragile.awaitResult(id).pipe(Effect.forkChild)
        yield* settle
        yield* TestClock.adjust("1 second")
        expect(yield* Fiber.join(result)).toBe("second run ok")

        const ledger = yield* Fragile.attempts(id)
        expect(ledger.map((entry) => [entry.attempt, entry.outcome])).toEqual([
          [1, "failed"],
          [2, "completed"]
        ])
      }).pipe(Effect.provide(harness(handlers)))
    }))
})

describe("review gap coverage", () => {
  it.effect("a default-store job on a named-store worker dies at registration", () =>
    Effect.gen(function*() {
      const Durable = JobStore.named("durable")
      const DefaultJob = Job.make("DefaultJob", { payload: {} })
      const bad = DefaultJob.toLayer(() => Effect.void).pipe(
        Layer.provideMerge(Worker.layer({ store: Durable })),
        Layer.provideMerge(MemoryJobStore.layerFor(Durable))
      )
      const result = yield* Effect.exit(Effect.provide(Effect.void, bad))
      assert(Exit.isFailure(result))
      expect(Exit.hasDies(result)).toBe(true)
    }))

  it.effect("Job.attempts decodes stalled entries as exit-less runs", () =>
    Effect.gen(function*() {
      const store = yield* MemoryJobStore.make
      const Stalls = Job.make("Stalls", { payload: {} })
      const id = yield* Stalls.enqueue({}).pipe(
        Effect.provide(Layer.succeed(JobStore.JobStore, store))
      )
      const claim = yield* store.claim({
        queue: QueueName("default"),
        names: ["Stalls"],
        token: "crashed",
        lockDurationMs: 1000
      })
      assert(claim._tag === "Claimed")
      yield* TestClock.adjust(1000)
      yield* store.recoverStalled({ maxStalledCount: 1 })

      const ledger = yield* Stalls.attempts(id).pipe(
        Effect.provide(Layer.succeed(JobStore.JobStore, store))
      )
      expect(ledger).toHaveLength(1)
      expect(ledger[0]?.outcome).toBe("stalled")
      expect(Option.isNone(ledger[0]?.exit ?? Option.none())).toBe(true)
      expect(ledger[0]?.startedAt).toBeDefined()
    }))

  it.effect("Job-level keep defaults flow through enqueue and prune terminal records", () =>
    Effect.gen(function*() {
      const Pruned = Job.make("Pruned", {
        payload: { n: Schema.Number },
        defaults: { keep: { count: 1, age: "1 minute" } }
      })
      let done = 0
      const handlers = Pruned.toLayer(() => Effect.sync(() => void done++))

      yield* Effect.gen(function*() {
        const store = yield* JobStore.JobStore
        const first = yield* Pruned.enqueue({ n: 1 })
        const record = yield* store.getJob(first)
        assert(Option.isSome(record))
        // The flat shorthand normalizes to every terminal state.
        expect(record.value.keep).toEqual({
          completed: { count: 1, ageMs: 60_000 },
          failed: { count: 1, ageMs: 60_000 },
          cancelled: { count: 1, ageMs: 60_000 }
        })

        yield* settle
        yield* TestClock.adjust("1 second")
        const second = yield* Pruned.enqueue({ n: 2 })
        yield* settle
        yield* TestClock.adjust("1 second")
        yield* settle
        expect(done).toBe(2)
        // count: 1 keeps only the most recent completed record.
        expect(Option.isNone(yield* store.getJob(first))).toBe(true)
        expect(Option.isSome(yield* store.getJob(second))).toBe(true)
      }).pipe(Effect.provide(harness(handlers)))
    }))

  it.effect("poll exposes the metadata projection", () =>
    Effect.gen(function*() {
      const Tagged = Job.make("Tagged", {
        payload: { invoiceId: Schema.String },
        metadata: ({ invoiceId }) => ({ invoiceId })
      })
      yield* Effect.gen(function*() {
        const id = yield* Tagged.enqueue(
          { invoiceId: "emp-9" },
          { metadata: { source: "api" } }
        )
        const status = yield* Tagged.poll(id)
        assert(Option.isSome(status))
        expect(status.value.metadata).toEqual({ invoiceId: "emp-9", source: "api" })
      }).pipe(Effect.provide(MemoryJobStore.layer))
    }))
})

describe("redaction through the worker", () => {
  it.effect("handlers receive re-wrapped Redacted payload values", () =>
    Effect.gen(function*() {
      const Secretive = Job.make("Secretive", {
        payload: { apiKey: Schema.Redacted(Schema.String) },
        success: Schema.Boolean
      })
      const handlers = Secretive.toLayer(({ apiKey }) =>
        Effect.sync(() =>
          Redacted.isRedacted(apiKey) && Redacted.value(apiKey) === "secret-9"
        )
      )
      const result = yield* Effect.gen(function*() {
        const fiber = yield* Effect.forkChild(
          Secretive.execute({ apiKey: Redacted.make("secret-9") })
        )
        yield* settle
        yield* TestClock.adjust("1 second")
        return yield* Fiber.join(fiber)
      }).pipe(Effect.provide(harness(handlers)))
      expect(result).toBe(true)
    }))
})

describe("handler context isolation", () => {
  it.effect("two jobs on one queue each see their own locally provided service", () =>
    Effect.gen(function*() {
      class Dep extends Context.Service<Dep, { readonly n: number }>()("test/Dep") {}
      const A = Job.make("IsoA", { payload: {}, success: Schema.Number, queue: "iso" })
      const B = Job.make("IsoB", { payload: {}, success: Schema.Number, queue: "iso" })

      const handlerFor = () => Effect.map(Dep, (dep) => dep.n)
      const handlers = Layer.mergeAll(
        A.toLayer(handlerFor).pipe(Layer.provide(Layer.succeed(Dep, { n: 1 }))),
        B.toLayer(handlerFor).pipe(Layer.provide(Layer.succeed(Dep, { n: 2 })))
      )

      const [a, b] = yield* Effect.gen(function*() {
        const fiberA = yield* Effect.forkChild(A.execute({}))
        const fiberB = yield* Effect.forkChild(B.execute({}))
        yield* settle
        yield* TestClock.adjust("1 second")
        yield* settle
        yield* TestClock.adjust("1 second")
        return yield* Effect.all([Fiber.join(fiberA), Fiber.join(fiberB)])
      }).pipe(Effect.provide(harness(handlers)))

      // Before the context-precedence fix, B's handler silently ran with A's
      // Dep instance (the taker fiber inherited the first registrant's context).
      expect(a).toBe(1)
      expect(b).toBe(2)
    }))
})
