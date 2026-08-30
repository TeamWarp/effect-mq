import { assert, describe, expect, it } from "@effect/vitest"
import { Effect, Layer, Logger, Metric, Option, Ref, Schema } from "effect"
import { TestClock } from "effect/testing"
import { Flow, Job, JobStore, MemoryJobStore, Metrics, Worker } from "../src/index.ts"

/** Let all currently runnable fibers make progress. */
const settle = Effect.gen(function*() {
  for (let i = 0; i < 10; i++) {
    yield* Effect.yieldNow
  }
})

/** Walk the clock in half-second steps so every heartbeat and tick lands. */
const walk = (seconds: number) =>
  Effect.gen(function*() {
    yield* settle
    for (let i = 0; i < seconds * 2; i++) {
      yield* TestClock.adjust("500 millis")
      yield* settle
    }
  })

/**
 * The first heartbeat hands a lock back for real and reports it lost — what
 * a losing worker sees when a stall sweep elsewhere takes its job. Handing
 * the lock back for real is the point: a staged "lost" would leave the
 * orphan's ack able to land, and then nothing here would be true.
 *
 * `target` is read at heartbeat time, so a test can name the job after
 * enqueueing it; unset steals whatever the first heartbeat carries.
 */
const stealingStore = (
  base: JobStore.Service,
  target: () => JobStore.JobId | undefined = () => undefined
): JobStore.Service => {
  let stolen = false
  return JobStore.JobStore.of({
    ...base,
    extendLocks: (locks, durationMs) =>
      Effect.gen(function*() {
        const wanted = target()
        const taking = stolen
          ? []
          : locks.filter((lock) => wanted === undefined || lock.id === wanted)
        if (taking.length === 0) return yield* base.extendLocks(locks, durationMs)
        stolen = true
        for (const lock of taking) {
          yield* base.release(lock.id, lock.token).pipe(Effect.catchCause(() => Effect.void))
        }
        const keeping = locks.filter((lock) => !taking.includes(lock))
        const rest = keeping.length === 0
          ? { lost: [], cancelRequested: [] }
          : yield* base.extendLocks(keeping, durationMs)
        return {
          lost: [...rest.lost, ...taking.map((lock) => lock.id)],
          cancelRequested: rest.cancelRequested
        }
      })
  })
}

/**
 * A handler that works for ten simulated seconds and records a tick every
 * second, labelled by run — so how far each run got is directly readable.
 */
const tickingJob = (name: string) => {
  class Ticking extends Job.make(name, { payload: {}, defaults: { attempts: 3 } }) {}
  const ticks: Array<string> = []
  const interruptedRuns: Array<number> = []
  let runs = 0
  const handlers = Ticking.toLayer(() =>
    Effect.suspend(() => {
      const run = ++runs
      return Effect.gen(function*() {
        for (let i = 0; i < 10; i++) {
          yield* Effect.sleep("1 second")
          ticks.push(`run${run}-tick${i}`)
        }
      }).pipe(Effect.onInterrupt(() => Effect.sync(() => void interruptedRuns.push(run))))
    })
  )
  return { Ticking, handlers, ticks, interruptedRuns }
}

/** Collect every log line the worker emits, as plain strings. */
const collectLogs = (lines: Array<string>) =>
  Logger.layer([
    Logger.make((options) => {
      lines.push(String(options.message))
    })
  ])

/**
 * One taker, a 2.5s heartbeat, and this worker's own stall sweep parked out
 * of the way — the stealing store is the only thing taking locks here.
 */
const options = (onLockLost: "ignore" | "interrupt"): Worker.WorkerOptions => ({
  lockDuration: "10 seconds",
  lockRenewInterval: "2500 millis",
  stalledInterval: "1 hour",
  concurrency: 1,
  onLockLost
})

const harness = (
  handlers: Layer.Layer<never, never, Worker.Worker | JobStore.JobStore>,
  store: JobStore.Service,
  workerOptions: Worker.WorkerOptions
) =>
  handlers.pipe(
    Layer.provideMerge(Worker.layer(workerOptions)),
    Layer.provideMerge(Layer.succeed(JobStore.JobStore, store))
  )

describe("lost locks: the default is unchanged", () => {
  it.effect("the run whose lock was lost keeps going, all the way to its end", () =>
    Effect.gen(function*() {
      const logs: Array<string> = []
      const base = yield* MemoryJobStore.make
      const { handlers, interruptedRuns, ticks, Ticking } = tickingJob("IgnoreLong")

      yield* Effect.gen(function*() {
        yield* Ticking.enqueue({})
        // Twelve seconds: the lock goes at 2.5s, the run wants ten.
        yield* walk(12)

        expect(ticks).toContain("run1-tick9")
        expect(interruptedRuns).toEqual([])
        expect(logs.some((line) => line.includes("failed to renew locks; jobs may run twice"))).toBe(true)
        // The orphan's ack was refused: another worker owns the job now.
        expect(logs.some((line) => line.includes("ack dropped (LockLostError)"))).toBe(true)
      }).pipe(
        Effect.provide(harness(handlers, stealingStore(base), options("ignore"))),
        Effect.provide(collectLogs(logs))
      )
    }))

  it.effect("the orphan holds its taker slot, so the re-queued job waits for it", () =>
    Effect.gen(function*() {
      const base = yield* MemoryJobStore.make
      const { handlers, ticks, Ticking } = tickingJob("IgnoreSlot")

      yield* Effect.gen(function*() {
        yield* Ticking.enqueue({})
        yield* walk(14)

        // The job is runnable again from 2.5s on, but the only taker is
        // parked on the orphan: no run2 tick lands before the last run1 one.
        const firstRun2 = ticks.findIndex((tick) => tick.startsWith("run2-"))
        const lastRun1 = ticks.findLastIndex((tick) => tick.startsWith("run1-"))
        expect(lastRun1).toBeGreaterThanOrEqual(0)
        expect(firstRun2 === -1 || firstRun2 > lastRun1).toBe(true)
      }).pipe(Effect.provide(harness(handlers, stealingStore(base), options("ignore"))))
    }))
})

describe("lost locks: onLockLost \"interrupt\"", () => {
  it.effect("the run is stopped within one heartbeat of the loss", () =>
    Effect.gen(function*() {
      const base = yield* MemoryJobStore.make
      const { handlers, interruptedRuns, ticks, Ticking } = tickingJob("InterruptLong")

      yield* Effect.gen(function*() {
        yield* Ticking.enqueue({})
        yield* walk(12)

        expect(interruptedRuns).toContain(1)
        // The lock went at 2.5s, so run 1 got two ticks and no more.
        expect(ticks.filter((tick) => tick.startsWith("run1-"))).toEqual(["run1-tick0", "run1-tick1"])
      }).pipe(Effect.provide(harness(handlers, stealingStore(base), options("interrupt"))))
    }))

  it.effect("no attempt is spent and no failure is reported", () =>
    Effect.gen(function*() {
      const queue = "mq-lost-lock-accounting"
      class Doomed extends Job.make("LostLockDoomed", { payload: {}, queue, defaults: { attempts: 3 } }) {}
      const handlers = Doomed.toLayer(() => Effect.never)
      const base = yield* MemoryJobStore.make
      const failures: Array<Worker.JobFailure> = []

      const runs = (outcome: string) =>
        Metric.value(Metrics.jobRuns.pipe(
          Metric.withAttributes({ name: "LostLockDoomed", queue, outcome })
        ))
      const before = {
        lost: (yield* runs("lock-lost")).count,
        retried: (yield* runs("retried")).count,
        failed: (yield* runs("failed")).count
      }

      const id = yield* Effect.gen(function*() {
        const id = yield* Doomed.enqueue({})
        yield* walk(5)
        return id
      }).pipe(
        Effect.provide(harness(handlers, stealingStore(base), {
          ...options("interrupt"),
          onJobFailure: (failure) => Effect.sync(() => void failures.push(failure))
        }))
      )

      // Without the branch, the interrupt-only exit becomes a spurious failed
      // attempt: routeFailure retries it and onJobFailure is told about it.
      expect(failures).toEqual([])
      expect((yield* runs("lock-lost")).count - before.lost).toBe(1)
      expect((yield* runs("retried")).count - before.retried).toBe(0)
      expect((yield* runs("failed")).count - before.failed).toBe(0)

      const record = yield* base.getJob(id)
      assert(Option.isSome(record))
      expect(record.value.attemptsMade).toBe(0)
      expect(record.value.state).not.toBe("failed")
      // Nothing was acked, so the orphan left no entry in the run ledger.
      expect(yield* base.getAttempts(id)).toEqual([])
    }))

  it.effect("the taker slot is freed, so the re-queued job starts right away", () =>
    Effect.gen(function*() {
      const base = yield* MemoryJobStore.make
      const { handlers, ticks, Ticking } = tickingJob("InterruptSlot")

      yield* Effect.gen(function*() {
        yield* Ticking.enqueue({})
        yield* walk(8)

        // Run 2 is claimed by the freed taker and is ticking well before run
        // 1 would have finished — the mirror of the "ignore" case above.
        expect(ticks).toContain("run2-tick0")
        expect(ticks).not.toContain("run1-tick9")
      }).pipe(Effect.provide(harness(handlers, stealingStore(base), options("interrupt"))))
    }))

  it.effect("a slow interrupt finalizer does not stall the other jobs' locks", () =>
    Effect.gen(function*() {
      class SlowCleanup extends Job.make("LostLockSlowCleanup", { payload: {} }) {}
      class Bystander extends Job.make("LostLockBystander", { payload: {} }) {}
      const base = yield* MemoryJobStore.make
      const interrupted = yield* Ref.make(false)
      // Only the first run is slow; the re-queued job is claimed again while
      // the finalizer is still going, and must not hold the test open.
      let slowRuns = 0
      const handlers = Layer.mergeAll(
        SlowCleanup.toLayer(() =>
          Effect.suspend(() =>
            ++slowRuns > 1 ? Effect.void : Effect.never.pipe(
              // Far longer than the lock: the heartbeat must not wait for it.
              Effect.onInterrupt(() =>
                Ref.set(interrupted, true).pipe(Effect.andThen(Effect.sleep("2 minutes")))
              )
            )
          )
        ),
        Bystander.toLayer(() => Effect.never)
      )

      let slowId: JobStore.JobId | undefined
      yield* Effect.gen(function*() {
        slowId = yield* SlowCleanup.enqueue({})
        const bystanderId = yield* Bystander.enqueue({})
        yield* walk(10)

        expect(yield* Ref.get(interrupted)).toBe(true)
        // Ten heartbeats into the slow finalizer, the bystander is still
        // locked and running: renewal never parked behind it.
        const bystander = yield* base.getJob(bystanderId)
        assert(Option.isSome(bystander))
        expect(bystander.value.state).toBe("active")
        expect(bystander.value.stalledCount).toBe(0)

        // Let the finalizer run out, so the scope closes on a quiet worker.
        yield* TestClock.adjust("2 minutes")
        yield* settle
      }).pipe(
        Effect.provide(harness(
          handlers,
          stealingStore(base, () => slowId),
          { ...options("interrupt"), concurrency: 2 }
        ))
      )
    }))

  it.effect("a lock reported lost AND cancel-requested takes the lost path", () =>
    Effect.gen(function*() {
      const queue = "mq-lost-lock-and-cancel"
      class Both extends Job.make("LostLockAndCancel", { payload: {}, queue }) {}
      const handlers = Both.toLayer(() => Effect.never)
      const base = yield* MemoryJobStore.make
      // The store names the same job in both halves of one heartbeat.
      let reported = false
      const both = JobStore.JobStore.of({
        ...base,
        extendLocks: (locks, durationMs) =>
          Effect.gen(function*() {
            if (reported || locks.length === 0) return yield* base.extendLocks(locks, durationMs)
            reported = true
            for (const lock of locks) {
              yield* base.release(lock.id, lock.token).pipe(Effect.catchCause(() => Effect.void))
            }
            const ids = locks.map((lock) => lock.id)
            return { lost: ids, cancelRequested: ids }
          })
      })

      const runs = (outcome: string) =>
        Metric.value(Metrics.jobRuns.pipe(
          Metric.withAttributes({ name: "LostLockAndCancel", queue, outcome })
        ))
      const before = { lost: (yield* runs("lock-lost")).count, cancelled: (yield* runs("cancelled")).count }

      yield* Effect.gen(function*() {
        yield* Both.enqueue({})
        yield* walk(5)
      }).pipe(Effect.provide(harness(handlers, both, options("interrupt"))))

      // The lost branch drops the flight before the cancel branch looks for
      // it, so this worker acks nothing; the new owner settles the cancel.
      expect((yield* runs("lock-lost")).count - before.lost).toBe(1)
      expect((yield* runs("cancelled")).count - before.cancelled).toBe(0)
    }))

  it.effect("a flow parent that loses its lock mid-fan-out enqueues no children", () =>
    Effect.gen(function*() {
      const queue = "mq-lost-lock-flow"
      class Child extends Job.make("LostLockFlowChild", { payload: { n: Schema.Number }, queue }) {}
      class Parent extends Job.make("LostLockFlowParent", { payload: {}, queue }) {}
      const base = yield* MemoryJobStore.make
      const ChildFlow = Flow.make("lost-lock-flow", { parent: Parent, children: [Child] })
      // Both runs fan out slowly, so nothing has been enqueued by the time
      // the assertions below run.
      const parentSide = ChildFlow.toLayer({
        fanOut: () =>
          Effect.sleep("6 seconds").pipe(
            Effect.as(Flow.children(Child, [{ key: "a", payload: { n: 1 } }]))
          ),
        collect: () => Effect.void
      })

      const runs = (outcome: string) =>
        Metric.value(Metrics.jobRuns.pipe(
          Metric.withAttributes({ name: "LostLockFlowParent", queue, outcome })
        ))
      const before = { lost: (yield* runs("lock-lost")).count, fanned: (yield* runs("fanned-out")).count }

      yield* Effect.gen(function*() {
        yield* Parent.enqueue({})
        yield* walk(5)

        // The manifest never landed, so the children must not exist: another
        // worker re-runs fanOut from scratch.
        const children = yield* base.list({ name: "LostLockFlowChild" })
        expect(children.items).toEqual([])
      }).pipe(
        Effect.provide(harness(
          parentSide.pipe(Layer.provideMerge(Child.toLayer(() => Effect.void))),
          stealingStore(base),
          options("interrupt")
        ))
      )

      expect((yield* runs("lock-lost")).count - before.lost).toBe(1)
      expect((yield* runs("fanned-out")).count - before.fanned).toBe(0)
    }))
})

describe("lost locks: races with the handler's own exit", () => {
  it.effect("a run inside an uninterruptible region is dropped when it leaves it", () =>
    Effect.gen(function*() {
      const queue = "mq-lost-lock-uninterruptible"
      class Stubborn extends Job.make("LostLockStubborn", { payload: {}, queue }) {}
      const base = yield* MemoryJobStore.make
      const failures: Array<Worker.JobFailure> = []
      const done: Array<number> = []
      // An interrupt delivered into an uninterruptible region is deferred,
      // not dropped: the work finishes, then the exit is interrupt-only.
      // Only run 1 is stubborn — the re-queued job must not hold the scope.
      let runs = 0
      const handlers = Stubborn.toLayer(() =>
        Effect.suspend(() => {
          const run = ++runs
          return run > 1 ? Effect.void : Effect.uninterruptible(
            Effect.sleep("6 seconds").pipe(Effect.andThen(Effect.sync(() => void done.push(run))))
          )
        })
      )

      const lockLost = Metric.value(Metrics.jobRuns.pipe(
        Metric.withAttributes({ name: "LostLockStubborn", queue, outcome: "lock-lost" })
      ))
      const before = (yield* lockLost).count

      yield* Effect.gen(function*() {
        yield* Stubborn.enqueue({})
        // The lock goes at 2.5s; the region does not end until 6s.
        yield* walk(4)
        expect(done).toEqual([])
        yield* walk(4)
      }).pipe(
        Effect.provide(harness(handlers, stealingStore(base), {
          ...options("interrupt"),
          onJobFailure: (failure) => Effect.sync(() => void failures.push(failure))
        }))
      )

      // The work ran to the end of its region, and only then was the run
      // dropped — no ack, and not counted as a failed attempt.
      expect(done).toEqual([1])
      expect(failures).toEqual([])
      expect((yield* lockLost).count - before).toBe(1)
    }))

  it.effect("a run that ignores the interrupt and fails is still reported", () =>
    Effect.gen(function*() {
      class Boom extends Schema.TaggedError<Boom>()("Boom", { reason: Schema.String }) {}
      class Failing extends Job.make("LostLockFailing", {
        payload: {},
        error: Boom,
        defaults: { attempts: 3 }
      }) {}
      const base = yield* MemoryJobStore.make
      const failures: Array<Worker.JobFailure> = []
      // Only run 1 fails; the re-queued job succeeds, so the count below is
      // this run's failure and nothing else.
      let failingRuns = 0
      const handlers = Failing.toLayer(() =>
        Effect.suspend(() =>
          ++failingRuns > 1 ? Effect.void : Effect.uninterruptible(
            Effect.sleep("6 seconds").pipe(Effect.andThen(new Boom({ reason: "unrelated" })))
          )
        )
      )

      yield* Effect.gen(function*() {
        yield* Failing.enqueue({})
        yield* walk(8)
      }).pipe(
        Effect.provide(harness(handlers, stealingStore(base), {
          ...options("interrupt"),
          onJobFailure: (failure) => Effect.sync(() => void failures.push(failure))
        }))
      )

      // A real failure that merely coincided with the lost lock must not be
      // swallowed: the branch only claims interrupt-only exits.
      expect(failures).toHaveLength(1)
      expect(failures[0]?.willRetry).toBe(true)
    }))

  it.effect("positive control: a cancel request still interrupts and acks cancelled", () =>
    Effect.gen(function*() {
      const base = yield* MemoryJobStore.make
      const { handlers, interruptedRuns, ticks, Ticking } = tickingJob("LostLockControl")

      yield* Effect.gen(function*() {
        const id = yield* Ticking.enqueue({})
        yield* walk(2)
        yield* Ticking.cancel(id)
        yield* walk(10)

        // Same handler, same option, an interrupt this harness can see —
        // so the assertions above are not passing vacuously.
        expect(interruptedRuns).toEqual([1])
        expect(ticks).not.toContain("run1-tick9")
        const record = yield* base.getJob(id)
        assert(Option.isSome(record))
        expect(record.value.state).toBe("cancelled")
      }).pipe(Effect.provide(harness(handlers, base, options("interrupt"))))
    }))
})
