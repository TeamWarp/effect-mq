import { assert, describe, expect, it } from "@effect/vitest"
import { Cause, Deferred, Effect, Exit, Fiber, Layer, Option, Ref, Result, Schema } from "effect"
import { TestClock } from "effect/testing"
import { Job, JobStore, MemoryJobStore, Worker } from "../src/index.ts"

const { JobId, QueueName } = JobStore

class SendFailure extends Schema.TaggedError<SendFailure>()("SendFailure", {
  reason: Schema.String
}) {}

/** Let all currently runnable fibers make progress. */
const settle = Effect.gen(function*() {
  for (let i = 0; i < 10; i++) {
    yield* Effect.yieldNow
  }
})

const rawRequest = (name: string, id?: string): JobStore.EnqueueRequest => ({
  id: id === undefined ? undefined : JobId(id),
  name,
  queue: QueueName("default"),
  payload: {},
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

describe("cancellation", () => {
  it.effect("cancel interrupts a running handler via the heartbeat and acks cancelled", () =>
    Effect.gen(function*() {
      const store = yield* MemoryJobStore.make
      const storeLayer = Layer.succeed(JobStore.JobStore, store)
      class Long extends Job.make("Long", { payload: {} }) {}
      const started = yield* Deferred.make<void>()
      const interrupted = yield* Ref.make(false)
      const handlers = Long.toLayer(() =>
        Deferred.succeed(started, void 0).pipe(
          Effect.andThen(Effect.never),
          Effect.onInterrupt(() => Ref.set(interrupted, true))
        )
      )

      yield* Effect.gen(function*() {
        const id = yield* Long.enqueue({})
        yield* Deferred.await(started)

        yield* Long.cancel(id)
        // The running fiber is only interrupted on the next lock heartbeat.
        yield* TestClock.adjust("1 second")
        yield* settle

        expect(yield* Ref.get(interrupted)).toBe(true)
        const status = yield* store.getJob(id)
        assert(Option.isSome(status))
        expect(status.value.state).toBe("cancelled")
        const ledger = yield* store.getAttempts(id)
        expect(ledger.at(-1)?.outcome).toBe("cancelled")

        // awaitResult treats cancellation as a defect, not a typed failure.
        const result = yield* Effect.exit(Long.awaitResult(id))
        assert(Exit.isFailure(result))
        const defect = Cause.findDefect(result.cause)
        assert(Result.isSuccess(defect))
        expect(defect.success).toBeInstanceOf(JobStore.JobCancelledError)
      }).pipe(
        Effect.provide(
          handlers.pipe(
            Layer.provideMerge(Worker.layer({ lockRenewInterval: "1 second" })),
            Layer.provideMerge(storeLayer)
          )
        )
      )
    }))

  it.effect("cancelling a delayed job is immediate; promote runs one now", () =>
    Effect.gen(function*() {
      const store = yield* MemoryJobStore.make
      const storeLayer = Layer.succeed(JobStore.JobStore, store)
      class Later extends Job.make("Later", { payload: { n: Schema.Number } }) {}
      const ran: Array<number> = []
      const handlers = Later.toLayer((payload) => Effect.sync(() => void ran.push(payload.n)))

      yield* Effect.gen(function*() {
        const doomed = yield* Later.enqueue({ n: 1 }, { delay: "1 hour" })
        const promoted = yield* Later.enqueue({ n: 2 }, { delay: "1 hour" })

        yield* Later.cancel(doomed)
        const status = yield* store.getJob(doomed)
        assert(Option.isSome(status))
        expect(status.value.state).toBe("cancelled")

        // No clock movement: promote alone must make the job runnable.
        yield* Later.promote(promoted)
        yield* settle
        expect(ran).toEqual([2])
      }).pipe(
        Effect.provide(
          handlers.pipe(
            Layer.provideMerge(Worker.layer()),
            Layer.provideMerge(storeLayer)
          )
        )
      )
    }))
})

describe("cancellation under load", () => {
  it.effect("cancelling a job with a slow finalizer does not stall other in-flight locks", () =>
    Effect.gen(function*() {
      const store = yield* MemoryJobStore.make
      const storeLayer = Layer.succeed(JobStore.JobStore, store)
      class SlowCleanup extends Job.make("SlowCleanup", { payload: {} }) {}
      class Bystander extends Job.make("Bystander", { payload: {} }) {}
      const slowStarted = yield* Deferred.make<void>()
      const bystanderStarted = yield* Deferred.make<void>()
      const handlers = Layer.mergeAll(
        SlowCleanup.toLayer(() =>
          Deferred.succeed(slowStarted, void 0).pipe(
            Effect.andThen(Effect.never),
            // The interrupt finalizer takes far longer than the lock: the
            // heartbeat must not wait for it.
            Effect.onInterrupt(() => Effect.sleep("2 minutes"))
          )
        ),
        Bystander.toLayer(() =>
          Deferred.succeed(bystanderStarted, void 0).pipe(Effect.andThen(Effect.never))
        )
      )

      yield* Effect.gen(function*() {
        const slowId = yield* SlowCleanup.enqueue({})
        const bystanderId = yield* Bystander.enqueue({})
        yield* Deferred.await(slowStarted)
        yield* Deferred.await(bystanderStarted)

        yield* SlowCleanup.cancel(slowId)
        // Ten heartbeats while the cancelled handler's finalizer is still
        // sleeping: the bystander's lock must keep being renewed.
        for (let i = 0; i < 10; i++) {
          yield* TestClock.adjust("1 second")
        }
        const bystander = yield* store.getJob(bystanderId)
        assert(Option.isSome(bystander))
        expect(bystander.value.state).toBe("active")
        expect(bystander.value.stalledCount).toBe(0)

        // Once the finalizer completes, the cancel ack lands.
        yield* TestClock.adjust("2 minutes")
        yield* settle
        const slow = yield* store.getJob(slowId)
        assert(Option.isSome(slow))
        expect(slow.value.state).toBe("cancelled")
      }).pipe(
        Effect.provide(
          handlers.pipe(
            Layer.provideMerge(Worker.layer({
              concurrency: 2,
              lockDuration: "3 seconds",
              lockRenewInterval: "1 second",
              stalledInterval: "2 seconds"
            })),
            Layer.provideMerge(storeLayer)
          )
        )
      )
    }))
})

describe("timeouts", () => {
  it.effect("a handler's own TimeoutError failure is not mistaken for the job timeout", () =>
    Effect.gen(function*() {
      const store = yield* MemoryJobStore.make
      const storeLayer = Layer.succeed(JobStore.JobStore, store)
      class Inner extends Job.make("Inner", {
        payload: {},
        error: Schema.Any,
        // Nothing is retryable: a TYPED failure must fail on attempt 1. A
        // defect (which the old timeout wrapper produced from the handler's
        // own TimeoutError) would bypass this predicate and burn the budget.
        retryable: () => false,
        defaults: { attempts: 3, timeout: "1 hour" }
      }) {}
      const handlers = Inner.toLayer(() =>
        // The handler's own internal timeout fires long before the job's.
        Effect.never.pipe(Effect.timeout("1 second"))
      )

      yield* Effect.gen(function*() {
        const id = yield* Inner.enqueue({})
        yield* settle
        yield* TestClock.adjust("1 second")
        yield* settle
        const status = yield* store.getJob(id)
        assert(Option.isSome(status))
        expect(status.value.state).toBe("failed")
        expect(status.value.attemptsMade).toBe(1)
      }).pipe(
        Effect.provide(
          handlers.pipe(
            Layer.provideMerge(Worker.layer()),
            Layer.provideMerge(storeLayer)
          )
        )
      )
    }))

  it.effect("defaults.timeout interrupts the handler; the timeout consumes attempts", () =>
    Effect.gen(function*() {
      const store = yield* MemoryJobStore.make
      const storeLayer = Layer.succeed(JobStore.JobStore, store)
      class Slow extends Job.make("Slow", {
        payload: {},
        defaults: { attempts: 2, timeout: "5 seconds" }
      }) {}
      let runs = 0
      const handlers = Slow.toLayer(() =>
        Effect.sync(() => void runs++).pipe(Effect.andThen(Effect.never))
      )

      yield* Effect.gen(function*() {
        const id = yield* Slow.enqueue({})
        yield* settle
        expect(runs).toBe(1)

        // Attempt 1 times out; with no backoff the retry is claimed at once.
        yield* TestClock.adjust("5 seconds")
        yield* settle
        expect(runs).toBe(2)

        // Attempt 2 times out too — the budget is spent, the job fails.
        yield* TestClock.adjust("5 seconds")
        yield* settle
        const status = yield* store.getJob(id)
        assert(Option.isSome(status))
        expect(status.value.state).toBe("failed")
        const outcomes = (yield* store.getAttempts(id)).map((attempt) => attempt.outcome)
        expect(outcomes).toEqual(["retried", "failed"])
      }).pipe(
        Effect.provide(
          handlers.pipe(
            Layer.provideMerge(Worker.layer()),
            Layer.provideMerge(storeLayer)
          )
        )
      )
    }))

  it.effect("a per-enqueue timeout overrides the definition default", () =>
    Effect.gen(function*() {
      const store = yield* MemoryJobStore.make
      const storeLayer = Layer.succeed(JobStore.JobStore, store)
      class Slow extends Job.make("Slow", {
        payload: {},
        defaults: { timeout: "1 hour" }
      }) {}
      const handlers = Slow.toLayer(() => Effect.never)

      yield* Effect.gen(function*() {
        const id = yield* Slow.enqueue({}, { timeout: "1 second" })
        yield* settle
        yield* TestClock.adjust("1 second")
        yield* settle
        const status = yield* store.getJob(id)
        assert(Option.isSome(status))
        expect(status.value.state).toBe("failed")
      }).pipe(
        Effect.provide(
          handlers.pipe(
            Layer.provideMerge(Worker.layer()),
            Layer.provideMerge(storeLayer)
          )
        )
      )
    }))
})

describe("unrecoverable errors", () => {
  it.effect("Job.unrecoverable skips the remaining retry budget", () =>
    Effect.gen(function*() {
      const store = yield* MemoryJobStore.make
      const storeLayer = Layer.succeed(JobStore.JobStore, store)
      class Flaky extends Job.make("Flaky", {
        payload: {},
        error: SendFailure,
        defaults: { attempts: 5 }
      }) {}
      const handlers = Flaky.toLayer(() =>
        Effect.fail(Job.unrecoverable(new SendFailure({ reason: "tenant deleted" })))
      )

      yield* Effect.gen(function*() {
        const id = yield* Flaky.enqueue({})
        yield* settle

        const status = yield* store.getJob(id)
        assert(Option.isSome(status))
        expect(status.value.state).toBe("failed")
        expect(status.value.attemptsMade).toBe(1)
        expect((yield* store.getAttempts(id)).map((attempt) => attempt.outcome))
          .toEqual(["failed"])

        // The typed error still round-trips to awaiters.
        const result = yield* Effect.exit(Flaky.awaitResult(id))
        assert(Exit.isFailure(result))
        const error = Cause.findErrorOption(result.cause)
        assert(Option.isSome(error))
        expect(error.value).toBeInstanceOf(SendFailure)
      }).pipe(
        Effect.provide(
          handlers.pipe(
            Layer.provideMerge(Worker.layer()),
            Layer.provideMerge(storeLayer)
          )
        )
      )
    }))

  it.effect("a retryable predicate routes matching failures past the retry budget", () =>
    Effect.gen(function*() {
      const store = yield* MemoryJobStore.make
      const storeLayer = Layer.succeed(JobStore.JobStore, store)
      class Picky extends Job.make("Picky", {
        payload: { fatal: Schema.Boolean },
        error: SendFailure,
        idempotencyKey: ({ fatal }) => `picky-${fatal}`,
        retryable: (error) => error.reason !== "fatal",
        defaults: { attempts: 3 }
      }) {}
      const handlers = Picky.toLayer((payload) =>
        Effect.fail(new SendFailure({ reason: payload.fatal ? "fatal" : "transient" }))
      )

      yield* Effect.gen(function*() {
        const fatal = yield* Picky.enqueue({ fatal: true })
        yield* settle
        const fatalStatus = yield* store.getJob(fatal)
        assert(Option.isSome(fatalStatus))
        expect(fatalStatus.value.state).toBe("failed")
        expect(fatalStatus.value.attemptsMade).toBe(1)

        // The transient failure burns the whole budget (immediate retries,
        // no backoff) — the contrast with the fatal path is the attempt count.
        const transient = yield* Picky.enqueue({ fatal: false })
        yield* settle
        const transientStatus = yield* store.getJob(transient)
        assert(Option.isSome(transientStatus))
        expect(transientStatus.value.state).toBe("failed")
        expect(transientStatus.value.attemptsMade).toBe(3)
      }).pipe(
        Effect.provide(
          handlers.pipe(
            Layer.provideMerge(Worker.layer()),
            Layer.provideMerge(storeLayer)
          )
        )
      )
    }))
})

describe("throwing predicates", () => {
  it.effect("a throwing retryable predicate never leaves the job stuck active", () =>
    Effect.gen(function*() {
      const store = yield* MemoryJobStore.make
      const storeLayer = Layer.succeed(JobStore.JobStore, store)
      class Booby extends Job.make("Booby", {
        payload: {},
        error: SendFailure,
        retryable: () => {
          throw new Error("predicate exploded")
        },
        defaults: { attempts: 2 }
      }) {}
      const handlers = Booby.toLayer(() => Effect.fail(new SendFailure({ reason: "x" })))

      yield* Effect.gen(function*() {
        const id = yield* Booby.enqueue({})
        yield* settle
        // A throw is treated as "retryable": the budget runs its course and
        // the job is acked — never abandoned in the active state.
        const status = yield* store.getJob(id)
        assert(Option.isSome(status))
        expect(status.value.state).toBe("failed")
        expect(status.value.attemptsMade).toBe(2)
      }).pipe(
        Effect.provide(
          handlers.pipe(
            Layer.provideMerge(Worker.layer()),
            Layer.provideMerge(storeLayer)
          )
        )
      )
    }))
})

describe("pause and resume", () => {
  it.effect("a paused queue is not claimed; resume wakes the takers", () =>
    Effect.gen(function*() {
      const store = yield* MemoryJobStore.make
      const storeLayer = Layer.succeed(JobStore.JobStore, store)
      class Work extends Job.make("Work", { payload: {} }) {}
      let runs = 0
      const handlers = Work.toLayer(() => Effect.sync(() => void runs++))

      yield* Effect.gen(function*() {
        yield* store.pause(QueueName("default"))
        const id = yield* Work.enqueue({})
        yield* settle
        expect(runs).toBe(0)
        const paused = yield* store.getJob(id)
        assert(Option.isSome(paused))
        expect(paused.value.state).toBe("waiting")

        yield* store.resume(QueueName("default"))
        yield* settle
        expect(runs).toBe(1)
        const done = yield* store.getJob(id)
        assert(Option.isSome(done))
        expect(done.value.state).toBe("completed")
      }).pipe(
        Effect.provide(
          handlers.pipe(
            Layer.provideMerge(Worker.layer()),
            Layer.provideMerge(storeLayer)
          )
        )
      )
    }))
})

describe("trace propagation", () => {
  it.effect("the handler span joins the producer's trace and carries effectMqJobId", () =>
    Effect.gen(function*() {
      const store = yield* MemoryJobStore.make
      const storeLayer = Layer.succeed(JobStore.JobStore, store)
      class Traced extends Job.make("Traced", { payload: {} }) {}
      let seen: {
        name: string
        jobId: unknown
        parentTraceId: string | undefined
      } | undefined
      const handlers = Traced.toLayer(() =>
        Effect.gen(function*() {
          const span = yield* Effect.currentSpan
          seen = {
            name: span.name,
            jobId: span.attributes.get("effectMqJobId"),
            parentTraceId: Option.isSome(span.parent) ? span.parent.value.traceId : undefined
          }
        }).pipe(Effect.orDie)
      )

      let producerTraceId: string | undefined
      let jobId: JobStore.JobId | undefined
      yield* Effect.gen(function*() {
        yield* Effect.gen(function*() {
          const span = yield* Effect.currentSpan
          producerTraceId = span.traceId
          jobId = yield* Traced.enqueue({})
        }).pipe(Effect.withSpan("producer-business-op"))
        yield* settle
      }).pipe(
        Effect.provide(
          handlers.pipe(
            Layer.provideMerge(Worker.layer()),
            Layer.provideMerge(storeLayer)
          )
        )
      )

      assert(seen !== undefined)
      expect(seen.name).toBe("Traced.run")
      expect(seen.jobId).toBe(jobId)
      // The handler's span parent chain carries the PRODUCER's trace id —
      // producer -> handler connect across the storage boundary.
      expect(seen.parentTraceId).toBe(producerTraceId)
    }))

  it.effect("auto linking: delayed jobs start their own trace with a causal link", () =>
    Effect.gen(function*() {
      const store = yield* MemoryJobStore.make
      const storeLayer = Layer.succeed(JobStore.JobStore, store)
      class Later extends Job.make("LaterTraced", { payload: {} }) {}
      let seen: {
        ownTraceId: string
        parentTraceId: string | undefined
        linkTraceIds: Array<string>
      } | undefined
      const handlers = Later.toLayer(() =>
        Effect.gen(function*() {
          const span = yield* Effect.currentSpan
          seen = {
            ownTraceId: span.traceId,
            parentTraceId: Option.isSome(span.parent) ? span.parent.value.traceId : undefined,
            linkTraceIds: span.links.map((link) => link.span.traceId)
          }
        }).pipe(Effect.orDie)
      )

      let producerTraceId: string | undefined
      yield* Effect.gen(function*() {
        yield* Effect.gen(function*() {
          producerTraceId = (yield* Effect.currentSpan).traceId
          yield* Later.enqueue({}, { delay: "5 seconds" })
        }).pipe(Effect.withSpan("scheduling-op"))
        yield* settle
        yield* TestClock.adjust("5 seconds")
        yield* settle
      }).pipe(
        Effect.provide(
          handlers.pipe(
            Layer.provideMerge(Worker.layer()),
            Layer.provideMerge(storeLayer)
          )
        )
      )

      assert(seen !== undefined && producerTraceId !== undefined)
      // Own trace root (no producer parent), causally linked back.
      expect(seen.ownTraceId).not.toBe(producerTraceId)
      expect(seen.parentTraceId).not.toBe(producerTraceId)
      expect(seen.linkTraceIds).toEqual([producerTraceId])
    }))

  it.effect("traceLinking overrides: parent forces continuation, none drops the edge", () =>
    Effect.gen(function*() {
      const run = (linking: "parent" | "none") =>
        Effect.gen(function*() {
          const store = yield* MemoryJobStore.make
          const storeLayer = Layer.succeed(JobStore.JobStore, store)
          class Forced extends Job.make(`Forced${linking}`, { payload: {} }) {}
          let seen: { parentTraceId: string | undefined; links: number } | undefined
          const handlers = Forced.toLayer(() =>
            Effect.gen(function*() {
              const span = yield* Effect.currentSpan
              seen = {
                parentTraceId: Option.isSome(span.parent) ? span.parent.value.traceId : undefined,
                links: span.links.length
              }
            }).pipe(Effect.orDie)
          )
          let producerTraceId: string | undefined
          yield* Effect.gen(function*() {
            yield* Effect.gen(function*() {
              producerTraceId = (yield* Effect.currentSpan).traceId
              // Delayed: auto would link, the override must win.
              yield* Forced.enqueue({}, { delay: "1 second" })
            }).pipe(Effect.withSpan("op"))
            yield* settle
            yield* TestClock.adjust("1 second")
            yield* settle
          }).pipe(
            Effect.provide(
              handlers.pipe(
                Layer.provideMerge(Worker.layer({ traceLinking: linking })),
                Layer.provideMerge(storeLayer)
              )
            )
          )
          assert(seen !== undefined && producerTraceId !== undefined)
          return { seen, producerTraceId }
        })

      const parent = yield* run("parent")
      expect(parent.seen.parentTraceId).toBe(parent.producerTraceId)
      expect(parent.seen.links).toBe(0)

      const none = yield* run("none")
      expect(none.seen.parentTraceId).not.toBe(none.producerTraceId)
      expect(none.seen.links).toBe(0)
    }))

  it.effect("handlerSpanName customizes the run span's name", () =>
    Effect.gen(function*() {
      const store = yield* MemoryJobStore.make
      const storeLayer = Layer.succeed(JobStore.JobStore, store)
      class Named extends Job.make("Named", { payload: {} }) {}
      let spanName: string | undefined
      const handlers = Named.toLayer(() =>
        Effect.gen(function*() {
          spanName = (yield* Effect.currentSpan).name
        }).pipe(Effect.orDie)
      )

      yield* Effect.gen(function*() {
        yield* Named.enqueue({})
        yield* settle
      }).pipe(
        Effect.provide(
          handlers.pipe(
            Layer.provideMerge(Worker.layer({
              handlerSpanName: (context) => `queue-processor ${context.queue}/${context.name}`
            })),
            Layer.provideMerge(storeLayer)
          )
        )
      )
      expect(spanName).toBe("queue-processor default/Named")
    }))
})

describe("absolute-time scheduling", () => {
  it.effect("`at` runs the job at the wall-clock instant without duration math", () =>
    Effect.gen(function*() {
      const store = yield* MemoryJobStore.make
      const storeLayer = Layer.succeed(JobStore.JobStore, store)
      class Invite extends Job.make("Invite", { payload: { employeeId: Schema.String } }) {}
      const ran: Array<string> = []
      const handlers = Invite.toLayer(({ employeeId }) => Effect.sync(() => void ran.push(employeeId)))

      yield* Effect.gen(function*() {
        // TestClock starts at the epoch; schedule for one hour in.
        const id = yield* Invite.enqueue({ employeeId: "e1" }, { at: 3_600_000 })
        yield* settle
        expect(ran).toEqual([])
        const status = yield* store.getJob(id)
        assert(Option.isSome(status))
        expect(status.value.state).toBe("delayed")
        expect(status.value.runAt).toBe(3_600_000)

        yield* TestClock.adjust("1 hour")
        yield* settle
        expect(ran).toEqual(["e1"])

        // An `at` in the past runs immediately.
        yield* Invite.enqueue({ employeeId: "e2" }, { at: 0 })
        yield* settle
        expect(ran).toEqual(["e1", "e2"])
      }).pipe(
        Effect.provide(
          handlers.pipe(
            Layer.provideMerge(Worker.layer()),
            Layer.provideMerge(storeLayer)
          )
        )
      )
    }))

  it("setting both `delay` and `at` is a compile error", () => {
    class Both extends Job.make("Both", { payload: {} }) {}
    // @ts-expect-error - the RunTimeInput union makes delay/at exclusive
    const invalid = () => Both.enqueue({}, { delay: "5 seconds", at: 123 })
    const delayOnly = () => Both.enqueue({}, { delay: "5 seconds" })
    const atOnly = () => Both.enqueue({}, { at: 123 })
    void invalid
    void delayOnly
    void atOnly
    expect(true).toBe(true)
  })

  it.effect("schedule, reschedule by key, cancel by key — the invite lifecycle", () =>
    Effect.gen(function*() {
      const store = yield* MemoryJobStore.make
      const storeLayer = Layer.succeed(JobStore.JobStore, store)
      class SendInvite extends Job.make("SendInvite", {
        payload: { employeeId: Schema.String },
        dedupe: ({ employeeId }) => ({ key: employeeId, replace: true })
      }) {}

      yield* Effect.gen(function*() {
        const first = yield* SendInvite.enqueue({ employeeId: "e1" }, { at: 3_600_000 })

        // Reschedule = same idempotent call with a new time: same job moves.
        const second = yield* SendInvite.enqueue({ employeeId: "e1" }, { at: 7_200_000 })
        expect(second).toBe(first)
        const moved = yield* store.getJob(first)
        assert(Option.isSome(moved))
        expect(moved.value.runAt).toBe(7_200_000)
        expect(moved.value.state).toBe("delayed")

        // They are not onboarding after all.
        expect(yield* SendInvite.cancelByKey("e1")).toBe(true)
        const gone = yield* store.getJob(first)
        assert(Option.isSome(gone))
        expect(gone.value.state).toBe("cancelled")
        expect(yield* SendInvite.cancelByKey("e1")).toBe(false)
      }).pipe(Effect.provide(storeLayer))
    }))
})

describe("repeatable schedules", () => {
  it.effect("a cron schedule enqueues one slot-deterministic job per occurrence", () =>
    Effect.gen(function*() {
      const store = yield* MemoryJobStore.make
      const storeLayer = Layer.succeed(JobStore.JobStore, store)
      class Tick extends Job.make("Tick", { payload: { label: Schema.String } }) {}
      const seen: Array<string> = []
      const handlers = Tick.toLayer((payload, ctx) =>
        Effect.sync(() => void seen.push(`${payload.label}:${ctx.jobId}`))
      )

      yield* Effect.gen(function*() {
        const key = yield* Tick.schedule("hourly", {
          cron: "0 * * * *",
          payload: { label: "sweep" }
        })
        expect(key).toBe("Tick/hourly")

        // TestClock starts at the epoch; the first occurrence is 01:00.
        yield* TestClock.adjust("1 hour")
        yield* settle
        expect(seen).toEqual(["sweep:sched/Tick/hourly/3600000"])

        const job = yield* store.getJob(JobId("sched/Tick/hourly/3600000"))
        assert(Option.isSome(job))
        expect(job.value.state).toBe("completed")
        expect(job.value.metadata.scheduledFor).toBe("1970-01-01T01:00:00.000Z")

        yield* TestClock.adjust("1 hour")
        yield* settle
        expect(seen.length).toBe(2)

        // Removing the schedule stops future occurrences.
        expect(yield* Tick.unschedule("hourly")).toBe(true)
        yield* TestClock.adjust("3 hours")
        yield* settle
        expect(seen.length).toBe(2)
      }).pipe(
        Effect.provide(
          handlers.pipe(
            Layer.provideMerge(Worker.layer({ scheduleSweepInterval: "1 second" })),
            Layer.provideMerge(storeLayer)
          )
        )
      )
    }))

  it.effect("an every-interval schedule first fires one interval from now", () =>
    Effect.gen(function*() {
      const store = yield* MemoryJobStore.make
      const storeLayer = Layer.succeed(JobStore.JobStore, store)
      class Beat extends Job.make("Beat", { payload: {} }) {}
      let runs = 0
      const handlers = Beat.toLayer(() => Effect.sync(() => void runs++))

      yield* Effect.gen(function*() {
        yield* Beat.schedule("pulse", { every: "10 seconds", payload: {} })
        yield* settle
        expect(runs).toBe(0)

        yield* TestClock.adjust("10 seconds")
        yield* settle
        expect(runs).toBe(1)

        yield* TestClock.adjust("10 seconds")
        yield* settle
        expect(runs).toBe(2)

        // Re-scheduling the same key replaces the cadence, not duplicates it.
        yield* Beat.schedule("pulse", { every: "1 hour", payload: {} })
        yield* TestClock.adjust("30 seconds")
        yield* settle
        expect(runs).toBe(2)
      }).pipe(
        Effect.provide(
          handlers.pipe(
            Layer.provideMerge(Worker.layer({ scheduleSweepInterval: "1 second" })),
            Layer.provideMerge(storeLayer)
          )
        )
      )
    }))

  it.effect("schedule options must set exactly one of cron or every", () =>
    Effect.gen(function*() {
      class Bad extends Job.make("Bad", { payload: {} }) {}
      const neither = yield* Effect.exit(Bad.schedule("x", { payload: {} }))
      expect(Exit.hasDies(neither)).toBe(true)
      const both = yield* Effect.exit(
        Bad.schedule("x", { cron: "* * * * *", every: "1 minute", payload: {} })
      )
      expect(Exit.hasDies(both)).toBe(true)
      const invalid = yield* Effect.exit(Bad.schedule("x", { cron: "not a cron", payload: {} }))
      expect(Exit.hasDies(invalid)).toBe(true)
    }).pipe(Effect.provide(MemoryJobStore.layer)))
})

describe("deduplication", () => {
  it.effect("definition-level dedupe derives from the payload; per-call overrides win", () =>
    Effect.gen(function*() {
      const store = yield* MemoryJobStore.make
      const storeLayer = Layer.succeed(JobStore.JobStore, store)
      class Sync extends Job.make("Sync", {
        payload: { employerId: Schema.String },
        dedupe: ({ employerId }) => employerId
      }) {}

      yield* Effect.gen(function*() {
        const first = yield* Sync.enqueue({ employerId: "e1" })
        const deduped = yield* Sync.enqueue({ employerId: "e1" })
        expect(deduped).toBe(first)

        const different = yield* Sync.enqueue({ employerId: "e2" })
        expect(different).not.toBe(first)

        // A per-call dedupe replaces the definition's derived key entirely.
        const overridden = yield* Sync.enqueue({ employerId: "e1" }, { dedupe: "custom" })
        expect(overridden).not.toBe(first)
      }).pipe(Effect.provide(storeLayer))
    }))

  it.effect("dedupe works alongside idGenerator without touching the generated id", () =>
    Effect.gen(function*() {
      let n = 0
      const store = yield* MemoryJobStore.makeWith({
        idGenerator: () => `job_${++n}`
      })
      const request = {
        id: undefined,
        name: "Gen",
        queue: QueueName("default"),
        payload: {},
        metadata: {},
        priority: 0,
        attemptsMax: 1,
        backoff: undefined,
        keep: undefined,
        timeoutMs: undefined,
        dedupe: { key: "k", ttlMs: undefined, extend: false, replace: false },
        trace: undefined,
        delayMs: 0
      }
      const first = yield* store.enqueue(request)
      expect(first).toEqual({ id: "job_1", duplicate: false })
      // Deduplicated: the generator is not consulted, the id not rewritten.
      const second = yield* store.enqueue(request)
      expect(second).toEqual({ id: "job_1", duplicate: true })
      expect(n).toBe(1)
    }).pipe(Effect.scoped))

  it.effect("dedupe extend without a ttl dies with a clear error", () =>
    Effect.gen(function*() {
      class Bad extends Job.make("Bad", { payload: {} }) {}
      const result = yield* Effect.exit(
        Bad.enqueue({}, { dedupe: { key: "x", extend: true } })
      )
      expect(Exit.hasDies(result)).toBe(true)
    }).pipe(Effect.provide(MemoryJobStore.layer)))
})

describe("wake registry", () => {
  it.live("waking a taker that immediately re-parks does not livelock the store", () =>
    Effect.gen(function*() {
      const store = yield* MemoryJobStore.make
      let tokens = 0
      // The Worker taker shape: claim; on Empty, park on awaitWake; repeat.
      // Two takers + one job means the loser re-parks synchronously inside
      // the winner's wake signal — the regression this test pins hung the
      // whole event loop here.
      const taker = Effect.gen(function*() {
        while (true) {
          const result = yield* store.claim({
            queue: QueueName("default"),
            names: ["Only"],
            token: `t-${++tokens}`,
            lockDurationMs: 5_000
          })
          if (result._tag === "Claimed") return result.job.id
          yield* store.awaitWake([QueueName("default")], result.wakeToken)
        }
      })
      const first = yield* Effect.forkChild(taker)
      const second = yield* Effect.forkChild(taker)
      yield* Effect.sleep("50 millis")

      yield* store.enqueue(rawRequest("Only", "one")).pipe(Effect.timeout("2 seconds"))
      yield* store.enqueue(rawRequest("Only", "two")).pipe(Effect.timeout("2 seconds"))
      const claimed = yield* Fiber.join(first).pipe(
        Effect.zip(Fiber.join(second)),
        Effect.timeout("2 seconds")
      )
      expect(new Set(claimed)).toEqual(new Set(["one", "two"]))
    }))
})

describe("history TTL", () => {
  it.effect("a per-state historyTtl sweeps only the states it names", () =>
    Effect.gen(function*() {
      const store = yield* MemoryJobStore.makeWith({
        historyTtl: { failed: "1 minute" },
        historySweepInterval: "10 seconds"
      })
      const done = (yield* store.enqueue(rawRequest("Split", "done"))).id
      const claimA = yield* store.claim({
        queue: QueueName("default"),
        names: ["Split"],
        token: "a",
        lockDurationMs: 60_000
      })
      assert(claimA._tag === "Claimed")
      yield* store.ack(done, "a", { _tag: "Complete", exit: undefined })

      const bad = (yield* store.enqueue(rawRequest("Split", "bad"))).id
      const claimB = yield* store.claim({
        queue: QueueName("default"),
        names: ["Split"],
        token: "b",
        lockDurationMs: 60_000
      })
      assert(claimB._tag === "Claimed")
      yield* store.ack(bad, "b", { _tag: "Fail", exit: undefined })

      yield* TestClock.adjust("2 minutes")
      expect(Option.isNone(yield* store.getJob(bad))).toBe(true)
      expect(Option.isSome(yield* store.getJob(done))).toBe(true)
    }).pipe(Effect.scoped))

  it.effect("the timer sweep honours a stricter per-job keep.age without further acks", () =>
    Effect.gen(function*() {
      const store = yield* MemoryJobStore.makeWith({
        historyTtl: "1 hour",
        historySweepInterval: "10 seconds"
      })
      const id = (yield* store.enqueue({
        ...rawRequest("Quiet", "quiet-1"),
        keep: { completed: { ageMs: 60_000 } }
      })).id
      const claim = yield* store.claim({
        queue: QueueName("default"),
        names: ["Quiet"],
        token: "t",
        lockDurationMs: 60_000
      })
      assert(claim._tag === "Claimed")
      yield* store.ack(id, "t", { _tag: "Complete", exit: undefined })

      // No further acks for this name: only the timer can prune it, and it
      // must use the row's 1-minute keep age, not the 1-hour ceiling.
      yield* TestClock.adjust("2 minutes")
      expect(Option.isNone(yield* store.getJob(id))).toBe(true)
    }).pipe(Effect.scoped))

  it.effect("terminal jobs are swept after the retention window; live jobs survive", () =>
    Effect.gen(function*() {
      const store = yield* MemoryJobStore.makeWith({
        historyTtl: "1 minute",
        historySweepInterval: "10 seconds"
      })

      const doneId = (yield* store.enqueue(rawRequest("Sweep", "done"))).id
      const claimed = yield* store.claim({
        queue: QueueName("default"),
        names: ["Sweep"],
        token: "t",
        lockDurationMs: 60_000
      })
      assert(claimed._tag === "Claimed")
      yield* store.ack(doneId, "t", { _tag: "Complete", exit: undefined })

      const waitingId = (yield* store.enqueue(rawRequest("Sweep", "still-waiting"))).id

      // Within the window both remain.
      yield* TestClock.adjust("30 seconds")
      expect(Option.isSome(yield* store.getJob(doneId))).toBe(true)

      // Past the window only the terminal record is swept.
      yield* TestClock.adjust("1 minute")
      expect(Option.isNone(yield* store.getJob(doneId))).toBe(true)
      expect(Option.isSome(yield* store.getJob(waitingId))).toBe(true)
    }).pipe(Effect.scoped))
})
