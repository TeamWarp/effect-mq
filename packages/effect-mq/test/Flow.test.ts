import { assert, describe, expect, it } from "@effect/vitest"
import { Cause, type Duration, Effect, Exit, Fiber, Layer, Option, Schema } from "effect"
import { TestClock } from "effect/testing"
import { Flow, Job, JobStore, MemoryJobStore, Worker } from "../src/index.ts"

class SendFailure extends Schema.TaggedError<SendFailure>()("SendFailure", {
  reason: Schema.String
}) {}

const ChildStore = JobStore.named("flow-children")

class SendEmail extends Job.make("SendEmail", {
  payload: { userId: Schema.String },
  success: Schema.String,
  error: SendFailure,
  queue: "email",
  store: ChildStore
}) {}

class SendDigest extends Job.make("SendDigest", {
  payload: { tenant: Schema.String },
  success: Schema.Struct({ sent: Schema.Number, failed: Schema.Number })
}) {}

/** Let all currently runnable fibers make progress. */
const settle = Effect.gen(function*() {
  for (let i = 0; i < 10; i++) {
    yield* Effect.yieldNow
  }
})

/** Alternate fiber progress and clock time so both workers drain. */
const drain = (steps: number, duration: Duration.Input = "1 second") =>
  Effect.gen(function*() {
    yield* settle
    for (let i = 0; i < steps; i++) {
      yield* TestClock.adjust(duration)
      yield* settle
    }
  })

const stores = Layer.mergeAll(MemoryJobStore.layer, MemoryJobStore.layerFor(ChildStore))

describe("Flow", () => {
  it.effect("fans out cross-store, reports back, and collects typed results", () =>
    Effect.gen(function*() {
      const DigestFlow = Flow.make("digest", {
        parent: SendDigest,
        children: [SendEmail]
      })
      let captured: Flow.ChildResults<typeof SendEmail> | undefined
      const parentSide = DigestFlow.toLayer({
        fanOut: (payload) =>
          Effect.succeed(Flow.children(SendEmail, [
            { key: "u1", payload: { userId: `${payload.tenant}-u1` } },
            { key: "u2", payload: { userId: `${payload.tenant}-u2` } },
            { key: "u3", payload: { userId: `${payload.tenant}-u3` } }
          ])),
        collect: (_payload, results) =>
          Effect.sync(() => {
            captured = results
            return { sent: results.completed.length, failed: results.failed.length }
          })
      }).pipe(Layer.provide(Worker.layer()))
      const childSide = SendEmail.toLayer((payload) =>
        payload.userId.endsWith("u2")
          ? new SendFailure({ reason: "bounced" })
          : Effect.succeed(`sent:${payload.userId}`)
      ).pipe(Layer.provide(Worker.layer({ store: ChildStore, flows: [DigestFlow] })))

      yield* Effect.gen(function*() {
        const fiber = yield* Effect.forkChild(DigestFlow.execute({ tenant: "acme" }))
        yield* drain(5)
        const result = yield* Fiber.join(fiber)
        expect(result).toEqual({ sent: 2, failed: 1 })

        assert(captured !== undefined)
        expect(captured.completed.map((child) => child.value).toSorted()).toEqual([
          "sent:acme-u1",
          "sent:acme-u3"
        ])
        expect(captured.cancelled).toEqual([])
        const failure = captured.failed[0]
        assert(failure !== undefined)
        expect(failure.key).toBe("u2")
        expect(failure.name).toBe("SendEmail")
        const typed = Cause.findErrorOption(failure.cause)
        assert(Option.isSome(typed))
        expect(typed.value._tag).toBe("SendFailure")
        expect(typed.value.reason).toBe("bounced")

        // The children are real, deterministic-id jobs in the CHILD store,
        // carrying the parent envelope.
        const childStore = yield* ChildStore
        const list = yield* childStore.list({ name: "SendEmail" })
        expect(list.items).toHaveLength(3)
        for (const child of list.items) {
          expect(child.id.startsWith("flow/effect-mq/JobStore/")).toBe(true)
          expect(child.parent?.flowName).toBe("digest")
          expect(child.parent?.parentStoreKey).toBe("effect-mq/JobStore")
        }

        // Parent ledger: fanned-out, then the collect completion.
        const parentStore = yield* JobStore.JobStore
        const parents = yield* parentStore.list({ name: "SendDigest" })
        const parentId = parents.items[0]?.id
        assert(parentId !== undefined)
        const attempts = yield* parentStore.getAttempts(parentId)
        expect(attempts.map((attempt) => attempt.outcome)).toEqual(["fanned-out", "completed"])
      }).pipe(Effect.provide(
        Layer.mergeAll(parentSide, childSide).pipe(Layer.provideMerge(stores))
      ))
    }))

  it.effect("childResults exposes the recorded rows outside the handler", () =>
    Effect.gen(function*() {
      const DigestFlow = Flow.make("digest", {
        parent: SendDigest,
        children: [SendEmail]
      })
      const parentSide = DigestFlow.toLayer({
        fanOut: () => Effect.succeed(Flow.children(SendEmail, [{ key: "only", payload: { userId: "u" } }])),
        collect: (_payload, results) =>
          Effect.succeed({ sent: results.completed.length, failed: results.failed.length })
      }).pipe(Layer.provide(Worker.layer()))
      const childSide = SendEmail.toLayer(() => Effect.succeed("ok")).pipe(
        Layer.provide(Worker.layer({ store: ChildStore, flows: [DigestFlow] }))
      )

      yield* Effect.gen(function*() {
        const flowId = yield* DigestFlow.enqueue({ tenant: "acme" })
        yield* drain(3)
        const results = yield* DigestFlow.childResults(flowId)
        expect(results.completed).toEqual([{ key: "only", name: "SendEmail", value: "ok" }])
      }).pipe(Effect.provide(
        Layer.mergeAll(parentSide, childSide).pipe(Layer.provideMerge(stores))
      ))
    }))

  it.effect("fail-fast settles the parent and cascades cancels into the child store", () =>
    Effect.gen(function*() {
      const FailFastFlow = Flow.make("digest", {
        parent: SendDigest,
        children: [SendEmail],
        onChildFailure: "fail"
      })
      const parentSide = FailFastFlow.toLayer({
        fanOut: () =>
          Effect.succeed(Flow.children(SendEmail, [
            { key: "bad", payload: { userId: "bad" } },
            { key: "slow", payload: { userId: "slow" } }
          ])),
        collect: () => Effect.die(new Error("collect must not run on a fail-fast settle"))
      }).pipe(Layer.provide(Worker.layer()))
      const childSide = SendEmail.toLayer((payload) =>
        payload.userId === "bad"
          ? new SendFailure({ reason: "hard bounce" })
          : Effect.never
      ).pipe(Layer.provide(Worker.layer({
        store: ChildStore,
        flows: [FailFastFlow],
        concurrency: 2
      })))

      yield* Effect.gen(function*() {
        const fiber = yield* Effect.forkChild(Effect.exit(FailFastFlow.execute({ tenant: "acme" })))
        yield* drain(3)

        const parentStore = yield* JobStore.JobStore
        const parents = yield* parentStore.list({ name: "SendDigest" })
        const parent = parents.items[0]
        assert(parent !== undefined)
        expect(parent.state).toBe("failed")
        expect(parent.failedReason).toContain("bad")

        // awaitResult dies: the parent failed store-side without an exit.
        const exit = yield* Fiber.join(fiber)
        assert(Exit.isFailure(exit))
        expect(Cause.hasInterruptsOnly(exit.cause)).toBe(false)

        // The sweeper cascades the still-running sibling: cancel delivered
        // cross-store, handler interrupted on the child worker's heartbeat.
        yield* drain(3, "30 seconds")
        const childStore = yield* ChildStore
        const slow = yield* childStore.getJob(JobStore.JobId(`flow/effect-mq/JobStore/${parent.id}/slow`))
        assert(Option.isSome(slow))
        expect(slow.value.state).toBe("cancelled")

        // Cascade work drains to empty once delivered.
        const work = yield* parentStore.flowSweepWork({ pendingAgeMs: 0 })
        expect(work.cascade).toEqual([])
      }).pipe(Effect.provide(
        Layer.mergeAll(parentSide, childSide).pipe(Layer.provideMerge(stores))
      ))
    }))

  it.effect("a worker without the flow registration fails children visibly and the sweeper reports them", () =>
    Effect.gen(function*() {
      const DigestFlow = Flow.make("digest", {
        parent: SendDigest,
        children: [SendEmail]
      })
      let collected: Flow.ChildResults<typeof SendEmail> | undefined
      const parentSide = DigestFlow.toLayer({
        fanOut: () => Effect.succeed(Flow.children(SendEmail, [{ key: "u1", payload: { userId: "u1" } }])),
        collect: (_payload, results) =>
          Effect.sync(() => {
            collected = results
            return { sent: results.completed.length, failed: results.failed.length }
          })
      }).pipe(Layer.provide(Worker.layer()))
      // Misconfigured: runs SendEmail but has no `flows` registration.
      const childSide = SendEmail.toLayer(() => Effect.succeed("ok")).pipe(
        Layer.provide(Worker.layer({ store: ChildStore }))
      )

      yield* Effect.gen(function*() {
        const fiber = yield* Effect.forkChild(DigestFlow.execute({ tenant: "acme" }))
        yield* drain(2)

        // The child was failed unrecoverably, visibly, in its own store.
        const childStore = yield* ChildStore
        const children = yield* childStore.list({ name: "SendEmail" })
        expect(children.items[0]?.state).toBe("failed")

        // The parent-side flow sweeper converts the store-side failure into
        // a failed report and the flow completes under "continue".
        yield* drain(3, "30 seconds")
        const result = yield* Fiber.join(fiber)
        expect(result).toEqual({ sent: 0, failed: 1 })
        assert(collected !== undefined)
        const cause = collected.failed[0]?.cause
        assert(cause !== undefined)
        expect(Cause.pretty(cause)).toContain("cannot report to flow")
      }).pipe(Effect.provide(
        Layer.mergeAll(parentSide, childSide).pipe(Layer.provideMerge(stores))
      ))
    }))

  it.effect("a scheduled parent fans out with no flow awareness in the schedule row", () =>
    Effect.gen(function*() {
      const DigestFlow = Flow.make("digest", {
        parent: SendDigest,
        children: [SendEmail]
      })
      const parentSide = DigestFlow.toLayer({
        fanOut: () => Effect.succeed(Flow.children(SendEmail, [{ key: "u1", payload: { userId: "u1" } }])),
        collect: (_payload, results) =>
          Effect.succeed({ sent: results.completed.length, failed: results.failed.length })
      }).pipe(Layer.provide(Worker.layer()))
      const childSide = SendEmail.toLayer(() => Effect.succeed("ok")).pipe(
        Layer.provide(Worker.layer({ store: ChildStore, flows: [DigestFlow] }))
      )

      yield* Effect.gen(function*() {
        yield* DigestFlow.schedule("hourly", {
          every: "1 hour",
          payload: { tenant: "acme" }
        })
        yield* drain(2, "31 minutes")
        yield* drain(4)

        const parentStore = yield* JobStore.JobStore
        const parents = yield* parentStore.list({ name: "SendDigest" })
        const tick = parents.items[0]
        assert(tick !== undefined)
        expect(tick.id.startsWith("sched/SendDigest/hourly/")).toBe(true)
        expect(tick.state).toBe("completed")
        expect(tick.flow).toEqual({ failFast: false, pending: 0 })
      }).pipe(Effect.provide(
        Layer.mergeAll(parentSide, childSide).pipe(Layer.provideMerge(stores))
      ))
    }))

  it.effect("duplicate child keys fail the fan-out unrecoverably (no retry burn)", () =>
    Effect.gen(function*() {
      const DigestFlow = Flow.make("digest", {
        parent: SendDigest,
        children: [SendEmail]
      })
      const parentSide = DigestFlow.toLayer({
        fanOut: () =>
          Effect.succeed(Flow.children(SendEmail, [
            { key: "dup", payload: { userId: "a" } },
            { key: "dup", payload: { userId: "b" } }
          ])),
        collect: () => Effect.succeed({ sent: 0, failed: 0 })
      }).pipe(Layer.provide(Worker.layer()))

      yield* Effect.gen(function*() {
        const flowId = yield* DigestFlow.enqueue({ tenant: "acme" }, { attempts: 3 })
        yield* drain(3)
        const parentStore = yield* JobStore.JobStore
        const parent = yield* parentStore.getJob(flowId)
        assert(Option.isSome(parent))
        expect(parent.value.state).toBe("failed")
        // Unrecoverable: one attempt, not three.
        expect(parent.value.attemptsMade).toBe(1)
        expect(parent.value.flow).toBeUndefined()
      }).pipe(Effect.provide(parentSide.pipe(Layer.provideMerge(stores))))
    }))
})
