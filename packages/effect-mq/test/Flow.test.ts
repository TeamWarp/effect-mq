import { assert, describe, expect, it } from "@effect/vitest"
import { Cause, type Duration, Effect, Exit, Fiber, Layer, Option, Schema, Stream } from "effect"
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

// An inner flow parent for the nesting tests: a flow child of SendDigest
// flows AND the parent of its own SendEmail fan-out, all on the child store.
class SendBatch extends Job.make("SendBatch", {
  payload: { group: Schema.String },
  success: Schema.Number,
  store: ChildStore,
  queue: "batches"
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
      let captured: Flow.SettledChildren<typeof SendEmail> | undefined
      let streamed: Array<Flow.SettledChild<typeof SendEmail>> | undefined
      const parentSide = DigestFlow.toLayer({
        fanOut: (payload) =>
          Effect.succeed(Flow.children(SendEmail, [
            { key: "u1", payload: { userId: `${payload.tenant}-u1` } },
            { key: "u2", payload: { userId: `${payload.tenant}-u2` } },
            { key: "u3", payload: { userId: `${payload.tenant}-u3` } }
          ])),
        collect: (_payload, results) =>
          Effect.gen(function*() {
            captured = yield* results.all
            streamed = yield* Stream.runCollect(results.stream)
            return { sent: results.counts.completed, failed: results.counts.failed }
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
        // The stream sees the same settled set, tagged by outcome.
        assert(streamed !== undefined)
        expect(streamed.map((child) => child.outcome).toSorted()).toEqual([
          "completed",
          "completed",
          "failed"
        ])
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
          expect(child.id).toBe(
            Flow.childJobId("effect-mq/JobStore", child.parent?.flowId ?? "", child.parent?.childKey ?? "")
          )
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
          Effect.succeed({ sent: results.counts.completed, failed: results.counts.failed })
      }).pipe(Layer.provide(Worker.layer()))
      const childSide = SendEmail.toLayer(() => Effect.succeed("ok")).pipe(
        Layer.provide(Worker.layer({ store: ChildStore, flows: [DigestFlow] }))
      )

      yield* Effect.gen(function*() {
        const flowId = yield* DigestFlow.enqueue({ tenant: "acme" })
        yield* drain(3)
        const results = yield* DigestFlow.childResults(flowId)
        expect(results.counts).toEqual({ pending: 0, completed: 1, failed: 0, cancelled: 0 })
        const all = yield* results.all
        expect(all.completed).toEqual([{ outcome: "completed", key: "only", name: "SendEmail", value: "ok" }])
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
        const slow = yield* childStore.getJob(
          JobStore.JobId(Flow.childJobId("effect-mq/JobStore", parent.id, "slow"))
        )
        assert(Option.isSome(slow))
        expect(slow.value.state).toBe("cancelled")

        // Cascade work drains to empty once delivered.
        const work = yield* parentStore.flowSweepWork({ pendingAgeMs: 0 })
        expect(work.cascade).toEqual([])
      }).pipe(Effect.provide(
        Layer.mergeAll(parentSide, childSide).pipe(Layer.provideMerge(stores))
      ))
    }))

  it.effect("children complete on workers without the flow registration; the sweeper delivers the results", () =>
    Effect.gen(function*() {
      const DigestFlow = Flow.make("digest", {
        parent: SendDigest,
        children: [SendEmail]
      })
      const parentSide = DigestFlow.toLayer({
        fanOut: () => Effect.succeed(Flow.children(SendEmail, [{ key: "u1", payload: { userId: "u1" } }])),
        collect: (_payload, results) =>
          Effect.succeed({ sent: results.counts.completed, failed: results.counts.failed })
      }).pipe(Layer.provide(Worker.layer()))
      // No `flows` registration: the child still runs and its ack still
      // appends the outbox entry — this worker just cannot relay it.
      const childSide = SendEmail.toLayer(() => Effect.succeed("ok")).pipe(
        Layer.provide(Worker.layer({ store: ChildStore }))
      )

      yield* Effect.gen(function*() {
        const fiber = yield* Effect.forkChild(DigestFlow.execute({ tenant: "acme" }))
        yield* drain(2)

        // The child completed normally in its own store; its report sits in
        // the child store's outbox with no relay to push it.
        const childStore = yield* ChildStore
        const children = yield* childStore.list({ name: "SendEmail" })
        expect(children.items[0]?.state).toBe("completed")
        expect((yield* childStore.peekOutbox({ limit: 10 })).length).toBe(1)

        // The parent-side flow sweeper reads the child's terminal state
        // directly and the flow completes with the real result.
        yield* drain(3, "30 seconds")
        const result = yield* Fiber.join(fiber)
        expect(result).toEqual({ sent: 1, failed: 0 })
      }).pipe(Effect.provide(
        Layer.mergeAll(parentSide, childSide).pipe(Layer.provideMerge(stores))
      ))
    }))

  it.effect("flows nest: an inner flow settles and reports upward through the outbox relay", () =>
    Effect.gen(function*() {
      const InnerFlow = Flow.make("inner-sends", {
        parent: SendBatch,
        children: [SendEmail]
      })
      const OuterFlow = Flow.make("digest", {
        parent: SendDigest,
        children: [SendBatch]
      })
      const outerSide = OuterFlow.toLayer({
        fanOut: () =>
          Effect.succeed(Flow.children(SendBatch, [
            { key: "g1", payload: { group: "g1" } },
            { key: "g2", payload: { group: "g2" } }
          ])),
        collect: (_payload, results) =>
          Effect.gen(function*() {
            const all = yield* results.all
            const sent = all.completed.reduce((sum, child) => sum + child.value, 0)
            return { sent, failed: results.counts.failed }
          })
      }).pipe(Layer.provide(Worker.layer()))
      // One worker on the child store runs BOTH the inner parents and the
      // leaves; `flows: [OuterFlow]` lets its relay push inner results to
      // the main store.
      const innerSide = Layer.mergeAll(
        InnerFlow.toLayer({
          fanOut: (payload) =>
            Effect.succeed(Flow.children(SendEmail, [
              { key: `${payload.group}-a`, payload: { userId: `${payload.group}-a` } },
              { key: `${payload.group}-b`, payload: { userId: `${payload.group}-b` } }
            ])),
          collect: (_payload, results) => Effect.succeed(results.counts.completed)
        }),
        SendEmail.toLayer(() => Effect.succeed("ok"))
      ).pipe(Layer.provide(Worker.layer({ store: ChildStore, flows: [OuterFlow] })))

      yield* Effect.gen(function*() {
        const fiber = yield* Effect.forkChild(OuterFlow.execute({ tenant: "acme" }))
        yield* drain(8)
        const result = yield* Fiber.join(fiber)
        // 2 inner flows × 2 leaves each, every level's results decoded.
        expect(result).toEqual({ sent: 4, failed: 0 })

        // The inner parents are real flow children with their own manifests.
        const childStore = yield* ChildStore
        const inner = yield* childStore.list({ name: "SendBatch" })
        expect(inner.items.map((job) => job.state)).toEqual(["completed", "completed"])
        for (const job of inner.items) {
          expect(job.parent?.flowName).toBe("digest")
          expect(job.flow?.pending).toBe(0)
        }
      }).pipe(Effect.provide(
        Layer.mergeAll(outerSide, innerSide).pipe(Layer.provideMerge(stores))
      ))
    }))

  it.effect("the nesting depth cap fails runaway flows unrecoverably", () =>
    Effect.gen(function*() {
      const DigestFlow = Flow.make("digest", {
        parent: SendDigest,
        children: [SendEmail]
      })
      const parentSide = DigestFlow.toLayer({
        fanOut: () => Effect.succeed(Flow.children(SendEmail, [{ key: "u1", payload: { userId: "u1" } }])),
        collect: () => Effect.succeed({ sent: 0, failed: 0 })
      }).pipe(Layer.provide(Worker.layer()))

      yield* Effect.gen(function*() {
        // A parent that is itself a level-8 flow child — what a cyclic
        // definition produces after eight rounds of fan-out. Depth rides
        // the envelope, so names containing "flow/" cannot false-positive.
        const parentStore = yield* JobStore.JobStore
        const { id: flowId } = yield* parentStore.enqueue({
          id: undefined,
          name: "SendDigest",
          queue: JobStore.QueueName("default"),
          payload: { tenant: "acme" },
          metadata: {},
          priority: 0,
          attemptsMax: 3,
          backoff: undefined,
          keep: undefined,
          timeoutMs: undefined,
          dedupe: undefined,
          trace: undefined,
          parent: {
            flowName: "digest",
            flowId: JobStore.JobId("level-7-parent"),
            childKey: "runaway",
            parentStoreKey: "effect-mq/JobStore",
            depth: 8
          },
          delayMs: 0
        })
        yield* drain(3)
        const parent = yield* parentStore.getJob(flowId)
        assert(Option.isSome(parent))
        expect(parent.value.state).toBe("failed")
        expect(parent.value.attemptsMade).toBe(1)
      }).pipe(Effect.provide(parentSide.pipe(Layer.provideMerge(stores))))
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
          Effect.succeed({ sent: results.counts.completed, failed: results.counts.failed })
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
        expect(tick.flow).toEqual({ failFast: false, pending: 0, completed: 1, failed: 0, cancelled: 0 })
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
