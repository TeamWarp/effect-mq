import { assert, describe, expect, it } from "@effect/vitest"
import { Effect, Exit, Schema } from "effect"
import { TestClock } from "effect/testing"
import { Job, JobSchedules, JobStore, MemoryJobStore } from "../src/index.ts"

class Digest extends Job.make("Digest", { payload: { edition: Schema.String } }) {}
class Invoice extends Job.make("Invoice", { payload: {} }) {}

const declared = (keys: ReadonlyArray<"daily" | "monthly">) =>
  JobSchedules.layer({
    group: "billing",
    removal: "group",
    schedules: keys.map((key) =>
      key === "daily"
        ? JobSchedules.schedule(Digest, "daily", { every: "1 hour", payload: { edition: "am" } })
        : JobSchedules.schedule(Invoice, "monthly", { every: "1 day", payload: {} })
    )
  })

describe("JobSchedules", () => {
  it.effect("declares the set with the group label", () =>
    Effect.gen(function*() {
      const store = yield* JobStore.JobStore
      yield* Effect.void.pipe(Effect.provide(
        JobSchedules.layer({
          group: "billing",
          schedules: [
            JobSchedules.schedule(Digest, "daily", { every: "1 hour", payload: { edition: "am" } }),
            JobSchedules.schedule(Invoice, "monthly", { every: "1 day", payload: {} })
          ]
        })
      ))
      const members = yield* store.listSchedules({ group: "billing" })
      expect(members.map((member) => member.key).toSorted()).toEqual(["Digest/daily", "Invoice/monthly"])
      expect(members.every((member) => member.group === "billing")).toBe(true)
    }).pipe(Effect.provide(MemoryJobStore.layer)))

  it.effect("reconciling again preserves cadence anchors", () =>
    Effect.gen(function*() {
      const store = yield* JobStore.JobStore
      yield* Effect.void.pipe(Effect.provide(declared(["daily"])))
      const first = yield* store.listSchedules({ group: "billing" })
      // A later startup (same declaration) must not re-anchor the grid.
      yield* TestClock.adjust("10 minutes")
      yield* Effect.void.pipe(Effect.provide(declared(["daily"])))
      const second = yield* store.listSchedules({ group: "billing" })
      expect(second[0]?.nextRunAt).toBe(first[0]?.nextRunAt)
    }).pipe(Effect.provide(MemoryJobStore.layer)))

  it.effect("the default warn mode never prunes", () =>
    Effect.gen(function*() {
      const store = yield* JobStore.JobStore
      yield* Effect.void.pipe(Effect.provide(declared(["daily", "monthly"])))
      // Next release drops "monthly" but keeps the default removal.
      yield* Effect.void.pipe(Effect.provide(
        JobSchedules.layer({
          group: "billing",
          schedules: [
            JobSchedules.schedule(Digest, "daily", { every: "1 hour", payload: { edition: "am" } })
          ]
        })
      ))
      const members = yield* store.listSchedules({ group: "billing" })
      expect(members.map((member) => member.key).toSorted()).toEqual(["Digest/daily", "Invoice/monthly"])
    }).pipe(Effect.provide(MemoryJobStore.layer)))

  it.effect("removal: \"group\" prunes undeclared group members and nothing else", () =>
    Effect.gen(function*() {
      const store = yield* JobStore.JobStore
      // Group member that the next declaration drops:
      yield* Effect.void.pipe(Effect.provide(declared(["daily", "monthly"])))
      // Unlabeled schedule (plain .schedule()) and a foreign group:
      yield* Digest.schedule("manual", { every: "1 hour", payload: { edition: "pm" } })
      yield* Digest.schedule("foreign", { every: "1 hour", payload: { edition: "pm" }, group: "other-service" })

      yield* Effect.void.pipe(Effect.provide(declared(["daily"])))

      const remaining = (yield* store.listSchedules()).map((member) => member.key).toSorted()
      expect(remaining).toEqual(["Digest/daily", "Digest/foreign", "Digest/manual"])
      expect(yield* store.listSchedules({ group: "billing" })).toHaveLength(1)
    }).pipe(Effect.provide(MemoryJobStore.layer)))

  it.effect("removeAfter defers the prune, re-checks, and dies with the layer scope", () =>
    Effect.gen(function*() {
      const store = yield* JobStore.JobStore
      yield* Effect.void.pipe(Effect.provide(declared(["daily", "monthly"])))

      const deferred = JobSchedules.layer({
        group: "billing",
        removal: "group",
        removeAfter: "10 minutes",
        schedules: [
          JobSchedules.schedule(Digest, "daily", { every: "1 hour", payload: { edition: "am" } })
        ]
      })

      // While the layer is alive: nothing pruned before the window; an
      // unlabeled row created during the window survives the prune.
      yield* Effect.gen(function*() {
        expect(yield* store.listSchedules({ group: "billing" })).toHaveLength(2)
        yield* Digest.schedule("manual", { every: "1 hour", payload: { edition: "pm" } })
        yield* TestClock.adjust("10 minutes")
        const members = yield* store.listSchedules({ group: "billing" })
        expect(members.map((member) => member.key)).toEqual(["Digest/daily"])
        expect((yield* store.listSchedules()).map((member) => member.key).toSorted())
          .toEqual(["Digest/daily", "Digest/manual"])
      }).pipe(Effect.provide(deferred))

      // Closing the layer scope before the window fires skips the prune.
      yield* Effect.void.pipe(Effect.provide(
        JobSchedules.layer({
          group: "billing",
          removal: "group",
          removeAfter: "10 minutes",
          schedules: []
        })
      ))
      yield* TestClock.adjust("10 minutes")
      expect(yield* store.listSchedules({ group: "billing" })).toHaveLength(1)
    }).pipe(Effect.provide(MemoryJobStore.layer)))

  it.effect("config mistakes die loudly", () =>
    Effect.gen(function*() {
      const twice = yield* Effect.exit(Effect.void.pipe(Effect.provide(
        JobSchedules.layer({
          group: "billing",
          schedules: [
            JobSchedules.schedule(Digest, "daily", { every: "1 hour", payload: { edition: "am" } }),
            JobSchedules.schedule(Digest, "daily", { every: "2 hours", payload: { edition: "pm" } })
          ]
        })
      )))
      assert(Exit.isFailure(twice))
      expect(Exit.hasDies(twice)).toBe(true)

      const graceWithoutOptIn = yield* Effect.exit(Effect.void.pipe(Effect.provide(
        JobSchedules.layer({
          group: "billing",
          removeAfter: "10 minutes",
          schedules: []
        })
      )))
      assert(Exit.isFailure(graceWithoutOptIn))
      expect(Exit.hasDies(graceWithoutOptIn)).toBe(true)
    }).pipe(Effect.provide(MemoryJobStore.layer)))

  it.effect("stores option reconciles a store with zero declared entries", () =>
    Effect.gen(function*() {
      const store = yield* JobStore.JobStore
      yield* Effect.void.pipe(Effect.provide(declared(["daily"])))
      // The next release drops the LAST schedule; drift detection must still
      // reach the store.
      yield* Effect.void.pipe(Effect.provide(
        JobSchedules.layer({
          group: "billing",
          removal: "group",
          schedules: [],
          stores: [JobStore.JobStore]
        })
      ))
      expect(yield* store.listSchedules({ group: "billing" })).toEqual([])
    }).pipe(Effect.provide(MemoryJobStore.layer)))
})
