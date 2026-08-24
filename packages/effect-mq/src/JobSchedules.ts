/**
 * Declarative schedule reconciliation.
 *
 * `.schedule()` is imperative: it creates and updates, and a schedule whose
 * call was deleted from code keeps firing forever (deletion drift). This
 * module declares the FULL schedule set for a service as a layer; on
 * startup it upserts everything declared (idempotent, cadence-preserving)
 * and detects group members that are no longer declared:
 *
 * ```ts
 * const SchedulesLive = JobSchedules.layer({
 *   group: "billing-service",
 *   schedules: [
 *     JobSchedules.schedule(SendDigest, "daily", { cron: "0 9 * * *", payload: {} }),
 *     JobSchedules.schedule(GenerateInvoice, "monthly", { cron: "0 0 1 * *", payload: {} })
 *   ],
 *   removal: "group",           // default "warn": log drift, prune nothing
 *   removeAfter: "10 minutes"   // optional grace window for rolling deploys
 * })
 * ```
 *
 * Safety model: pruning is scoped to the ownership `group`. Schedules
 * created by plain `.schedule()` calls carry no group and are NEVER pruned;
 * other groups' schedules are never touched. The default `removal: "warn"`
 * only logs — destructive pruning is an explicit opt-in.
 *
 * @since 0.5.0
 */
import { type Context, Duration, Effect, Layer } from "effect"
import type { ScheduleOptions } from "./Job.ts"
import type { ScheduleKey, Service as StoreService } from "./JobStore.ts"

/**
 * The structural view of a `Job.make` class that `schedule` needs: its tag,
 * its store key, and its bound `schedule` verb.
 *
 * @since 0.5.0
 */
export interface SchedulableJob<PayloadInput, R> {
  readonly _tag: string
  readonly store: Context.Key<any, StoreService>
  readonly schedule: (
    key: string,
    options: ScheduleOptions<PayloadInput>
  ) => Effect.Effect<ScheduleKey, never, R>
}

/**
 * One declared schedule: a job, its key, and its cadence/options. Built
 * with `JobSchedules.schedule`; consumed by `JobSchedules.layer`.
 *
 * @since 0.5.0
 */
export interface ScheduleEntry<R> {
  readonly jobName: string
  readonly key: string
  readonly store: Context.Key<any, StoreService>
  readonly register: (group: string) => Effect.Effect<ScheduleKey, never, R>
}

/**
 * Declare one schedule for the reconciled set. Identical semantics to
 * `MyJob.schedule(key, options)` — including cadence preservation on
 * unchanged `cron`/`tz`/`every` — plus the layer's ownership `group`.
 *
 * @since 0.5.0
 */
export const schedule = <PayloadInput, R>(
  job: SchedulableJob<PayloadInput, R>,
  key: string,
  options: Omit<ScheduleOptions<PayloadInput>, "group">
): ScheduleEntry<R> => ({
  jobName: job._tag,
  key,
  store: job.store,
  register: (group) => job.schedule(key, { ...options, group })
})

/**
 * Options for `JobSchedules.layer`.
 *
 * @since 0.5.0
 */
export interface ReconcileOptions<
  Entries extends ReadonlyArray<ScheduleEntry<any>>,
  Stores extends ReadonlyArray<Context.Key<any, StoreService>>
> {
  /**
   * Ownership label. Everything this layer declares is upserted with this
   * group, and only schedules carrying this group are candidates for drift
   * detection and pruning. Use one group per service/deployable.
   */
  readonly group: string
  /** The full declared schedule set for this group. */
  readonly schedules: Entries
  /**
   * What to do with group members that are no longer declared:
   * - `"warn"` (default): log them at warning level, prune nothing.
   * - `"group"`: remove them (never touches unlabeled or other-group rows).
   */
  readonly removal?: "warn" | "group" | undefined
  /**
   * Grace window before pruning (requires `removal: "group"`). During a
   * rolling deploy, replicas on the previous release re-declare schedules
   * the new release dropped; pruning immediately and re-adding would
   * re-anchor `every` grids and double-fire crons. With a window, the prune
   * runs this long after startup, re-checks the store, and removes only
   * members still undeclared. Shutting down before the window fires skips
   * the prune (the next startup re-evaluates).
   */
  readonly removeAfter?: Duration.Input | undefined
  /**
   * Extra store keys to reconcile even when no declared entry references
   * them — needed when a release drops the LAST schedule a store had, since
   * drift detection only reaches stores it can see.
   */
  readonly stores?: Stores | undefined
}

const reconcile = (options: {
  readonly group: string
  readonly schedules: ReadonlyArray<ScheduleEntry<any>>
  readonly removal?: "warn" | "group" | undefined
  readonly removeAfter?: Duration.Input | undefined
  readonly stores?: ReadonlyArray<Context.Key<any, StoreService>> | undefined
}) =>
  Effect.gen(function*() {
    const removal = options.removal ?? "warn"
    if (options.removeAfter !== undefined && removal !== "group") {
      return yield* Effect.die(
        new Error(`effect-mq: \`removeAfter\` requires \`removal: "group"\``)
      )
    }
    // Duplicate declarations are a config bug: the second would silently
    // overwrite the first's cadence/payload on every startup.
    const seen = new Set<string>()
    for (const entry of options.schedules) {
      const id = `${entry.jobName}/${entry.key}`
      if (seen.has(id)) {
        return yield* Effect.die(
          new Error(`effect-mq: schedule "${id}" is declared twice in group "${options.group}"`)
        )
      }
      seen.add(id)
    }

    // Upsert every declared schedule, collecting the declared key set per
    // store (Context.Key identity groups entries onto their stores).
    const byStore = new Map<Context.Key<any, StoreService>, Set<string>>()
    for (const storeKey of options.stores ?? []) {
      byStore.set(storeKey, new Set())
    }
    for (const entry of options.schedules) {
      const registered = yield* entry.register(options.group)
      const declared = byStore.get(entry.store) ?? new Set<string>()
      declared.add(registered)
      byStore.set(entry.store, declared)
    }

    // Drift detection per store: group members not in the declared set.
    for (const [storeKey, declared] of byStore) {
      const store = yield* storeKey
      const prune = Effect.gen(function*() {
        const members = yield* store.listSchedules({ group: options.group }).pipe(
          Effect.retry({ times: 5 }),
          Effect.orDie
        )
        const undeclared = members.filter((member) => !declared.has(member.key))
        if (undeclared.length === 0) return
        if (removal === "warn") {
          return yield* Effect.logWarning(
            `effect-mq: ${undeclared.length} schedule(s) in group "${options.group}" ` +
              `are no longer declared and keep firing; \`removal: "group"\` would prune them`,
            undeclared.map((member) => member.key)
          )
        }
        for (const member of undeclared) {
          yield* store.removeSchedule(member.key).pipe(Effect.retry({ times: 5 }), Effect.orDie)
        }
        yield* Effect.logInfo(
          `effect-mq: pruned ${undeclared.length} undeclared schedule(s) from group "${options.group}"`,
          undeclared.map((member) => member.key)
        )
      })
      if (removal === "group" && options.removeAfter !== undefined) {
        // Deferred prune, tied to the layer scope: it re-lists at fire time,
        // so anything re-declared during the window survives.
        yield* prune.pipe(
          Effect.delay(Duration.toMillis(options.removeAfter)),
          Effect.catchCause((cause) =>
            Effect.logError(
              `effect-mq: deferred schedule prune failed (group "${options.group}")`,
              cause
            )
          ),
          Effect.forkScoped
        )
      } else {
        yield* prune
      }
    }
  })

/**
 * Reconcile the declared schedule set on startup: upsert everything
 * declared, then warn about (or, with `removal: "group"`, prune) group
 * members that are no longer declared. See the module docs for the safety
 * model.
 *
 * @since 0.5.0
 */
export const layer = <
  const Entries extends ReadonlyArray<ScheduleEntry<any>>,
  const Stores extends ReadonlyArray<Context.Key<any, StoreService>> = readonly []
>(
  options: ReconcileOptions<Entries, Stores>
): Layer.Layer<never, never, EntryServices<Entries[number]> | StoreId<Stores[number]>> =>
  Layer.effectDiscard(reconcile(options))

// Naked-parameter conditionals so empty tuples distribute to `never` instead
// of inferring `unknown` from nothing.
type EntryServices<E> = E extends ScheduleEntry<infer R> ? R : never
type StoreId<K> = K extends Context.Key<infer Id, StoreService> ? Id : never
