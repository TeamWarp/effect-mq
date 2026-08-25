/**
 * Cross-store parent-child flows.
 *
 * A flow's PARENT job fans out N child jobs, parks in `waiting-children`
 * until every child settles, then resumes with their typed results. The
 * children may live on a **different store** than the parent — a
 * cron-scheduled parent in Postgres can fan out thousands of idempotent
 * sends into Redis and collect the outcomes back in Postgres.
 *
 * ```ts
 * const DigestFlow = Flow.make("daily-digest", {
 *   parent: SendDigest,          // a Job bound to the Postgres store
 *   children: [SendEmail],       // Jobs, each bound to their own store
 *   onChildFailure: "continue"   // default; "fail" settles on first failure
 * })
 *
 * // the parent worker runs both phases (requires the parent AND child stores)
 * const DigestWorker = DigestFlow.toLayer({
 *   fanOut: (payload) =>
 *     Effect.gen(function*() {
 *       const users = yield* Users.active
 *       return Flow.children(SendEmail, users.map((user) => ({
 *         key: user.id,
 *         payload: { userId: user.id }
 *       })))
 *     }),
 *   collect: (payload, results) =>
 *     Effect.succeed({ sent: results.counts.completed, failed: results.counts.failed })
 *     // or: yield* results.all (materialized buckets), results.stream (paged)
 * })
 *
 * // workers that run the CHILDREN list the flow so results push instantly:
 * // Worker.layer({ store: EmailStore, flows: [DigestFlow] })
 * ```
 *
 * Architecture (see `designs/parent-child-flows.md`): the parent's store
 * owns the flow — child manifest, per-child results, outcome counters — so
 * "the flow settles exactly once" is single-store atomic. Cross-store needs
 * only two at-least-once idempotent mechanisms: every terminal child
 * transition atomically appends its report to the CHILD store's outbox,
 * which worker relays *push* into the parent store in batches (children
 * keep completing even while the parent store is down); and the parent
 * worker's flow sweeper *reconciles* from child-store state (repairs
 * everything the push path can miss: crashes mid-enqueue, dropped outbox
 * entries, children terminal on stores no relay reaches).
 *
 * Flows nest: a child may itself be another flow's parent, reporting upward
 * through the same machinery when it settles. Depth is capped (8) so a
 * cyclic definition surfaces as an unrecoverable failure instead of an
 * unbounded chain.
 *
 * Flow children bypass the child definition's `idempotencyKey`/`dedupe` —
 * the child `key` (unique within the flow) IS the idempotency mechanism,
 * carried in the deterministic job id. Handlers should be idempotent, as
 * everywhere under at-least-once.
 *
 * @since 0.6.0
 */
import { Cause, Context, Duration, Effect, Exit, Layer, Option, Schema, Stream } from "effect"
import {
  type AnyStructSchema,
  type JobOptions,
  normalizeBackoff,
  normalizeKeep,
  type ResolvedDefaults
} from "./Job.ts"
import {
  type EnqueueRequest,
  type FlowChildRecord,
  type FlowState,
  JobId,
  type QueueName,
  type Service as StoreService,
  unrecoverable
} from "./JobStore.ts"
import { type FlowDescriptor, type JobContext, type RegisterOptions, Worker } from "./Worker.ts"

/**
 * The structural view of a `Job.make` class a flow needs from its members.
 * Contravariant callback members are typed with `never` parameters so every
 * concrete job satisfies the constraint; the runtime only ever passes a
 * job's own payload back into them.
 *
 * @since 0.6.0
 */
export interface MemberJob {
  readonly _tag: string
  readonly queue: QueueName
  readonly store: Context.Key<any, StoreService>
  readonly payloadSchema: AnyStructSchema
  readonly payloadJsonSchema: Schema.Top
  readonly successSchema: Schema.Top
  readonly errorSchema: Schema.Top
  readonly exitSchema: Schema.Top
  readonly defaults: ResolvedDefaults
  readonly metadata: ((payload: never) => Readonly<Record<string, string>>) | undefined
  readonly retryable: ((error: never) => boolean) | undefined
}

/**
 * The structural view of the parent job: a `MemberJob` plus the producer
 * surface the flow delegates (`Flow.enqueue` IS the parent's `enqueue`).
 *
 * @since 0.6.0
 */
export interface ParentJob extends MemberJob {
  readonly enqueue: (...args: ReadonlyArray<never>) => Effect.Effect<any, any, any>
  readonly enqueueMany: (...args: ReadonlyArray<never>) => Effect.Effect<any, any, any>
  readonly execute: (...args: ReadonlyArray<never>) => Effect.Effect<any, any, any>
  readonly poll: (...args: ReadonlyArray<never>) => Effect.Effect<any, any, any>
  readonly attempts: (...args: ReadonlyArray<never>) => Effect.Effect<any, any, any>
  readonly awaitResult: (...args: ReadonlyArray<never>) => Effect.Effect<any, any, any>
  readonly retry: (...args: ReadonlyArray<never>) => Effect.Effect<any, any, any>
  readonly cancel: (...args: ReadonlyArray<never>) => Effect.Effect<any, any, any>
  readonly promote: (...args: ReadonlyArray<never>) => Effect.Effect<any, any, any>
  readonly schedule: (...args: ReadonlyArray<never>) => Effect.Effect<any, any, any>
  readonly unschedule: (...args: ReadonlyArray<never>) => Effect.Effect<any, any, any>
}

// Naked-parameter conditionals so unions of member jobs distribute (and
// empty tuples land on `never`, not `unknown`).
type NameOf<J> = J extends { readonly _tag: infer N extends string } ? N : never
type PayloadMakeIn<J> = J extends { readonly payloadSchema: infer P extends AnyStructSchema } ? P["~type.make.in"]
  : never
type PayloadType<J> = J extends { readonly payloadSchema: infer P extends AnyStructSchema } ? P["Type"] : never
type PayloadEncodingServices<J> = J extends { readonly payloadSchema: infer P extends AnyStructSchema }
  ? P["EncodingServices"]
  : never
type SuccessValue<J> = J extends { readonly successSchema: infer S extends Schema.Top } ? S["Type"] : never
type SuccessDecodingServices<J> = J extends { readonly successSchema: infer S extends Schema.Top }
  ? S["DecodingServices"]
  : never
type SuccessEncodingServices<J> = J extends { readonly successSchema: infer S extends Schema.Top }
  ? S["EncodingServices"]
  : never
type ErrorValue<J> = J extends { readonly errorSchema: infer E extends Schema.Top } ? E["Type"] : never
type ErrorDecodingServices<J> = J extends { readonly errorSchema: infer E extends Schema.Top }
  ? E["DecodingServices"]
  : never
type ErrorEncodingServices<J> = J extends { readonly errorSchema: infer E extends Schema.Top }
  ? E["EncodingServices"]
  : never
type PayloadDecodingServices<J> = J extends { readonly payloadSchema: infer P extends AnyStructSchema }
  ? P["DecodingServices"]
  : never

/**
 * The StoreIds of a flow's members — what `toLayer` requires so the parent
 * worker is guaranteed able to reconcile and cascade into every child store.
 *
 * @since 0.6.0
 */
export type MemberStores<J> = J extends { readonly store: Context.Key<infer Id, StoreService> } ? Id : never

/**
 * Per-child options accepted by `Flow.children` items — the shared
 * `JobOptions` minus `delay` (flow children run immediately), plus
 * `metadata` merged over the child definition's callback.
 *
 * @since 0.6.0
 */
export type ChildOptions = Omit<JobOptions, "delay"> & {
  readonly metadata?: Readonly<Record<string, string>> | undefined
}

/**
 * One child to fan out: its flow-unique `key` (the idempotency mechanism —
 * re-runs of `fanOut` re-produce the same child, never a second one), the
 * payload, and optional per-child options.
 *
 * @since 0.6.0
 */
export interface ChildItem<PayloadInput> {
  readonly key: string
  readonly payload: PayloadInput
  readonly options?: ChildOptions | undefined
}

/**
 * A group of children of one member type, built by `Flow.children`. A
 * `fanOut` handler returns one group or an array of them.
 *
 * @since 0.6.0
 */
export interface ChildGroup<out J extends MemberJob = MemberJob> {
  readonly job: J
  readonly items: ReadonlyArray<ChildItem<PayloadMakeIn<J>>>
}

/**
 * A naked-parameter distribution of `ChildGroup` over a union of members,
 * so a group literal must pair a member's `job` with THAT member's payloads
 * (the undistributed `ChildGroup<A | B>` would accept `A`'s job with `B`'s
 * items).
 *
 * @since 0.6.0
 */
export type GroupsOf<J> = J extends MemberJob ? ChildGroup<J> : never

/**
 * What `fanOut` returns: children of any of the flow's member types.
 *
 * @since 0.6.0
 */
export type ChildrenInput<J extends MemberJob> = GroupsOf<J> | ReadonlyArray<GroupsOf<J>>

/**
 * Declare children of one member type. Payloads are validated through the
 * child's schema when the specs are built; duplicate keys (across ALL
 * groups) fail the fan-out unrecoverably.
 *
 * @since 0.6.0
 */
export const children = <J extends MemberJob>(
  job: J,
  items: ReadonlyArray<ChildItem<PayloadMakeIn<J>>>
): ChildGroup<J> => ({ job, items })

/**
 * A child that completed, with its decoded success value.
 *
 * @since 0.6.0
 */
export interface CompletedChild<Name extends string, A> {
  readonly key: string
  readonly name: Name
  readonly value: A
}

/**
 * A child that failed terminally. `cause` carries the decoded typed failure
 * — or a die for store-side failures that never produced an exit (stall
 * exhaustion, workers unable to report to the flow).
 *
 * @since 0.6.0
 */
export interface FailedChild<Name extends string, E> {
  readonly key: string
  readonly name: Name
  readonly cause: Cause.Cause<E>
}

/**
 * A child that was cancelled — directly in its store, or by a flow settle
 * (fail-fast, or a cancel of the waiting parent).
 *
 * @since 0.6.0
 */
export interface CancelledChild<Name extends string> {
  readonly key: string
  readonly name: Name
}

type CompletedOf<J> = J extends MemberJob ? CompletedChild<NameOf<J>, SuccessValue<J>> : never
type FailedOf<J> = J extends MemberJob ? FailedChild<NameOf<J>, ErrorValue<J>> : never
type CancelledOf<J> = J extends MemberJob ? CancelledChild<NameOf<J>> : never

/**
 * Per-outcome tallies, read straight off the parent's persisted `FlowState`
 * — no dependency-row reads. When `collect` runs, `pending` is 0.
 *
 * @since 0.6.0
 */
export interface ChildCounts {
  readonly pending: number
  readonly completed: number
  readonly failed: number
  readonly cancelled: number
}

/**
 * One settled child, discriminated by `outcome` (and by `name` across
 * member types), decoded through its member's schemas.
 *
 * @since 0.6.0
 */
export type SettledChild<J extends MemberJob> =
  | ({ readonly outcome: "completed" } & CompletedOf<J>)
  | ({ readonly outcome: "failed" } & FailedOf<J>)
  | ({ readonly outcome: "cancelled" } & CancelledOf<J>)

/**
 * Every settled child, materialized into outcome buckets.
 *
 * @since 0.6.0
 */
export interface SettledChildren<J extends MemberJob> {
  readonly completed: ReadonlyArray<Extract<SettledChild<J>, { readonly outcome: "completed" }>>
  readonly failed: ReadonlyArray<Extract<SettledChild<J>, { readonly outcome: "failed" }>>
  readonly cancelled: ReadonlyArray<Extract<SettledChild<J>, { readonly outcome: "cancelled" }>>
}

/**
 * What `collect` (and `Flow.childResults`) receives: `counts` for free,
 * `all` to materialize the v1-style buckets (one array — fine into the
 * tens of thousands), and `stream` to fold huge flows one page at a time.
 *
 * @since 0.6.0
 */
export interface ChildResults<J extends MemberJob> {
  readonly counts: ChildCounts
  readonly all: Effect.Effect<SettledChildren<J>>
  readonly stream: Stream.Stream<SettledChild<J>>
}

/**
 * The two-phase flow handler. `fanOut` runs once per flow (persisted phase
 * dispatch — a resumed parent can never fan out twice) and returns the
 * children; `collect` runs after every child settled, with their results.
 * Both draw on the PARENT's attempt budget: a failing `fanOut` retries
 * until the manifest lands, a failing `collect` retries with what remains.
 *
 * @since 0.6.0
 */
export interface FlowHandlers<
  Parent extends ParentJob,
  Children extends ReadonlyArray<MemberJob>,
  R1,
  R2
> {
  readonly fanOut: (
    payload: PayloadType<Parent>,
    context: JobContext
  ) => Effect.Effect<ChildrenInput<Children[number]>, ErrorValue<Parent>, R1>
  readonly collect: (
    payload: PayloadType<Parent>,
    results: ChildResults<Children[number]>,
    context: JobContext
  ) => Effect.Effect<SuccessValue<Parent>, ErrorValue<Parent>, R2>
}

/**
 * A flow definition. The producer surface (`enqueue`, `execute`, `poll`,
 * `awaitResult`, `schedule`, ...) IS the parent job's — a flow parent is a
 * real job row in the parent store, and a scheduled parent needs no flow
 * awareness in its schedule row.
 *
 * Note on fail-fast (`onChildFailure: "fail"`): the first failed child
 * settles the parent terminally `failed` store-side (`failedReason` names
 * the child; there is no parent exit, so `awaitResult` dies — like stall
 * exhaustion) and the remaining children are cancelled. The failed child's
 * own exit stays inspectable via `childResults`. An admin `retry` of the
 * parent re-enters `collect` with the mixed results.
 *
 * @since 0.6.0
 */
export interface Flow<
  Name extends string,
  Parent extends ParentJob,
  Children extends ReadonlyArray<MemberJob>
> {
  readonly name: Name
  readonly parent: Parent
  readonly children: Children
  /** True when `onChildFailure` is `"fail"`. */
  readonly failFast: boolean

  readonly enqueue: Parent["enqueue"]
  readonly enqueueMany: Parent["enqueueMany"]
  readonly execute: Parent["execute"]
  readonly poll: Parent["poll"]
  readonly attempts: Parent["attempts"]
  readonly awaitResult: Parent["awaitResult"]
  readonly retry: Parent["retry"]
  readonly cancel: Parent["cancel"]
  readonly promote: Parent["promote"]
  readonly schedule: Parent["schedule"]
  readonly unschedule: Parent["unschedule"]

  /**
   * The flow's recorded child results, in any parent state: live `counts`
   * plus the `all`/`stream` accessors — children still pending are absent
   * from both.
   */
  readonly childResults: (
    flowId: JobId
  ) => Effect.Effect<
    ChildResults<Children[number]>,
    never,
    | MemberStores<Parent>
    | SuccessDecodingServices<Children[number]>
    | ErrorDecodingServices<Children[number]>
  >

  /**
   * Register the parent's two phases on a worker (bound to the parent's
   * store). Requires every member store — this is what makes the parent
   * worker the one process guaranteed capable of reconciling and cascading
   * across the whole flow.
   */
  readonly toLayer: <R1, R2>(
    handlers: FlowHandlers<Parent, Children, R1, R2>,
    options?: RegisterOptions | undefined
  ) => Layer.Layer<
    never,
    never,
    | Worker
    | R1
    | R2
    | MemberStores<Parent>
    | MemberStores<Children[number]>
    | PayloadDecodingServices<Parent>
    | SuccessEncodingServices<Parent>
    | ErrorEncodingServices<Parent>
    | PayloadEncodingServices<Children[number]>
    | SuccessDecodingServices<Children[number]>
    | ErrorDecodingServices<Children[number]>
  >
}

/**
 * Define a flow over existing job definitions.
 *
 * Throws (synchronously, at definition time) on duplicate child names or a
 * parent that is also a child — flows are one level deep in v1.
 *
 * @since 0.6.0
 */
export const make = <
  const Name extends string,
  Parent extends ParentJob,
  const Children extends ReadonlyArray<MemberJob>
>(
  name: Name,
  options: {
    /** The parent job: its store owns the flow. */
    readonly parent: Parent
    /** The closed set of member definitions `fanOut` may produce. */
    readonly children: Children
    /**
     * What a failed child does to the flow:
     * - `"continue"` (default): every child settles; `collect` sees the
     *   failures in `results.failed`.
     * - `"fail"`: the first failed child settles the parent as `failed`
     *   and cancels the remaining children (see the `Flow` docs).
     */
    readonly onChildFailure?: "continue" | "fail" | undefined
  }
): Flow<Name, Parent, Children> => {
  const parent = options.parent
  const byName = new Map<string, MemberJob>()
  for (const child of options.children) {
    if (byName.has(child._tag)) {
      throw new Error(`effect-mq: flow "${name}" declares child "${child._tag}" twice`)
    }
    byName.set(child._tag, child)
  }
  if (byName.has(parent._tag)) {
    throw new Error(
      `effect-mq: flow "${name}" uses "${parent._tag}" as both parent and child; nested flows are not supported`
    )
  }
  const failFast = options.onChildFailure === "fail"

  // A dependency row decoded into a settled, tagged entry. The outcome
  // follows the DECODED exit where one exists (a defensive net for
  // outcome/exit drift); rows without an exit follow their recorded status.
  // Pending rows decode to undefined (only reachable via `childResults` on
  // an unsettled flow).
  type SettledEntry =
    | { readonly outcome: "completed"; readonly key: string; readonly name: string; readonly value: unknown }
    | { readonly outcome: "failed"; readonly key: string; readonly name: string; readonly cause: Cause.Cause<unknown> }
    | { readonly outcome: "cancelled"; readonly key: string; readonly name: string }
  const decodeRow = (row: FlowChildRecord) =>
    Effect.gen(function*() {
      if (row.status === "pending") return undefined
      if (row.status === "cancelled") {
        const entry: SettledEntry = { outcome: "cancelled", key: row.childKey, name: row.name }
        return entry
      }
      if (row.exit === undefined) {
        // Store-side failure: no exit was ever produced.
        const entry: SettledEntry = {
          outcome: "failed",
          key: row.childKey,
          name: row.name,
          cause: Cause.die(
            new Error(row.failedReason ?? `effect-mq: flow child "${row.childKey}" failed without a result`)
          )
        }
        return entry
      }
      const member = byName.get(row.name)
      if (member === undefined) {
        const entry: SettledEntry = {
          outcome: "failed",
          key: row.childKey,
          name: row.name,
          cause: Cause.die(
            new Error(`effect-mq: flow "${name}" has no member definition for child "${row.name}"`)
          )
        }
        return entry
      }
      const decoded = yield* Schema.decodeUnknownEffect(member.exitSchema)(row.exit).pipe(Effect.orDie)
      // SAFETY: a member's exitSchema Type is Exit<Success, Error> by
      // construction in Job.make.
      const exit = decoded as Exit.Exit<unknown, unknown>
      const entry: SettledEntry = Exit.isSuccess(exit)
        ? { outcome: "completed", key: row.childKey, name: row.name, value: exit.value }
        : { outcome: "failed", key: row.childKey, name: row.name, cause: exit.cause }
      return entry
    })

  // Build the collect-facing accessors over a flow's dependency rows.
  // Accessors are lazy and may run in the caller's fiber later, so they
  // carry the context captured at construction (the registration context
  // under a worker; the caller's own context via `childResults`) — that is
  // what lets their public types declare no requirements.
  const makeResults = (
    store: StoreService,
    flow: FlowState,
    flowId: JobId,
    services: Context.Context<never>
  ) => {
    // SAFETY: the captured context carries the members' decoding services,
    // declared on toLayer's / childResults' public signatures.
    const withCaptured = <A, E>(effect: Effect.Effect<A, E, unknown>): Effect.Effect<A, E> =>
      effect.pipe(
        Effect.updateContext((input) => Context.merge(input, services) as Context.Context<unknown>)
      ) as Effect.Effect<A, E>
    const decodePage = (cursor: string | undefined) =>
      store.listChildResults(flowId, { cursor }).pipe(
        Effect.orDie,
        Effect.flatMap((page) =>
          Effect.forEach(page.items, decodeRow).pipe(
            Effect.map((entries) =>
              [
                entries.filter((entry) => entry !== undefined),
                Option.fromNullishOr(page.cursor)
              ] as const
            )
          )
        )
      )
    const stream = Stream.paginate<string | undefined, SettledEntry>(
      undefined,
      (cursor) => withCaptured(decodePage(cursor))
    )
    const all = withCaptured(Effect.gen(function*() {
      const completed: Array<SettledEntry> = []
      const failed: Array<SettledEntry> = []
      const cancelled: Array<SettledEntry> = []
      let cursor: string | undefined
      do {
        const [entries, next] = yield* decodePage(cursor)
        for (const entry of entries) {
          if (entry.outcome === "completed") completed.push(entry)
          else if (entry.outcome === "failed") failed.push(entry)
          else cancelled.push(entry)
        }
        cursor = Option.getOrUndefined(next)
      } while (cursor !== undefined)
      return { completed, failed, cancelled }
    }))
    const counts = {
      pending: flow.pending,
      completed: flow.completed,
      failed: flow.failed,
      cancelled: flow.cancelled
    }
    return { counts, all, stream }
  }

  // Turn the fanOut handler's groups into complete FlowChildSpecs:
  // deterministic ids, `parent` envelopes, encoded payloads, and the
  // fan-out run's span context for cross-store trace linking. Definition
  // bugs (unknown members, duplicate keys) die UNRECOVERABLY — retrying a
  // deterministic bug burns the budget for nothing.
  const buildSpecs = (
    input: ChildrenInput<MemberJob>,
    context: JobContext
  ) =>
    Effect.gen(function*() {
      // Child ids embed the full ancestor chain, so "flow/" occurrences in
      // the parent's own id count the nesting depth. Definitions cannot be
      // cycle-checked statically (a job does not know which flows parent
      // it), so a runaway A→B→A recursion surfaces here instead of growing
      // forever.
      const depth = context.jobId.split("flow/").length - 1
      if (depth >= 8) {
        return yield* Effect.die(unrecoverable(
          new Error(
            `effect-mq: flow "${name}" exceeds the nesting depth limit (8) — flow definitions must not form cycles`
          )
        ))
      }
      const groups = Array.isArray(input) ? input : [input]
      const span = yield* Effect.currentSpan.pipe(
        Effect.map((current) => ({
          traceId: current.traceId,
          spanId: current.spanId,
          sampled: current.sampled,
          delayed: false
        })),
        Effect.catchTag("NoSuchElementError", () => Effect.succeed(undefined))
      )
      const seen = new Set<string>()
      const specs: Array<{ childKey: string; storeKey: string; request: EnqueueRequest }> = []
      for (const group of groups) {
        const member = byName.get(group.job._tag)
        if (member === undefined) {
          return yield* Effect.die(unrecoverable(
            new Error(`effect-mq: flow "${name}" fanned out to "${group.job._tag}", which is not a declared child`)
          ))
        }
        for (const item of group.items) {
          if (item.key === "") {
            return yield* Effect.die(unrecoverable(
              new Error(`effect-mq: flow "${name}" produced a child with an empty key`)
            ))
          }
          if (seen.has(item.key)) {
            return yield* Effect.die(unrecoverable(
              new Error(`effect-mq: flow "${name}" produced duplicate child key "${item.key}"`)
            ))
          }
          seen.add(item.key)
          // Payload validation/encode failures are deterministic definition
          // bugs like duplicate keys: retrying the fan-out would burn the
          // whole attempt budget on the same throw, so die unrecoverably.
          let payload: unknown
          try {
            payload = member.payloadSchema.make(item.payload)
          } catch (error) {
            return yield* Effect.die(unrecoverable(new Error(
              `effect-mq: flow "${name}" child "${item.key}" payload failed validation: ${String(error)}`
            )))
          }
          const encoded = yield* Schema.encodeEffect(member.payloadJsonSchema)(payload).pipe(
            Effect.catch((error) =>
              Effect.die(unrecoverable(new Error(
                `effect-mq: flow "${name}" child "${item.key}" payload failed to encode: ${String(error)}`
              )))
            )
          )
          // SAFETY: `metadata` is declared with a `never` parameter only to
          // make the structural constraint universal; at runtime it accepts
          // its own definition's payload, which is what `payload` is.
          const memberMetadata = member.metadata?.(payload as never)
          const itemOptions = item.options
          specs.push({
            childKey: item.key,
            storeKey: member.store.key,
            request: {
              // Namespaced by the parent store key: two parent stores
              // sharing one child store can never collide on parent ids.
              id: JobId(`flow/${parent.store.key}/${context.jobId}/${item.key}`),
              name: member._tag,
              queue: member.queue,
              payload: encoded,
              metadata: { ...memberMetadata, ...itemOptions?.metadata },
              priority: itemOptions?.priority ?? member.defaults.priority,
              attemptsMax: Math.max(1, itemOptions?.attempts ?? member.defaults.attempts),
              backoff: itemOptions?.backoff !== undefined
                ? normalizeBackoff(itemOptions.backoff)
                : member.defaults.backoff,
              keep: itemOptions?.keep !== undefined
                ? normalizeKeep(itemOptions.keep)
                : member.defaults.keep,
              timeoutMs: itemOptions?.timeout !== undefined
                ? Duration.toMillis(itemOptions.timeout)
                : member.defaults.timeoutMs,
              // The child key is the idempotency mechanism; the member's
              // idempotencyKey/dedupe callbacks do NOT apply to flow
              // children (documented loudly).
              dedupe: undefined,
              trace: span,
              parent: {
                flowName: name,
                flowId: context.jobId,
                childKey: item.key,
                parentStoreKey: parent.store.key
              },
              delayMs: 0
            }
          })
        }
      }
      return specs
    })

  const toLayer = (
    handlers: FlowHandlers<ParentJob, ReadonlyArray<MemberJob>, unknown, unknown>,
    registerOptions?: RegisterOptions | undefined
  ) => {
    const descriptor: FlowDescriptor = {
      name,
      parent: {
        _tag: parent._tag,
        queue: parent.queue,
        store: parent.store,
        payloadJsonSchema: parent.payloadJsonSchema,
        exitSchema: parent.exitSchema,
        retryable: parent.retryable
      },
      failFast,
      childStores: options.children.map((child) => child.store),
      fanOut: (payload, context) =>
        // SAFETY: the worker decodes through the parent's payload schema
        // before calling this, so `payload` is the parent's payload type.
        handlers.fanOut(payload as never, context).pipe(
          Effect.flatMap((produced) => buildSpecs(produced, context))
        ),
      collect: (payload, flowState, context) =>
        Effect.gen(function*() {
          const store = yield* parent.store
          const services = yield* Effect.context<never>()
          const results = makeResults(store, flowState, context.jobId, services)
          // SAFETY: same as fanOut for `payload`; the accessors decode
          // through each member's schemas, matching ChildResults.
          return yield* handlers.collect(payload as never, results as never, context)
        })
    }
    return Layer.effectDiscard(
      Effect.flatMap(Worker, (worker) => worker.registerFlow(descriptor, registerOptions))
    )
  }

  const emptyFlow: FlowState = { failFast, pending: 0, completed: 0, failed: 0, cancelled: 0 }
  const childResults = (flowId: JobId) =>
    Effect.gen(function*() {
      const store = yield* parent.store
      const services = yield* Effect.context<never>()
      const record = yield* store.getJob(flowId).pipe(Effect.orDie)
      const flowState = Option.isSome(record) && record.value.flow !== undefined
        ? record.value.flow
        : emptyFlow
      return makeResults(store, flowState, flowId, services)
    })

  // SAFETY: the `Flow` interface re-declares the precise member signatures
  // (delegated producer methods carry the parent's own types; toLayer's
  // requirements are declared there); the implementation is assembled
  // dynamically, mirroring Job.make's prototype pattern.
  const flow: any = {
    name,
    parent,
    children: options.children,
    failFast,
    enqueue: parent.enqueue,
    enqueueMany: parent.enqueueMany,
    execute: parent.execute,
    poll: parent.poll,
    attempts: parent.attempts,
    awaitResult: parent.awaitResult,
    retry: parent.retry,
    cancel: parent.cancel,
    promote: parent.promote,
    schedule: parent.schedule,
    unschedule: parent.unschedule,
    childResults,
    toLayer
  }
  return flow
}
