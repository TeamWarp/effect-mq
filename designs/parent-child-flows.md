# Design: parent-child flows (cross-store fan-out)

Status: implemented (v1) for 0.6.0 — revision 2, rewritten after an
adversarial design review (3 lenses, 22 confirmed findings folded in below).

As-built deviations from this document:

- **Phase marker**: persisted as `JobRecord.flow` (`{ failFast, pending }`);
  its *presence* is the collect-phase marker, instead of a separate
  `flowPhase` enum field. The fail-fast policy is persisted inside it.
- **Fail-fast settle**: the store settles the parent as `failed` with
  `failedReason: 'effect-mq: flow child "<key>" failed'` and no exit — the
  same shape as stall exhaustion — rather than synthesizing an encoded
  `FlowChildFailedError` exit (stores never encode exits). The failed
  child's own exit stays inspectable via `childResults`; `awaitResult` on
  the parent dies. The report deliverer (child worker or sweeper) logs the
  settle at error level.
- **Store ops**: the cascade flag write is its own op,
  `markChildrenCascaded(flowId, childKeys)`, and the sweep threshold is a
  caller argument (`flowSweepWork({ pendingAgeMs, limit })`). Applied
  reports set `cascaded` immediately (the outcome came from the child's
  store; no cancel owed).
- **Double fan-out**: instead of rejecting, a FanOut ack that finds an
  existing manifest converges on it (ignores the new children, transitions
  per the persisted pending count) — safer for the only way it can occur
  (a bug or a lost-lock re-run).
- **Missing child at reconcile**: always re-enqueued from the stored spec
  (at-least-once); there is no "child lost" failure synthesis. The
  retention guidance (child terminal `keep` > sweep interval) is what
  prevents pruned-terminal-but-unreported re-runs.
- **Report ordering** (revised after the implementation review): the child
  worker acks FIRST and reports only after its ack landed, inverting this
  document's report-before-ack. Report-first let a lock-lost worker's stale
  result poison the flow (its rejected ack meant the child re-ran, but its
  report had already won the row first-writer-wins). Ack-first means only
  the run that owns the lock reports; a report that then fails to deliver
  is synthesized by the sweeper from the child's own terminal record — so
  children even keep completing through parent-store outages, which this
  document had deferred to the v2 outbox.
- **Sweep fairness**: `flowSweepWork` re-arms the eligibility timestamp of
  every reconcile row it returns, so pages rotate instead of pinning their
  head (healthy in-flight children and rows a given sweeper cannot act on
  no longer starve the rows behind them). Redis additionally purges staged
  orphans (rows whose parent went terminal without ever landing a
  manifest) during the sweep.
- **Retention vs cascades**: automatic pruning (keep policies, historyTtl)
  skips a settled parent whose rows still owe cascade cancels — those rows
  are the only record that cancels are due in the child stores. `remove`
  stays an operator override.

## Goal

A parent job fans out N child jobs, suspends until all of them settle, then
resumes with their typed results. Children may live on a **different store**
than the parent: a cron-scheduled parent in Postgres fans out 10,000
idempotent email sends into Redis and collects the outcomes back in Postgres.

Non-goals for v1: nested flows, trees deeper than one level, a child-store
outbox relay (v2), paginated `childResults` (v2 — v1 loads settled results
as one array).

## The architectural invariant

**The parent's store owns the flow.** The child manifest, per-child status +
returned value, the pending counter, and every settle decision live in the
parent's store, so "the flow settles exactly once" is a single-store
transaction — the same atomicity class as every existing op.

Cross-store then requires two at-least-once mechanisms, both idempotent:

1. **Reports** (child side, push): a child worker writes the result into the
   parent store before acking its own store.
2. **Reconciliation** (parent side, pull): the parent worker's flow sweeper
   reads child state cross-store and repairs whatever the push path missed.

The review's central lesson: the push path alone is NOT sufficient. Children
reach terminal states with no worker ack — stall exhaustion flips a job to
`failed` store-side, a producer cancels a waiting child, `remove()` deletes
one — and a report blocked on a parent-store outage outlives the child's
lock (the worker's heartbeat stops at handler completion), stalling the
child into exactly that no-ack terminal path. Reconciliation is therefore a
first-class mechanism, not a backstop.

## Definition API

```ts
const DigestFlow = Flow.make("daily-digest", {
  parent: SendDigest,            // a Job bound to the Postgres store
  children: [SendEmail],         // Jobs, each bound to its own store
  onChildFailure: "continue"     // "continue" (default) | "fail"
})
```

- Producer surface delegates to the parent job: `DigestFlow.enqueue`,
  `.execute`, `.schedule` (cron parent → fan-out), `.poll`, `.cancel`. The
  parent is a real job row in the parent store; a scheduled parent needs no
  flow awareness in the schedule row — flow behavior comes from the
  registered handler and the persisted flow phase (below).
- `children` is the closed set of member definitions: store key, exit
  schema (for decoding results), and name for each.

### The two-phase handler

```ts
DigestFlow.toLayer({
  fanOut: (payload, ctx) =>
    Effect.gen(function*() {
      const users = yield* Users.active
      return Flow.children(SendEmail, users.map((user) => ({
        key: user.id,                          // unique within the flow
        payload: { userId: user.id }
      })))
    }),

  collect: (payload, children, ctx) =>
    Effect.succeed({ sent: children.completed.length, failed: children.failed.length })
})
```

- `fanOut` returns a `Flow.ChildrenSpec` (groups per child type; duplicate
  keys throw at construction). Payloads are validated and encoded through
  each child's schema at spec construction.
- **Phase dispatch is persisted, not inferred.** The parent record carries
  `flowPhase: "fan-out" | "collect"` (parent stores only). A fresh parent
  claims as `fan-out`; the settle that flips `waiting-children → waiting`
  sets `collect` in the same transaction. The worker dispatches on the
  claimed record's phase — a resumed parent can never re-run `fanOut`, and
  an empty `ChildrenSpec` settles to `collect` inside the FanOut ack itself
  (no `waiting-children` visit, no loop).
- The parent's `attempts` budget spans both phases: `fanOut` retries until
  the manifest lands; after resume, a failing `collect` retries with the
  remaining budget.
- `Flow.toLayer` requires the parent's StoreId **and every child's StoreId**
  (the R channel composes as with `Job.toLayer`). This is what makes the
  parent worker the one process guaranteed capable of reconciliation and
  cascade.

### Fan-out mechanics (runtime-owned, not producer machinery)

Review finding: `enqueueMany` cannot express per-item ids, and child
`idempotencyKey`/`dedupe` callbacks would collide across flows or fight the
flow's own identity scheme. So the flow runtime builds raw store
`EnqueueRequest`s itself:

- id: `flow/<parentStoreKey>/<flowId>/<childKey>` — namespaced by the
  parent store key so two parent stores sharing one child store can never
  collide (parent job ids like `j-1` repeat across stores).
- `dedupe: undefined`, and the child definition's `idempotencyKey`/`dedupe`
  callbacks are **not applied** to flow children — `childKey` is the
  idempotency mechanism. Documented loudly.
- `parent` envelope attached: `{ flowName, flowId, childKey, parentStoreKey }`.
- trace: the runtime captures the fan-out run's span context before acking
  and stamps it on the child requests (the children are enqueued *after*
  the FanOut ack, outside the handler span — the captured context, not an
  ambient span, is what links them). Note for docs: 10k children under one
  trace argues for `traceLinking: "link"` or sampling.
- Store writes go through the store contract's plain `enqueue`/`enqueueMany`
  with explicit per-request ids (contract already supports ids on each
  `EnqueueRequest`; only the `Job`-level batch API hides them).

### Child workers

Children are plain jobs registered normally on their own workers. A worker
that runs flow children declares it:

```ts
Worker.layer({ store: EmailStore, flows: [DigestFlow] })
```

`flows` pulls each flow's parent StoreId into the layer requirements.
**Misconfiguration policy** (review: release loops starve the whole queue —
the same worker re-claims the job immediately): a worker that claims a child
whose flow is not registered fails it **unrecoverably** with
`failedReason: "worker cannot report to flow <name>; register it in
Worker.layer({ flows })"`, logs at error level, and bumps a metric. The
failure is store-side terminal — which the reconciliation sweep converts
into a failed report, so the parent learns about the misconfiguration
instead of hanging. Visible failure over silent livelock.

## Store contract changes

### All drivers

- `EnqueueRequest.parent` / `JobRecord.parent`: opaque persisted envelope
  (same treatment as `trace`). PG: jsonb column; Redis: hash field.

### Parent-capable stores (all three drivers implement)

New job state **`waiting-children`**. Ripple (review: enumerate it):
`JobState` union grows; `counts()` gains the bucket; `claim` must never
return it; `promote`/`retry` reject it (`JobNotPromotable`/`JobNotRetryable`);
`cancel` applies (see settles); `list` filters include it; dashboards see a
new state. Breaking for driver authors and state-enumerating UIs.

1. **`AckOutcome` gains `FanOut`** — `{ _tag: "FanOut", children: ReadonlyArray<ChildSpec> }`
   where `ChildSpec` carries the full child enqueue request (name, store
   key, encoded payload, options, childKey). Atomically: parent →
   `waiting-children` + `flowPhase: "collect"` scheduled for resume,
   `pendingCount = children.length`, one dependency row per child (status
   `pending`, full spec stored — storing the spec is what makes single-store
   crash recovery possible). Empty spec: settle straight to `waiting` +
   phase `collect`. Ledger records the attempt with outcome `"fanned-out"`.
   Lock-token-guarded like every ack.

2. **`recordChildResult(report)`**:
   ```ts
   (report: {
     flowId: JobId; childKey: string
     outcome: "completed" | "failed" | "cancelled"
     exit: unknown                      // encoded child exit
   }) => Effect<{ applied: boolean; parentSettled: boolean }, JobStoreError>
   ```
   Atomic: dependency row `pending → outcome` (+exit), decrement counter;
   on zero — or on first failure under the `"fail"` policy persisted at
   FanOut time — settle the parent. **Settles that are not
   last-report-decrements (fail-fast, cancel of a waiting-children parent)
   flip every remaining `pending` row to `cancelled` in the same atomic
   op** (review: otherwise rows dangle forever and late reports need a
   separate flow-settled check — with marking, the existing "row already
   terminal → `applied: false`" rule covers them, and `listChildResults`
   stays truthful). Duplicate and unknown reports: `applied: false`.
   **Lock ordering rule** (review; BullMQ's PG backend hit this class):
   every writer touches the dependency row first, the parent row second —
   reports, fail-fast marking, and cancel marking all follow it, so
   report-vs-settle cannot deadlock.

3. **`listChildResults(flowId, { cursor?, limit? })`** — feeds `collect`
   and dashboards.

4. **`flowSweepWork()`** — returns two work classes, **scoped by parent
   state** (review: the unscoped version re-runs real work on settled
   flows):
   - *reconcile*: parents in `waiting-children` with `pending` rows older
     than a threshold, with the stored child specs;
   - *cascade*: settled parents whose rows are `cancelled` but not yet
     `cascaded` (a per-row flag set by the sweeper once the child store
     confirms).

### Child stores

No new ops in v1. Children are plain jobs plus the `parent` envelope.

## The flow sweeper (parent worker, `flowSweepInterval`, default 30s)

The reconciliation engine. For each *reconcile* item, the sweeper reads the
child job in its store (`getJob` by deterministic id — the flow layer holds
every child store):

| Child store state | Action |
| --- | --- |
| Missing | Enqueue from the stored spec (first drive, or the child was `remove()`d / pruned before reporting — see retention) |
| Waiting / delayed / active | Nothing (review: no blind re-enqueue — the existence check is what prevents re-driving every unsettled child every 30s) |
| Terminal (`completed`/`failed`/`cancelled`) | Synthesize the idempotent `recordChildResult` from the store-recorded exit/`failedReason` — this is how stall-exhausted, producer-cancelled, and misconfigured-worker children reach the parent |

For each *cascade* item: issue `cancel` into the child's store (idempotent,
`JobNotFound`/`JobNotCancellable` treated as done), then mark the row
`cascaded`. The sweeper owns cascades because it is the only process
guaranteed to hold every child store layer — the child worker that delivers
a fail-fast report has the parent store but *not* sibling stores, and a
parked parent has no heartbeat for a cancel signal to ride on (review: as
previously written, the cascade had no deliverer).

Sweeper work is idempotent and crash-safe by construction: enqueue dedups by
id, reports dedup by row state, cancels dedup by child state, `cascaded`
flags dedup the flag write.

## Crash matrix (revised)

| Failure | Recovery |
| --- | --- |
| Crash during `fanOut`, before manifest | Plain handler retry (attempts budget) |
| Crash after manifest, before/mid child enqueue | Sweeper reconcile: child missing → enqueue from spec |
| Report written, crash before child ack | Child stalls → re-runs (documented at-least-once) → duplicate report drops → ack |
| Report blocked (parent store down) until the child's lock expires | Child stalls; if requeued, the re-run reports; if stall-exhausted to store-side `failed`, sweeper reconcile synthesizes the failed report once the parent store returns |
| Child stall-exhausted / cancelled-while-waiting / removed (no worker ack ever) | Sweeper reconcile synthesizes the report from child-store state (or a "child lost" failure when the row is gone) |
| Last two reports race | Dependency-row-then-parent-row lock order serializes; exactly one settle (conformance-pinned) |
| Fail-fast settle races the final completion report | Whichever commits first wins; the loser finds its row already terminal (`cancelled` by the settle's marking) and drops |
| Parent cancelled during `waiting-children` | Settle + row marking in one op; sweeper cascades child cancels; late reports drop on terminal rows |
| Parent retention mid-flow | Impossible: `waiting-children` is not a terminal state, retention never touches it (review: the previous matrix listed a non-event). The real race is **child** retention pruning a terminal-but-unreported child — reconcile then reports "child lost"; guidance below prevents it |
| Resume crash before `collect` acks | Parent is `waiting` like any job; re-claimed; `flowPhase` dispatches `collect` |
| Admin `retry` of a failed flow parent | Manifest exists → phase stays `collect`, results replayed (fail-fast-failed flows retry into `collect` with mixed results); no manifest → plain `fan-out`. Double-fan-out impossible: the FanOut ack finds the manifest and rejects |

## Failure semantics

- `"continue"` (default): all children settle; `collect` sees
  `children.completed` / `failed` / `cancelled`, each `{ key, name, exit }`
  decoded per child schema (the flow definition supplies the schemas).
- `"fail"`: first failed report settles the parent as `failed` with
  `FlowChildFailedError` carrying that child's decoded exit; remaining rows
  marked `cancelled` in the same op; sweeper cascades real cancels;
  `collect` does not run (until an admin `retry`, which enters `collect`).
- `"ignore"` from the old roadmap collapses into `"continue"`.

## Scale profile (the 10k case)

- Fan-out: FanOut ack inserts 10k dependency rows (chunked multi-row
  VALUES, existing machinery); Redis-side child enqueue via chunked scripts.
  Redis as a *parent* store carrying 10k specs in ARGV also chunks —
  `FanOut` ack accepts the manifest in chunks under one lock-token guard,
  with the state flip in the final chunk.
- Reports: one parent-store write per child completion in v1. The
  dependency-row update parallelizes (distinct rows); **the pending-counter
  decrement serializes on the parent row** — sub-millisecond lock hold, so
  thousands/sec through one flow is fine, but it is a per-flow ceiling and
  the doc says so plainly (review: volume was analyzed, contention was
  not). Parent store down: children block at the report step and may stall
  into the reconcile path — at-least-once holds end to end, "children keep
  completing during outages" is **v2 outbox** territory, not a v1 claim.
- v2 outbox: child ack atomically appends the report to a child-store
  outbox; a relay drains it into the parent store in idempotent batches
  (batched decrements also lift the counter ceiling). The child-store stall
  sweep must append outbox reports too, or reconciliation stays load-bearing
  for stalls (it stays as defense-in-depth regardless).
- `collect` gets one array in v1; paginated/folding accessor in v2.
- Retention: results are copied into the parent store, so child stores may
  prune aggressively — but **child terminal `keep` must exceed
  `flowSweepInterval`** (with sweeps-are-behind margin), or reconcile finds
  pruned children and reports "child lost". Recommended:
  `keep: { completed: { age: "1 hour" } }` with the default 30s sweep.
  Dependency rows delete with the parent (CASCADE / keyed cleanup).

## Observability

- Metrics: `flow_fanouts{flow}`, `flow_child_reports{flow,outcome,source}`
  (`source: "report" | "reconcile"`), `flow_resumes{flow,settled}`,
  `flow_cascades{flow}`, unreportable-child failures.
- Child spans link to the captured fan-out span context (see fan-out
  mechanics); `onJobFailure` fires for child failures on their worker and
  for `FlowChildFailedError` on the parent worker — no new hook.

## Conformance additions

Parent-store section: FanOut ack (state + phase + counter + rows + empty
spec + manifest-exists rejection + ledger outcome), `recordChildResult`
(idempotency, decrement, duplicate/unknown drop, exactly-one-settle under
concurrent last reports, fail-fast marking of remaining rows, lock-order
pin), `listChildResults` pagination, `flowSweepWork` scoping ("yields
nothing after fail-fast settle / after cancel"), reconcile synthesis from a
store-side-failed and a store-side-cancelled child, cascade flagging,
`waiting-children` rejection by claim/promote/retry, `retry`-of-flow-parent
phase dispatch. Cross-store e2e: two memory stores under TestClock (unit);
Postgres parent + Redis children (docker integration).

## Phasing

- **v1 (0.5.0):** all of the above except the outbox — contract + three
  drivers as parent stores, `parent` envelope everywhere, `Flow.make` /
  `Flow.toLayer` / `Worker.layer({ flows })`, sweeper with reconcile +
  cascade, conformance, docs.
- **v2:** child-store outbox + relay (including stall-sweep outbox
  appends), batched decrements, paginated results, nested flows
  reconsidered.

## Decisions settled here

- Two-phase handler with **persisted phase dispatch** (`flowPhase`).
- Reconciliation is first-class: the sweeper reads child state cross-store;
  the push path is the fast path, never the only path.
- Sweeper work is scoped by parent state; settles terminally mark rows.
- The parent worker owns cascades (only process with every store layer).
- Flow children bypass child `idempotencyKey`/`dedupe`; ids are
  `flow/<parentStoreKey>/<flowId>/<childKey>`.
- Misconfigured workers fail children unrecoverably (visible, reconciled)
  rather than release-looping.
- Policies: `"continue" | "fail"`.
- One lock-ordering rule everywhere: dependency row, then parent row.
