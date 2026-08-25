# Changelog

All notable changes to `effect-mq`. Versions follow 0.x semver: **minor
bumps may break** (the `JobStore` driver contract in particular); patch
bumps are additive.

## 0.7.0 — unreleased

- **List ordering** — `list` gains `orderBy` (`enqueuedAt` | `runAt` |
  `finishedAt`; missing fields sort as 0) and `order` (`asc` | `desc`),
  with the id tiebreak following the direction and cursors valid only
  with the options that produced them. The contract defines a required
  (filter, orderBy) surface every driver serves (conformance-pinned);
  beyond it a driver either serves the query or dies with the new
  `ListOrderUnsupportedError`, never a silent full scan. Memory and
  Postgres serve every combination. Breaking for driver authors (new
  required list behavior); the default order is unchanged.
- **Redis list indexes** — `list` on Redis now routes through indexes
  instead of scanning every job: new `byname`/`byqueue` zsets (maintained
  at the insert/delete choke points) for the immutable dimensions, and
  the existing delayed/terminal structures for `runAt`/`finishedAt`
  ordering. On by default; opt out per index with
  `RedisJobStore.layer({ indexes })`, after which a query that needs a
  disabled index dies with `ListIndexDisabledError`. The first startup
  after the upgrade backfills the indexes from existing rows (cursor-
  scanned in pages, marker-gated, idempotent), and every later startup
  heals the tail of rows enqueued since the marker — so rolling deploys
  with still-running 0.6.0 writers converge on their last instance's
  boot; no manual migration. Two invariants: the `indexes` config is
  per-PREFIX (every store sharing a prefix must agree), and disabling an
  index removes only its marker, never shared zsets. `metadata`
  filtering remains a per-row predicate by design.

## 0.6.0 — 2026-08-25

- **Parent-child flows, cross-store, nestable** — the new `Flow` module: a
  parent job fans out N children (each bound to its own store: a Postgres
  cron parent can fan 10k idempotent sends into Redis), parks in the new
  `waiting-children` state, and resumes with their typed results.
  `Flow.make({ parent, children, onChildFailure })` +
  `Flow.children(job, items)` + the two-phase
  `flow.toLayer({ fanOut, collect })` handler; the producer surface
  (`enqueue`/`execute`/`poll`/`awaitResult`/`cancel`/`schedule`, ...)
  delegates to the parent job. `collect` reads results three ways:
  `counts` (plain numbers off the parent record), `all` (materialized
  typed buckets), or `stream` (paged, for flows too large to hold).
  Flows nest (a child may be another flow's parent), with a depth cap
  of 8 as the cycle guard. Child keys are the idempotency mechanism
  (deterministic ids derived from parent store + flow id + key; the
  child's `idempotencyKey`/`dedupe` do not apply); duplicate keys and
  payload-validation bugs fail the fan-out unrecoverably.
- **The outbox: reports that cannot be lost** — every terminal child
  transition (ack, cancel, stall exhaustion, a nested parent's store-side
  settle) appends the child's report to its own store's outbox in the
  same atomic operation. Worker relays (`Worker.layer({ flows })`) drain
  outboxes into the parent stores in batches (on each ack, and on a
  fallback cadence), so children keep completing through parent-store
  outages and per-flow report throughput amortizes across batches. A
  parent-side flow sweeper (`flowSweepInterval`, default 30s)
  reconciles anything the push path misses, from storage alone, and
  cascades cancels after fail-fast/cancel settles. New metrics:
  `effect_mq_flow_fanouts`, `effect_mq_flow_child_reports` (by
  `outcome`/`source`), `effect_mq_flow_cascades`,
  `effect_mq_flow_outbox_skipped`; the parent's ledger records a
  `fanned-out` entry (consuming no attempt).
  Docs: [Parent-child flows](https://www.effect-mq.com/guide/flows).
- **Postgres migration (applies to every drizzle user, flows or not)** —
  the jobs table gains nullable columns (`parent` jsonb, `flow_fail_fast`
  boolean, `flow_pending`/`flow_completed`/`flow_failed`/`flow_cancelled`
  integers); two new tables ship via the `mqFlowChildren()` and
  `mqFlowOutbox()` schema factories; and `DrizzleJobStore.layer` now
  **requires** the `flowChildren` and `flowOutbox` table options (a
  compile error until you add them; run `drizzle-kit generate`/`migrate`
  before deploying). The Redis driver needs no migration: old hashes
  decode without the new fields.
- **Store contract (breaking for driver authors)** — `JobState` gains
  `"waiting-children"` (counts bucket included; never claimable;
  promote/retry reject it; cancel settles it and marks its children);
  `EnqueueRequest.parent`/`JobRecord.parent` (opaque envelope, persisted
  like `trace`) and `JobRecord.flow` (phase marker + per-outcome
  counters); `AckOutcome` gains `FanOut`; new ops `recordChildResults`
  (idempotent batches, apply-all-rows-then-settle, fail-fast wins ties,
  dependency-rows-then-parent lock order), `listChildResults`,
  `flowSweepWork` (returned rows re-arm, so pages rotate),
  `markChildrenCascaded`, and the outbox pair `peekOutbox`/`deleteOutbox`
  with the terminal-transition append invariant; `AttemptRecord.outcome`
  gains `"fanned-out"`; automatic retention must skip settled parents
  whose rows still owe cascade cancels. All conformance-pinned (the suite
  grew ~26 flow tests, including the settle-race, fail-fast-tie,
  batch-semantics, outbox-invariant, cancel-racing-fan-out, and
  retention-exemption pins).
- **`Worker.CurrentJob` replaces the handler context parameter** — handlers
  are now `(payload) => Effect<...>`; the running attempt (`jobId`, `name`,
  `queue`, `attempt`, `attemptsMax`) comes from the `Worker.CurrentJob`
  service, readable at any depth of the handler's call graph without
  threading a parameter. `toLayer` (and `Flow.toLayer`, whose `fanOut`/
  `collect` drop their context arguments the same way) subtracts the
  service from the handler's requirements, so declaring it makes "this
  code only runs inside a job" a compile-time fact. Breaking for handlers
  that used the second argument; payload-only handlers compile unchanged.
  Workers also annotate every handler log line with
  `effectMqJobId`/`effectMqQueue`/`effectMqAttempt`.
- Also fixed while under review: the Postgres driver's `list()` had been
  omitting `dedupeKey` and `trace` from listed records (now
  conformance-pinned via a list/getJob parity check).

## 0.5.0 — 2026-08-24

- **Declarative schedule reconciliation** — `JobSchedules.layer({ group,
  schedules, removal, removeAfter, stores })` declares a service's full
  schedule set: startup upserts everything declared (idempotent,
  cadence-preserving) and detects deletion drift, the schedule whose
  `.schedule()` call was deleted from code but keeps firing. Pruning is
  scoped to the ownership `group`: unlabeled schedules (plain `.schedule()`)
  and other groups are never touched, the default `removal: "warn"` only
  logs, and destructive pruning is the explicit `removal: "group"` opt-in
  with an optional `removeAfter` grace window that keeps rolling deploys
  from thrash-pruning (the deferred prune re-checks the store when it
  fires and dies with the layer scope). Store contract (breaking for
  driver authors): `ScheduleRecord.group` + `listSchedules({ group })`;
  `ScheduleOptions.group` on `Job.schedule`. Postgres adds a nullable
  `group_name` column to the schedules table (one drizzle-kit migration).

## 0.4.2 — 2026-08-24

- **Failure logging + `onJobFailure` hook** — workers now log every failed
  run through Effect's logger: `logWarning` while retries remain (with the
  backoff delay), `logError` once a job lands terminal `failed` — including
  jobs failed by stall exhaustion — annotated with
  `effectMqJobId`/`effectMqQueue`/`effectMqAttempt` and the failure cause,
  so log-based alerting works with no setup. For custom reporting,
  `Worker.layer({ onJobFailure })` receives `{ jobId, name, queue, attempt,
  attemptsMax, willRetry, cause }` after each failed ack; the hook runs
  isolated (a failing hook is logged, never disturbs processing).
- **Error schema lists** — `Job.make({ error: [A, B, C] })` accepts a list
  of schemas and unions the members (the `HttpApiEndpoint` style), so
  multi-way failures need no manual `Schema.Union`. `retryable`, `execute`,
  `awaitResult`, and the decoded attempts ledger see the union type. Single
  schemas are unchanged.

## 0.4.1 — 2026-08-23

- **Typed queue and job-name columns in the drizzle factories** — the
  `queue` columns are now generic, mirroring the existing `JobName`
  parameter on `mqJobs`: `mqJobs<JobName, Queue>` (new second type
  parameter), `mqSchedules<JobName, Queue>` (also types the previously
  plain-`string` `jobName` column), `mqDedupe<JobName>` (types `name`),
  and `mqQueueControl<Queue>`. Pass your own branded type or literal
  union; everything defaults to the prior types (`QueueName` brand /
  `string`), and the annotations are compile-time only — the store still
  brands values at the boundary. Note for explicit-type-argument callers:
  `mqJobs`'s `Extend` parameter moved from second to third position
  (inferred usage via the `extend` option is unaffected).

## 0.4.0 — 2026-08-23

- **Atomic schedule ticks** — repeatable-job occurrences are now claimed
  and enqueued in a single atomic store op (`tickSchedule(key,
  expectedRunAt, next, request) -> fired`; breaking for driver authors).
  Previously exactly-once rested on the slot job's row existing, so a
  sweeper stale past the history-retention window could re-fire a pruned
  slot; the compare-and-swap now refuses regardless of retention.
  Conformance-pinned, including the pruned-slot regression.
- **Batch enqueue** — `MyJob.enqueueMany(payloads, options?)` inserts a
  whole batch of plain items in one store round trip per chunk (store
  contract: `enqueueMany`, breaking for driver authors). Postgres uses
  multi-row `INSERT ... ON CONFLICT` VALUES, Redis one Lua script per
  chunk; items that carry a dedup key run through the single-enqueue path
  individually, in order. Per-item semantics match `enqueue` —
  `idempotencyKey`/`dedupe`/`metadata` callbacks run per payload,
  duplicates report positionally — while shared options apply batch-wide
  (`jobId`/`dedupe` are excluded at the type level). The batch is not one
  transaction: a mid-batch failure can leave a subset applied, which is
  safe under at-least-once with deterministic ids.

- **Trace propagation** — the enqueue span's `traceId`/`spanId`/`sampled`
  persist on the job record and become the handler span's parent
  (`Tracer.externalSpan`), so producer → handler traces connect across
  processes and time. Run spans default to `` `${name}.run` ``
  (configurable via `Worker.layer({ handlerSpanName })`) and carry
  `effectMqJobId`/`effectMqQueue`/`effectMqAttempt` attributes. Attachment
  is policy-driven (`Worker.layer({ traceLinking })`, default `"auto"`):
  immediate enqueues continue the producer trace as parent-child, while
  explicitly delayed/`at`-scheduled jobs start their own trace with a
  causal span link back — keyed off scheduling intent captured at enqueue,
  so queue backlog never changes trace shapes. Store contract:
  `EnqueueRequest.trace` + `JobRecord.trace`; Postgres adds a `trace` jsonb
  column (drizzle-kit diffs it).

- **Absolute-time scheduling** — `enqueue(payload, { at })` accepts any
  `DateTime.Input` (a zone-aware `DateTime`, `Date`, ISO string, epoch
  millis, or date parts), so "9am Monday in New York" needs no duration
  math. `delay` and `at` are mutually exclusive at the type level; a past
  `at` runs immediately. No store-contract change — the delay is resolved
  against the Effect Clock at enqueue.
- **Cancel by dedup key** — `MyJob.cancelByKey(key)` (store contract:
  `cancelByDedupe(name, key)`, breaking for driver authors) cancels
  whatever pending job holds the key: delayed/waiting terminally, active
  via the heartbeat flag. Idempotent — returns false when nothing is
  pending. Together with dedupe `replace` + `at`, this completes the
  schedule/reschedule/cancel lifecycle for one-shot future work keyed by
  business ids.

## 0.3.2 — 2026-08-22

- **`TestJobStore` test harness** (`effect-mq/testing`) — assert what your
  services enqueue in unit tests: `TestJobStore.layer` provides an in-memory
  store to the code under test, and `enqueuedOf(JobClass)` returns the
  accumulated jobs with payloads decoded through the job's schema (typed
  `Redacted`/`DateTime`/branded values, not stored JSON), oldest-first,
  drained past pagination. `layerFor` covers named stores; the raw store is
  exposed for claim/ack simulation.

## 0.3.1 — 2026-08-22

- **Effect `Metric` integration** — the new `Metrics` module exposes the
  instruments workers and producers emit: enqueues, runs + durations by
  outcome, queue latency (`effect_mq_job_wait_duration_ms`), claim churn,
  in-flight and depth gauges (opt-in `queueMetricsInterval` sampler), lock
  losses, cancel interrupts, stall recoveries, and schedule ticks. Metrics
  are process-local — export them with your observability stack; durable
  history stays in the store (`counts()`, the attempt ledger).

## 0.3.0 — 2026-08-22

- **Retention split by terminal state** — both per-job `keep` and the
  store-level `historyTtl` accept `{ completed?, failed?, cancelled? }`
  (flat values still apply to all states), so completed noise and failed
  evidence get independent lifetimes. The background sweep now also honours
  stricter per-row `keep.age` rules, pruning quiet job names on the timer.
- **Queue-filtered wake-ups** — the Postgres NOTIFY payload and the Redis
  pub/sub message now name the queue, and `awaitWake` filters waiters by it:
  an enqueue wakes only the takers watching its queue instead of every idle
  taker on the store, removing the claim amplification that made many queues
  expensive.
- **Upgrade compatibility with 0.2.x data**: rows persisted with the old flat
  `keep` shape keep pruning (read-side fallback in every driver), and the
  Redis driver lazily migrates the old unsplit `finished` zset during sweeps.
- **Fixed** (pre-release review): a livelock when a wake resumed a taker
  that immediately re-parked (worker concurrency ≥ 2 could hang the process
  on live clocks); dedupe replace now wakes the keyed job's actual queue;
  `historyTtl: {}` (or a DurationObject) now fails loudly instead of
  normalizing to a 0 ms ceiling and wiping history; the Redis sweep
  self-heals orphaned finished-zset members instead of looping.

- **Deduplication modes** — `dedupe: { key, ttl?, extend?, replace? }` at the
  definition (`Job.make({ dedupe: (payload) => key })`) or enqueue level:
  pending dedup, throttle, debounce, and replace-while-delayed. The dedup key
  is name-scoped and separate from the job id by design — ids (explicit,
  `idGenerator`, or store-assigned) are never rewritten. Terminal transitions
  free pending-mode keys; history sweeps prune dead entries.
- **Custom indexes on the drizzle factories** — every factory takes
  `extraConfig: (table) => [index(...).on(...)]` (the same shape as drizzle's
  third `pgTable` argument), appended after the built-in indexes.
- **Custom columns on the jobs table** — `mqJobs({ extend: { companyId:
  text("company_id").notNull(), ... } })`: extended columns are filled at
  enqueue from the job's `metadata` entry with the same TS key (override or
  coerce with the store's `extraValues` option), rewritten by dedupe
  `replace`, visible to `extraConfig` indexes, and fully typed in your own
  drizzle queries.
- Store contract (breaking for driver authors): `EnqueueRequest.dedupe`,
  `JobRecord.dedupeKey`; Postgres adds the `effect_mq_dedupe` table and a
  `dedupe_key` jobs column (one drizzle-kit migration).

## 0.2.0 — 2026-08-21

- **Repeatable jobs** — durable cron/interval schedules
  (`Job.schedule`/`Job.unschedule`) with slot-deterministic tick ids
  (exactly-once per occurrence across workers); re-registering an unchanged
  cadence preserves the next occurrence.
- **Cancellation** — waiting/delayed jobs become terminal immediately;
  running jobs are interrupted cross-process via the lock heartbeat
  (finalizers run) and land in the terminal `cancelled` state. Cancellation
  wins over every revival path.
- **Admin verbs** — `promote` (delayed → now) and queue-level
  `pause`/`resume`, honoured atomically inside `claim`.
- **Handler timeouts** — `defaults.timeout` / per-enqueue `timeout`
  interrupts the handler and routes through normal retry accounting.
- **Unrecoverable errors** — `Job.unrecoverable(error)` and the
  definition-level `retryable` predicate skip the remaining retry budget.
- **Per-store history TTL** — `historyTtl` retention ceiling on every store,
  swept in the background.
- **Redis store** — `effect-mq/redis`: one atomic Lua script per `JobStore`
  operation over `effect/unstable/persistence`'s client-agnostic `Redis`
  service (provide `NodeRedis`/`BunRedis` layers or `Redis.make`); pub/sub
  wake channel; the full conformance suite runs against a real server under
  TestClock.
- **Configurable job ids** — `idGenerator` on every store for
  store-assigned ids (e.g. `` ({ name }) => `${name}_${ulid()}` ``); sync or
  Effect-returning, bounded collision retries.
- **Renamed** `effect-mq/drizzle` → `effect-mq/drizzle-postgres` (the
  implementation is Postgres-specific).

## 0.1.0 — 2026-08-20

- Initial release: schema-first job definitions (`Job.make`), the atomic
  `JobStore` seam, `MemoryJobStore`, the `Worker` runtime (at-least-once,
  token-guarded locks, heartbeat renewal, stalled recovery, durable retries
  with backoff), attempt ledger, idempotency keys, metadata projection,
  `list`/`retry`/`keep` admin surface, named multi-store routing, the
  drizzle Postgres store (`FOR UPDATE SKIP LOCKED` claims, LISTEN/NOTIFY
  wake-ups), redaction-aware schema-encoded persistence, and the
  `effect-mq/testing` conformance suite (TestClock against real storage).
