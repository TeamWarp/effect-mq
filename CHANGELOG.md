# Changelog

All notable changes to `effect-mq`. Versions follow 0.x semver: **minor
bumps may break** (the `JobStore` driver contract in particular); patch
bumps are additive.

## 0.4.0 (unreleased)

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

## 0.3.0 (unreleased)

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
