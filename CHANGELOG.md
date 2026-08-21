# Changelog

All notable changes to `effect-mq`. Versions follow 0.x semver: **minor
bumps may break** (the `JobStore` driver contract in particular); patch
bumps are additive.

## 0.3.0 (unreleased)

- **Deduplication modes** — `dedupe: { key, ttl?, extend?, replace? }` at the
  definition (`Job.make({ dedupe: (payload) => key })`) or enqueue level:
  pending dedup, throttle, debounce, and replace-while-delayed. The dedup key
  is name-scoped and separate from the job id by design — ids (explicit,
  `idGenerator`, or store-assigned) are never rewritten. Terminal transitions
  free pending-mode keys; history sweeps prune dead entries.
- **Custom indexes on the drizzle factories** — every factory takes
  `extraConfig: (table) => [index(...).on(...)]` (the same shape as drizzle's
  third `pgTable` argument), appended after the built-in indexes.
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
