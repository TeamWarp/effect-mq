# Roadmap

Prioritized backlog. Effort: **S** (≤ a day), **M** (a few days), **L** (a week+).
Design notes reference prior art researched during the initial build (BullMQ v6's
Redis/Postgres backends, `effect/unstable/workflow`, `DurableQueue`).

## P0 — production readiness

- [x] **Repeatable / cron jobs** (M) — *shipped in 0.2.0 as `Job.schedule`/`Job.unschedule`.*
  Original notes: — the most-requested queue feature after
  retries. Model on BullMQ v6's `JobScheduler`: a scheduler row per schedule;
  each completion enqueues the next delayed occurrence. Effect ships `Cron` +
  `Schedule.cron`; combined with idempotency keys (`key = cron slot`) this
  gives exactly-one-job-per-tick across processes nearly for free.
- [x] **Handler timeout + unrecoverable errors** (S) — *shipped in 0.2.0 (`defaults.timeout`, `Job.unrecoverable`, `retryable`).*
  Original notes: — `defaults.timeout`
  interrupting the handler cleanly (an Effect-native advantage; BullMQ cannot
  kill a running processor) and routing through normal retry/fail accounting;
  plus an `Unrecoverable` error wrapper that skips remaining retries.
- [x] **Admin verbs for dashboards** (M) — *shipped in 0.2.0 (`cancel`/`promote`/`pause`/`resume`, heartbeat-driven cancel of running jobs).*
  Original notes: — `pause`/`resume` (store-level flag
  honored by `claim`), `promote` (delayed → runnable now), and **cancel a
  running job** cross-process (a cancel flag checked by the lock-renewal
  heartbeat, interrupting the handler). Completes the verb set next to the
  existing `retry`/`remove`.
- [x] **Per-store history TTL** (S) — *shipped in 0.2.0 (`historyTtl` on both stores).*
  Original notes: — store/layer-level retention default
  applied to ALL terminal records (e.g.
  `DrizzleJobStore.layer({ historyTtl: "90 days" })`), swept periodically by
  the worker's maintenance loop rather than only at ack time. Semantics: the
  store TTL is the ceiling; per-job `keep` may only be stricter. Prevents
  unbounded table growth from day one.

## P1 — product polish

- [ ] **Atomic schedule ticks** (S) — close the last exactly-once gap in
  repeatable jobs: today a *pathologically* stale sweeper (lagging longer
  than the history retention window) could re-enqueue a pruned slot, because
  dedup rests on the slot job's row existing. Add a store op that claims the
  slot and enqueues in one transaction (`tickSchedule(key, expectedRunAt,
  next, request) -> fired`), conformance-pinned.
- [ ] **Deduplication modes** (M) — beyond id-idempotency: throttle (`ttl`),
  debounce (`extend`), and replace-while-delayed (latest payload wins), per
  BullMQ's dedup-key table. Turns "sync at most once per minute per employer"
  into one enqueue option.
- [ ] **Trace propagation** (S) — persist `traceId`/`spanId`/`sampled` at
  enqueue and restore the handler's span parent from it
  (`Tracer.externalSpan`, exactly as `DurableQueue` does), so producer →
  handler traces connect across processes.
- [ ] **Cross-process event stream** (M) — append-only events table with
  cursor replay (the `QueueEvents` analogue): powers live dashboard updates,
  `waitUntilFinished`-style APIs without polling, and audit trails. NOTIFY
  coalescing caveats already solved for the wake channel apply here.
- [ ] **Effect `Metric` integration** (S) — counters/histograms for queue
  depth, run duration, retries, stalls; exported from the worker loops.
- [ ] **Batch enqueue** (S) — `enqueueMany` in one round trip (one INSERT with
  multi-row VALUES); flagged as an easy perf win during the storage research.
- [ ] **Drizzle schema customization + typed queue registry** (M, pair
  these) — three related factory features:
  - *Custom columns*: `mqJobs<Names>(name, { extend: {...} })` with extras
    flowing through the generics (the driver already INSERTs an explicit
    column list, so extras need only be nullable/defaulted; the drift guard
    gains a superset mode).
  - *Configurable column names*: `mqJobs(name, { columns: { runAt:
    "scheduled_at", ... } })` for shops with naming conventions. Prerequisite:
    the store's raw SQL must derive every column identifier from the table
    config (`${jobs.runAt}` refs / `getTableConfig`) — today the INSERT
    column lists and SET fragments hardcode the snake_case names.
  - *Typed queue registry*: queue names as a user-declared union checked at
    `Job.make` and `Worker.layer` — the same generics machinery.

## P2 — scale & topology

- [ ] **Parent-child fan-out (flows)** (L) — `waiting-children` state +
  dependency table (parent key, child key, status, returned value); children
  decrement the parent's pending count in their ack transaction; per-child
  failure policies (fail/continue/ignore parent). Known trap from BullMQ's PG
  backend: child-finish vs parent-removal deadlocks through the FK cascade —
  serialize on a per-parent advisory lock.
- [ ] **Global queue concurrency + rate limiting** (M) — store-enforced "≤ N
  active per queue" checked in `claim` (today concurrency is per-worker), and
  a `{ max, duration }` limiter with `claim` returning the retry-after so
  idle workers sleep precisely.
- [x] **Redis store** (L) — *shipped in 0.2.0 as `effect-mq/redis`*: one
  atomic Lua script per `JobStore` op over `effect/unstable/persistence`'s
  client-agnostic `Redis` service (users provide `NodeRedis`/`BunRedis`
  layers), pub/sub wake channel, full conformance under TestClock against a
  real server. Follow-ups live in P1/P2 (indexed `list`, cluster support).
- [ ] **Redis store hardening** (M) — indexed `list` filters (secondary
  index sets instead of the Lua scan), Redis Cluster support (hash-tagged
  keys so scripts stay single-slot), and a `waiting`-set benchmark.
- [ ] **Archive-table split** (M) — move terminal rows to an append-only
  archive so the hot claim index stays small with indefinite history; the
  drizzle factories grow an archive table and `list` reads the union.
- [ ] **Standalone `@effect/sql-pg` driver** (M) — same table layout, no
  drizzle required; gets LISTEN/NOTIFY natively. Also: an adapter for
  promise-based drizzle databases.

## P3 — later

- [ ] **Reference dashboard** (M) — example app (Bun.serve + HTML imports)
  over `list`/`getAttempts`/`retry`/the event stream; living documentation of
  the admin API.
- [ ] **Sandboxed processors** (L) — run handlers in worker threads/processes
  (`effect/unstable/workers`) for CPU-heavy or crash-isolated jobs.
- [ ] **Workflow interop** (M) — run a job as an `effect/unstable/workflow`
  Activity and/or complete a `DurableDeferred` from a job's result.
