# Roadmap

Open work only, prioritized. Shipped features are documented in the
[package README](./packages/effect-mq/README.md) and recorded in the
[CHANGELOG](./CHANGELOG.md). Design notes reference prior art researched
during the initial build (BullMQ v6's Redis/Postgres backends,
`effect/unstable/workflow`, `DurableQueue`).

## P1 — product polish

- [ ] **Atomic schedule ticks** — close the last exactly-once gap in
  repeatable jobs: today a *pathologically* stale sweeper (lagging longer
  than the history retention window) could re-enqueue a pruned slot, because
  dedup rests on the slot job's row existing. Add a store op that claims the
  slot and enqueues in one transaction (`tickSchedule(key, expectedRunAt,
  next, request) -> fired`), conformance-pinned.
- [ ] **Trace propagation** — persist `traceId`/`spanId`/`sampled` at
  enqueue and restore the handler's span parent from it
  (`Tracer.externalSpan`, exactly as `DurableQueue` does), so producer →
  handler traces connect across processes.
- [ ] **Cross-process event stream** — append-only events table with
  cursor replay (the `QueueEvents` analogue): powers live dashboard updates,
  `waitUntilFinished`-style APIs without polling, and audit trails. NOTIFY
  coalescing caveats already solved for the wake channel apply here.
- [ ] **Batch enqueue** — `enqueueMany` in one round trip (one INSERT with
  multi-row VALUES); flagged as an easy perf win during the storage research.
- [ ] **Drizzle schema customization + typed queue registry** (pair these) —
  the remaining factory features (custom *indexes* via `extraConfig` and
  custom *columns* via `extend`/`extraValues` shipped in 0.3.0):
  - *Configurable column names*: `mqJobs(name, { columns: { runAt:
    "scheduled_at", ... } })` for shops with naming conventions. Prerequisite:
    the store's raw SQL must derive every column identifier from the table
    config (`${jobs.runAt}` refs / `getTableConfig`) — today the INSERT
    column lists and SET fragments hardcode the snake_case names.
  - *Typed queue registry*: queue names as a user-declared union checked at
    `Job.make` and `Worker.layer` — the same generics machinery.

## P2 — scale & topology

- [ ] **Parent-child fan-out (flows)** — `waiting-children` state +
  dependency table (parent key, child key, status, returned value); children
  decrement the parent's pending count in their ack transaction; per-child
  failure policies (fail/continue/ignore parent). Known trap from BullMQ's PG
  backend: child-finish vs parent-removal deadlocks through the FK cascade —
  serialize on a per-parent advisory lock.
- [ ] **Global queue concurrency + rate limiting** — store-enforced "≤ N
  active per queue" checked in `claim` (today concurrency is per-worker), and
  a `{ max, duration }` limiter with `claim` returning the retry-after so
  idle workers sleep precisely.
- [ ] **Redis store hardening** — indexed `list` filters (secondary
  index sets instead of the Lua scan), Redis Cluster support (hash-tagged
  keys so scripts stay single-slot), and a `waiting`-set benchmark.
- [ ] **Archive-table split** — move terminal rows to an append-only
  archive so the hot claim index stays small with indefinite history; the
  drizzle factories grow an archive table and `list` reads the union.
- [ ] **Standalone `@effect/sql-pg` driver** — same table layout, no
  drizzle required; gets LISTEN/NOTIFY natively. Also: an adapter for
  promise-based drizzle databases.
- [ ] **Single-process `KeyValueStore` driver** — restart-durable queues
  over any `effect/unstable/persistence` `KeyValueStore` (file system, etc.),
  explicitly documented as single-process (plain KV has no cross-process
  atomicity; multi-process stays Postgres/Redis territory).

## P3 — later

- [ ] **Reference dashboard** — example app (Bun.serve + HTML imports)
  over `list`/`getAttempts`/`retry`/the event stream; living documentation of
  the admin API.
- [ ] **Sandboxed processors** — run handlers in worker threads/processes
  (`effect/unstable/workers`) for CPU-heavy or crash-isolated jobs.
- [ ] **Workflow interop** — run a job as an `effect/unstable/workflow`
  Activity and/or complete a `DurableDeferred` from a job's result.
