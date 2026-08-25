# Roadmap

Open work only, prioritized. Shipped features are documented in the
[package README](./packages/effect-mq/README.md) and recorded in the
[CHANGELOG](./CHANGELOG.md). Design notes reference prior art researched
during the initial build (BullMQ v6's Redis/Postgres backends,
`effect/unstable/workflow`, `DurableQueue`).

## P1 — product polish

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
  - *Configurable primary id columns*: native Postgres id generation on the
    drizzle layer — `mqJobs(name, { id: "uuidv7" | "bigserial" | <column> })`
    with the INSERT omitting the id so the database default (`uuidv7()`,
    identity) generates it, returned via RETURNING. Known constraints to
    design around: the attempts-table FK type must follow; `JobId` stays a
    string brand (uuid text is fine, bigint stringifies); deterministic text
    ids clash with non-text columns — `idempotencyKey` and the schedule slot
    ids (`sched/<key>/<slot>`) both mint text ids, so either restrict native
    id types to definitions that use neither, or split identity like
    graphile-worker (native PK + a separate unique text `key` column that
    carries idempotency/slot identity — pairs naturally with the atomic
    `tickSchedule` item above, which removes the slot-id dependence).
  - *Configurable timestamp column types*: let users swap the `timestamptz`
    columns for their conventions — plain `timestamp`, epoch-millis
    `bigint`, different precision. Constraint: the driver's raw SQL binds
    `Date` params and compares/derives on these columns everywhere
    (`run_at <= $now`, retention `GREATEST`/`make_interval` math, RETURNING
    mappers), so the column config must also swap the bind encoding and the
    read mapping — same table-config-derived mechanism as column renames,
    which is why these ship together.

## P2 — scale & topology

- [ ] **Global queue concurrency + rate limiting** — store-enforced "≤ N
  active per queue" checked in `claim` (today concurrency is per-worker), and
  a `{ max, duration }` limiter with `claim` returning the retry-after so
  idle workers sleep precisely.
- [ ] **Redis store hardening** — indexed `list` filters, Redis Cluster
  support (hash-tagged keys so scripts stay single-slot), and a
  `waiting`-set benchmark. Indexed-list design notes (settled in
  discussion, deliberately deferred until the reference dashboard or a
  real slow-list report creates a consumer):
  - Driver-internal only — the `list` contract and its conformance-pinned
    behavior do not change, Postgres is untouched (its factory indexes
    already serve `list` as SQL), and shipping it is a patch, not a
    driver-contract break.
  - `name`/`queue` are immutable per job, so the two missing indexes
    (`<prefix>:byname:<name>`, `<prefix>:byqueue:<queue>` zsets) only need
    maintaining in `insertJobRow` and `deleteJob` — the single insert and
    delete choke points every path already flows through. `finished:<state>`,
    `terminal:<name>:<state>`, `active`, and the per-queue pending zsets
    already exist and cover the rest.
  - The list script picks the narrowest applicable index for the filter
    combination and post-filters residual predicates per row (no
    ZINTERSTORE); `metadata` filtering stays a scan by design — an
    inverted index per key/value is a cardinality bomb, and metadata
    querying is documented as Postgres territory.
  - The real work is migration: old rows have no index entries, so an
    indexed scan would silently MISS them (wrong results, not slow ones).
    Needs a lazy backfill with a readiness flag — fall back to the full
    `all`-zset scan until a sweep finishes backfilling — like the 0.3.0
    finished-zset migration.
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

## Maybe — filed for later

Designed but deliberately not committed; revisit when there's a concrete
pull.

- [ ] **Job event stream (pub/sub)** — a durable, cursor-ordered event log,
  NOT ephemeral broadcast: the store appends a slim `JobEvent` (cursor,
  type, jobId, name, queue, timestamp) *inside the same transaction/Lua
  script* as each transition; Postgres = append-only table with a bigserial
  cursor, Redis = `XADD` stream with a `MAXLEN` cap. The log is a capped
  feed, not the archive (the attempts ledger stays the archive). Consumer
  architecture: `JobEvents.stream({ names?, queues?, after? })` — an Effect
  Stream that reads batches after a cursor and parks on the queue-filtered
  wake channel when caught up (push latency, lossless: a missed notify is
  found by the next read). At-least-once from the caller's cursor; global
  cursor order per store. Ships together with event-driven `awaitResult`
  (replacing its poll loop) and `retryAll({ name?, queue?, before? })` bulk
  redrive. Explicitly out of scope: general application messaging (this is
  a job-event feed, not a message bus).
  - *v2, on top*: durable named subscribers
    (`JobEvents.durable("alerts", handler)`) — store-persisted cursors plus
    a lease so exactly one instance is active; this is the general "dead
    letter hook" mechanism.
  - *Dead-letter sugar* (`deadLetter: { job, map }` on `Job.make`): enqueue
    a compensation job on terminal failure. Note there is no DLQ *queue* to
    build — the terminal `failed` state with the ledger, `list`, `retry`,
    and per-state retention already is the dead-letter store (same
    conclusion BullMQ reached with its failed set).

## P3 — later

- [ ] **Reference dashboard** — example app (Bun.serve + HTML imports)
  over `list`/`getAttempts`/`retry`/the event stream; living documentation of
  the admin API.
- [ ] **Sandboxed processors** — run handlers in worker threads/processes
  (`effect/unstable/workers`) for CPU-heavy or crash-isolated jobs.
- [ ] **Workflow interop** — run a job as an `effect/unstable/workflow`
  Activity and/or complete a `DurableDeferred` from a job's result.
