# Design: list ordering + Redis list indexes

Status: build-ready. Target: 0.7.0 (the `orderBy` contract addition breaks
the driver contract; the Redis indexes alone would be a patch).

## Goal

`list` today filters by `queue`/`name`/`states`/`metadata` on every driver,
in one fixed order (`enqueuedAt` desc). Two gaps:

1. No ordering control. A dashboard wants "what runs next" (`runAt` asc)
   and "what just finished" (`finishedAt` desc).
2. On Redis, every `list` is a Lua scan over the whole `all` zset with
   per-row filtering: O(all jobs) for any filter.

Postgres serves both today (SQL) and stays untouched apart from honoring
the new options. This design adds ordering to the shared contract and
makes Redis serve the existing filters from indexes.

## Contract: `ListOptions.orderBy` / `order`

```ts
interface ListOptions {
  // ...existing filters...
  readonly orderBy?: "enqueuedAt" | "runAt" | "finishedAt" | undefined
  readonly order?: "asc" | "desc" | undefined
}
```

- Defaults preserve today's pinned behavior exactly: `enqueuedAt`, `desc`,
  id-desc tiebreak. `asc` flips both the field and the tiebreak.
- Jobs missing the field (`finishedAt` on non-terminal rows) sort as 0.
- The cursor encodes `<orderValue>:<id>`; a cursor is only valid with the
  same options that produced it (already implied by the contract, now
  stated).
- Not every driver serves every (filter, orderBy) combination without a
  full scan. The contract defines a REQUIRED surface every driver must
  serve (conformance-pinned); beyond it, a driver either serves the query
  or dies with the tagged defect `ListOrderUnsupportedError` — never a
  silent scan. Memory and Postgres serve everything.

Required surface (the conformance floor):

| orderBy | filters |
| --- | --- |
| `enqueuedAt` | any combination (today's behavior, plus `asc`) |
| `runAt` | `states: ["delayed"]` with a `queue` |
| `finishedAt` | `states` ⊆ terminal, with or without `name`/`queue` |

## Redis: index inventory

Two new zsets, both scored by `enqueuedAt` (the default list order), both
maintained only at the two choke points every job already passes through
(`insertJobRow`, `deleteJob`) because `name` and `queue` are immutable:

- `p:byname:<name>` — member id
- `p:byqueue:<queue>` — member id

Everything else rides structures the driver already maintains:

| structure | score | serves |
| --- | --- | --- |
| `p:all` | enqueuedAt | unfiltered lists |
| `p:delayed:<queue>` | runAt | `runAt` ordering for delayed work |
| `p:finished:<state>` | finishedAt | `finishedAt` ordering by state |
| `p:terminal:<name>:<state>` | finishedAt | `finishedAt` ordering by name+state |

No per-state global index: state is mutable, so a global by-state zset
would add a write to every transition in the hot ack path, and the
structures above already cover the state queries worth serving.

## Redis: routing

The list script picks the narrowest structure for the query, pages it by
`(score, id)`, loads each candidate row, applies the remaining predicates
(`states`, `metadata`, whichever of `queue`/`name` the structure does not
already pin), and accumulates until `limit`:

1. `orderBy: "finishedAt"` → `terminal:<name>:<state>` when `name` is set,
   else a ≤3-way merge of `finished:<state>`; queue/metadata are residual.
2. `orderBy: "runAt"` → `delayed:<queue>`; name/metadata residual.
3. `orderBy: "enqueuedAt"` (default) → `byname:<name>` if `name` set, else
   `byqueue:<queue>` if `queue` set, else `all`; the rest residual.

Queries outside the driver's matrix die with `ListOrderUnsupportedError`.
`metadata` stays a residual predicate everywhere by design (an inverted
index per key/value is a cardinality bomb; metadata querying is documented
Postgres territory).

Self-heal: a routed scan that finds an index member whose job hash is gone
ZREMs the member (same idiom as the flow sweep's stale-membership heal).

## Redis: opt-out

```ts
RedisJobStore.layer({ indexes: { name: false } })   // or indexes: false
```

Only `name` and `queue` are toggleable (the other structures are core).
Default is on. A query that ROUTES to a disabled index dies with the
tagged defect `ListIndexDisabledError`, naming the filter and the config
key — the config said "we never list this way", so the contradiction is a
config bug, matching the library's die-on-config-mistake idiom. Routing
runs first: `list({ name, states: ["failed"], orderBy: "finishedAt" })`
uses `terminal:<name>:<state>` and works with the name index off.

A store with opted-out indexes is deliberately narrower than the contract
(conformance runs against the defaults).

## Redis: backfill and the tail heal (revised after the adversarial review)

Rows written before 0.7.0 (or while an index was off) have no index
entries; an indexed read would silently MISS them — wrong results, not
slow ones. Index writes are baked into each PROCESS's scripts, so
correctness is a keyspace property no single process can guarantee: during
a rolling deploy, still-running pre-index writers insert unindexed rows
AFTER a new instance's backfill completes. The review confirmed the
original one-shot marker made those rows permanently invisible. The
shipped lifecycle, at store init per index:

- enabled and `p:index:<kind>:ready` (`kind` = the literal `name` or
  `queue`) absent → full build: ZSCAN the `all` zset from the driver
  (cursor-based, linear, tie-immune — an earlier score-keyset design was
  quadratic on equal-`enqueuedAt` runs), each member chunk fed to a small
  script that HMGETs and ZADDs. Set the marker to the build's start time.
- enabled and the marker present → tail heal: re-index rows with
  `enqueuedAt > marker - 60s`, then advance the marker. Every boot. This
  is what makes rolling deploys converge: the last instance to boot heals
  everything older writers inserted before that moment, and afterwards
  every writer indexes live.
- disabled → delete the marker only, never the zsets (a sibling store may
  be reading them; stale members self-heal on reads, manual cleanup
  documented). Re-enabling later finds no marker and full-builds.

The `indexes` config is a per-prefix invariant — every store sharing a
prefix must agree — and the docs say so. No build lock: concurrent cold
boots duplicate idempotent ZADDs, a crash mid-build leaves the marker
unset and the next boot redoes it, and rows inserted during a build are
indexed live by `insertJobRow`. The deliberately-cut alternative — lazy
backfill with readiness flags and scan fallback — buys zero-pause upgrades
for datasets where the startup scan is too slow (millions of retained
rows); it slots into the same marker mechanism later without API change.

## Errors

Both are `Data.TaggedError` classes delivered as defects (config bugs, not
recoverable states): `ListOrderUnsupportedError { orderBy, message }`
(contract-level, JobStore.ts) and `ListIndexDisabledError { index, message }`
(Redis-only, RedisJobStore.ts), each message naming the offending filter
and, for the latter, the config key to flip.

## Conformance

- `orderBy`/`order` pins for the required surface: `enqueuedAt` asc + desc
  with cursoring across pages; `runAt` asc over delayed jobs in a queue;
  `finishedAt` desc over terminal states (with and without `name`);
  missing-`finishedAt` rows sorting as 0 where the driver serves mixed
  states (memory/PG only, driver tests).
- Default-order behavior stays pinned unchanged.
- Redis driver tests: routing per structure, opt-out defects, unsupported
  combination defect, backfill (insert with indexes off, re-init with
  them on, list correctness), marker skip on re-init, orphan self-heal,
  opt-out cleanup deleting the zsets.

## Non-goals

- Metadata indexes (Postgres territory, documented).
- Per-state global indexes (write amplification for queries the existing
  structures already serve).
- Lazy backfill (deferred until a dataset too large for the startup scan
  exists).
- Redis Cluster (unchanged: plain-prefixed keys, non-cluster).
