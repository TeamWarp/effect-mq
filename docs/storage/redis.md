# Redis

The Redis store (`effect-mq/redis`) implements every `JobStore` operation as one atomic Lua script, so it is safe across any number of producer and worker processes. It builds on Effect's client-agnostic `Redis` service — no Redis client is bundled; you provide one from your platform package.

## Provide the Redis service

::: code-group

```ts [Node]
import { RedisJobStore } from "effect-mq/redis"
import { NodeRedis } from "@effect/platform-node"   // node-redis under the hood
import { Layer } from "effect"

const JobStoreLive = RedisJobStore.layer({
  prefix: "myapp-jobs",          // key namespace (default "effect-mq")
  historyTtl: "30 days"          // optional retention ceiling
}).pipe(
  Layer.provide(NodeRedis.layer({ url: process.env.REDIS_URL }))
)
```

```ts [Bun]
import { RedisJobStore } from "effect-mq/redis"
import { BunRedis } from "@effect/platform-bun"     // Bun.redis under the hood
import { Layer } from "effect"

const JobStoreLive = RedisJobStore.layer().pipe(
  Layer.provide(BunRedis.layer({ url: process.env.REDIS_URL }))
)
```

:::

`@effect/platform-node`'s `NodeRedis` has a peer dependency on `redis` (`>=5 <7`); `@effect/platform-bun`'s `BunRedis` uses the built-in `Bun.redis` — nothing extra to install. Any other client works through `Redis.make` from `effect/unstable/persistence`.

`RedisJobStore.layer` accepts:

| Option | Default | Meaning |
| --- | --- | --- |
| `prefix` | `effect-mq` | key prefix for everything this store writes |
| `historyTtl` | off | store-level retention ceiling — one duration or a per-state split (see [Retention](/guide/retention)) |
| `historySweepInterval` | 1 minute | history sweep cadence |
| `idGenerator` | `j-<n>` | generator for store-assigned job ids |

`RedisJobStore.layerFor(MyStore, options)` binds the store to a [named store](/storage/stores) key instead of the default `JobStore`.

## One Lua script per operation

Every mutation — enqueue, claim, ack, dedup decision, schedule tick — runs as a single server-side Lua script, loaded once via `SCRIPT LOAD` and invoked with `EVALSHA` thereafter. That gives the same atomicity guarantees as a Postgres transaction: a claim can never race an ack, a schedule tick fires exactly once no matter how many workers sweep, and batch enqueues insert a whole chunk in one round trip.

Two disciplines make it testable: all time enters via `ARGV` from the Effect Clock (never Redis `TIME`), so the [conformance suite](/storage/writing-a-driver) — the same one that runs against Postgres — drives this driver under `TestClock` against a real server; and no operation is ever split across scripts.

## Key layout

Everything lives under the configurable prefix (`p` below):

| Key | Type | Holds |
| --- | --- | --- |
| `p:job:<id>` | hash | the job record |
| `p:attempts:<id>` | list | JSON attempt-ledger entries |
| `p:waiting:<queue>` | zset | runnable jobs — priority desc, FIFO within a priority |
| `p:delayed:<queue>` | zset | delayed jobs, scored by `runAt` |
| `p:active` | zset | claimed jobs, scored by lock expiry (stalled sweep) |
| `p:all` / `p:finished:<state>` / `p:terminal:<name>:<state>` | zset | listing pagination and retention indexes |
| `p:counts` | hash | `<queue>\|<state>` → integer (O(1) `counts()`) |
| `p:paused` | set | paused queues |
| `p:schedules` / `p:schedule:<key>` | zset + hash | repeatable-job schedules by `nextRunAt` |
| `p:dedupe:<name>\0<key>` / `p:dedupes` | hash + zset | dedup-key registry and window index |

The layout is an implementation detail — inspect it freely with `redis-cli`, but mutate only through the store, the same [rule as Postgres](/storage/postgres#reads-yes-writes-no).

## Wake-ups

Wake-ups ride pub/sub on `<prefix>:wake` with queue-filtered messages: an enqueue publishes its queue's name, so idle workers in other processes wake promptly and only the takers watching that queue react. The worker's `pollInterval` is the fallback; if the subscription drops (Bun subscribers don't auto-reconnect), the store resubscribes and wake-ups degrade to polling in the meantime.

## Operational notes

- Keys are plain-prefixed (no hash tags) — point the store at a single Redis / Valkey node or a cluster-unaware proxy, **not Redis Cluster**.
- `list` filters scan server-side in Lua: fine for dashboards, not for millions of terminal rows — set `historyTtl`/`keep` accordingly. `counts` is O(1) (maintained counters).
- There is no SQL surface. The queryable metadata projection (`store.list({ metadata: { companyId } })`) replaces the custom-columns story Postgres has.

## Redis or Postgres?

| | Redis | Postgres |
| --- | --- | --- |
| Throughput profile | high-rate, disposable work | business-critical work |
| Querying | `store.list` / `counts` | full SQL, typed drizzle queries, custom columns, joins to your domain tables |
| Durability | your Redis persistence config | your database's guarantees |
| Migrations | none needed | drizzle-kit, owned by you |

A common split: invoices and anything auditable in Postgres, cache warming and notification fan-out in Redis. You don't have to choose globally — [named stores](/storage/stores) let jobs bind to different stores in one app, enforced at compile time.

## Where to next

- [Multiple stores](/storage/stores) — Postgres and Redis side by side.
- [Retention](/guide/retention) — keep `list` fast with `historyTtl` and `keep`.
- [Workers & handlers](/guide/workers) — `pollInterval`, locks, and the stalled sweep.
- [Writing a driver](/storage/writing-a-driver) — the conformance suite behind these guarantees.
