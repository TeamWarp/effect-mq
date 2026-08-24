# Writing a driver

Everything effect-mq does goes through one interface: the `JobStore` service. Implement it and the whole library (producers, workers, schedules, dedup, admin verbs) runs on your storage. Drivers stay dumb: payloads and exits arrive already schema-encoded (JSON-safe values), so a store never decodes, validates, or inspects them. Encoding happens in `Job` on the producer side and `Worker` on the consumer side.

A driver is a `Layer` that provides `JobStore.Service` for the default `JobStore` key (and, where possible, a `layerFor` variant for [named stores](/storage/stores)). The full contract with per-operation JSDoc lives in [`src/JobStore.ts`](https://github.com/TeamWarp/effect-mq/blob/main/packages/effect-mq/src/JobStore.ts); [`MemoryJobStore`](https://github.com/TeamWarp/effect-mq/blob/main/packages/effect-mq/src/MemoryJobStore.ts) is the reference implementation to read alongside it.

## The contract, by concern

Every operation returns an Effect failing with `JobStoreError` (plus operation-specific typed errors noted below).

| Concern | Operations | The gist |
| --- | --- | --- |
| Enqueue | `enqueue`, `enqueueMany` | Insert jobs: `delayMs > 0` routes to `delayed`, otherwise `waiting`. Duplicate ids are a silent no-op returning `duplicate: true`. `enqueueMany` inserts plain items in bulk (one round trip per chunk); items carrying a dedup key run through the single-enqueue decision tree individually, in order. |
| Claim & run | `claim`, `ack`, `release`, `extendLocks`, `recoverStalled` | `claim` atomically promotes due delayed jobs, then hands out the best runnable job matching `queue` + `names` (highest priority first, FIFO within a priority) under a lock token — never a `waiting-children` flow parent. `ack` applies a `Complete`/`Retry`/`Fail`/`Cancelled`/`FanOut` outcome and appends to the attempts ledger. `release` returns a job to `waiting` without consuming an attempt (worker shutdown). `extendLocks` is the heartbeat; it also reports pending cancel requests. `recoverStalled` sweeps expired locks. |
| Flows | `recordChildResult`, `listChildResults`, `flowSweepWork`, `markChildrenCascaded` | The parent-store side of [flows](/guide/flows). The `FanOut` ack is the subtle one: persist the `FlowState` marker + one `pending` dependency row per child (spec included) and park the parent in `waiting-children`, all atomically, consuming NO attempt and writing a `fanned-out` ledger entry; an existing manifest is kept untouched (double fan-outs converge). `recordChildResult` is idempotent on the row, decrements the pending counter, and settles the parent exactly once — fail-fast settles win ties and mark remaining rows `cancelled`/not-`cascaded`, with one lock order everywhere: dependency row first, parent row second. `flowSweepWork` re-arms the rows it returns (page rotation), and automatic retention must skip parents whose rows still owe cascades. |
| Wake-ups | `awaitWake` | Resolve when new work *may* be runnable for the given queues since the `wakeToken` a previous empty `claim` returned. Must be interruptible; polling-only drivers may never resolve (the worker combines it with `pollInterval`). |
| Inspection | `getJob`, `getAttempts`, `list`, `counts` | The dashboard data layer: single records, the per-run ledger (oldest first), filtered newest-first pagination, and per-state depth counts. |
| Admin | `retry`, `cancel`, `cancelByDedupe`, `promote`, `remove`, `pause`/`resume`/`pausedQueues` | `retry` re-runs a failed job with a fresh attempt budget, ledger intact. `cancel` makes waiting/delayed jobs terminal immediately and flags active ones for the owning worker's heartbeat. `remove` refuses active jobs (returns `false`). `pause` durably stops claims per queue; `resume` must wake idle workers. |
| Schedules | `upsertSchedule`, `removeSchedule`, `listSchedules`, `dueSchedules`, `tickSchedule`, `advanceSchedule` | Durable repeatable-job rows keyed by `ScheduleKey`. `tickSchedule` is the atomic heart: iff `nextRunAt` still equals `expectedRunAt`, insert the tick job **and** advance the schedule in one transaction. A stale sweeper's tick returns `false` without inserting, so each occurrence fires exactly once no matter how many workers sweep. |

The typed errors: `ack` and `release` fail with `LockLostError` when the presented token no longer owns the job (and `JobNotFoundError` for unknown ids); `retry`/`cancel`/`promote` fail with `JobNotFoundError` and a state-mismatch error (`JobNotRetryableError`, `JobNotCancellableError`, `JobNotPromotableError`).

Stores also accept an `idGenerator` for store-assigned ids. Custom ids, idempotency keys, and schedule tick ids (`sched/<key>/<slot>`) always win over it; the store retries colliding generated ids a bounded number of times, then fails the enqueue.

## Non-negotiable invariants

The conformance suite checks these behaviorally. Internalize them before writing a line of storage code:

1. **Every operation is atomic.** `claim` promotes-then-claims in one step; `ack` verifies the token, increments `attemptsMade`, appends the ledger entry, and applies the outcome as one unit; `tickSchedule` is a compare-and-swap plus insert plus advance. Two workers racing any operation must never both win.
2. **Acks are token-guarded.** A worker presents the lock token from its claim on every `ack`/`release`; if the job stalled and was recovered, or another worker claimed it, the store must refuse with `LockLostError`. Token checks keep at-least-once delivery from turning into accidentally-twice.
3. **All time comes from the Effect `Clock`, passed into storage.** Take `now` as a bind parameter (or Lua `ARGV`), never SQL `now()` or the server's clock. This rule lets the conformance suite drive real Postgres and real Redis under `TestClock`: the test advances virtual time and your queries see it.
4. **Wake-ups are queue-filtered and token-versioned.** `awaitWake` takes the `wakeToken` observed by the previous empty `claim`, so a wake that fires between the claim and the wait is not lost. Spurious wake-ups are fine: the worker claims again and finds nothing. A lost wake-up leaves a job unclaimed until the poll fallback.
5. **Delivery is at-least-once.** Design every mutation assuming the process can die between any two operations; `recoverStalled` plus token guards make the redelivery safe.

::: warning
Item 3 is the one drivers most often get wrong. A single `DEFAULT now()` column or `redis.call("TIME")` in a Lua script breaks TestClock conformance, and with it deterministic testing of delays, backoff, retention, and schedules.
:::

## The conformance suite

`effect-mq/testing` exports the suite every driver must pass: ninety-odd behavioral tests covering claims, locks, retries, stalls, dedup, schedules, retention, flows, pause/resume, and wake-up semantics. Run it from a vitest file (it needs `@effect/vitest`, the suite's only extra peer):

```ts
import { jobStoreConformance } from "effect-mq/testing"
import { MyJobStore } from "./MyJobStore.ts"

jobStoreConformance("MyJobStore", () => MyJobStore.layer)
```

The suite calls the factory per test, so each test gets a fresh store; layers needing live resources can wrap connection setup inside. The suite runs under `TestClock` even against real storage: the Postgres and Redis drivers in this repo pass the same suite against a real database and a real Redis, virtual time included.

Start from `MemoryJobStore`: it is small, single-file, and it spells out each subtle decision (promote-before-claim, ledger numbering across `retry`, dedup lifecycle, the `wakeToken` versioning) in plain data-structure code before you translate it to SQL or Lua.

## Where to next

- [Memory & multiple stores](/storage/stores): named store keys your driver should support via `layerFor`.
- [Postgres](/storage/postgres) and [Redis](/storage/redis): how the shipped drivers map the contract to real storage.
- [Testing your app](/guide/testing): the application-level testing story built on the same seam.
