# Retention & history

effect-mq keeps everything forever by default: terminal job records and their attempt ledgers are durable history, queryable through `poll`, `attempts`, and `store.list`. When you want pruning, two layers control it, and both split by terminal state (`completed` / `failed` / `cancelled`) because completed jobs are usually noise and failed ones evidence.

## Per-job `keep`

`keep` is a per-enqueue option (usually set in `defaults`) persisted on the record. The flat form applies one rule to all terminal states; the split form sets independent rules per state:

```ts
import { Job } from "effect-mq"
import { Schema } from "effect"

class GenerateInvoice extends Job.make("generate-invoice", {
  payload: { invoiceId: Schema.String },
  defaults: {
    keep: { completed: { age: "1 day" }, failed: { count: 500, age: "90 days" } }
  }
}) {}

// Or flat: the same rule for completed, failed, and cancelled:
// keep: { count: 100, age: "30 days" }
```

| Field | Meaning |
| --- | --- |
| `count` | keep at most this many terminal records, per job name + state |
| `age` | remove terminal records older than this (`Duration.Input`) |

Mixing the two forms (`{ count: 100, completed: { ... } }`) throws at definition time: the same keys would mean different things in each shape, so the normalizer refuses the ambiguity.

The store applies `keep` after every terminal ack, scoped to jobs sharing the same **name and state**: when a `GenerateInvoice` run completes, the store prunes the completed `GenerateInvoice` group to its policy and leaves the failed group untouched. In the split form, an absent state keeps its records until the store-level ceiling below (forever without one).

## Store-level `historyTtl`

`keep` only fires when a job in the same name+state group reaches a terminal ack, so nothing prunes a job name that has gone quiet. The store-level ceiling covers that gap: every driver accepts `historyTtl` and sweeps it in the background on an interval.

```ts
MemoryJobStore.layerWith({ historyTtl: "7 days" })

DrizzleJobStore.layer({
  ...tables,
  historyTtl: { completed: "1 day", failed: "90 days" },
  historySweepInterval: "5 minutes"     // default: 1 minute
})
```

A single duration applies to all three terminal states. The per-state object sets independent ceilings, and the timer never sweeps an absent state; the sweep touches its rows only when they carry their own `keep.age`.

For each row the sweep honours **`min(per-row keep.age, per-state ceiling)`**: the timer prunes a record whose own `keep.age` is stricter than the ceiling at that age, without waiting for the group's next ack, and the ceiling caps everything else.

::: warning
`historyTtl: {}`, or any object without a `completed`/`failed`/`cancelled` key, throws at construction. Such an input would otherwise normalize like a `DurationObject` to a 0 ms ceiling and wipe all history on the first sweep without a word of warning, so the store refuses it up front. Use `"90 days"`-style inputs or an explicit per-state split.
:::

## What gets deleted

Retention deletes whole job records: the attempts ledger goes with its job (`ON DELETE CASCADE` on Postgres; deleted with the job in Memory and Redis). There is no partial pruning: a retained job keeps its full run history, and manual `retry` keeps appending to it. The sweep also drops dead [dedup](/guide/deduplication) bookkeeping: expired throttle windows, and pending-dedup keys whose job has vanished.

Repeatable-job tick records are ordinary jobs and prune like any other. That is safe by construction: [occurrence claiming](/guide/repeatable-jobs) is a compare-and-swap on the schedule row itself, so ticks stay exactly-once no matter how hard you prune old tick jobs.

## Queue history is not business history

Set `keep` and `historyTtl` so they can prune aggressively. Domain facts ("what did this invoice run produce") belong in your own tables, joined by the deterministic job id from `idempotencyKey`; see [Defining jobs](/guide/defining-jobs). The queue's history is operational evidence with a shelf life. Keep business history in your own tables, on your own schedule.

## Where to next

- [Cancellation & admin](/guide/cancellation-and-admin): `store.remove`, and the ledger admins read.
- [Repeatable jobs](/guide/repeatable-jobs): why schedule ticks survive pruning.
- [Postgres](/storage/postgres): the tables retention operates on.
- [Reference: options](/reference/options): every retention knob in one table.
