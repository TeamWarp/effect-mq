# Repeatable jobs

`MyJob.schedule(key, options)` registers a durable cron or interval schedule. A schedule is a **row in the store rather than a process-local timer**: it survives restarts, and any number of workers can sweep it without double-firing. Same key means replace, so you can run registration at startup on every boot:

```ts
import { Effect } from "effect"

const program = Effect.gen(function*() {
  yield* SendDigest.schedule("daily", {
    cron: "0 9 * * *",              // or: every: "10 minutes"
    tz: "America/New_York",         // IANA zone, cron only (default UTC)
    payload: { edition: "morning" }
  })

  // Later: returns false when the key did not exist.
  yield* SendDigest.unschedule("daily")
})
```

`schedule` is a producer verb: it needs the job's store in context, never the `Worker`. The returned `ScheduleKey` is name-qualified (`SendDigest/daily`), so keys from different jobs never collide.

## Cadence: `cron` or `every`

Set exactly one of the two (setting both or neither dies with a clear message):

- **`cron`**: a 5-field expression (`"0 9 * * 1"` = 9:00 every Monday), with an optional IANA `tz`. The first occurrence is the *next matching time*.
- **`every`**: a fixed interval (any `Duration.Input`). The first occurrence is one interval from now, and later occurrences stay on that original grid, so a slow sweep never causes drift.

## Options

Beyond the cadence, `ScheduleOptions` mirrors what an enqueue accepts; the worker creates each occurrence's job with them:

| Option | Meaning |
| --- | --- |
| `payload` | the payload every occurrence is enqueued with (schema-encoded, like any enqueue) |
| `metadata` | merged over the definition's `metadata(payload)` |
| `priority` | higher runs first; default 0 |
| `attempts` | attempt budget per occurrence |
| `backoff` | retry backoff per occurrence |
| `keep` | retention for each occurrence's terminal record |
| `timeout` | per-run execution limit |

The worker also stamps each tick job's metadata with `scheduledFor`, the occurrence's ISO timestamp, so a dashboard can tell the 9:00 run from a 9:04 catch-up.

## Exactly-once ticks

Every worker runs a schedule sweep (cadence set by `Worker.layer({ scheduleSweepInterval })`, default 15 seconds; see [Workers](/guide/workers)). When a schedule is due, the sweep claims the occurrence and enqueues its job in **one atomic store operation**: a compare-and-swap on the schedule's `nextRunAt`, the tick-job insert, and the advance to the next occurrence happen together. Concurrent sweepers lose the CAS and insert nothing.

Two consequences:

- **Exactly-once does not depend on history.** Tick jobs get deterministic ids (`sched/<name>/<key>/<slot>`, where the slot is the occurrence's epoch millis), but the claim is the CAS rather than an id-uniqueness check, so retention can prune old tick jobs as aggressively as you like without a pruned row ever letting a slot fire twice.
- **Missed occurrences collapse.** If workers are down across several slots, the next sweep enqueues the single overdue slot once, then advances past *now*: one catch-up run instead of a burst of stale ones.

The sweep handles each due schedule in isolation, so one bad row (an unsatisfiable cron, a storage error) never starves the rest.

## Deploy-safe registration

Re-registering an existing key with an **unchanged cadence** (same `cron`/`tz`/`every`) is a no-op for the next occurrence: it neither re-anchors an `every` grid nor drops a pending catch-up run, so calling `schedule` in a startup layer on every deploy is safe. Changing the cadence resets the next occurrence to the new rule.

::: warning
The payload and options are part of the row: re-registering with a new payload replaces what future occurrences run with, even when the cadence is unchanged. On deploy that is usually what you want; don't expect two processes registering *different* payloads under one key to coexist.
:::

## Declaring the full set: reconciliation

`.schedule()` creates and updates, but a schedule whose call was deleted from code keeps firing forever. `JobSchedules.layer` declares a service's full schedule set; on startup it upserts everything declared and detects the drift:

```ts
import { JobSchedules } from "effect-mq"

const SchedulesLive = JobSchedules.layer({
  group: "billing-service",
  schedules: [
    JobSchedules.schedule(SendDigest, "daily", { cron: "0 9 * * *", payload: {} }),
    JobSchedules.schedule(GenerateInvoice, "monthly", { cron: "0 0 1 * *", payload: {} })
  ],
  removal: "group",           // default "warn": log drift, prune nothing
  removeAfter: "10 minutes"   // grace window for rolling deploys
})
```

The safety model rests on the ownership `group`:

- The layer only ever prunes schedules carrying *its own* group. Plain `.schedule()` calls are unlabeled and never pruned; other groups' schedules are never touched. Use one group per service.
- The default `removal: "warn"` logs undeclared group members and removes nothing. Destructive pruning is the explicit `removal: "group"` opt-in.
- `removeAfter` delays the prune and re-checks the store when it fires. During a rolling deploy, replicas on the previous release re-declare schedules the new release dropped; pruning immediately and re-adding would re-anchor `every` grids. Shutting down before the window fires skips the prune; the next startup re-evaluates.
- Reconciliation reaches the stores your entries reference. When a release drops the last schedule a store had, pass the store key in `stores: [Durable]` so drift detection still runs there.

Upserts stay cadence-preserving, so running the layer on every deploy never re-anchors unchanged schedules. Duplicate declarations and `removeAfter` without the `"group"` opt-in fail loudly at startup.

## One-shot future work

Some future-dated work runs once. For "run once at time T, reschedulable and cancellable" (a signup invite, a trial-expiry email), skip the schedule row and combine three primitives: a [`replace` dedup key](/guide/deduplication#replace-while-delayed), an absolute `at`, and `cancelByKey`:

```ts
import { DateTime, Schema } from "effect"
import { Job } from "effect-mq"

class SendInvite extends Job.make("send-invite", {
  payload: { userId: Schema.String },
  dedupe: ({ userId }) => ({ key: userId, replace: true })
}) {}

// Schedule for a wall-clock instant:
yield* SendInvite.enqueue({ userId }, {
  at: DateTime.makeZonedUnsafe(
    { year: 2026, month: 8, day: 24, hours: 9 },
    { timeZone: "America/New_York", adjustForTimeZone: true }
  )
})

// Reschedule: the same call with a new time; replace moves the delayed job.
yield* SendInvite.enqueue({ userId }, { at: nextDay })

// The user bailed: cancel whatever is pending, if anything.
const wasPending = yield* SendInvite.cancelByKey(userId)
```

Your business logic keeps no job-id bookkeeping: the dedup key is the handle, and every verb is idempotent.

## Where to next

- [Deduplication](/guide/deduplication): the four dedup modes behind the one-shot pattern.
- [Workers](/guide/workers): the sweep loops a worker runs and how to tune them.
- [Retention](/guide/retention): `keep` and store ceilings for pruning tick history.
- [Reference: options](/reference/options): every schedule, enqueue, and worker knob in one table.
