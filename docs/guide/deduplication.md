# Deduplication

Deduplication stops redundant enqueues of the same logical work: syncing an account's payments twice in the same minute, or rebuilding a search index for every keystroke. A dedup key is a **separate, name-scoped value**: it never rewrites job ids. Whatever produced the id (an explicit `jobId`, `idempotencyKey`, a store `idGenerator`, or the default sequence) stays in charge; dedup only decides whether a new enqueue creates a job at all.

A deduplicated enqueue is a silent no-op that returns the keyed job's id (`duplicate: true` at the store level), so producers keep a usable handle whether they created the job or joined an existing one.

## Choosing the key

Set the key in the definition, derived per payload, or per enqueue. The per-enqueue value overrides the definition's callback:

```ts
import { Job } from "effect-mq"
import { Effect, Schema } from "effect"

class SyncPayments extends Job.make("sync-payments", {
  payload: { accountId: Schema.String },
  dedupe: ({ accountId }) => accountId          // string shorthand = { key }
}) {}

const program = Effect.gen(function*() {
  // Definition-derived key:
  yield* SyncPayments.enqueue({ accountId: "acct-1" })

  // Per-enqueue override, with a mode:
  yield* SyncPayments.enqueue(
    { accountId: "acct-1" },
    { dedupe: { key: "acct-1", ttl: "1 minute" } }
  )
})
```

A bare string is shorthand for `{ key }`. Keys must be non-empty and are scoped per job name, so `"acct-1"` on `sync-payments` and `"acct-1"` on `send-invite` never collide.

## The four modes

Three fields compose into four behaviors:

| Options | Behavior |
| --- | --- |
| `{ key }` | dedupe while the keyed job is pending; finishing frees the key |
| `{ key, ttl }` | throttle: at most one job per window, even after completion |
| `{ key, ttl, extend: true }` | debounce: every dropped enqueue pushes the window out |
| `{ key, replace: true }` | latest-wins while the keyed job is still delayed |

### Pending dedup (the default)

`{ key }` alone dedupes while the keyed job is *pending*: waiting, delayed, or active. The moment that job reaches a terminal state (completed, failed, or cancelled), the key is free and the next enqueue creates a fresh job. This is the "one in flight at a time" mode: enqueue as often as you like, at most one sync per account is ever queued or running.

### Throttle

Add a `ttl` and the key holds for the whole window, whether or not the keyed job has finished, so you get at most one job per key per window:

```ts
// At most one cache refresh per account per five minutes:
yield* RefreshCache.enqueue({ accountId }, {
  dedupe: { key: accountId, ttl: "5 minutes" }
})
```

### Debounce

`extend: true` (requires `ttl`, validated at enqueue) makes every deduplicated enqueue push the window out again. The first edit in a burst creates the job; the store drops each later one and re-arms the window, so the key frees `ttl` after the burst ends, and only then can the next edit create a new job.

### Replace while delayed

`{ key, replace: true }` is latest-wins for future work. While the keyed job is still **delayed**, a new enqueue replaces its payload, `metadata`, `priority`, `attempts`, `backoff`, `keep`, `timeout`, and `delay`/`at`: same job id, run ledger preserved. With a `ttl` set, a landed replace re-arms the window from the replace. In any other state (waiting, active), normal dedup applies: the store drops the enqueue and returns the existing id.

This is the mode behind the [one-shot future work pattern](/guide/repeatable-jobs#one-shot-future-work): schedule something for later and reschedule it by enqueueing again, with no job-id bookkeeping.

## Cancelling by key

`cancelByKey` cancels whatever pending job holds a dedup key. It is idempotent by design: it returns `false` when nothing pending holds the key, so "cancel it if anything is scheduled" needs no existence check:

```ts
const wasPending = yield* SyncPayments.cancelByKey("acct-1")
```

Cancellation semantics match [`cancel`](/guide/cancellation-and-admin): waiting/delayed jobs become terminal immediately; the worker interrupts an active job's handler fiber on its next heartbeat.

## Dedup vs `idempotencyKey`

Both prevent duplicates; they answer different questions and coexist on purpose:

| | `idempotencyKey` | `dedupe` |
| --- | --- | --- |
| What it controls | the job **id** itself | whether an enqueue creates a job |
| Lifetime | permanent; the id exists as long as the record does | temporal; pending state or a `ttl` window |
| Use it for | deterministic identity, joinable from your domain tables | rate policy: one-in-flight, throttle, debounce, latest-wins |

`idempotencyKey` makes enqueue a no-op while a job with that id *exists at all* (retention decides how long that is); `dedupe` is policy with its own lifecycle and never touches the id. See [Defining jobs](/guide/defining-jobs) for the idempotency side.

::: tip
On Postgres, dedup adds one table and one jobs column: export `mqDedupe()` from your schema and `drizzle-kit generate` diffs both into one migration. Memory and Redis need nothing. See [Postgres](/storage/postgres).
:::

## Where to next

- [Repeatable jobs](/guide/repeatable-jobs): cron/interval schedules, and the replace + `at` + `cancelByKey` one-shot pattern.
- [Enqueueing](/guide/enqueueing): `delay`, `at`, priorities, and the rest of the enqueue options.
- [Cancellation & admin](/guide/cancellation-and-admin): `cancel`, `promote`, pause/resume.
