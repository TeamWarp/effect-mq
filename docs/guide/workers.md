# Workers & handlers

A worker is the runtime that claims jobs from a store and runs your handlers. You build one from two layers: `MyJob.toLayer(handler)` registers a handler, and `Worker.layer(options)` provides the runtime underneath. Handlers are Effect fibers, which is what lets the worker time them out, cancel them cross-process, and shut down without losing work.

## Registering handlers

`toLayer` takes the handler and optional registration options:

```ts
import { Worker, MemoryJobStore } from "effect-mq"
import { Effect, Layer } from "effect"

const SendEmailWorker = SendEmail.toLayer(
  (payload, ctx) =>
    Effect.gen(function*() {
      yield* Effect.log(`run ${ctx.attempt}/${ctx.attemptsMax} of ${ctx.jobId}`)
      return `message-${ctx.jobId}`
    }),
  { concurrency: 5 }
)

const RunnerLive = SendEmailWorker.pipe(
  Layer.provideMerge(Worker.layer()),
  Layer.provideMerge(MemoryJobStore.layer)
)
```

The payload arrives decoded through the job's schema. The second argument is the `JobContext` for this run:

| Field | Type | Meaning |
| --- | --- | --- |
| `jobId` | `JobId` | the job being run |
| `name` | `string` | the job's tag |
| `queue` | `QueueName` | the queue this run was claimed from |
| `attempt` | `number` | 1-based attempt number |
| `attemptsMax` | `number` | total attempts allowed for this job |

Anything the handler requires (`R`) surfaces on the layer — provide your services to the handler layer and they are captured at registration time, so taker fibers never leak one job's locally provided services into another's. Registering two handlers for the same job name is a defect.

The registration options are `concurrency` (taker fibers for this job's queue — the first registration for a queue decides; later values for the same queue are ignored) and `queue` (consume from a different queue than the definition's).

## Worker options

`Worker.layer(options)` — all durations accept `Duration.Input`:

| Option | Default | Meaning |
| --- | --- | --- |
| `store` | default `JobStore` | which named store this worker claims from |
| `concurrency` | 1 | default taker fibers per queue |
| `queues` | — | per-queue overrides: `{ email: { concurrency: 5 } }` |
| `lockDuration` | 30s | how long a claim's lock lasts before the job counts as stalled |
| `lockRenewInterval` | half of `lockDuration` | heartbeat cadence (also delivers cross-process cancels) |
| `stalledInterval` | 30s | how often to sweep for stalled jobs |
| `maxStalledCount` | 1 | stalls tolerated before a job is failed outright |
| `pollInterval` | 5s | idle fallback when no wake-up arrives |
| `scheduleSweepInterval` | 15s | how often to tick due [repeatable-job schedules](/guide/repeatable-jobs) |
| `queueMetricsInterval` | off | sample `store.counts()` per queue into the depth gauge |
| `handlerSpanName` | `` `${name}.run` `` | name of the span wrapping each handler run |
| `traceLinking` | `auto` | how handler spans attach to the producer trace — see [Observability](/guide/observability) |
| `id` | random | identifier used in lock tokens (handy for telling workers apart in the store) |

Per-queue concurrency resolves as: the worker's `queues` entry, then the registration's `concurrency`, then the worker's `concurrency`, then 1.

## The claim loop

Each queue gets its configured number of taker fibers, all running the same loop: claim → decode payload → run handler → ack. A claim atomically promotes due delayed jobs and hands out the best runnable one (highest priority first, FIFO within a priority), locked with a worker-generated token for `lockDuration`.

When the claim comes back empty, the taker does not busy-poll. It races three wake sources:

- **A push wake-up from the store**, filtered to this taker's queue — LISTEN/NOTIFY on Postgres, pub/sub on Redis, an in-process signal on the memory store. An enqueue wakes only the takers watching its queue, so many queues do not amplify into many claims.
- **A timer** capped at `pollInterval`, shortened to the next delayed job's `runAt` when the store knows one.
- **A local pulse** fired when a new handler registers.

Because wake-ups are push-based, the 5s `pollInterval` default is fine even on Postgres — it is the fallback, not the mechanism.

## Locks and heartbeats

Every `lockRenewInterval` the worker extends the locks of all in-flight jobs in one store call. A lock that cannot be renewed was lost — stall recovery or another worker owns the job now — so the worker logs a warning (the job may run twice; delivery is at-least-once) and the run's eventual ack surfaces the lost lock instead of overwriting the new owner's state.

The heartbeat also delivers cross-process [cancellation](/guide/cancellation-and-admin): when a cancel request is flagged on an in-flight job, the worker interrupts that handler fiber and acks the job as cancelled, without blocking the heartbeat on the handler's finalizers.

::: warning
`lockDuration` is the crash-detection window, not a run-time limit — the heartbeat keeps long handlers locked indefinitely. But anything that stops the heartbeat for longer than `lockDuration` (a blocked event loop, a long process pause) makes the job count as stalled and another worker will re-run it. Keep handlers idempotent.
:::

## Stalled recovery

Every `stalledInterval`, the worker sweeps the store for active jobs whose lock has expired — the signature of a crashed or partitioned worker. Any worker on the store performs recovery, not just the one that lost the job. Each recovered job gets a `stalled` entry in its run ledger and its stall counter incremented; it returns to `waiting` unless the counter now exceeds `maxStalledCount`, in which case it is failed outright with a `failedReason` (a job that keeps taking workers down should stop being retried). With the default of 1, the first stall re-queues the job and a second fails it.

Stalls use their own counter — they do not consume the retry budget, since the handler may never have misbehaved. See [Retries & timeouts](/guide/retries-and-timeouts) for how the two budgets interact in the ledger.

## Graceful shutdown

Closing the worker layer's scope interrupts in-flight handlers and releases their jobs back to `waiting` — no attempt consumed, no ledger entry, immediately claimable by another worker. The claim and ack steps run uninterruptibly, so a shutdown can never orphan a just-claimed job. One deliberate exception: a job whose cancel request arrives during shutdown is acked as cancelled rather than released — cancellation must not revive.

## Multiple workers per process

Workers bind to exactly one store. To run several against different infrastructure, provide each `Worker.layer({ store })` *locally* to its handler group with `Layer.provide` (not `provideMerge`, which would share one worker across groups):

```ts
import { JobStore, Worker } from "effect-mq"
import { Layer } from "effect"

const Durable = JobStore.named("durable")

const durableWorkers = GenerateInvoice.toLayer(handleInvoice).pipe(
  Layer.provide(Worker.layer({ store: Durable }))
)

const defaultWorkers = SendEmail.toLayer(handleEmail).pipe(
  Layer.provide(Worker.layer({ concurrency: 5 }))
)
```

Registering a job on a worker bound to a different store is a defect with a message naming both stores — a job's runs must live where its producers enqueue them. See [Multiple stores](/storage/stores) for the producer side.

## Where to next

- [Retries & timeouts](/guide/retries-and-timeouts) — the attempt budget, backoff, and the run ledger.
- [Cancellation & admin](/guide/cancellation-and-admin) — cancel, promote, pause/resume.
- [Observability](/guide/observability) — spans, trace linking, and worker metrics.
- [Options reference](/reference/options) — every knob in one place.
