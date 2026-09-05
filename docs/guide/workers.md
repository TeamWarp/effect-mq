# Workers & handlers

A worker is the runtime that claims jobs from a store and runs your handlers. You build one from two layers: `MyJob.toLayer(handler)` registers a handler, and `Worker.layer(options)` provides the runtime underneath. Handlers are Effect fibers, which lets the worker time them out, cancel them cross-process, and shut down without losing work.

## Registering handlers

`toLayer` takes the handler and optional registration options:

```ts
import { Worker, MemoryJobStore } from "effect-mq"
import { Effect, Layer } from "effect"

const SendEmailWorker = SendEmail.toLayer(
  (payload) =>
    Effect.gen(function*() {
      const { attempt, attemptsMax, jobId } = yield* Worker.CurrentJob
      yield* Effect.log(`run ${attempt}/${attemptsMax} of ${jobId}`)
      return `message-${jobId}`
    }),
  { concurrency: 5 }
)

const RunnerLive = SendEmailWorker.pipe(
  Layer.provideMerge(Worker.layer()),
  Layer.provideMerge(MemoryJobStore.layer)
)
```

The payload arrives decoded through the job's schema. `Worker.CurrentJob` is a service the worker provides around every run, so anything the handler calls, however deeply nested, can read it without threading a parameter:

| Field | Type | Meaning |
| --- | --- | --- |
| `jobId` | `JobId` | the job being run |
| `name` | `string` | the job's tag |
| `queue` | `QueueName` | the queue this run was claimed from |
| `attempt` | `number` | 1-based attempt number |
| `attemptsMax` | `number` | total attempts allowed for this job |

Code that declares `Worker.CurrentJob` in its requirements can only run inside a job, and the type system enforces it: `toLayer` subtracts the service (the worker supplies it per run), while calling the same code from anywhere else demands an explicit `Effect.provideService(Worker.CurrentJob, {...})`. The worker also annotates every log line a handler writes with `effectMqJobId`/`effectMqQueue`/`effectMqAttempt`, so log-based tooling can group by job with no setup.

Anything else the handler requires (`R`) surfaces on the layer: provide your services to the handler layer and the worker captures them at registration time, so taker fibers never leak one job's locally provided services into another's. Registering two handlers for the same job name is a defect.

The registration options are `concurrency` (taker fibers for this job's queue; the first registration for a queue decides, and the worker ignores later values for the same queue) and `queue` (consume from a different queue than the definition's).

## Worker options

`Worker.layer(options)` takes the following; all durations accept `Duration.Input`:

| Option | Default | Meaning |
| --- | --- | --- |
| `store` | default `JobStore` | which named store this worker claims from |
| `concurrency` | 1 | default taker fibers per queue |
| `queues` | none | per-queue overrides: `{ email: { concurrency: 5 } }` |
| `lockDuration` | 30s | how long a claim's lock lasts before the job counts as stalled |
| `lockRenewInterval` | half of `lockDuration` | heartbeat cadence (also delivers cross-process cancels) |
| `stalledInterval` | 30s | how often to sweep for stalled jobs |
| `maxStalledCount` | 1 | stalls tolerated before a job is failed outright |
| `onLockLost` | `ignore` | whether to interrupt a running handler whose lock was lost; see [Locks and heartbeats](#locks-and-heartbeats) |
| `pollInterval` | 5s | idle fallback when no wake-up arrives |
| `scheduleSweepInterval` | 15s | how often to tick due [repeatable-job schedules](/guide/repeatable-jobs) |
| `queueMetricsInterval` | off | sample `store.counts()` per queue into the depth gauge |
| `handlerSpanName` | `` `${name}.run` `` | name of the span wrapping each handler run |
| `traceLinking` | `auto` | how handler spans attach to the producer trace; see [Observability](/guide/observability) |
| `onJobFailure` | none | callback after each failed run is acked; see [Failure reporting](#failure-reporting) |
| `id` | random | identifier used in lock tokens (handy for telling workers apart in the store) |

Per-queue concurrency resolves as: the worker's `queues` entry, then the registration's `concurrency`, then the worker's `concurrency`, then 1.

## The claim loop

Each queue gets its configured number of taker fibers, all running the same loop: claim → decode payload → run handler → ack. A claim atomically promotes due delayed jobs and hands out the best runnable one (highest priority first, FIFO within a priority), locked with a worker-generated token for `lockDuration`.

When the claim comes back empty, the taker does not busy-poll. It races three wake sources:

- **A push wake-up from the store**, filtered to this taker's queue: LISTEN/NOTIFY on Postgres, pub/sub on Redis, an in-process signal on the memory store. An enqueue wakes only the takers watching its queue, so many queues do not amplify into many claims.
- **A timer** capped at `pollInterval`, shortened to the next delayed job's `runAt` when the store knows one.
- **A local pulse** fired when a new handler registers.

Wake-ups are push-based, so the 5s `pollInterval` default is fine even on Postgres; the timer serves as a fallback rather than the delivery mechanism.

## Locks and heartbeats

Every `lockRenewInterval` the worker extends the locks of all in-flight jobs in one store call. A lock that fails to renew was lost: stall recovery or another worker owns the job now. The worker logs a warning (the job may run twice; delivery is at-least-once), and the run's eventual ack surfaces the lost lock instead of overwriting the new owner's state.

The heartbeat also delivers cross-process [cancellation](/guide/cancellation-and-admin): when a cancel request is flagged on an in-flight job, the worker interrupts that handler fiber and acks the job as cancelled, without blocking the heartbeat on the handler's finalizers.

By default the run that lost its lock is left alone. It keeps going to its natural end, holds its taker slot until then, has its ack refused, and — because cancel requests only reach jobs the worker still counts as in-flight — can no longer be cancelled. That is usually fine for short handlers. For long or expensive ones, stop it at the moment the lock is known gone:

```ts
Worker.layer({ onLockLost: "interrupt" }) // default: "ignore"
```

Under `"interrupt"` the worker interrupts the handler the same way it delivers a cancel: finalizers run, nothing is acked (another worker owns the job now), and no attempt is spent. Latency is one `lockRenewInterval`. Either setting leaves delivery at-least-once — the job still runs again elsewhere — so handlers must stay idempotent regardless; the option only decides whether the losing run keeps burning time and money.

A lock can also go without anyone being able to say so. If the heartbeat cannot reach the store at all — a partition rather than a hand-off — no `lost` list ever comes back, and the worker only knows its own calls are failing. So every in-flight job carries the deadline its lock was claimed under, and once a deadline passes with no successful renewal behind it, the worker reports that lock as lost on its own: `effect_mq_locks_lost` and the "may run twice" warning fire once, and under `"interrupt"` the run is stopped just as it would be for a store-reported loss. Under `"ignore"` the worker keeps renewing the lock regardless — the store has not said it is gone, and a run that outlives the partition keeps its lock if the store comes back before a stall sweep takes the job — so the default changes no job outcome, only what gets reported. The check runs only after a heartbeat has actually failed, so a misconfigured `lockRenewInterval` longer than `lockDuration` cannot mass-expire healthy runs. Detection here is one heartbeat plus the store-retry budget behind the deadline, rather than immediate.

::: warning
`lockDuration` is the crash-detection window rather than a run-time limit: the heartbeat keeps long handlers locked indefinitely. But anything that stops the heartbeat for longer than `lockDuration` (a blocked event loop, a long process pause) makes the job count as stalled and another worker will re-run it. Keep handlers idempotent.
:::

## Stalled recovery

Every `stalledInterval`, the worker sweeps the store for active jobs whose lock has expired, the signature of a crashed or partitioned worker. Any worker on the store performs recovery; the one that lost the job holds no special role. Each recovered job gets a `stalled` entry in its run ledger and its stall counter incremented; it returns to `waiting` unless the counter now exceeds `maxStalledCount`, in which case the worker fails it outright with a `failedReason` (a job that keeps taking workers down should stop retrying). With the default of 1, the first stall re-queues the job and a second fails it.

Stalls use their own counter: they do not consume the retry budget, since the handler may never have misbehaved. See [Retries & timeouts](/guide/retries-and-timeouts) for how the two budgets interact in the ledger.

## Failure reporting

The worker logs each failed run through Effect's logger: `logWarning` while retries remain (with the backoff delay), `logError` once a job lands terminal `failed`, including jobs failed by stall exhaustion. Log entries carry `effectMqJobId`, `effectMqQueue`, and `effectMqAttempt` annotations plus the failure cause, so log-based alerting works out of the box. Silence or reroute them with standard `Logger` configuration; the worker adds no extra knob.

For custom reporting (error trackers, paging), pass `onJobFailure`:

```ts
Worker.layer({
  onJobFailure: ({ jobId, name, attempt, attemptsMax, willRetry, cause }) =>
    willRetry
      ? Effect.void
      : Sentry.report(name, jobId, cause)
})
```

The hook receives job identity, attempt accounting, `willRetry`, and the `Cause`. It runs isolated after the ack: a failing hook is logged and never disturbs job processing, and it cannot delay or lose the ledger write.

## Graceful shutdown

Closing the worker layer's scope interrupts in-flight handlers and releases their jobs back to `waiting`: no attempt consumed, no ledger entry, and another worker can claim them at once. The claim and ack steps run uninterruptibly, so a shutdown can never orphan a newly claimed job. One exception: the worker acks a job whose cancel request arrives during shutdown as cancelled rather than releasing it, since a release would revive a cancelled job.

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

Registering a job on a worker bound to a different store is a defect with a message naming both stores: a job's runs must live where its producers enqueue them. See [Multiple stores](/storage/stores) for the producer side.

## Where to next

- [Retries & timeouts](/guide/retries-and-timeouts): the attempt budget, backoff, and the run ledger.
- [Cancellation & admin](/guide/cancellation-and-admin): cancel, promote, pause/resume.
- [Observability](/guide/observability): spans, trace linking, and worker metrics.
- [Options reference](/reference/options): every knob in one place.
