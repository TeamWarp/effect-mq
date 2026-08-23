# Tracing & metrics

effect-mq is built on Effect's tracer and `Metric` primitives, so observability is wiring, not integration work: attach any Effect-compatible tracer/exporter and spans and instruments flow into your existing stack. Without one, the overhead is negligible.

## Tracing across processes

Producer → handler traces connect **across processes and storage**. When you enqueue inside a span, that span's context — `traceId`, `spanId`, `sampled` — is persisted on the job record. When a worker later claims the job (possibly hours later, on another machine), it wraps the handler run in a span attached to that external context via `Tracer.externalSpan`. Your invite handler appears in the same trace as the HTTP request that scheduled it.

The run span is named `` `${name}.run` `` by default and carries three attributes:

| Attribute | Value |
| --- | --- |
| `effectMqJobId` | the job's id |
| `effectMqQueue` | the queue it ran on |
| `effectMqAttempt` | the attempt number (1-based) |

Rename it per worker when your backend has naming conventions:

```ts
import { Worker } from "effect-mq"

const WorkerLive = Worker.layer({
  handlerSpanName: (ctx) => `job ${ctx.name} #${ctx.attempt}`
})
```

Producer verbs (`enqueue`, `poll`, `cancel`, `schedule`, ...) already run in their own spans. The worker's claim-loop iterations are deliberately unspanned — the handler run is the meaningful trace unit, and per-claim spans would flood your backend.

## How the handler span attaches

A five-day-delayed job as a *child* of its producer span makes a five-day-wide trace: it renders badly and defeats tail sampling. So the attachment mode follows the delay, controlled by `Worker.layer({ traceLinking })`:

| Mode | Behavior |
| --- | --- |
| `"auto"` (default) | immediate enqueues continue the producer trace (parent-child); explicitly delayed/`at`-scheduled jobs start their own trace with a causal **span link** back |
| `"parent"` | always parent-child |
| `"link"` | always a span link |
| `"none"` | spans and attributes only, no cross-trace edge |

Two properties make `"auto"` deterministic:

- The delayed/immediate split keys off **scheduling intent captured at enqueue** — did the producer pass `delay`/`at` — not off when the job actually ran. Queue backlog never changes your trace shapes.
- The flag is persisted on the record, so **every retry attempt keeps its mode**: attempt 3 of an immediate job is still a child of the producer span.

## Metrics

Workers and producers emit Effect `Metric` instruments, exported as the `Metrics` module from `effect-mq`. They are **process-local operational signal, not persisted state** — they live in the emitting process's metric registry, and you export them with whatever your app already runs (the Otlp modules from `effect/unstable/observability`, `@effect/opentelemetry`, a Prometheus scraper). Retention lives in that backend.

The *durable* analogues stay in the store: `store.counts()` for live depth and the [attempts ledger](/guide/retries-and-timeouts) for per-run history — both queryable forever.

| Instrument | Metric name | Type | Tags |
| --- | --- | --- | --- |
| `jobsEnqueued` | `effect_mq_jobs_enqueued` | counter | `name`, `queue`, `duplicate` |
| `jobRuns` | `effect_mq_job_runs` | counter | `name`, `queue`, `outcome` (completed \| retried \| failed \| cancelled \| released) |
| `jobRunDuration` | `effect_mq_job_run_duration_ms` | histogram | `name`, `queue`, `outcome` |
| `jobWaitDuration` | `effect_mq_job_wait_duration_ms` | histogram | `name`, `queue` |
| `claims` | `effect_mq_claims` | counter | `queue`, `result` (claimed \| empty) |
| `jobsInFlight` | `effect_mq_jobs_in_flight` | gauge | `queue` |
| `queueDepth` | `effect_mq_queue_depth` | gauge | `queue`, `state` |
| `locksLost` | `effect_mq_locks_lost` | counter | — |
| `cancelInterrupts` | `effect_mq_cancel_interrupts` | counter | — |
| `stalledRecovered` | `effect_mq_stalled_recovered` | counter | `outcome` (requeued \| failed) |
| `scheduleTicks` | `effect_mq_schedule_ticks` | counter | `name` |

Everything is tagged with low-cardinality attributes only — job name, queue, outcome — never ids or keys.

A few of these deserve reading notes:

- `job_wait_duration_ms` is the queue-latency headline: time between a job becoming runnable (past its `runAt`) and its claim.
- `job_run_duration_ms` measures claim to ack for a single run, so payload decode and the ack round trip are included, not just handler time.
- `job_runs{outcome="released"}` counts jobs handed back on graceful shutdown — they consumed no attempt.
- `locks_lost` counts locks found gone at heartbeat renewal; each one means a job may run twice, so a nonzero rate is worth an alert.
- A high `claims{result="empty"}` ratio means takers outnumber work — lower `concurrency` or consolidate workers.

## Queue depth is opt-in

`queue_depth` samples `store.counts()` per registered queue, which costs one store query per queue per tick. Turn it on with a cadence:

```ts
const WorkerLive = Worker.layer({ queueMetricsInterval: "15 seconds" })
```

## Reading instruments directly

Because the module is exported, dashboards and tests can read the same instruments the runtime writes:

```ts
import { Metrics } from "effect-mq"
import { Effect, Metric } from "effect"

const failedRuns = Effect.gen(function*() {
  const state = yield* Metric.value(Metrics.jobRuns.pipe(
    Metric.withAttributes({ name: "SendEmail", queue: "email", outcome: "failed" })
  ))
  return state.count
})
```

::: tip
Metric names are stable, dashboard-ready strings — build your Grafana/Prometheus queries against the `effect_mq_*` names in the table above.
:::

## Where to next

- [Retries & timeouts](/guide/retries-and-timeouts) — the attempt ledger behind the run metrics.
- [Workers & handlers](/guide/workers) — the worker options referenced here, in context.
- [Reference: options](/reference/options) — every `Worker.layer` knob with defaults.
