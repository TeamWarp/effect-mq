/**
 * The metrics effect-mq emits, as Effect `Metric` instruments.
 *
 * Metrics are **process-local operational signal, not persisted state**:
 * they live in the Effect metric registry of the emitting process and are
 * exported by whatever exporter the application runs (the Otlp modules from
 * `effect/unstable/observability`, `@effect/opentelemetry`, a Prometheus
 * scraper, ...). Retention belongs to that metrics backend. The queue's
 * *durable* analogues remain in the store — `store.counts()` for live depth
 * and the attempt ledger for per-run history — and stay queryable forever.
 *
 * Producers emit `jobsEnqueued`; workers emit the rest from their loops.
 * Everything is tagged with low-cardinality attributes only (job name,
 * queue, outcome/result) — never ids or keys.
 *
 * This module is exported so applications can read the same instruments
 * (e.g. `Metric.value(Metrics.jobRuns.pipe(Metric.withAttributes({...})))`)
 * or reference the names when building dashboards.
 *
 * @since 0.3.1
 */
import { Metric } from "effect"

/** Milliseconds, 5ms .. ~55 minutes. */
const durationBoundaries = Metric.exponentialBoundaries({ start: 5, factor: 2, count: 20 })

/**
 * Producer-side enqueues. Tags: `name`, `queue`, `duplicate` ("true" when
 * the enqueue deduplicated against an existing id or dedup key).
 *
 * @since 0.3.1
 */
export const jobsEnqueued = Metric.counter("effect_mq_jobs_enqueued", {
  description: "Jobs submitted via Job.enqueue/execute, including deduplicated submissions"
})

/**
 * Finished runs, one per handler attempt. Tags: `name`, `queue`, `outcome`
 * (completed | retried | failed | cancelled | released | fanned-out |
 * lock-lost).
 *
 * @since 0.3.1
 */
export const jobRuns = Metric.counter("effect_mq_job_runs", {
  description: "Handler runs by outcome (released = handed back on worker shutdown)"
})

/**
 * Handler execution time in milliseconds (claim to ack). Tags: `name`,
 * `queue`, `outcome`.
 *
 * @since 0.3.1
 */
export const jobRunDuration = Metric.histogram("effect_mq_job_run_duration_ms", {
  description: "Handler execution time in milliseconds, claim to ack",
  boundaries: durationBoundaries
})

/**
 * Queue latency in milliseconds: how long a job was runnable (past its
 * `runAt`) before a worker claimed it. Tags: `name`, `queue`.
 *
 * @since 0.3.1
 */
export const jobWaitDuration = Metric.histogram("effect_mq_job_wait_duration_ms", {
  description: "Time between a job becoming runnable and its claim, in milliseconds",
  boundaries: durationBoundaries
})

/**
 * Claim attempts by result. Tags: `queue`, `result` (claimed | empty).
 * A high empty ratio means takers outnumber work.
 *
 * @since 0.3.1
 */
export const claims = Metric.counter("effect_mq_claims", {
  description: "Store claim attempts by result"
})

/**
 * Handlers currently executing. Tags: `queue`.
 *
 * @since 0.3.1
 */
export const jobsInFlight = Metric.gauge("effect_mq_jobs_in_flight", {
  description: "Handlers currently executing"
})

/**
 * Queue depth by state, sampled from `store.counts()` when
 * `Worker.layer({ queueMetricsInterval })` is set. Tags: `queue`, `state`.
 *
 * @since 0.3.1
 */
export const queueDepth = Metric.gauge("effect_mq_queue_depth", {
  description: "Jobs per state, sampled from store.counts() per registered queue"
})

/**
 * Locks found gone at renewal, counted once per lost lock (the job may run
 * twice). Tags: none.
 *
 * @since 0.3.1
 */
export const locksLost = Metric.counter("effect_mq_locks_lost", {
  description: "Locks found lost at heartbeat renewal, once per lock"
})

/**
 * Cross-process cancel requests delivered to running handlers by the
 * heartbeat. Tags: none.
 *
 * @since 0.3.1
 */
export const cancelInterrupts = Metric.counter("effect_mq_cancel_interrupts", {
  description: "Running handlers interrupted by a cancel request"
})

/**
 * Jobs recovered by the stalled sweep. Tags: `outcome` (requeued | failed).
 *
 * @since 0.3.1
 */
export const stalledRecovered = Metric.counter("effect_mq_stalled_recovered", {
  description: "Stalled jobs recovered (requeued) or failed past the stall limit"
})

/**
 * Repeatable-schedule occurrences enqueued by this worker's sweep. Tags:
 * `name` (the job name).
 *
 * @since 0.3.1
 */
export const scheduleTicks = Metric.counter("effect_mq_schedule_ticks", {
  description: "Repeatable-schedule occurrences enqueued by the schedule sweep"
})

/**
 * Flow fan-outs acked (manifests landed). Tags: `flow`.
 *
 * @since 0.6.0
 */
export const flowFanOuts = Metric.counter("effect_mq_flow_fanouts", {
  description: "Flow fan-out acks (child manifests persisted)"
})

/**
 * Child results recorded into parent stores. Tags: `flow`, `outcome`
 * (completed | failed | cancelled), `source` (`report` = delivered by a
 * worker's outbox relay; `reconcile` = synthesized by the flow sweeper from
 * child-store state). Only applied reports count — duplicates dropped by
 * the dependency row do not.
 *
 * @since 0.6.0
 */
export const flowChildReports = Metric.counter("effect_mq_flow_child_reports", {
  description: "Applied flow child-result reports by outcome and delivery path"
})

/**
 * Cancels delivered into child stores after a flow settle. Tags: none.
 *
 * @since 0.6.0
 */
export const flowCascades = Metric.counter("effect_mq_flow_cascades", {
  description: "Child cancels cascaded into child stores after a flow settled"
})

/**
 * Undelivered outbox entries a relay drain had to leave behind (their
 * parent store is not reachable from this worker — no matching `flows`
 * registration). Tags: none. Sustained growth means no process anywhere
 * can relay these flows; reconciliation keeps the flows correct meanwhile.
 *
 * @since 0.6.0
 */
export const flowOutboxSkipped = Metric.counter("effect_mq_flow_outbox_skipped", {
  description: "Outbox entries left undelivered by a relay drain (parent store unknown here)"
})
