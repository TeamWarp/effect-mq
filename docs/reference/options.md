# Options at a glance

Every knob in one place: job definition options, per-enqueue options, schedule options, worker tuning, and store layer construction. All durations accept `Duration.Input` (`"30 seconds"`, `Duration.minutes(5)`, millis).

## `Job.make(name, options)`

The definition is the shared contract between producers and runners; see [Defining jobs](/guide/defining-jobs) for the full treatment.

| Option | Default | Description |
| --- | --- | --- |
| `payload` | required | Payload schema: a `Schema.Struct` or its bare fields object |
| `success` | `Schema.Void` | Schema for the handler's success value, decodable via `awaitResult`/`attempts` |
| `error` | `Schema.Never` | Schema for the handler's typed failure, or a list of schemas unioned for you; round-trips through storage |
| `idempotencyKey` | none | `(payload) => string`: derives a stable job id; re-enqueueing the same key is a no-op |
| `dedupe` | none | `(payload) => DedupeInput`: derives a dedup key (never changes the job id); see [Deduplication](/guide/deduplication) |
| `metadata` | none | `(payload) => Record<string, string>`: queryable business context, indexed by every driver |
| `retryable` | none | `(error) => boolean`: returning `false` for a typed failure skips the remaining retry budget |
| `queue` | `"default"` | The queue this job runs on |
| `store` | default `JobStore` | Bind to a named store key from `JobStore.named(...)`; see [Stores](/storage/stores) |
| `defaults` | none | Default per-enqueue options (`delay`, `priority`, `attempts`, `backoff`, `keep`, `timeout`); per-enqueue values override |

## Per-enqueue options

Accepted by `enqueue` and `execute`. `enqueueMany` takes the same set **minus `jobId` and `dedupe`**: a shared id or dedup key would collapse the batch into one job (per-item dedup still runs via the definition's `dedupe` callback).

| Option | Default | Description |
| --- | --- | --- |
| `jobId` | store-assigned | Explicit job id; an existing id is a silent no-op returning it. Overrides `idempotencyKey` |
| `queue` | definition's queue | Send to a different queue |
| `metadata` | none | Merged over the definition-derived `metadata` |
| `dedupe` | definition's `dedupe` | Per-enqueue dedup key/mode (`DedupeInput`) |
| `delay` | none | Run this long after enqueue (relative) |
| `at` | none | Run at an absolute instant (any `DateTime.Input`); a past `at` runs immediately |
| `priority` | `0` | Higher runs first; ties are FIFO |
| `attempts` | `1` | Total attempts including the first run (`1` = no retries) |
| `backoff` | immediate | Retry delay: `{ type: "fixed" \| "exponential", delay, factor? }` (`factor` defaults to 2) |
| `keep` | keep forever | Retention for the terminal record: flat `{ count, age }` or split per state; see [Retention](/guide/retention) |
| `timeout` | none | Per-run limit; the worker interrupts the handler fiber past it |

`delay` and `at` are mutually exclusive: the options type is a union, so setting both is a compile error. See [Enqueueing](/guide/enqueueing) for the details.

## `ScheduleOptions`

Passed to `MyJob.schedule(key, options)`. Set exactly one of `cron` or `every`; see [Repeatable jobs](/guide/repeatable-jobs).

| Option | Default | Description |
| --- | --- | --- |
| `cron` | none | 5-field cron expression; first fires at the next matching occurrence |
| `tz` | UTC | IANA time zone for `cron` (e.g. `"America/New_York"`) |
| `every` | none | Fixed interval; first fires one interval from now and stays on that grid |
| `payload` | required | The payload enqueued for every occurrence |
| `metadata` | none | Merged over the definition's `metadata` |
| `priority` | `0` | Priority for each occurrence |
| `attempts` | `1` | Attempt budget for each occurrence |
| `backoff` | immediate | Retry backoff for each occurrence |
| `keep` | keep forever | Retention for each occurrence's terminal record |
| `timeout` | none | Per-run limit for each occurrence |
| `group` | none | Ownership label for reconciliation; set by `JobSchedules.layer`, unlabeled rows are never pruned |

## `JobSchedules.layer(options)`

Declarative schedule reconciliation; see [Repeatable jobs](/guide/repeatable-jobs#declaring-the-full-set-reconciliation).

| Option | Default | Description |
| --- | --- | --- |
| `group` | required | Ownership label; the layer only prunes schedules carrying this group |
| `schedules` | required | The full declared set: `JobSchedules.schedule(job, key, options)` entries |
| `removal` | `"warn"` | `"warn"` logs undeclared group members; `"group"` prunes them |
| `removeAfter` | none | Grace window before pruning (requires `removal: "group"`) |
| `stores` | none | Extra store keys to reconcile when no entry references them |

## `Worker.layer(options)`

All optional. See [Workers & handlers](/guide/workers) for how these interact at runtime.

| Option | Default | Description |
| --- | --- | --- |
| `store` | default `JobStore` | Which named store this worker claims from |
| `concurrency` | `1` | Default taker fibers per queue |
| `queues` | none | Per-queue overrides: `{ email: { concurrency: 5 } }` |
| `lockDuration` | `30 seconds` | How long a claim's lock lasts before the job counts as stalled |
| `lockRenewInterval` | half of `lockDuration` | Heartbeat cadence; also delivers cross-process cancels |
| `stalledInterval` | `30 seconds` | How often to sweep for stalled jobs |
| `maxStalledCount` | `1` | Stalls tolerated before a job is failed outright |
| `pollInterval` | `5 seconds` | Idle fallback when no wake-up arrives; wake-ups are push-based and queue-filtered, so the default is fine |
| `scheduleSweepInterval` | `15 seconds` | How often to tick due repeatable-job schedules |
| `queueMetricsInterval` | off | Sample `store.counts()` per registered queue into the depth gauge at this cadence |
| `handlerSpanName` | `` `${name}.run` `` | `(context) => string`: names the span wrapping each handler run |
| `traceLinking` | `"auto"` | Parent for immediate jobs, causal link for delayed ones; `"parent"` / `"link"` force a mode, `"none"` disables the cross-trace edge |
| `onJobFailure` | none | Callback after each failed run is acked (`{ jobId, name, queue, attempt, attemptsMax, willRetry, cause }`); runs isolated |
| `id` | random | Identifier used in lock tokens |

`MyJob.toLayer(handler, options)` also accepts `concurrency` (taker fibers for this job's queue; the first registration for a queue decides) and `queue` (consume a different queue than the definition's).

::: tip
Locks, stalls, and retries are covered in depth in [Retries & timeouts](/guide/retries-and-timeouts); the tracing and metrics options in [Observability](/guide/observability).
:::

## Store layer options

Every driver accepts `idGenerator` (generator for store-assigned ids; default a compact `j-<n>` sequence), `historyTtl` (retention ceiling: one duration or a per-state split), and `historySweepInterval` (sweep cadence, default 1 minute).

| Option | Memory | Postgres (drizzle) | Redis |
| --- | --- | --- | --- |
| `idGenerator` / `historyTtl` / `historySweepInterval` | yes | yes | yes |
| Table instances (`jobs`, `attempts`, `schedules`, `queues`, `dedupe`) | no | required | no |
| `extraValues`: fill `extend`ed columns at enqueue | no | yes | no |
| `store`: bind to a named store key | via `layerFor` | option | via `layerFor` |
| `validate`: probe tables at startup (default `true`) | no | yes | no |
| `prefix`: key namespace (default `"effect-mq"`) | no | no | yes |

Constructors:

```ts
MemoryJobStore.layer                        // no options
MemoryJobStore.layerWith({ historyTtl: "7 days" })
MemoryJobStore.layerFor(Durable, options)   // named store

DrizzleJobStore.layer({ jobs, attempts, schedules, queues, dedupe, ...options })

RedisJobStore.layer({ prefix: "myapp-jobs" })
RedisJobStore.layerFor(Ephemeral, options)  // named store
```

See [Postgres](/storage/postgres) and [Redis](/storage/redis) for full setup, including the drizzle schema factories and the Redis client layer.

## Job verbs

Everything a definition class exposes:

| Verb | Purpose |
| --- | --- |
| `enqueue(payload, options?)` | Queue the job; returns the `JobId` |
| `enqueueMany(payloads, options?)` | Queue a batch in one store round trip per chunk; ids come back aligned with payloads |
| `execute(payload, options?)` | `enqueue` + `awaitResult` in one call |
| `poll(jobId)` | Read current status as `Option<JobStatus>` |
| `awaitResult(jobId, options?)` | Poll until terminal, then return the typed result (accepts a custom `pollSchedule`) |
| `attempts(jobId)` | The decoded run ledger, oldest first |
| `retry(jobId)` | Re-run a failed job with a fresh attempt budget; ledger preserved |
| `cancel(jobId)` | Waiting/delayed become `cancelled` now; a running handler fiber is interrupted on the worker's next heartbeat |
| `cancelByKey(key)` | Cancel whatever pending job holds this dedup key; idempotent, returns `boolean` |
| `promote(jobId)` | Run a delayed job now |
| `schedule(key, options)` | Create or replace a durable repeatable schedule |
| `unschedule(key)` | Remove a schedule; `false` when it did not exist |
| `toLayer(handler, options?)` | Register the handler, as a layer to provide on top of `Worker.layer()` |

All producer verbs require only the job's store in context, never the `Worker`. Admin semantics (cancel, promote, pause/resume) are covered in [Cancellation & admin](/guide/cancellation-and-admin).

## Where to next

- [Defining jobs](/guide/defining-jobs): how the definition options fit together.
- [Workers & handlers](/guide/workers): the worker options in action.
- [Retention](/guide/retention): `keep` vs. `historyTtl` and how they compose.
- [Stores](/storage/stores): named stores and multi-store setups.
