/**
 * The interactive tour: a live dashboard over both stores PLUS buttons that
 * drive every scenario — everything main.ts does, click by click, and a few
 * things it doesn't (recurring flow schedules, cancelling a RUNNING job,
 * admin retry, delayed + promote).
 *
 *   bun src/dashboard.ts     → http://localhost:4400
 *
 * One process: the workers run in here (so buttons take effect instantly),
 * the UI polls the public read APIs (`counts`, `list`, `pausedQueues`,
 * `listSchedules`), and every button is a POST running the same producer
 * code an app would. main.ts remains the scripted, terminal-only tour.
 */
import type { PgClient } from "@effect/sql-pg"
import { Console, Effect, Layer, ManagedRuntime, Schema } from "effect"
import type { Redis } from "effect/unstable/persistence"
import { Flow, Job, JobStore, Worker } from "effect-mq"
import { DigestFlow, GenerateInvoice, RefreshCache, RenderReport, SendBounced, SendEmail } from "./jobs.ts"
import { EmailStore, PgLive, PgStoreLive, RedisLive, RedisStoreLive, resetRedis, resetTables } from "./stores.ts"

const { QueueName } = JobStore

// ── Extra demo jobs (button-only scenarios) ─────────────────────────────

class CrunchNumbers extends Job.make("crunch-numbers", {
  payload: { batch: Schema.String }
}) {}

class FlakyImport extends Job.make("flaky-import", {
  payload: { source: Schema.String }
}) {}

// ── Handlers (the workers live in THIS process) ─────────────────────────

const InvoiceWorker = GenerateInvoice.toLayer(({ amountCents, invoiceId }) =>
  Effect.succeed(`pdf/${invoiceId}-${amountCents}`)
)

const DigestWorker = DigestFlow.toLayer({
  fanOut: ({ audience, tenant }) =>
    Effect.succeed(Flow.children(
      SendEmail,
      Array.from({ length: audience }, (_, i) => ({
        key: `u${i + 1}`,
        payload: { userId: `${tenant}-u${i + 1}` }
      }))
    )),
  collect: (_payload, results) =>
    Effect.succeed({ sent: results.counts.completed, bounced: results.counts.failed })
})

const EmailWorker = SendEmail.toLayer(({ userId }) =>
  userId.endsWith("u7")
    ? new SendBounced({ address: `${userId}@example.com` })
    // A touch of latency so fan-outs visibly drain on the dashboard.
    : Effect.sleep("150 millis").pipe(Effect.as(`msg-${userId}`))
)

const CrunchWorker = CrunchNumbers.toLayer(() => Effect.sleep("60 seconds"))

// Fails the first run of any id, succeeds after an admin retry.
const failedOnce = new Set<string>()
const FlakyWorker = FlakyImport.toLayer(() =>
  Effect.gen(function*() {
    const { jobId } = yield* Worker.CurrentJob
    if (!failedOnce.has(jobId)) {
      failedOnce.add(jobId)
      return yield* Effect.die(new Error("upstream API returned 500 (simulated)"))
    }
  })
)

// NOTE: RenderReport is deliberately NOT registered here — the kill-a-worker
// button spawns the victim process for it, then a scoped recovery worker.

const Workers = Layer.mergeAll(
  Layer.mergeAll(InvoiceWorker, DigestWorker, CrunchWorker, FlakyWorker).pipe(
    Layer.provide(Worker.layer({
      id: "dashboard-pg",
      pollInterval: "250 millis",
      // Snappy cross-process cancel + stall pickup for demo pacing.
      lockDuration: "4 seconds",
      lockRenewInterval: "1 second",
      stalledInterval: "1 second"
    }))
  ),
  EmailWorker.pipe(
    Layer.provide(Worker.layer({
      id: "dashboard-redis",
      store: EmailStore,
      flows: [DigestFlow],
      concurrency: 4,
      pollInterval: "250 millis"
    }))
  )
)

const AppLayer = Workers.pipe(
  Layer.provideMerge(Layer.mergeAll(PgStoreLive, RedisStoreLive)),
  Layer.provideMerge(Layer.mergeAll(PgLive, RedisLive))
)

const runtime = ManagedRuntime.make(AppLayer)

// ── Actions (each button = one of these Effects) ────────────────────────

let lastDelayedId: string | undefined
let lastFlakyId: string | undefined

type ActionServices = JobStore.JobStore | JobStore.Named<"emails"> | PgClient.PgClient | Redis.Redis

const actions = {
  invoice: GenerateInvoice.enqueue({ invoiceId: "inv_1042", amountCents: 129_900 }).pipe(
    Effect.map((id) => `enqueued → ${id} (click again: same id, no second job)`)
  ),

  burst: Effect.gen(function*() {
    const ids = new Set<string>()
    for (let i = 0; i < 5; i++) {
      ids.add(yield* RefreshCache.enqueue({ accountId: "acct_9" }))
    }
    return `5 enqueues → ${ids.size} job (30s throttle on "acct_9")`
  }),

  "cancel-key": RefreshCache.cancelByKey("acct_9").pipe(
    Effect.map((wasPending) => `cancelByKey("acct_9") → ${wasPending}`)
  ),

  "kill-worker": Effect.gen(function*() {
    const id = yield* RenderReport.enqueue({ reportId: `deck-${Date.now().toString(36)}` })
    const victim = Bun.spawn(["bun", "src/victim.ts"], {
      cwd: import.meta.dir + "/..",
      stdout: "inherit",
      stderr: "inherit"
    })
    yield* Effect.sleep("2500 millis")
    victim.kill(9)
    const Recovery = RenderReport.toLayer(({ reportId }) => Effect.succeed(`pdf/${reportId}`)).pipe(
      Layer.provide(Worker.layer({ id: "recovery", stalledInterval: "1 second", pollInterval: "250 millis" }))
    )
    yield* RenderReport.awaitResult(id).pipe(Effect.provide(Recovery))
    const ledger = yield* RenderReport.attempts(id)
    return `killed pid ${victim.pid} mid-run; ledger: ${
      ledger.map((entry) => `#${entry.attempt} ${entry.outcome}`).join(" → ")
    }`
  }),

  "cancel-running": Effect.gen(function*() {
    const id = yield* CrunchNumbers.enqueue({ batch: `b-${Date.now().toString(36)}` })
    yield* Effect.sleep("1200 millis") // let a worker claim it
    yield* CrunchNumbers.cancel(id).pipe(Effect.orDie)
    return `cancel(${id}) requested — the worker interrupts the RUNNING fiber on its next heartbeat`
  }),

  flaky: Effect.gen(function*() {
    lastFlakyId = yield* FlakyImport.enqueue({ source: `s3://batch-${Date.now().toString(36)}` })
    return `enqueued ${lastFlakyId} — it will fail terminally; then hit retry`
  }),

  retry: Effect.gen(function*() {
    if (lastFlakyId === undefined) return `nothing to retry — enqueue the flaky import first`
    yield* FlakyImport.retry(JobStore.JobId(lastFlakyId)).pipe(Effect.orDie)
    return `retry(${lastFlakyId}) → fresh attempt budget, ledger preserved`
  }),

  delayed: Effect.gen(function*() {
    lastDelayedId = yield* GenerateInvoice.enqueue(
      { invoiceId: `inv-${Date.now().toString(36)}`, amountCents: 500 },
      { delay: "1 hour" }
    )
    return `enqueued ${lastDelayedId} delayed 1 hour — now promote it`
  }),

  promote: Effect.gen(function*() {
    if (lastDelayedId === undefined) return `nothing to promote — enqueue the delayed invoice first`
    yield* GenerateInvoice.promote(JobStore.JobId(lastDelayedId)).pipe(Effect.orDie)
    return `promote(${lastDelayedId}) → runs now instead of in an hour`
  }),

  pause: Effect.gen(function*() {
    const emails = yield* EmailStore
    yield* emails.pause(QueueName("email"))
    return `paused the "email" queue on the Redis store`
  }),

  resume: Effect.gen(function*() {
    const emails = yield* EmailStore
    yield* emails.resume(QueueName("email"))
    return `resumed "email" — watch the backlog drain`
  }),

  flow: DigestFlow.enqueue({ tenant: "acme", audience: 12 }).pipe(
    Effect.map((id) => `flow ${id} started — Postgres parent, 12 Redis children (u7 bounces)`)
  ),

  schedule: DigestFlow.schedule("tour", {
    every: "15 seconds",
    payload: { tenant: "acme", audience: 5 }
  }).pipe(Effect.map((key) => `schedule "${key}" → a full cross-store flow every 15s, exactly-once per tick`)),

  unschedule: DigestFlow.unschedule("tour").pipe(
    Effect.map((existed) => `unschedule("tour") → ${existed}`)
  ),

  clear: Effect.gen(function*() {
    yield* Effect.all([resetTables, resetRedis])
    failedOnce.clear()
    lastDelayedId = undefined
    lastFlakyId = undefined
    return "cleared — tables recreated, Redis prefix wiped, schedules gone"
  })
} satisfies Record<string, Effect.Effect<string, JobStore.JobStoreError, ActionServices>>

// ── State polling for the panels ────────────────────────────────────────

const panel = (store: JobStore.Service) =>
  Effect.gen(function*() {
    const counts = yield* store.counts()
    const paused = yield* store.pausedQueues()
    const schedules = yield* store.listSchedules()
    const page = yield* store.list({ limit: 14 })
    return {
      counts,
      paused,
      schedules: schedules.map((entry) => ({ key: entry.key, next: entry.nextRunAt })),
      jobs: page.items.map((job) => ({
        id: job.id,
        name: job.name,
        queue: job.queue,
        state: job.state,
        attempts: `${job.attemptsMade}/${job.attemptsMax}`,
        enqueuedAt: job.enqueuedAt,
        runAt: job.runAt,
        processedAt: job.processedAt,
        finishedAt: job.finishedAt,
        flow: job.flow === undefined
          ? undefined
          : { pending: job.flow.pending, completed: job.flow.completed, failed: job.flow.failed }
      }))
    }
  })

/**
 * The code behind each section, curated to what matters. Served as JSON so
 * the page template never has to escape backticks.
 */
const SNIPPETS = {
  stores: `import { JobStore } from "effect-mq"
import { DrizzleJobStore, mqJobs, mqJobAttempts, /* ... */ } from "effect-mq/drizzle-postgres"
import { RedisJobStore } from "effect-mq/redis"

// Two stores, one contract. Postgres is the DEFAULT JobStore; disposable
// email jobs live in Redis under a named store key. Producers that enqueue
// an email REQUIRE this key in context — wiring enforced at compile time.
export const EmailStore = JobStore.named("emails")

// The tables live in YOUR drizzle schema; drizzle-kit owns the migrations.
const jobs = mqJobs("effect_mq_jobs", {
  // customizable: tenant columns filled from job metadata at enqueue...
  extend: { companyId: text("company_id").notNull() },
  // ...and your own indexes over them
  extraConfig: (table) => [index("jobs_company_idx").on(table.companyId, table.state)]
})

export const PgStoreLive = DrizzleJobStore.layer({
  jobs, attempts, schedules, queues, dedupe, flowChildren, flowOutbox,
  idGenerator: ({ name }) => \`\${name}_\${ulid()}\`,           // your id scheme
  historyTtl: { completed: "7 days", failed: "90 days" }   // retention ceiling
})

export const RedisStoreLive = RedisJobStore.layerFor(EmailStore, {
  prefix: "effect-mq-tour",
  historyTtl: "1 day"
})`,

  jobs: `class GenerateInvoice extends Job.make("generate-invoice", {
  payload: { invoiceId: Schema.String, amountCents: Schema.Number },
  success: Schema.String,
  idempotencyKey: ({ invoiceId }) => invoiceId,    // same input -> same job id
  metadata: ({ invoiceId }) => ({ invoiceId }),    // indexed, queryable
  defaults: { attempts: 3, backoff: { type: "exponential", delay: "250 millis" } }
}) {}

class RefreshCache extends Job.make("refresh-cache", {
  payload: { accountId: Schema.String },
  dedupe: ({ accountId }) => ({ key: accountId, ttl: "30 seconds" })   // throttle
}) {}

// the buttons run exactly this:
yield* GenerateInvoice.enqueue({ invoiceId: "inv_1042", amountCents: 129_900 })
yield* RefreshCache.enqueue({ accountId: "acct_9" })   // x5 -> 1 job
yield* RefreshCache.cancelByKey("acct_9")              // no job-id bookkeeping`,

  durability: `// Handlers are Effects registered on a Worker layer. A claim holds a
// lock the worker heartbeats; these knobs are tightened for demo pacing.
const InvoiceWorker = GenerateInvoice.toLayer(({ invoiceId }) =>
  Effect.succeed(\`pdf/\${invoiceId}\`)
)
Worker.layer({ lockDuration: "4 seconds", lockRenewInterval: "1 second" })

// kill -9 a worker: no release happens, the lock just expires. The stall
// sweeper recovers the job and the ledger shows it:
yield* RenderReport.attempts(id)   // -> #1 stalled -> #2 completed

// cancel reaches a RUNNING fiber cross-process via the heartbeat:
yield* CrunchNumbers.cancel(id)

// a terminal failure keeps its ledger; retry grants a fresh budget:
yield* FlakyImport.retry(id)`,

  scheduling: `// Relative delays or absolute instants (delay/at are mutually exclusive
// at the type level); promote runs a delayed job now.
yield* GenerateInvoice.enqueue(payload, { delay: "1 hour" })
yield* GenerateInvoice.promote(id)

// Durable schedules are rows in the store, not process timers. Each
// occurrence is claimed atomically: exactly-once per tick, no matter how
// many workers sweep. Here the scheduled job is a whole cross-store flow:
yield* DigestFlow.schedule("tour", {
  every: "15 seconds",             // or cron: "0 9 * * *", tz: "America/New_York"
  payload: { tenant: "acme", audience: 5 }
})
yield* DigestFlow.unschedule("tour")`,

  flows: `// A flow: the parent lives in Postgres (which owns the manifest, the
// per-child results, and the settle), the children run in Redis.
export const DigestFlow = Flow.make("daily-digest", {
  parent: SendDigest,
  children: [SendEmail],
  onChildFailure: "continue"       // or "fail" to fail fast + cancel siblings
})

const DigestWorker = DigestFlow.toLayer({
  fanOut: ({ tenant, audience }) =>
    Effect.succeed(Flow.children(SendEmail, users.map((user) => ({
      key: user.id,                // the idempotency mechanism for children
      payload: { userId: user.id }
    })))),
  collect: (_payload, results) =>
    Effect.succeed({ sent: results.counts.completed, bounced: results.counts.failed })
})

// pause/resume are store-level queue controls:
yield* emails.pause(QueueName("email"))
yield* emails.resume(QueueName("email"))`
} satisfies Record<string, string>

let latest = JSON.stringify({ ready: false })

const refresh = Effect.gen(function*() {
  const pg = yield* JobStore.JobStore
  const emails = yield* EmailStore
  const tick = Effect.gen(function*() {
    const [postgres, redis] = yield* Effect.all([panel(pg), panel(emails)])
    latest = JSON.stringify({ ready: true, postgres, redis, at: Date.now() })
    // A mid-reset tick may fail; hold the last good snapshot.
  }).pipe(Effect.catchCause(() => Effect.void))
  return yield* tick.pipe(Effect.andThen(Effect.sleep("400 millis")), Effect.forever)
})

// ── Boot ────────────────────────────────────────────────────────────────

await runtime.runPromise(Effect.all([resetTables, resetRedis]))
runtime.runFork(refresh)

Bun.serve({
  port: 4400,
  async fetch(request) {
    const { pathname } = new URL(request.url)
    if (pathname === "/api/state") {
      return new Response(latest, { headers: { "content-type": "application/json" } })
    }
    if (pathname === "/api/snippets") {
      return Response.json(SNIPPETS)
    }
    if (pathname.startsWith("/api/action/") && request.method === "POST") {
      const name = pathname.slice("/api/action/".length)
      // SAFETY: `Object.hasOwn` proves `name` is a key of the literal
      // `actions` object; TypeScript cannot carry that through a string.
      const action: Effect.Effect<string, JobStore.JobStoreError, ActionServices> | undefined =
        Object.hasOwn(actions, name)
          ? actions[name as keyof typeof actions]
          : undefined
      if (action === undefined) return new Response("unknown action", { status: 404 })
      const message = await runtime.runPromise(
        action.pipe(Effect.catchCause((cause) => Effect.succeed(`failed: ${cause}`)))
      )
      return Response.json({ message })
    }
    return new Response(PAGE, { headers: { "content-type": "text/html; charset=utf-8" } })
  }
})
await runtime.runPromise(Console.log("tour → http://localhost:4400  (workers run in this process)"))

const PAGE = `<!doctype html>
<html>
<head>
<meta charset="utf-8" />
<title>effect-mq · tour</title>
<style>
  :root { --ink: #16161d; --paper: #faf9f5; --panel: #f3f1ea; --line: #dedbd0; --dim: #757575; }
  * { box-sizing: border-box; border-radius: 0 !important; }
  body { margin: 0; background: var(--paper); color: var(--ink); display: grid;
         grid-template-columns: 220px 1fr; min-height: 100vh;
         font: 13px/1.6 ui-monospace, "SF Mono", SFMono-Regular, Menlo, Consolas, monospace; }

  nav { border-right: 1px solid var(--line); display: flex; flex-direction: column; }
  nav .brand { padding: 14px 16px; border-bottom: 1px solid var(--line); font-weight: 700; font-size: 15px; }
  nav a { display: block; padding: 10px 16px; color: var(--ink); text-decoration: none;
          border-bottom: 1px solid var(--line); cursor: pointer; }
  nav a small { display: block; color: var(--dim); font-size: 11px; }
  nav a.current { background: var(--panel); border-left: 3px solid var(--ink);
                  padding-left: 13px; font-weight: 700; }
  nav .spacer { flex: 1; }
  nav button.clear { margin: 16px; width: calc(100% - 32px); }

  .content { display: flex; flex-direction: column; min-width: 0; }
  header { display: flex; justify-content: flex-end; padding: 14px 20px;
           border-bottom: 1px solid var(--line); color: var(--dim); font-size: 12px; }

  .stage { padding: 16px 20px; border-bottom: 1px solid var(--line); }
  .stage h1 { margin: 0 0 6px; font-size: 15px; }
  .stage p { margin: 0 0 12px; color: var(--dim); max-width: 76ch; }
  .stage .section { display: none; }
  .stage .section.current { display: block; }

  button { font: inherit; font-size: 12px; background: transparent; color: var(--ink);
           border: 1px solid var(--ink); padding: 5px 12px; margin: 0 8px 8px 0; cursor: pointer; }
  button:hover { background: var(--ink); color: var(--paper); }
  button:disabled { opacity: 0.4; cursor: wait; }

  #log { padding: 8px 20px; border-bottom: 1px solid var(--line); background: var(--panel);
         min-height: 34px; font-size: 12px; }
  #log div { color: var(--dim); } #log div:first-child { color: var(--ink); }

  main { display: grid; grid-template-columns: 1fr 1fr; flex: 1; }
  section.store { padding: 16px 20px; min-width: 0; overflow-x: auto; }
  section.store + section.store { border-left: 1px solid var(--line); }
  h2 { font-size: 12px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.08em;
       margin: 0 0 12px; display: flex; gap: 10px; align-items: baseline; }
  h2 small { color: var(--dim); text-transform: none; letter-spacing: 0; font-weight: 400; }
  .counts { display: flex; flex-wrap: wrap; border: 1px solid var(--line); margin-bottom: 14px; }
  .counts div { padding: 6px 12px; border-right: 1px solid var(--line); background: var(--panel); }
  .counts div:last-child { border-right: none; }
  .counts b { display: block; font-size: 16px; }
  .counts span { color: var(--dim); font-size: 11px; }
  .paused, .sched { border: 1px solid var(--ink); border-left: 3px solid var(--ink);
                    padding: 6px 10px; margin-bottom: 14px; }
  .sched { border-color: var(--line); border-left-color: var(--ink); color: var(--dim); }
  table { width: 100%; border-collapse: collapse; }
  th, td { border: 1px solid var(--line); padding: 4px 8px; text-align: left; font-size: 12px; }
  th { background: var(--panel); font-weight: 600; }
  td.id { color: var(--dim); max-width: 150px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  td.time { color: var(--dim); white-space: nowrap; }
  .st-active, .st-waiting-children { font-weight: 700; }
  .st-completed { color: var(--dim); }
  .st-failed { font-weight: 700; text-decoration: underline; text-underline-offset: 3px; }
  .st-cancelled { color: var(--dim); text-decoration: line-through; }
  .empty { color: var(--dim); padding: 20px 0; }
  button.code-toggle { border-color: var(--line); color: var(--dim); }
  button.code-toggle:hover { border-color: var(--ink); color: var(--ink); background: transparent; }
  button.code-toggle.open { border-color: var(--ink); color: var(--ink); }
  pre.code { display: none; background: var(--panel); border: 1px solid var(--line);
             padding: 12px 14px; margin: 6px 0 0; overflow-x: auto;
             font-size: 12px; line-height: 1.55; max-width: 100ch; }
  pre.code.open { display: block; }
  .code .k { color: #cf222e; }
  .code .s { color: #0a3069; }
  .code .c { color: #6e7781; font-style: italic; }
  .code .t { color: #953800; }
  .code .n { color: #0550ae; }
</style>
</head>
<body>
<nav>
  <div class="brand">effect-mq tour</div>
  <a data-section="stores" class="current">0. Stores<small>postgres + redis, one contract</small></a>
  <a data-section="jobs">1. Jobs<small>typed, idempotent, deduplicated</small></a>
  <a data-section="durability">2. Durability<small>kill, cancel, retry</small></a>
  <a data-section="scheduling">3. Scheduling<small>delayed, promoted, recurring</small></a>
  <a data-section="flows">4. Flows &amp; queue control<small>cross-store fan-out, pause</small></a>
  <div class="spacer"></div>
  <button class="clear" data-action="clear">clear — reset both stores</button>
</nav>
<div class="content">
<header><span id="meta">connecting…</span></header>
<div class="stage">
  <div class="section current" data-section="stores">
    <h1>0. Stores</h1>
    <p>Everything below runs against two real stores through one contract:
       business-critical jobs in Postgres (tables that live in your own
       drizzle schema — extend them with tenant columns, your indexes, your
       id scheme, your retention), and disposable sends in Redis under a
       named store key the type system makes producers provide. Swap either
       for the in-memory store in tests; the same conformance suite keeps
       all three honest.</p>
    <button class="code-toggle" data-code="stores">view code</button>
    <pre class="code" data-code="stores"></pre>
  </div>
  <div class="section" data-section="jobs">
    <h1>1. Jobs</h1>
    <p>Payloads are schemas, and the idempotency key derives the job id from
       business data: click the invoice twice and the same id comes back with
       no second job. Dedup keys throttle without touching ids, and
       cancelByKey needs no job-id bookkeeping.</p>
    <button data-action="invoice">enqueue invoice #1042</button>
    <button data-action="burst">5× throttled refresh</button>
    <button data-action="cancel-key">cancel by key</button>
    <button class="code-toggle" data-code="jobs">view code</button>
    <pre class="code" data-code="jobs"></pre>
  </div>
  <div class="section" data-section="durability">
    <h1>2. Durability</h1>
    <p>Kill-a-worker spawns a real process, lets it claim the job, and
       SIGKILLs it: the lock expires, the stall sweeper recovers the job, and
       the response is the attempts ledger. Cancel reaches a RUNNING fiber
       through the heartbeat; a failed import retries with its ledger intact.</p>
    <button data-action="kill-worker">kill a worker mid-job</button>
    <button data-action="cancel-running">cancel a RUNNING job</button>
    <button data-action="flaky">fail an import</button>
    <button data-action="retry">retry it</button>
    <button class="code-toggle" data-code="durability">view code</button>
    <pre class="code" data-code="durability"></pre>
  </div>
  <div class="section" data-section="scheduling">
    <h1>3. Scheduling</h1>
    <p>Delayed jobs sit in the store until due (promote runs one now). The
       recurring schedule is the headline: a full cross-store flow every 15
       seconds, claimed exactly-once per tick no matter how many workers run.</p>
    <button data-action="delayed">enqueue delayed 1h</button>
    <button data-action="promote">promote it</button>
    <button data-action="schedule">flow every 15s</button>
    <button data-action="unschedule">unschedule</button>
    <button class="code-toggle" data-code="scheduling">view code</button>
    <pre class="code" data-code="scheduling"></pre>
  </div>
  <div class="section" data-section="flows">
    <h1>4. Flows &amp; queue control</h1>
    <p>Pause email first, then run the flow: the Postgres parent parks in
       waiting-children while 12 children wait in Redis under the paused
       queue. Resume drains them (u7 bounces), the reports collect back into
       Postgres, and the parent row carries the flow counters.</p>
    <button data-action="pause">pause email</button>
    <button data-action="flow">run digest flow (12)</button>
    <button data-action="resume">resume email</button>
    <button class="code-toggle" data-code="flows">view code</button>
    <pre class="code" data-code="flows"></pre>
  </div>
</div>
<div id="log"><div>click a button — every one runs the same producer API your app would</div></div>
<main>
  <section class="store"><h2>postgres <small>business-critical · drizzle tables</small></h2><div id="postgres"></div></section>
  <section class="store"><h2>redis <small>disposable sends · lua scripts</small></h2><div id="redis"></div></section>
</main>
</div>
<script>
const STATES = ["waiting", "delayed", "active", "waiting-children", "completed", "failed", "cancelled"]
const esc = (value) => String(value).replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]))
const clock = (ms) => ms == null ? "" : new Date(ms).toLocaleTimeString([], { hour12: false })
// When it ran (claim time), or when it is due to.
const ranOrDue = (job) =>
  job.state === "delayed"
    ? "due " + clock(job.runAt)
    : job.processedAt != null
    ? clock(job.processedAt)
    : "enq " + clock(job.enqueuedAt)
const took = (job) => {
  if (job.state === "active" && job.processedAt != null) return ((Date.now() - job.processedAt) / 1000).toFixed(1) + "s…"
  if (job.processedAt == null || job.finishedAt == null) return ""
  const ms = job.finishedAt - job.processedAt
  return ms < 1000 ? ms + "ms" : (ms / 1000).toFixed(1) + "s"
}
const renderPanel = (panel) => {
  const counts = STATES
    .filter((state) => state in panel.counts)
    .map((state) => \`<div><b>\${panel.counts[state]}</b><span>\${esc(state)}</span></div>\`)
    .join("")
  const paused = panel.paused.length > 0
    ? \`<div class="paused">paused queues: \${panel.paused.map(esc).join(", ")}</div>\`
    : ""
  const sched = (panel.schedules ?? []).length > 0
    ? \`<div class="sched">schedules: \${panel.schedules.map((entry) =>
        \`\${esc(entry.key)} · next \${new Date(entry.next).toLocaleTimeString()}\`).join(" · ")}</div>\`
    : ""
  const rows = panel.jobs.map((job) => {
    const flow = job.flow
      ? \` · flow \${job.flow.completed}✓ \${job.flow.failed}✕ \${job.flow.pending} pending\`
      : ""
    return \`<tr>
      <td>\${esc(job.name)}</td>
      <td class="id">\${esc(job.id)}</td>
      <td>\${esc(job.queue)}</td>
      <td class="st-\${esc(job.state)}">\${esc(job.state)}\${flow}</td>
      <td>\${esc(job.attempts)}</td>
      <td class="time">\${esc(ranOrDue(job))}</td>
      <td class="time">\${esc(took(job))}</td>
    </tr>\`
  }).join("")
  const table = panel.jobs.length > 0
    ? \`<table><tr><th>job</th><th>id</th><th>queue</th><th>state</th><th>att</th><th>when</th><th>took</th></tr>\${rows}</table>\`
    : \`<div class="empty">no jobs yet</div>\`
  return \`<div class="counts">\${counts}</div>\${paused}\${sched}\${table}\`
}
const log = (message) => {
  const el = document.getElementById("log")
  const line = document.createElement("div")
  line.textContent = new Date().toLocaleTimeString() + "  " + message
  el.prepend(line)
  while (el.children.length > 3) el.removeChild(el.lastChild)
}
document.querySelectorAll("nav a[data-section]").forEach((item) => {
  item.addEventListener("click", () => {
    document.querySelectorAll("nav a[data-section]").forEach((other) =>
      other.classList.toggle("current", other === item))
    document.querySelectorAll(".stage .section").forEach((section) =>
      section.classList.toggle("current", section.dataset.section === item.dataset.section))
  })
})
document.querySelectorAll("button[data-action]").forEach((button) => {
  button.addEventListener("click", async () => {
    button.disabled = true
    try {
      const response = await fetch("/api/action/" + button.dataset.action, { method: "POST" })
      log((await response.json()).message)
    } catch (error) {
      log("request failed: " + error)
    } finally {
      button.disabled = false
    }
  })
})
// One-pass highlighter over ESCAPED text: each alternative is disjoint, so
// nothing ever re-scans injected markup. Curated snippets only — not a
// general TS parser.
const highlight = (code) =>
  esc(code).replace(
    /(\\/\\/[^\\n]*)|(&quot;(?:[^&]|&(?!quot;))*&quot;|\`[^\`]*\`)|\\b(import|from|export|const|class|extends|new|return|yield|function|type)\\b|\\b([A-Z][A-Za-z0-9]*)\\b|\\b(\\d[\\d_]*)\\b/g,
    (match, comment, string, keyword, typeName, number) =>
      comment !== undefined
        ? '<span class="c">' + comment + "</span>"
        : string !== undefined
        ? '<span class="s">' + string + "</span>"
        : keyword !== undefined
        ? '<span class="k">' + keyword + "</span>"
        : typeName !== undefined
        ? '<span class="t">' + typeName + "</span>"
        : '<span class="n">' + number + "</span>"
  )
fetch("/api/snippets").then((response) => response.json()).then((snippets) => {
  document.querySelectorAll("pre.code").forEach((pre) => {
    const code = snippets[pre.dataset.code]
    if (code) pre.innerHTML = highlight(code)
  })
  // ?code=open pre-opens every block (handy when presenting).
  if (new URLSearchParams(location.search).get("code") === "open") {
    document.querySelectorAll("button.code-toggle").forEach((button) => button.click())
  }
})
document.querySelectorAll("button.code-toggle").forEach((button) => {
  button.addEventListener("click", () => {
    const pre = document.querySelector('pre.code[data-code="' + button.dataset.code + '"]')
    const open = pre.classList.toggle("open")
    button.classList.toggle("open", open)
    button.textContent = open ? "hide code" : "view code"
  })
})
const refresh = async () => {
  try {
    const state = await (await fetch("/api/state")).json()
    if (!state.ready) return
    document.getElementById("postgres").innerHTML = renderPanel(state.postgres)
    document.getElementById("redis").innerHTML = renderPanel(state.redis)
    document.getElementById("meta").textContent = "live · " + new Date(state.at).toLocaleTimeString()
  } catch {
    document.getElementById("meta").textContent = "disconnected"
  }
}
refresh()
setInterval(refresh, 500)
</script>
</body>
</html>`
