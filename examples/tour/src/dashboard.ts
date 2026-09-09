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
import { Console, Effect, Layer, ManagedRuntime, Schema } from "effect"
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

type ActionServices = JobStore.JobStore | JobStore.Named<"emails">

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
  )
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
        flow: job.flow === undefined
          ? undefined
          : { pending: job.flow.pending, completed: job.flow.completed, failed: job.flow.failed }
      }))
    }
  })

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
  body { margin: 0; background: var(--paper); color: var(--ink);
         font: 13px/1.6 ui-monospace, "SF Mono", SFMono-Regular, Menlo, Consolas, monospace; }
  header { display: flex; justify-content: space-between; align-items: baseline;
           padding: 14px 20px; border-bottom: 1px solid var(--line); }
  header b { font-size: 15px; letter-spacing: -0.01em; }
  header span { color: var(--dim); font-size: 12px; }
  .actions { display: grid; grid-template-columns: repeat(4, 1fr); gap: 0;
             border-bottom: 1px solid var(--line); }
  .card { padding: 12px 16px; border-right: 1px solid var(--line); }
  .card:last-child { border-right: none; }
  .card h3 { margin: 0 0 8px; font-size: 11px; font-weight: 600; text-transform: uppercase;
             letter-spacing: 0.08em; color: var(--dim); }
  button { font: inherit; font-size: 12px; background: transparent; color: var(--ink);
           border: 1px solid var(--ink); padding: 4px 10px; margin: 0 6px 6px 0; cursor: pointer; }
  button:hover { background: var(--ink); color: var(--paper); }
  button:disabled { opacity: 0.4; cursor: wait; }
  #log { padding: 8px 20px; border-bottom: 1px solid var(--line); background: var(--panel);
         min-height: 34px; font-size: 12px; }
  #log div { color: var(--dim); } #log div:first-child { color: var(--ink); }
  main { display: grid; grid-template-columns: 1fr 1fr; min-height: 40vh; }
  section { padding: 16px 20px; }
  section + section { border-left: 1px solid var(--line); }
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
  td.id { color: var(--dim); max-width: 220px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .st-active, .st-waiting-children { font-weight: 700; }
  .st-completed { color: var(--dim); }
  .st-failed { font-weight: 700; text-decoration: underline; text-underline-offset: 3px; }
  .st-cancelled { color: var(--dim); text-decoration: line-through; }
  .empty { color: var(--dim); padding: 20px 0; }
</style>
</head>
<body>
<header><b>effect-mq tour</b><span id="meta">connecting…</span></header>
<div class="actions">
  <div class="card"><h3>jobs</h3>
    <button data-action="invoice">enqueue invoice #1042</button>
    <button data-action="burst">5× throttled refresh</button>
    <button data-action="cancel-key">cancel by key</button>
  </div>
  <div class="card"><h3>durability</h3>
    <button data-action="kill-worker">kill a worker mid-job</button>
    <button data-action="cancel-running">cancel a RUNNING job</button>
    <button data-action="flaky">fail an import</button>
    <button data-action="retry">retry it</button>
  </div>
  <div class="card"><h3>scheduling</h3>
    <button data-action="delayed">enqueue delayed 1h</button>
    <button data-action="promote">promote it</button>
    <button data-action="schedule">flow every 15s</button>
    <button data-action="unschedule">unschedule</button>
  </div>
  <div class="card"><h3>flows · queue control</h3>
    <button data-action="flow">run digest flow (12)</button>
    <button data-action="pause">pause email</button>
    <button data-action="resume">resume email</button>
  </div>
</div>
<div id="log"><div>click a button — every one runs the same producer API your app would</div></div>
<main>
  <section><h2>postgres <small>business-critical · drizzle tables</small></h2><div id="postgres"></div></section>
  <section><h2>redis <small>disposable sends · lua scripts</small></h2><div id="redis"></div></section>
</main>
<script>
const STATES = ["waiting", "delayed", "active", "waiting-children", "completed", "failed", "cancelled"]
const esc = (value) => String(value).replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]))
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
    </tr>\`
  }).join("")
  const table = panel.jobs.length > 0
    ? \`<table><tr><th>job</th><th>id</th><th>queue</th><th>state</th><th>att</th></tr>\${rows}</table>\`
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
