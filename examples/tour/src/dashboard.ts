/**
 * A minimal live dashboard over the demo's two stores, built entirely on the
 * public JobStore read APIs (`counts`, `list`, `pausedQueues`) — the
 * "dashboard data layer" rendered. One file, no build step.
 *
 *   bun src/dashboard.ts     → http://localhost:4400
 *
 * Start it before (or during) `bun src/main.ts` and watch jobs move:
 * scene 2's stall shows up as an active job whose attempts tick to 2, and
 * scene 4 shows 12 waiting emails in Redis while the Postgres parent sits
 * in waiting-children. Survives the demo's table resets by holding the
 * last good snapshot.
 */
import { Console, Effect, Layer } from "effect"
import { JobStore } from "effect-mq"
import { EmailStore, PgLive, PgStoreLive, RedisStoreLive } from "./stores.ts"

const panel = (store: JobStore.Service) =>
  Effect.gen(function*() {
    const counts = yield* store.counts()
    const paused = yield* store.pausedQueues()
    const page = yield* store.list({ limit: 14 })
    return {
      counts,
      paused,
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

const program = Effect.gen(function*() {
  const pg = yield* JobStore.JobStore
  const emails = yield* EmailStore
  const tick = Effect.gen(function*() {
    const [postgres, redis] = yield* Effect.all([panel(pg), panel(emails)])
    latest = JSON.stringify({ ready: true, postgres, redis, at: Date.now() })
    // A mid-rerun table reset makes a tick fail; keep the last snapshot.
  }).pipe(Effect.catchCause(() => Effect.void))
  yield* Effect.forkChild(tick.pipe(Effect.andThen(Effect.sleep("400 millis")), Effect.forever))

  Bun.serve({
    port: 4400,
    fetch(request) {
      const { pathname } = new URL(request.url)
      if (pathname === "/api/state") {
        return new Response(latest, { headers: { "content-type": "application/json" } })
      }
      return new Response(PAGE, { headers: { "content-type": "text/html; charset=utf-8" } })
    }
  })
  yield* Console.log("dashboard → http://localhost:4400")
  yield* Effect.never
})

const PAGE = `<!doctype html>
<html>
<head>
<meta charset="utf-8" />
<title>effect-mq · live</title>
<style>
  :root { --ink: #16161d; --paper: #faf9f5; --panel: #f3f1ea; --line: #dedbd0; --dim: #757575; }
  * { box-sizing: border-box; border-radius: 0 !important; }
  body { margin: 0; background: var(--paper); color: var(--ink);
         font: 13px/1.6 ui-monospace, "SF Mono", SFMono-Regular, Menlo, Consolas, monospace; }
  header { display: flex; justify-content: space-between; align-items: baseline;
           padding: 14px 20px; border-bottom: 1px solid var(--line); }
  header b { font-size: 15px; letter-spacing: -0.01em; }
  header span { color: var(--dim); font-size: 12px; }
  main { display: grid; grid-template-columns: 1fr 1fr; gap: 0; min-height: calc(100vh - 49px); }
  section { padding: 16px 20px; }
  section + section { border-left: 1px solid var(--line); }
  h2 { font-size: 12px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.08em;
       margin: 0 0 12px; display: flex; gap: 10px; align-items: baseline; }
  h2 small { color: var(--dim); text-transform: none; letter-spacing: 0; font-weight: 400; }
  .counts { display: flex; flex-wrap: wrap; gap: 0; border: 1px solid var(--line); margin-bottom: 14px; }
  .counts div { padding: 6px 12px; border-right: 1px solid var(--line); background: var(--panel); }
  .counts div:last-child { border-right: none; }
  .counts b { display: block; font-size: 16px; }
  .counts span { color: var(--dim); font-size: 11px; }
  .paused { border: 1px solid var(--ink); border-left: 3px solid var(--ink);
            padding: 6px 10px; margin-bottom: 14px; }
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
<header><b>effect-mq</b><span id="meta">connecting…</span></header>
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
  return \`<div class="counts">\${counts}</div>\${paused}\${table}\`
}
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

await Effect.runPromise(
  program.pipe(
    Effect.provide(Layer.mergeAll(PgStoreLive, RedisStoreLive).pipe(Layer.provideMerge(PgLive)))
  )
)
