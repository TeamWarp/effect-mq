/**
 * The effect-mq podcast demo: four scenes against real Postgres + Redis.
 *
 *   1. Typed jobs + idempotency        (Postgres)
 *   2. kill -9 a worker mid-job        (stall recovery + the attempts ledger)
 *   3. Dedup throttling + cancelByKey  (five enqueues, one job)
 *   4. Cross-store flow, queue paused  (PG parent parks, Redis children wait,
 *                                       resume drains, results collect back)
 *
 * Run from this directory:
 *   docker compose -f ../../docker-compose.yml up -d
 *   bun src/main.ts
 */
import { PgClient } from "@effect/sql-pg"
import { Console, Effect, Layer, Option } from "effect"
import { Flow, JobStore, Worker } from "effect-mq"

const { QueueName } = JobStore
import { DigestFlow, GenerateInvoice, RefreshCache, RenderReport, SendBounced, SendEmail } from "./jobs.ts"
import { EmailStore, PgLive, PgStoreLive, RedisStoreLive, resetTables } from "./stores.ts"

const rule = "─".repeat(64)
const scene = (n: number, title: string) => Console.log(`\n${rule}\n  SCENE ${n}  ${title}\n${rule}`)

// ── Workers that run for the whole demo ─────────────────────────────────

const InvoiceWorker = GenerateInvoice.toLayer(
  ({ amountCents, invoiceId }) =>
    Effect.gen(function*() {
      const ctx = yield* Worker.CurrentJob
      yield* Console.log(
        `  [pg-worker]     rendering ${invoiceId} ($${(amountCents / 100).toFixed(2)}) — attempt ${ctx.attempt}`
      )
      return `pdf/${invoiceId}`
    })
)

// The flow's parent side: fanOut runs in Postgres, produces Redis children.
const DigestWorker = DigestFlow.toLayer({
  fanOut: ({ audience, tenant }) =>
    Effect.gen(function*() {
      yield* Console.log(`  [pg-worker]     fanning ${audience} sends out to the Redis store…`)
      return Flow.children(
        SendEmail,
        Array.from({ length: audience }, (_, i) => ({
          key: `u${i + 1}`,
          payload: { userId: `${tenant}-u${i + 1}` }
        }))
      )
    }),
  collect: (_payload, results) =>
    Effect.gen(function*() {
      const settled = yield* results.all
      for (const failure of settled.failed) {
        yield* Console.log(`  [pg-worker]     collect: child "${failure.key}" failed — recorded in Postgres`)
      }
      return { sent: results.counts.completed, bounced: results.counts.failed }
    })
})

// The flow's child side: a plain worker on the Redis store that knows how to
// report back into the flow's parent store.
const EmailWorker = SendEmail.toLayer(({ userId }) =>
  userId.endsWith("u7")
    ? new SendBounced({ address: `${userId}@example.com` })
    : Console.log(`  [redis-worker]  sent → ${userId}@example.com`).pipe(
      Effect.as(`msg-${userId}`)
    )
)

const Workers = Layer.mergeAll(
  Layer.mergeAll(InvoiceWorker, DigestWorker).pipe(
    Layer.provide(Worker.layer({ id: "pg-worker", pollInterval: "500 millis" }))
  ),
  EmailWorker.pipe(
    Layer.provide(Worker.layer({
      id: "redis-worker",
      store: EmailStore,
      flows: [DigestFlow],
      concurrency: 4,
      pollInterval: "500 millis"
    }))
  )
)

const AppLayer = Workers.pipe(
  Layer.provideMerge(Layer.mergeAll(PgStoreLive, RedisStoreLive)),
  Layer.provideMerge(PgLive)
)

// ── The show ────────────────────────────────────────────────────────────

const program = Effect.gen(function*() {
  // 1 ─────────────────────────────────────────────────────────────────
  yield* scene(1, "Typed jobs + idempotency (Postgres)")
  const first = yield* GenerateInvoice.enqueue({ invoiceId: "inv_1042", amountCents: 129_900 })
  const second = yield* GenerateInvoice.enqueue({ invoiceId: "inv_1042", amountCents: 129_900 })
  yield* Console.log(`  enqueue #1 → ${first}`)
  yield* Console.log(`  enqueue #2 → ${second}  (idempotent no-op: ${first === second})`)
  const pdf = yield* GenerateInvoice.execute({ invoiceId: "inv_1043", amountCents: 4_500 })
  yield* Console.log(`  execute()  → typed result "${pdf}" awaited across the store`)

  // 2 ─────────────────────────────────────────────────────────────────
  yield* scene(2, "kill -9 a worker mid-job (stall recovery)")
  const reportId = yield* RenderReport.enqueue({ reportId: "q3-board-deck" })
  yield* Console.log(`  enqueued "${reportId}"; spawning a separate worker process…`)
  const victim = Bun.spawn(["bun", "src/victim.ts"], {
    cwd: import.meta.dir + "/..",
    stdout: "inherit",
    stderr: "inherit"
  })
  yield* Effect.sleep("2500 millis")
  victim.kill(9)
  yield* Console.log(`  [demo]          kill -9 ${victim.pid} — no goodbye, no release, lock just rots`)

  const Recovery = RenderReport.toLayer(({ reportId: r }) =>
    Console.log(`  [recovery]      picked "${r}" back up — finishing the render`).pipe(
      Effect.as(`pdf/${r}`)
    )
  ).pipe(Layer.provide(Worker.layer({
    id: "recovery",
    stalledInterval: "1 second",
    pollInterval: "500 millis"
  })))
  yield* Effect.gen(function*() {
    const rendered = yield* RenderReport.awaitResult(reportId)
    yield* Console.log(`  result          "${rendered}" — despite the murder`)
  }).pipe(Effect.provide(Recovery))

  const ledger = yield* RenderReport.attempts(reportId)
  yield* Console.log(
    `  attempts ledger ${ledger.map((entry) => `#${entry.attempt} ${entry.outcome}`).join("  →  ")}`
  )

  // 3 ─────────────────────────────────────────────────────────────────
  yield* scene(3, "Dedup throttle: five enqueues, one job")
  const ids = new Set<string>()
  for (let i = 0; i < 5; i++) {
    ids.add(yield* RefreshCache.enqueue({ accountId: "acct_9" }))
  }
  yield* Console.log(`  5 enqueues collapsed into ${ids.size} job (30s throttle window)`)
  const cancelled = yield* RefreshCache.cancelByKey("acct_9")
  yield* Console.log(`  cancelByKey("acct_9") → ${cancelled} — no job-id bookkeeping anywhere`)

  // 4 ─────────────────────────────────────────────────────────────────
  yield* scene(4, "Cross-store flow, with the email queue PAUSED")
  const emails = yield* EmailStore
  yield* emails.pause(QueueName("email"))
  yield* Console.log(`  paused the "email" queue on the REDIS store, then started the flow:`)
  const flowId = yield* DigestFlow.enqueue({ tenant: "acme", audience: 12 })

  // The parent fans out in Postgres; the children land in Redis and WAIT.
  let waiting = 0
  for (let i = 0; i < 40 && waiting < 12; i++) {
    yield* Effect.sleep("250 millis")
    waiting = (yield* emails.counts(QueueName("email"))).waiting
  }
  const parked = yield* DigestFlow.poll(flowId)
  const parentState = Option.isSome(parked) ? parked.value.state : "?"
  yield* Console.log(`  redis     counts("email")     → ${waiting} waiting, 0 running`)
  yield* Console.log(`  postgres  poll(parent).state  → "${parentState}"`)
  yield* Console.log(`  two databases, one flow, all of it durable state. resuming…`)

  yield* emails.resume(QueueName("email"))
  const digest = yield* DigestFlow.awaitResult(flowId)
  yield* Console.log(`  flow result     sent=${digest.sent} bounced=${digest.bounced}`)

  const client = yield* PgClient.PgClient
  const rows = yield* client.unsafe(
    `SELECT id, state, flow_completed, flow_failed FROM effect_mq_jobs WHERE name = 'send-digest'`
  ).pipe(Effect.orDie)
  yield* Console.log(`  and it's just rows — SELECT … FROM effect_mq_jobs WHERE name = 'send-digest':`)
  yield* Console.log(`  ${JSON.stringify(rows[0])}`)

  yield* Console.log(`\n${rule}\n  fin — durable, typed, and queryable. docker compose down -v to reset.\n${rule}`)
})

await Effect.runPromise(
  resetTables.pipe(Effect.provide(PgLive))
)
await Effect.runPromise(
  program.pipe(Effect.provide(AppLayer))
)
process.exit(0)
