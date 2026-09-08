# The effect-mq tour

The canonical demo: four scenes, ~15 seconds of runtime, against real
Postgres and Redis. Everything the library claims, shown live — typed
idempotent jobs, durability you can kill -9, queue control, and a
cross-store parent-child flow — with a one-file dashboard to watch it move.
Screen-share friendly by design.

```sh
# from the repo root
docker compose up -d --wait
cd examples/podcast-demo

bun src/main.ts                          # the four scenes (terminal)
bun src/dashboard.ts                     # live UI → http://localhost:4400
DEMO_PAUSE_SECONDS=15 bun src/main.ts    # linger on the paused flow while presenting

# rerun as often as you like — the script resets its tables and Redis keys
# each run. docker compose down -v wipes everything.
```

## The dashboard

`src/dashboard.ts` is one file: `Bun.serve` polling both stores through the
public read APIs (`counts`, `list`, `pausedQueues`) — the dashboard data
layer, rendered. Postgres and Redis side by side; during scene 4 it shows
the parent bold in `waiting-children · flow 12 pending` on the left while
the right panel shows `12 waiting` under a `paused queues: email` callout.
Start it before `main.ts` and leave it up across reruns; it holds the last
snapshot through table resets.

## What each scene shows

**Scene 1 — typed jobs + idempotency.** Two enqueues of the same invoice
return the same `JobId` (`idempotencyKey` derives it from business data);
`execute()` awaits the typed result across the store boundary.

**Scene 2 — kill -9 a worker mid-job.** The script spawns a real second
worker process, lets it claim the job, and SIGKILLs it: no release, no
goodbye, the lock just expires. A recovery worker's stall sweeper picks the
job up and finishes it. The receipts are the attempts ledger printed at the
end: `#1 stalled → #2 completed`. (The `WARN recovered stalled jobs` line is
the library's own logging — durability you can alert on.)

**Scene 3 — dedup throttle.** Five enqueues collapse into one job under a
30-second throttle key, then `cancelByKey` cancels it without any job-id
bookkeeping.

**Scene 4 — the cross-store flow, with the queue paused.** The `email`
queue on the Redis store gets paused *before* the flow starts. The Postgres
parent fans 12 sends out anyway, then the demo prints both databases' views
of the parked flow side by side: Redis `counts("email") → 12 waiting`,
Postgres `poll(parent).state → "waiting-children"`. Two stores, one flow,
nothing but durable state. `resume` drains the queue, one send bounces (the
`ERROR job "send-email" failed terminally` line is the built-in failure
logging), and `collect` resumes with typed outcomes. The closer is a plain
SQL `SELECT` against `effect_mq_jobs` showing the parent row with its flow
counters — the operational surface is your own database.

Crank the fan-out with the `audience` field in scene 4 (it chunks fine at
10,000; the terminal is the bottleneck). `DigestFlow.schedule("daily",
{ cron: "0 9 * * *" })` would put the whole flow on a cron — that sentence
("a cron in Postgres fans out ten thousand idempotent sends into Redis and
reports back") is the demo's thesis.

## Demo-only shortcuts

The script drop/creates the default-named tables at startup by borrowing the
repo's canonical test DDL. Real applications re-export the schema factories
from their drizzle schema and let drizzle-kit own migrations — see the
[Postgres guide](https://www.effect-mq.com/storage/postgres).
