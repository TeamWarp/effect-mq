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

bun src/dashboard.ts                     # interactive tour → http://localhost:4400
bun src/main.ts                          # or: the scripted four scenes (terminal)
DEMO_PAUSE_SECONDS=15 bun src/main.ts    # linger on the scripted paused flow

# rerun as often as you like — the script resets its tables and Redis keys
# each run. docker compose down -v wipes everything.
```

## The interactive tour (dashboard + buttons)

`src/dashboard.ts` is one process: the workers, a live view of both stores
through the public read APIs (`counts`, `list`, `pausedQueues`,
`listSchedules`), and a numbered sidebar — one section per demo, each with a
presenter blurb and its buttons, plus a **clear** button that resets both
stores between takes. Every button runs the same producer API an
application would:

| section | buttons |
| --- | --- |
| 1. jobs | enqueue invoice #1042 (click twice: same id), 5× throttled refresh, cancel by key |
| 2. durability | kill a worker mid-job (spawns + SIGKILLs a real process, reports the ledger), cancel a RUNNING job (heartbeat interrupt), fail an import → retry it |
| 3. scheduling | enqueue delayed 1h → promote it, flow every 15s (a recurring cross-store fan-out, exactly-once per tick) → unschedule |
| 4. flows · queue control | run digest flow (12), pause email, resume email |

Every section has a **view code** toggle showing the code that matters for
it (job definitions, the flow, the store layers with their customization
knobs) with syntax highlighting; `?code=open` in the URL pre-opens all of
them. The sidebar starts on **0. Stores**, an intro to the two-store
architecture.

A good live sequence: pause email → run digest flow → point at both panels
(Postgres `waiting-children · flow 12 pending`, Redis `12 waiting` under the
paused callout) → resume → watch it drain and collect. Then "flow every 15s"
and let it breathe while you talk.

The scripted `main.ts` covers the same ground in ~15 seconds of terminal
output if you prefer a hands-free run.

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
