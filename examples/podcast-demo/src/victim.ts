/**
 * Scene 2's sacrificial worker. It claims the report job, announces itself,
 * and "renders" for five minutes — the demo kills it with SIGKILL long
 * before that. Short lock + heartbeat settings make the stall visible fast.
 *
 * Spawned by main.ts; assumes the tables already exist.
 */
import { Console, Effect, Layer } from "effect"
import { Worker } from "effect-mq"
import { RenderReport } from "./jobs.ts"
import { PgLive, PgStoreLive } from "./stores.ts"

const VictimLive = RenderReport.toLayer(({ reportId }) =>
  Effect.gen(function*() {
    yield* Console.log(
      `  [victim ${process.pid}]  claimed "${reportId}" — rendering… (kill me whenever)`
    )
    yield* Effect.sleep("5 minutes")
    return "unreachable"
  })
).pipe(
  Layer.provide(Worker.layer({
    id: "victim",
    lockDuration: "3 seconds",
    lockRenewInterval: "1 second",
    pollInterval: "250 millis"
  })),
  Layer.provideMerge(PgStoreLive),
  Layer.provide(PgLive)
)

await Effect.runPromise(Effect.never.pipe(Effect.provide(VictimLive)))
