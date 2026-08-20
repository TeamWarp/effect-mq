/**
 * Minimal end-to-end demo: define a job, run a worker, queue jobs and watch
 * them execute in the background — all in-memory, on the live clock.
 *
 * Run with: bun src/main.ts
 */
import { Job, JobStore, MemoryJobStore, Worker } from "effect-mq"
import { Console, Effect, Layer, Schema } from "effect"

class SendEmail extends Job.make("SendEmail", {
  payload: { to: Schema.String, subject: Schema.String },
  success: Schema.String,
  idempotencyKey: ({ to, subject }) => `${to}:${subject}`,
  metadata: ({ to }) => ({ to }),
  queue: "email",
  defaults: { attempts: 3, backoff: { type: "exponential", delay: "50 millis" } }
}) {}

const SendEmailWorker = SendEmail.toLayer(
  (payload, context) =>
    Effect.gen(function*() {
      // Fail the first attempt to demonstrate retries with backoff.
      if (context.attempt === 1 && payload.to === "flaky@example.com") {
        return yield* Effect.die("smtp connection reset")
      }
      yield* Console.log(
        `[worker] sending "${payload.subject}" to ${payload.to} (attempt ${context.attempt})`
      )
      return `message-id-${context.jobId}`
    }),
  { concurrency: 2 }
)

const AppLayer = SendEmailWorker.pipe(
  Layer.provideMerge(Worker.layer({ pollInterval: "250 millis" })),
  Layer.provideMerge(MemoryJobStore.layer)
)

const program = Effect.gen(function*() {
  // Fire-and-forget enqueue.
  const id = yield* SendEmail.enqueue({ to: "ada@example.com", subject: "hi" })
  yield* Console.log(`[producer] enqueued ${id}`)

  // Same idempotency key: no duplicate work.
  const dup = yield* SendEmail.enqueue({ to: "ada@example.com", subject: "hi" })
  yield* Console.log(`[producer] duplicate enqueue returned same id: ${id === dup}`)

  // A delayed job.
  yield* SendEmail.enqueue(
    { to: "grace@example.com", subject: "later" },
    { delay: "300 millis" }
  )

  // Enqueue-and-wait, with a first-attempt failure and retry.
  const messageId = yield* SendEmail.execute({
    to: "flaky@example.com",
    subject: "retry me"
  })
  yield* Console.log(`[producer] flaky job completed with result: ${messageId}`)

  // Give the delayed job time to run before shutting down.
  yield* Effect.sleep("500 millis")

  // The dashboard data layer: list runs with status and inspect a ledger.
  const store = yield* JobStore.JobStore
  const listed = yield* store.list({ name: "SendEmail" })
  for (const record of listed.items) {
    yield* Console.log(
      `[list] ${record.id} -> ${record.state} (attempts: ${record.attemptsMade}, metadata: ${JSON.stringify(record.metadata)})`
    )
  }
  const flakyRuns = yield* SendEmail.attempts(
    JobStore.JobId("SendEmail/flaky@example.com:retry me")
  )
  yield* Console.log(
    `[ledger] flaky job runs: ${flakyRuns.map((run) => `#${run.attempt} ${run.outcome}`).join(", ")}`
  )
  yield* Console.log("[main] done, shutting worker down")
})

program.pipe(
  Effect.provide(AppLayer),
  Effect.runPromise
)
