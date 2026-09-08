/**
 * The demo's job definitions — shared by the main process and the victim
 * worker (scene 2), exactly like a real app shares definitions between
 * producers and runners.
 */
import { Schema } from "effect"
import { Flow, Job } from "effect-mq"
import { EmailStore } from "./stores.ts"

/** Scene 1: typed payloads + idempotency, on Postgres. */
export class GenerateInvoice extends Job.make("generate-invoice", {
  payload: { invoiceId: Schema.String, amountCents: Schema.Number },
  success: Schema.String,
  idempotencyKey: ({ invoiceId }) => invoiceId,
  metadata: ({ invoiceId }) => ({ invoiceId }),
  defaults: { attempts: 3, backoff: { type: "exponential", delay: "250 millis" } }
}) {}

/** Scene 2: the job whose worker gets murdered mid-run. */
export class RenderReport extends Job.make("render-report", {
  payload: { reportId: Schema.String },
  success: Schema.String,
  queue: "reports"
}) {}

/** Scene 3: throttled by a dedup key — at most one per account per window. */
export class RefreshCache extends Job.make("refresh-cache", {
  payload: { accountId: Schema.String },
  dedupe: ({ accountId }) => ({ key: accountId, ttl: "30 seconds" })
}) {}

export class SendBounced extends Schema.TaggedError<SendBounced>()("SendBounced", {
  address: Schema.String
}) {}

/** Scene 4 children: disposable sends, bound to the REDIS store. */
export class SendEmail extends Job.make("send-email", {
  payload: { userId: Schema.String },
  success: Schema.String,
  error: SendBounced,
  queue: "email",
  store: EmailStore
}) {}

/** Scene 4 parent: lives in Postgres, owns the flow. */
export class SendDigest extends Job.make("send-digest", {
  payload: { tenant: Schema.String, audience: Schema.Number },
  success: Schema.Struct({ sent: Schema.Number, bounced: Schema.Number })
}) {}

/**
 * The cross-store flow: a Postgres parent fans out into Redis and collects
 * the results back into Postgres. `DigestFlow.schedule("daily", { cron })`
 * would put the whole thing on a cron.
 */
export const DigestFlow = Flow.make("daily-digest", {
  parent: SendDigest,
  children: [SendEmail]
})
