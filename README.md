# effect-mq

Effect-native background jobs: schema-first job definitions, a storage-agnostic
queue core, a worker runtime, and a Postgres store that lives inside your
drizzle schema. Built on Effect v4; inspired by BullMQ's semantics and
`effect/workflow`'s DX.

**→ Full usage documentation lives in the package README:
[`packages/effect-mq`](./packages/effect-mq/README.md)** (also shown on npm).

```ts
import { Job } from "effect-mq"
import { Effect, Schema } from "effect"

class SendEmail extends Job.make("SendEmail", {
  payload: { to: Schema.String, subject: Schema.String },
  success: Schema.String,
  idempotencyKey: ({ to, subject }) => `${to}:${subject}`,
  metadata: ({ to }) => ({ to }),
  defaults: { attempts: 3, backoff: { type: "exponential", delay: "1 second" } }
}) {}

const producer = Effect.gen(function*() {
  const jobId = yield* SendEmail.enqueue({ to: "ada@example.com", subject: "hi" })
})
```

## Highlights

- **One package, tree-shakeable modules** — `effect-mq` (core),
  `effect-mq/drizzle-postgres` (Postgres store + schema factories),
  `effect-mq/redis` (Redis store), `effect-mq/testing` (driver conformance
  kit). Extra peers are optional.
- **At-least-once** with token-guarded locks, heartbeat renewal, and stalled
  recovery; **durable retries** routed through the store, never held in a
  worker's memory.
- **Run ledger**: every attempt (success, retry, failure, stall) is persisted
  and decodes back to typed exits.
- **Multi-store routing**: bind jobs to named stores (Postgres for
  business-critical runs, memory/Redis for disposable ones) — wiring enforced
  at compile time.
- **Dashboard data layer**: `list` with metadata filters and keyset
  pagination, `poll`, `retry`, per-job `keep` retention plus a per-store
  `historyTtl` ceiling.
- **Repeatable jobs**: durable cron/interval schedules; each occurrence is
  claimed and enqueued in one atomic store op — exactly one job per
  occurrence across any number of workers, regardless of retention.
- **Batch enqueue**: `enqueueMany` inserts a whole batch of plain items in
  one store round trip per chunk, with per-item idempotency and dedup
  semantics intact.
- **Fiber-native control**: handler `timeout`s, cross-process `cancel` of
  running jobs (the handler fiber is interrupted, finalizers run), `promote`,
  queue `pause`/`resume`, and unrecoverable errors that skip the retry budget.
- **Deduplication**: pending-dedup, throttle, debounce, and
  replace-while-delayed — a separate dedup key that never rewrites job ids.
- **Metrics**: Effect `Metric` instruments for runs, durations, queue
  latency, depth, and worker health — export with your observability stack.
- **Drizzle-native Postgres**: the job tables are drizzle-postgres schema
  factories you re-export — drizzle-kit owns migrations; queries are fully
  typed including the job-name union. `FOR UPDATE SKIP LOCKED` claims,
  LISTEN/NOTIFY wake-ups, Node and Bun.
- **Redis store**: every operation is one atomic Lua script over Effect's
  client-agnostic `Redis` service (`@effect/platform-node`/`-bun` provide
  it); pub/sub wake-ups; the same conformance suite runs against it.
- **Redaction-aware persistence**: everything is stored schema-encoded;
  `Schema.Redacted` round-trips (handlers get `Redacted` values),
  `disallowJsonEncode` refuses persistence entirely.

## Repository layout

```
packages/effect-mq        the published package (src + tests, incl. the
                          Postgres and Redis suites under test/)
examples/basic            runnable end-to-end demo: bun src/main.ts
docker-compose.yml        Postgres 17 (5433) + Redis 8 (6380) for the
                          storage test suites
tools/oxlint/anti-slop    local lint plugin enforcing honest types
```

## Development

```sh
bun install
bun run check     # TypeScript 7, per package
bun run lint      # oxlint (incl. anti-slop rules)
bun run test      # vitest 4 + @effect/vitest, TestClock-driven
                  # (storage suites self-skip without their backing service)
bun run test:pg   # docker compose up + the Postgres suite
bun run test:redis  # docker compose up + the Redis suite
bun run ready     # check + lint + test
bun run build     # emit dist (tsc, .ts imports rewritten to .js)
```

The store conformance suite (`effect-mq/testing`) runs against the
in-memory driver, real Postgres, and real Redis — under `TestClock` in all
cases, because drivers take all time as parameters from the Effect `Clock`.

Release history lives in [CHANGELOG.md](./CHANGELOG.md); planned work in
[ROADMAP.md](./ROADMAP.md).

## Releasing

CI (`.github/workflows/ci.yml`) runs check/lint/tests (with Postgres and
Redis service containers) on every push and PR. Publishing to npm happens on version tags:

```sh
# bump "version" in packages/effect-mq/package.json, then
git tag v0.2.0 && git push --tags
```

The publish job runs `npm publish` with provenance; it needs an `NPM_TOKEN`
repository secret. The package's `prepack` builds `dist` and swaps the
dev (TS-source) exports for dist exports; `postpack` restores them. Verify a
release candidate locally with:

```sh
cd packages/effect-mq && bun pm pack   # then inspect / install the tarball
```

## License

MIT
