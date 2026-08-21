
Default to using Bun instead of Node.js.

- Use `bun <file>` instead of `node <file>` or `ts-node <file>`
- Use `bun test` instead of `jest` or `vitest`
- Use `bun build <file.html|file.ts|file.css>` instead of `webpack` or `esbuild`
- Use `bun install` instead of `npm install` or `yarn install` or `pnpm install`
- Use `bun run <script>` instead of `npm run <script>` or `yarn run <script>` or `pnpm run <script>`
- Use `bunx <package> <command>` instead of `npx <package> <command>`
- Bun automatically loads .env, so don't use dotenv.

## APIs

- `Bun.serve()` supports WebSockets, HTTPS, and routes. Don't use `express`.
- `bun:sqlite` for SQLite. Don't use `better-sqlite3`.
- `Bun.redis` for Redis. Don't use `ioredis`.
- `Bun.sql` for Postgres. Don't use `pg` or `postgres.js`.
- `WebSocket` is built-in. Don't use `ws`.
- Prefer `Bun.file` over `node:fs`'s readFile/writeFile
- Bun.$`ls` instead of execa.

## Testing

Use `bun test` to run tests.

```ts#index.test.ts
import { test, expect } from "bun:test";

test("hello world", () => {
  expect(1).toBe(1);
});
```

## Frontend

Use HTML imports with `Bun.serve()`. Don't use `vite`. HTML imports fully support React, CSS, Tailwind.

Server:

```ts#index.ts
import index from "./index.html"

Bun.serve({
  routes: {
    "/": index,
    "/api/users/:id": {
      GET: (req) => {
        return new Response(JSON.stringify({ id: req.params.id }));
      },
    },
  },
  // optional websocket support
  websocket: {
    open: (ws) => {
      ws.send("Hello, world!");
    },
    message: (ws, message) => {
      ws.send(message);
    },
    close: (ws) => {
      // handle close
    }
  },
  development: {
    hmr: true,
    console: true,
  }
})
```

HTML files can import .tsx, .jsx or .js files directly and Bun's bundler will transpile & bundle automatically. `<link>` tags can point to stylesheets and Bun's CSS bundler will bundle.

```html#index.html
<html>
  <body>
    <h1>Hello, world!</h1>
    <script type="module" src="./frontend.tsx"></script>
  </body>
</html>
```

With the following `frontend.tsx`:

```tsx#frontend.tsx
import React from "react";
import { createRoot } from "react-dom/client";

// import .css files directly and it works
import './index.css';

const root = createRoot(document.body);

export default function Frontend() {
  return <h1>Hello, world!</h1>;
}

root.render(<Frontend />);
```

Then, run index.ts

```sh
bun --hot ./index.ts
```

For more information, read the Bun API docs in `node_modules/bun-types/docs/**.mdx`.

## effect-mq project notes

This is a bun monorepo (workspaces: `packages/*`, `examples/*`) building an
Effect-native background job library on **Effect v4** (`effect@4.0.0-rc.x`,
pinned in the root `catalog`).

- Architecture: ONE published package, `packages/effect-mq`, with subpath
  modules (`effect-mq`, `effect-mq/drizzle-postgres`, `effect-mq/redis`,
  `effect-mq/testing`; optional peers for the non-core subpaths). `src/JobStore.ts` is the storage seam
  (atomic driver interface, encoded payloads/exits, named store keys,
  attempts ledger, list/retry/keep); `src/MemoryJobStore.ts` is the reference
  driver; `src/Job.ts` the definition/producer API; `src/Worker.ts` the
  runner; `src/drizzle-postgres/` the Postgres store (drizzle 1.0 effect driver over
  `@effect/sql-pg`; schema factories owned by the user's drizzle-kit
  migrations); `src/redis/` the Redis store (atomic Lua scripts over
  `effect/unstable/persistence`'s `Redis` service; provide it via
  `@effect/platform-node` `NodeRedis` or `-bun` `BunRedis`); `src/testing/`
  the conformance suite every driver must pass.
- Publishing: `npm publish` from `packages/effect-mq` (CI does this on `v*`
  tags with NPM_TOKEN). `prepack` builds dist and swaps the dev TS-source
  exports for dist exports; `postpack` restores. bun does NOT apply
  `publishConfig.exports` — do not rely on it.
- Lint includes the local anti-slop plugin (`tools/oxlint/anti-slop`):
  type assertions need `SAFETY:` comments, no `as any`, no bare-`unknown`
  params — prefer inference, `satisfies`, and named contracts.
- Storage tests: `bun run test:pg` / `bun run test:redis` (docker compose:
  postgres on 5433, redis on 6380). Both suites self-skip when the backing
  service is unreachable. Drivers MUST
  take all time as bind parameters from the Effect Clock (never SQL `now()`)
  so conformance runs under TestClock against real storage.
- drizzle is pinned to 1.0 rc builds in the root catalog; verify drizzle APIs
  against the installed `node_modules/drizzle-orm` (v1 changed init/relational
  APIs; the effect driver lives at `drizzle-orm/effect-postgres`).
- Effect v4 differs from v3: services via `Context.Service`, `Effect.gen`
  without adapter, data-first Queue/Deferred ops, `Semaphore`/`Schedule.min`
  modules. Verify APIs against `node_modules/effect/src` — do not trust v3
  memory. Reference repos are cloned in `.repos/` (effect, bullmq,
  effect-agent).
- Testing: this repo uses **vitest 4 + @effect/vitest** (`bun run test`), NOT
  `bun test` — TestClock-based `it.effect` tests need the vitest integration.
  Typecheck with `bun run check` (TypeScript 7), lint with `bun run lint`
  (oxlint), everything with `bun run ready`.
- Invariants to preserve: producer API (`Job.enqueue/poll/awaitResult`) must
  never require the `Worker` service (type-level test enforces this); all
  `JobStore` methods must be atomic; acks are lock-token-checked
  (at-least-once); everything must work under both TestClock and the live
  clock.
