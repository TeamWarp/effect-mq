// Swap the development exports (TS source) for the published exports (dist)
// before packing; `postpack.mjs` restores the original file. Runs
// automatically via the npm/bun `prepack` lifecycle.
//
// Recovery: if a pack fails between prepack and postpack, package.json is
// left in the published state with the dev manifest in package.json.dev —
// run `node scripts/postpack.mjs` to restore it, then retry.
import { copyFileSync, existsSync, readFileSync, writeFileSync } from "node:fs"

const path = new URL("../package.json", import.meta.url)
const backup = new URL("../package.json.dev", import.meta.url)

if (existsSync(backup)) {
  process.stderr.write(
    "package.json.dev already exists — a previous pack failed before postpack " +
      "could restore it. Run `node scripts/postpack.mjs` to restore the dev " +
      "manifest, then retry.\n"
  )
  process.exit(1)
}
copyFileSync(path, backup)

const pkg = JSON.parse(readFileSync(path, "utf8"))
pkg.exports = {
  "./package.json": "./package.json",
  ".": { types: "./dist/index.d.ts", default: "./dist/index.js" },
  "./drizzle": { types: "./dist/drizzle/index.d.ts", default: "./dist/drizzle/index.js" },
  "./testing": { types: "./dist/testing/index.d.ts", default: "./dist/testing/index.js" }
}
// devDependencies use bun's `catalog:` protocol, which npm cannot parse —
// and they are irrelevant to consumers anyway.
delete pkg.devDependencies
delete pkg.scripts.prepack
delete pkg.scripts.postpack
writeFileSync(path, JSON.stringify(pkg, null, 2) + "\n")
