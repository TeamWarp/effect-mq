// Restore the development package.json swapped out by prepack.mjs.
import { renameSync } from "node:fs"

renameSync(
  new URL("../package.json.dev", import.meta.url),
  new URL("../package.json", import.meta.url)
)
