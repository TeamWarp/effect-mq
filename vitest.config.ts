import * as path from "node:path"
import { defineConfig, mergeConfig, type ViteUserConfig } from "vitest/config"

const shared: ViteUserConfig = {
  test: {
    passWithNoTests: true,
    include: ["test/**/*.test.ts"],
    setupFiles: [path.join(import.meta.dirname, "vitest.setup.ts")],
    sequence: { concurrent: true },
    exclude: ["**/node_modules/**", "**/dist/**", "**/*.d.ts"]
  }
}

const project = (name: string, directory: string) => mergeConfig(shared, { root: directory, test: { name } })

export default defineConfig({
  test: {
    passWithNoTests: true,
    projects: [
      project("effect-mq", "packages/effect-mq")
    ]
  }
})
