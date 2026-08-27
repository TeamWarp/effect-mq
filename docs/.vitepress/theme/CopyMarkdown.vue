<script setup lang="ts">
import { useData } from "vitepress"
import { ref } from "vue"

const { page } = useData()
const state = ref<"idle" | "copied" | "failed">("idle")

// The build emits a raw .md twin for every page (buildEnd in config.ts), so
// the source of truth is one fetch away. In dev there is no twin — the dev
// server answers .md routes with the SPA shell — hence the leading-'<' guard.
const copy = async () => {
  try {
    const response = await fetch(`/${page.value.filePath}`)
    const text = await response.text()
    if (!response.ok || text.trimStart().startsWith("<")) throw new Error("raw markdown not served")
    await navigator.clipboard.writeText(text)
    state.value = "copied"
  } catch {
    state.value = "failed"
  }
  setTimeout(() => {
    state.value = "idle"
  }, 2000)
}
</script>

<template>
  <div class="copy-markdown">
    <button type="button" :disabled="state !== 'idle'" @click="copy">
      {{ state === "copied" ? "copied" : state === "failed" ? "unavailable" : "copy as markdown" }}
    </button>
  </div>
</template>

<style scoped>
.copy-markdown {
  display: flex;
  justify-content: flex-end;
  margin-bottom: 8px;
}

.copy-markdown button {
  border: 1px solid var(--vp-c-border);
  padding: 2px 10px;
  font-size: 12px;
  line-height: 20px;
  color: var(--vp-c-text-2);
  background: transparent;
  transition: color 0.2s, border-color 0.2s;
}

.copy-markdown button:hover:not(:disabled) {
  color: var(--vp-c-text-1);
  border-color: var(--vp-c-text-2);
}
</style>
