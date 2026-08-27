import type { Theme } from "vitepress"
import DefaultTheme from "vitepress/theme"
import { h } from "vue"
import CopyMarkdown from "./CopyMarkdown.vue"
// Side-effect stylesheet import is the VitePress custom-theme convention.
// oxlint-disable-next-line import/no-unassigned-import
import "./custom.css"

export default {
  extends: DefaultTheme,
  Layout: () =>
    h(DefaultTheme.Layout, null, {
      "doc-before": () => h(CopyMarkdown)
    })
} satisfies Theme
