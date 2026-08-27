import { mkdir, readFile, writeFile } from "node:fs/promises"
import { dirname, join } from "node:path"
import { defineConfig, type DefaultTheme, type SiteConfig } from "vitepress"

const SITE = "https://www.effect-mq.com"
const BLURB =
  "Effect-native background jobs for TypeScript. Schema-typed payloads, swappable storage (Postgres, Redis, in-memory), at-least-once execution, parent-child flows."

const sidebar: Array<DefaultTheme.SidebarItem> = [
  {
    text: "Start",
    items: [
      { text: "What is effect-mq?", link: "/guide/introduction" },
      { text: "Getting started", link: "/guide/getting-started" }
    ]
  },
  {
    text: "Produce",
    items: [
      { text: "Defining jobs", link: "/guide/defining-jobs" },
      { text: "Enqueueing", link: "/guide/enqueueing" },
      { text: "Deduplication", link: "/guide/deduplication" },
      { text: "Repeatable jobs", link: "/guide/repeatable-jobs" }
    ]
  },
  {
    text: "Run",
    items: [
      { text: "Workers & handlers", link: "/guide/workers" },
      { text: "Parent-child flows", link: "/guide/flows" },
      { text: "Retries & timeouts", link: "/guide/retries-and-timeouts" },
      { text: "Cancellation & admin", link: "/guide/cancellation-and-admin" }
    ]
  },
  {
    text: "Operate",
    items: [
      { text: "Retention & history", link: "/guide/retention" },
      { text: "Tracing & metrics", link: "/guide/observability" },
      { text: "Testing your app", link: "/guide/testing" }
    ]
  },
  {
    text: "Storage",
    items: [
      { text: "Postgres (drizzle)", link: "/storage/postgres" },
      { text: "Redis", link: "/storage/redis" },
      { text: "Memory & multiple stores", link: "/storage/stores" },
      { text: "Writing a driver", link: "/storage/writing-a-driver" }
    ]
  },
  {
    text: "Reference",
    items: [
      { text: "Options at a glance", link: "/reference/options" },
      { text: "Changelog", link: "https://github.com/TeamWarp/effect-mq/blob/main/CHANGELOG.md" },
      { text: "Roadmap", link: "https://github.com/TeamWarp/effect-mq/blob/main/ROADMAP.md" }
    ]
  }
]

const stripFrontmatter = (source: string): string => {
  if (!source.startsWith("---\n")) return source
  const end = source.indexOf("\n---\n", 4)
  return end === -1 ? source : source.slice(end + 5).trimStart()
}

// Every doc page in sidebar order, internal links only (Changelog/Roadmap
// point at GitHub).
const docPages = sidebar.flatMap((group) =>
  (group.items ?? []).flatMap((item) =>
    item.link !== undefined && item.link.startsWith("/") && item.text !== undefined
      ? [{ group: group.text ?? "", text: item.text, link: item.link }]
      : []
  )
)

// Emit LLM-facing artifacts next to the rendered site: a raw .md twin for
// every page (append .md to any URL), llms.txt (the index), and
// llms-full.txt (all pages concatenated).
const buildLlmsArtifacts = async (siteConfig: SiteConfig): Promise<void> => {
  await Promise.all(
    siteConfig.pages.map(async (page) => {
      const source = await readFile(join(siteConfig.srcDir, page), "utf8")
      const target = join(siteConfig.outDir, page)
      await mkdir(dirname(target), { recursive: true })
      await writeFile(target, stripFrontmatter(source))
    })
  )

  const indexSections = sidebar.map((group) => {
    const lines = (group.items ?? []).map((item) => {
      const url = item.link !== undefined && item.link.startsWith("/") ? `${SITE}${item.link}.md` : item.link ?? ""
      return `- [${item.text ?? ""}](${url})`
    })
    return `## ${group.text ?? ""}\n\n${lines.join("\n")}`
  })
  const llmsTxt = [
    "# effect-mq",
    "",
    `> ${BLURB}`,
    "",
    "Every page below is also served as raw markdown: append `.md` to any page URL.",
    `The complete documentation in one file: ${SITE}/llms-full.txt`,
    "",
    indexSections.join("\n\n"),
    ""
  ].join("\n")
  await writeFile(join(siteConfig.outDir, "llms.txt"), llmsTxt)

  const fullSections = await Promise.all(
    docPages.map(async (page) => {
      const source = await readFile(join(siteConfig.srcDir, `${page.link.slice(1)}.md`), "utf8")
      return `---\nurl: ${SITE}${page.link}\n---\n\n${stripFrontmatter(source)}`
    })
  )
  const llmsFullTxt = `# effect-mq — full documentation\n\n> ${BLURB}\n\n${fullSections.join("\n\n")}`
  await writeFile(join(siteConfig.outDir, "llms-full.txt"), llmsFullTxt)
}

export default defineConfig({
  lang: "en-US",
  title: "effect-mq",
  description: "Effect-native background jobs. Schema-typed payloads, swappable storage, at-least-once execution.",
  cleanUrls: true,
  lastUpdated: true,
  head: [
    ["meta", { name: "theme-color", content: "#111111" }],
    ["meta", { property: "og:type", content: "website" }],
    ["meta", { property: "og:site_name", content: "effect-mq" }],
    ["meta", { property: "og:image", content: "https://www.effect-mq.com/og.png" }],
    ["meta", { property: "og:image:width", content: "1200" }],
    ["meta", { property: "og:image:height", content: "630" }],
    ["meta", { name: "twitter:card", content: "summary_large_image" }],
    ["meta", { name: "twitter:image", content: "https://www.effect-mq.com/og.png" }]
  ],
  // Per-page share cards: the branded og.png plus this page's own title and
  // description (og:title/og:description are per page, so they live here
  // instead of the static head).
  transformPageData(pageData) {
    const title = pageData.title === "" || pageData.title === "effect-mq"
      ? "effect-mq"
      : `${pageData.title} · effect-mq`
    const description = pageData.description === ""
      ? "Effect-native background jobs. Schema-typed payloads, swappable storage, at-least-once execution."
      : pageData.description
    pageData.frontmatter.head ??= []
    pageData.frontmatter.head.push(
      ["meta", { property: "og:title", content: title }],
      ["meta", { property: "og:description", content: description }]
    )
  },
  markdown: {
    theme: { light: "github-light", dark: "github-dark" }
  },
  buildEnd: buildLlmsArtifacts,
  themeConfig: {
    siteTitle: "effect-mq",
    search: { provider: "local" },
    nav: [
      { text: "Guide", link: "/guide/getting-started", activeMatch: "/guide/" },
      { text: "Storage", link: "/storage/postgres", activeMatch: "/storage/" },
      { text: "Reference", link: "/reference/options", activeMatch: "/reference/" },
      { text: "npm", link: "https://www.npmjs.com/package/effect-mq" }
    ],
    sidebar,
    socialLinks: [{ icon: "github", link: "https://github.com/TeamWarp/effect-mq" }],
    editLink: {
      pattern: "https://github.com/TeamWarp/effect-mq/edit/main/docs/:path",
      text: "Edit this page"
    },
    outline: { level: [2, 3] },
    footer: {
      message:
        'Released under the MIT License.<br>Made with ❤️ by <a href="https://www.warp.co/careers#open-roles">Warp (We\'re hiring)</a>.'
    }
  }
})
