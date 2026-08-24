import { defineConfig } from "vitepress"

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
  themeConfig: {
    siteTitle: "effect-mq",
    search: { provider: "local" },
    nav: [
      { text: "Guide", link: "/guide/getting-started", activeMatch: "/guide/" },
      { text: "Storage", link: "/storage/postgres", activeMatch: "/storage/" },
      { text: "Reference", link: "/reference/options", activeMatch: "/reference/" },
      { text: "npm", link: "https://www.npmjs.com/package/effect-mq" }
    ],
    sidebar: [
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
    ],
    socialLinks: [{ icon: "github", link: "https://github.com/TeamWarp/effect-mq" }],
    editLink: {
      pattern: "https://github.com/TeamWarp/effect-mq/edit/main/docs/:path",
      text: "Edit this page"
    },
    outline: { level: [2, 3] },
    footer: {
      message: "Released under the MIT License."
    }
  }
})
