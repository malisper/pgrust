#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const SOURCE = path.join(HERE, "source", "posts.json");
const SITE_ROOT = path.join(HERE, "..");
const REQUIRED_ICONS = [
  "favicon.ico",
  "favicon.svg",
  "favicon-16x16.png",
  "favicon-32x32.png",
  "favicon-48x48.png",
  "apple-touch-icon.png",
  "pgrust-icon-192x192.png",
  "pgrust-icon-512x512.png",
  "site.webmanifest",
];

for (const filename of REQUIRED_ICONS) {
  if (!fs.existsSync(path.join(SITE_ROOT, filename))) {
    throw new Error(`Missing site icon: ${filename}`);
  }
}

const posts = JSON.parse(fs.readFileSync(SOURCE, "utf8"))
  .sort((a, b) => b.date.localeCompare(a.date));

const metadata = {
  "jit-compiling-code-in-5-us": {
    description: "Build a copy-and-patch JIT compiler in Rust, generate ARM64 machine code, and benchmark it against an interpreter and handwritten implementation.",
    image: "jit-compiler.png",
  },
  "how-we-made-postgres-hundreds-of-times-faster-the-query-engine": {
    description: "How batching, operator fusion, and SIMD made pgrust analytical queries hundreds of times faster than PostgreSQL.",
    image: "query-engine.png",
  },
  "postgres-in-rust-regression-suite": {
    description: "Four attempts, three dead ends, and the process that produced a Rust rewrite passing PostgreSQL's full regression suite.",
    image: "regression-suite.png",
  },
  "pgrust-passes-100-of-postgresqls-regression-tests": {
    description: "pgrust reaches full PostgreSQL regression and isolation test compatibility.",
    image: "pgrust-default.png",
  },
  "the-four-horsemen-behind-thousands-of-postgres-outages": {
    description: "Four recurring PostgreSQL failure modes and how pgrust is designed to address them.",
    image: "pgrust-default.png",
  },
  "pgrust-update-at-67-postgres-compatibility-and-accelerating": {
    description: "How parallel coding agents, shared build artifacts, and a merge queue accelerated the PostgreSQL rewrite.",
    image: "compatibility-update.png",
  },
  "pgrust-rebuilding-postgres-in-rust-with-ai": {
    description: "The first two weeks of rebuilding PostgreSQL in Rust with coding agents, from the parser to a browser demo.",
    image: "launch.png",
  },
};

const slugByOldPath = new Map(posts.map((post) => [new URL(post.link).pathname.replace(/\/$/, ""), post.slug]));
const localImages = new Map([
  ["/wp-content/uploads/2026/04/image-1.png", "launch.png"],
  ["/wp-content/uploads/2026/04/image-2.png", "account-usage.png"],
  ["/wp-content/uploads/2026/04/image-3.png", "account-rotation.png"],
  ["/wp-content/uploads/2026/04/image-4.png", "compatibility-update.png"],
  ["/wp-content/uploads/2026/07/pgrust-planner-tweet.jpg", "planner-tweet.jpg"],
  ["/wp-content/uploads/2026/07/pgrust-dynamic-workflow-e1784182469629.jpg", "dynamic-workflow.jpg"],
  ["/wp-content/uploads/2026/07/pgrust-dependency-graph.jpg", "dependency-graph.jpg"],
  ["/wp-content/uploads/2026/07/pgrust-regression-suite-social.png", "regression-suite.png"],
  ["/wp-content/uploads/2026/08/image.png", "query-engine.png"],
  ["/wp-content/uploads/2026/08/pgrust-jit-featured-simple-1200x630-1.png", "jit-compiler.png"],
]);

function escapeHtml(value) {
  return value.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;").replaceAll('"', "&quot;");
}

function plainText(value) {
  return value
    .replace(/<[^>]+>/g, " ")
    .replace(/&#8217;|&rsquo;/g, "'")
    .replace(/&#8220;|&#8221;|&ldquo;|&rdquo;/g, '"')
    .replace(/&amp;/g, "&")
    .replace(/&nbsp;/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function formatDate(value) {
  return new Intl.DateTimeFormat("en-US", {
    month: "long",
    day: "numeric",
    year: "numeric",
    timeZone: "UTC",
  }).format(new Date(value));
}

function pacificDate(value) {
  return /(?:Z|[+-]\d\d:\d\d)$/.test(value) ? value : `${value}-07:00`;
}

function localizePostLinks(html) {
  return html.replace(/https?:\/\/malisper\.me\/([^"'#?\s<]+)\/?/g, (url) => {
    const oldPath = new URL(url).pathname.replace(/\/$/, "");
    const slug = slugByOldPath.get(oldPath);
    if (slug) return `/blog/${slug}/`;
    if (oldPath === "/subscribe") return "https://pgrust.com/#updates";
    return url;
  });
}

function localizeImages(html) {
  let output = html.replace(/\s(?:srcset|sizes)="[^"]*"/g, "");
  output = output.replace(/https:\/\/malisper\.me(\/wp-content\/uploads\/[^"'\s<]+)/g, (url, pathname) => {
    let original = pathname.replace(/-\d+x\d+(?=\.[^.]+$)/, "");
    if (pathname.includes("pgrust-dynamic-workflow")) original = "/wp-content/uploads/2026/07/pgrust-dynamic-workflow-e1784182469629.jpg";
    const filename = localImages.get(original) || localImages.get(pathname);
    return filename ? `/blog/assets/${filename}` : url;
  });
  return output;
}

function normalizeCode(html) {
  return html
    .replace(/<pre class="EnlighterJSRAW" data-enlighter-language="([^"]+)"[^>]*>/g, '<pre data-language="$1"><code>')
    .replace(/<pre class="brush: sql;[^>]*>/g, '<pre data-language="sql"><code>')
    .replace(/<pre class="brush: plain;[^>]*>/g, '<pre data-language="rust"><code>')
    .replace(/<\/pre>/g, "</code></pre>");
}

function normalizeTables(html) {
  return html.replace(/<table[^>]*>[\s\S]*?<\/table>/g, (table, offset, whole) => {
    const before = whole.slice(Math.max(0, offset - 120), offset);
    if (before.includes('class="pgrust-benchmark-wrap"')) return table;
    return `<div class="table-scroll" tabindex="0" role="region" aria-label="Scrollable table">${table}</div>`;
  });
}

function cleanContent(post) {
  let html = post.content.rendered;
  html = html.replace(/^<style>[\s\S]*?<\/style>\s*/, "");
  html = html.replace(/<h1>\s*<\/h1>/g, "");
  html = localizePostLinks(html);
  html = localizeImages(html);
  html = normalizeCode(html);
  html = normalizeTables(html);
  return html;
}

function pageHead({ title, description, canonical, image, type = "website", published, modified }) {
  const jsonLd = type === "article" ? `
<script type="application/ld+json">${JSON.stringify({
    "@context": "https://schema.org",
    "@type": "Article",
    headline: title,
    description,
    image: `https://pgrust.com/blog/assets/${image}`,
    datePublished: published,
    dateModified: modified,
    author: { "@type": "Person", name: "Michael Malis" },
    publisher: { "@type": "Organization", name: "pgrust", url: "https://pgrust.com/" },
    mainEntityOfPage: canonical,
  })}</script>` : "";
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>${escapeHtml(title)}${type === "article" ? " | pgrust" : ""}</title>
<meta name="description" content="${escapeHtml(description)}">
<link rel="canonical" href="${canonical}">
<link rel="alternate" type="application/rss+xml" title="pgrust blog" href="https://pgrust.com/blog/feed.xml">
<link rel="icon" href="/favicon.ico" sizes="any">
<link rel="icon" type="image/svg+xml" href="/favicon.svg">
<link rel="icon" type="image/png" sizes="48x48" href="/favicon-48x48.png">
<link rel="apple-touch-icon" sizes="180x180" href="/apple-touch-icon.png">
<link rel="manifest" href="/site.webmanifest">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&amp;family=IBM+Plex+Sans:wght@400;500;600&amp;display=swap" rel="stylesheet">
<link rel="stylesheet" href="/blog/blog.css">
<meta property="og:type" content="${type}">
<meta property="og:site_name" content="pgrust">
<meta property="og:title" content="${escapeHtml(title)}">
<meta property="og:description" content="${escapeHtml(description)}">
<meta property="og:url" content="${canonical}">
<meta property="og:image" content="https://pgrust.com/blog/assets/${image}">
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="${escapeHtml(title)}">
<meta name="twitter:description" content="${escapeHtml(description)}">
<meta name="twitter:image" content="https://pgrust.com/blog/assets/${image}">${published ? `
<meta property="article:published_time" content="${published}">
<meta property="article:modified_time" content="${modified}">` : ""}${jsonLd}
</head>`;
}

function header() {
  return `<header class="site-header">
  <div class="site-header-inner">
    <a class="wordmark" href="/">pgrust</a>
    <nav class="site-nav" aria-label="Primary">
      <a href="/">Demo</a>
      <a href="/blog/" aria-current="page">Blog</a>
      <a href="https://github.com/malisper/pgrust">GitHub</a>
      <a class="nav-updates" href="/#updates">Updates</a>
    </nav>
  </div>
</header>`;
}

function footer() {
  return `<footer class="site-footer">Postgres, rewritten in Rust. Built by Michael Malis and Jason Seibel.</footer>
<script src="/blog/analytics.js"></script>`;
}

function postFooterCta() {
  return `<section class="post-cta" aria-label="Follow pgrust">
  <div class="post-cta-copy">
    <p class="post-cta-title">Follow the rewrite</p>
    <p>Star pgrust and keep up with new releases.</p>
  </div>
  <nav class="post-cta-actions" aria-label="pgrust community links">
    <a class="post-cta-primary" href="https://github.com/malisper/pgrust" target="_blank" rel="noopener">Star pgrust on GitHub <span aria-hidden="true">↗</span></a>
    <a class="post-cta-action" href="https://discord.gg/FZZ4dbdvwU" target="_blank" rel="noopener">Join Discord</a>
    <a class="post-cta-action" href="/#updates">Email updates</a>
  </nav>
  <p class="post-cta-social">Follow on X: <a href="https://x.com/pgrustdb" target="_blank" rel="noopener">pgrust</a><span aria-hidden="true"> · </span><a href="https://x.com/mmalisper" target="_blank" rel="noopener">Malis</a><span aria-hidden="true"> · </span><a href="https://x.com/JasonSeibel" target="_blank" rel="noopener">Jason</a></p>
</section>`;
}

function renderIndex() {
  const latest = posts[0];
  const items = posts.map((post) => {
    const meta = metadata[post.slug];
    return `<li class="post-list-item">
      <div>
        <time datetime="${post.date}">${formatDate(post.date)}</time>
        <h2><a href="/blog/${post.slug}/">${post.title.rendered}</a></h2>
        <p>${escapeHtml(meta.description)}</p>
      </div>
      <a href="/blog/${post.slug}/" tabindex="-1" aria-hidden="true"><img class="post-thumb" src="/blog/assets/${meta.image}" alt="" loading="lazy"></a>
    </li>`;
  }).join("\n");
  return `${pageHead({
    title: "pgrust blog",
    description: "Engineering notes from the team rebuilding PostgreSQL in Rust.",
    canonical: "https://pgrust.com/blog/",
    image: metadata[latest.slug].image,
  })}
<body>
${header()}
<main class="blog-index">
  <section class="blog-intro">
    <p class="eyebrow">Engineering notes</p>
    <h1>Inside the Postgres rewrite</h1>
    <p>Compatibility work, query-engine performance, JIT compilation, and what we learn rebuilding PostgreSQL in Rust.</p>
  </section>
  <ol class="post-list">${items}</ol>
</main>
${footer()}
</body>
</html>`;
}

function renderPost(post) {
  const meta = metadata[post.slug];
  const title = plainText(post.title.rendered);
  const canonical = `https://pgrust.com/blog/${post.slug}/`;
  const published = pacificDate(post.date);
  const modified = pacificDate(post.modified);
  return `${pageHead({ title, description: meta.description, canonical, image: meta.image, type: "article", published, modified })}
<body>
${header()}
<main class="article-shell">
  <article>
    <header class="article-header">
      <p class="eyebrow">pgrust engineering</p>
      <h1>${post.title.rendered}</h1>
      <p class="article-dek">${escapeHtml(meta.description)}</p>
      <p class="article-meta">Michael Malis · <time datetime="${published}">${formatDate(post.date)}</time></p>
    </header>
    <div class="article-body">${cleanContent(post)}</div>
    <footer class="article-footer">
      ${postFooterCta()}
      <p class="article-back"><a href="/blog/">← All posts</a></p>
    </footer>
  </article>
</main>
${footer()}
</body>
</html>`;
}

function renderFeed() {
  const items = posts.map((post) => {
    const meta = metadata[post.slug];
    const url = `https://pgrust.com/blog/${post.slug}/`;
    return `<item>
      <title>${escapeHtml(plainText(post.title.rendered))}</title>
      <link>${url}</link>
      <guid isPermaLink="true">${url}</guid>
      <pubDate>${new Date(pacificDate(post.date)).toUTCString()}</pubDate>
      <description>${escapeHtml(meta.description)}</description>
    </item>`;
  }).join("\n");
  return `<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:atom="http://www.w3.org/2005/Atom">
  <channel>
    <title>pgrust blog</title>
    <link>https://pgrust.com/blog/</link>
    <description>Engineering notes from the team rebuilding PostgreSQL in Rust.</description>
    <language>en-us</language>
    <atom:link href="https://pgrust.com/blog/feed.xml" rel="self" type="application/rss+xml" />
    ${items}
  </channel>
</rss>`;
}

function renderSitemap() {
  const urls = [
    ["https://pgrust.com/", null],
    ["https://pgrust.com/blog/", posts[0].modified],
    ...posts.map((post) => [`https://pgrust.com/blog/${post.slug}/`, post.modified]),
  ];
  const entries = urls.map(([url, modified]) => `  <url>\n    <loc>${url}</loc>${modified ? `\n    <lastmod>${modified.slice(0, 10)}</lastmod>` : ""}\n  </url>`).join("\n");
  return `<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n${entries}\n</urlset>`;
}

fs.writeFileSync(path.join(HERE, "index.html"), renderIndex());
fs.writeFileSync(path.join(HERE, "feed.xml"), renderFeed());
fs.writeFileSync(path.join(HERE, "..", "sitemap.xml"), renderSitemap());
for (const post of posts) {
  const directory = path.join(HERE, post.slug);
  fs.mkdirSync(directory, { recursive: true });
  fs.writeFileSync(path.join(directory, "index.html"), renderPost(post));
}
console.log(`Built blog index, RSS feed, sitemap, and ${posts.length} posts.`);
