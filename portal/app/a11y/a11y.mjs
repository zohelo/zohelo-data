// Accessibility check (zohelo-data#2499). Shared runner: byte-identical in zohelo (scripts/) and
// zohelo-bi (ops/); everything repo-specific is in a11y.config.mjs next to it, the known violations
// in a11y-baseline.json next to it.
// Config (default export): root (build dir), spa (unknown paths get index.html, else the nearest
// 404.html), api (prefix the static server answers 503), ignoreResponses (path prefixes whose >= 400
// responses are expected), ready (selector that must be visible), failText, failSelectors, widths,
// prepare(page) (run after load), pages: [{ path, name, status, ready, failText, widths, open }].
//
// Serves the build (or scans --origin), opens each configured page at each width, refuses to scan a
// page that did not load (document status, uncaught page errors, same-origin responses >= 400, the
// page's ready selector, error texts), runs axe-core (@axe-core/playwright) with the WCAG 2.0/2.1/2.2
// A and AA rules, writes a job summary (counts by rule and page) and a JSON report, and exits 1 on:
// a serious or critical violation whose element is not in the baseline, a page error, or a baseline
// entry without a Bug. Moderate and minor violations are reported only.
//
//   node <dir>/a11y.mjs [--origin URL] [--report a11y-report.json] [--update-baseline]
//
// A baseline entry is page + width + rule + the elements (axe targets) known to fail; an element not
// listed fails the check even when the rule is already in the baseline. --update-baseline rewrites
// the file from today's serious and critical violations (only when every page loaded) and keeps the
// Bug of an entry with the same page and rule; a new entry needs its Bug before the check passes.
import { AxeBuilder } from "@axe-core/playwright";
import { appendFileSync, createReadStream, existsSync, readFileSync, readdirSync, writeFileSync } from "node:fs";
import { createServer } from "node:http";
import { extname, join, relative, resolve, sep } from "node:path";
import console from "node:console";
import process from "node:process";
import { URL, fileURLToPath } from "node:url";
import { chromium } from "playwright";

const config = (await import(new URL("./a11y.config.mjs", import.meta.url).href)).default;
const BASELINE = fileURLToPath(new URL("./a11y-baseline.json", import.meta.url));
const TAGS = ["wcag2a", "wcag2aa", "wcag21a", "wcag21aa", "wcag22aa"];
const BLOCKING = new Set(["serious", "critical"]);
const WIDTHS = config.widths ?? [1440, 390];

const args = process.argv.slice(2);
const opt = (name, fallback) => {
  const i = args.indexOf(name);
  return i >= 0 && args[i + 1] ? args[i + 1] : fallback;
};
const reportPath = opt("--report", "a11y-report.json");
const updateBaseline = args.includes("--update-baseline");
const originArg = opt("--origin", "");
const root = resolve(config.root);

const TYPES = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript",
  ".mjs": "text/javascript",
  ".css": "text/css",
  ".json": "application/json",
  ".svg": "image/svg+xml",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".webp": "image/webp",
  ".woff2": "font/woff2",
  ".woff": "font/woff",
  ".ico": "image/x-icon",
  ".xml": "application/xml",
  ".wasm": "application/wasm",
  ".geojson": "application/geo+json",
  ".parquet": "application/octet-stream",
};

/**
 * Every file of the build by URL path, read once: requests are looked up in this map, so a request
 * path never reaches the file system (no traversal outside the build).
 */
const FILES = new Map(
  existsSync(root)
    ? readdirSync(root, { recursive: true, withFileTypes: true })
        .filter((e) => e.isFile())
        .map((e) => {
          const abs = join(e.parentPath, e.name);
          return ["/" + relative(root, abs).split(sep).join("/"), abs];
        })
    : [],
);

/** Static file for a URL path: the file, <path>/index.html or <path>.html; null when none. */
function fileFor(urlPath) {
  let decoded;
  try {
    decoded = decodeURIComponent(urlPath.split("?")[0]);
  } catch {
    return null;
  }
  const base = decoded.replace(/\/+$/, "");
  for (const candidate of [decoded, `${base}/index.html`, `${base}.html`]) {
    const file = FILES.get(candidate);
    if (file) return file;
  }
  return null;
}

/** The nearest 404.html up the URL path, as Cloudflare Pages serves it (/en/x gets en/404.html). */
function notFoundFor(urlPath) {
  const parts = urlPath.split("?")[0].split("/").filter(Boolean).slice(0, -1);
  for (let i = parts.length; i >= 0; i--) {
    const candidate = fileFor(["", ...parts.slice(0, i), "404.html"].join("/"));
    if (candidate) return candidate;
  }
  return join(root, "404.html");
}

/** Static server: files; unknown paths get index.html (config.spa) or the nearest 404.html with status 404. */
function serve() {
  const server = createServer((req, res) => {
    const url = req.url ?? "/";
    if (config.api && url.startsWith(config.api)) {
      res.writeHead(503, { "content-type": "application/json" }).end('{"error":"API not served"}');
      return;
    }
    let path = fileFor(url);
    let status = 200;
    if (!path) {
      path = config.spa ? join(root, "index.html") : notFoundFor(url);
      status = config.spa ? 200 : 404;
    }
    if (!path || !existsSync(path)) {
      res.writeHead(404).end("not found");
      return;
    }
    res.writeHead(status, { "content-type": TYPES[extname(path)] ?? "application/octet-stream" });
    createReadStream(path).pipe(res);
  });
  return new Promise((ok) => server.listen(0, "127.0.0.1", () => ok(server)));
}

const label = (p) => p.name ?? p.path;

/** Loads one page and returns the reason it is not fit to scan, or null. */
async function load(page, origin, p) {
  const problems = [];
  page.on("pageerror", (err) => problems.push(`uncaught error: ${err.message.split("\n")[0]}`));
  page.on("response", (r) => {
    const sameOrigin = r.url().startsWith(origin);
    const isDocument = r.request().resourceType() === "document" && r.frame() === page.mainFrame();
    const path = r.url().slice(origin.length);
    const expected = (config.ignoreResponses ?? []).some((prefix) => path.startsWith(prefix));
    if (sameOrigin && !isDocument && !expected && r.status() >= 400) problems.push(`HTTP ${r.status()} ${path}`);
  });
  const response = await page.goto(origin + p.path, { waitUntil: "networkidle", timeout: 60_000 });
  const expected = p.status ?? 200;
  if (!response || response.status() !== expected) {
    return `HTTP ${response ? response.status() : "no response"} (expected ${expected})`;
  }
  if (config.prepare) await config.prepare(page);
  await page
    .locator(p.ready ?? config.ready ?? "main")
    .first()
    .waitFor({ state: "visible", timeout: 30_000 });
  if (p.open) {
    await page.locator(p.open).first().click();
    await page.waitForTimeout(300);
  }
  await page.waitForLoadState("networkidle");
  const text = await page.locator("body").innerText();
  for (const bad of p.failText ?? config.failText ?? []) {
    if (text.includes(bad)) problems.push(`page shows "${bad}"`);
  }
  for (const selector of config.failSelectors ?? []) {
    if (await page.locator(selector).count()) problems.push(`page has ${selector}`);
  }
  return problems.length ? problems.join("; ") : null;
}

async function scan(origin) {
  const browser = await chromium.launch();
  const results = [];
  const errors = [];
  try {
    for (const width of WIDTHS) {
      for (const p of config.pages.filter((x) => !x.widths || x.widths.includes(width))) {
        // A fresh context per page: no state (storage, open menus) leaks between pages.
        const context = await browser.newContext({ viewport: { width, height: 900 }, reducedMotion: "reduce" });
        const page = await context.newPage();
        try {
          const problem = await load(page, origin, p);
          if (problem) {
            errors.push({ page: label(p), width, problem });
            continue;
          }
          const axe = await new AxeBuilder({ page }).withTags(TAGS).analyze();
          for (const v of axe.violations) {
            results.push({
              page: label(p),
              width,
              rule: v.id,
              impact: v.impact ?? "unknown",
              nodes: v.nodes.length,
              help: v.help,
              helpUrl: v.helpUrl,
              targets: [...new Set(v.nodes.map((n) => n.target.join(" ")))].sort(),
            });
          }
        } catch (err) {
          errors.push({ page: label(p), width, problem: err instanceof Error ? err.message.split("\n")[0] : String(err) });
        } finally {
          await context.close();
        }
      }
    }
  } finally {
    await browser.close();
  }
  return { results, errors };
}

const key = (x) => `${x.page}\u0000${x.width}\u0000${x.rule}`;

function compare(results, errors, known) {
  const entries = new Map(known.map((k) => [key(k), k]));
  const blocking = results.filter((v) => BLOCKING.has(v.impact));
  const fresh = [];
  for (const v of blocking) {
    const entry = entries.get(key(v));
    const unknown = v.targets.filter((t) => !entry?.targets?.includes(t));
    if (unknown.length) fresh.push({ ...v, unknown });
  }
  const errored = new Set(errors.map((e) => `${e.page}\u0000${e.width}`));
  const seen = new Map(blocking.map((v) => [key(v), v]));
  const fixed = [];
  for (const k of known) {
    if (errored.has(`${k.page}\u0000${k.width}`)) continue;
    const v = seen.get(key(k));
    const gone = (k.targets ?? []).filter((t) => !v?.targets.includes(t));
    if (gone.length) fixed.push({ ...k, gone });
  }
  const noBug = known.filter((k) => !k.bug);
  return { blocking, fresh, fixed, noBug };
}

function summary(results, errors, outcome) {
  const { blocking, fresh, fixed, noBug } = outcome;
  const pages = config.pages.length;
  const lines = [
    "## Accessibility (axe-core)",
    "",
    `${pages} pages at ${WIDTHS.map((w) => `${w} px`).join(" and ")}; rules ${TAGS.join(", ")}. ` +
      "Blocking: serious and critical elements not in the baseline, page errors, baseline entries without a Bug.",
    "",
    `**New blocking elements: ${fresh.reduce((n, v) => n + v.unknown.length, 0)}** · page errors: ${errors.length} · ` +
      `known serious/critical violations: ${blocking.length - fresh.length} · moderate/minor (reported only): ` +
      `${results.length - blocking.length}`,
    "",
  ];
  if (results.length) {
    const byRule = new Map();
    for (const v of results) {
      const row = byRule.get(v.rule) ?? { impact: v.impact, nodes: 0, pages: new Set() };
      row.nodes += v.nodes;
      row.pages.add(v.page);
      byRule.set(v.rule, row);
    }
    lines.push("### By rule", "", "| rule | impact | elements (all widths) | pages |", "|---|---|---|---|");
    for (const [rule, row] of [...byRule].sort((a, b) => b[1].nodes - a[1].nodes)) {
      lines.push(`| ${rule} | ${row.impact} | ${row.nodes} | ${row.pages.size} |`);
    }
    const isFresh = new Map(fresh.map((v) => [key(v), v.unknown.length]));
    lines.push("", "### By page", "", "| page | width | rule | impact | elements | status |", "|---|---|---|---|---|---|");
    for (const v of [...results].sort((a, b) => a.page.localeCompare(b.page) || b.width - a.width || a.rule.localeCompare(b.rule))) {
      const n = isFresh.get(key(v));
      const status = !BLOCKING.has(v.impact) ? "reported" : n ? `**${n} new**` : "baseline";
      lines.push(`| \`${v.page}\` | ${v.width} | ${v.rule} | ${v.impact} | ${v.nodes} | ${status} |`);
    }
    lines.push("");
  } else {
    lines.push("No violations.", "");
  }
  if (errors.length) {
    lines.push("### Page errors (not scanned)", "");
    for (const e of errors) lines.push(`- \`${e.page}\` at ${e.width} px: ${e.problem}`);
    lines.push("");
  }
  if (noBug.length) {
    lines.push("### Baseline entries without a Bug", "");
    for (const k of noBug) lines.push(`- \`${k.page}\` ${k.width} px ${k.rule}`);
    lines.push("");
  }
  if (fixed.length) {
    lines.push("### Baseline elements no longer failing (remove them from the baseline)", "");
    for (const k of fixed) lines.push(`- \`${k.page}\` ${k.width} px ${k.rule} (${k.bug || "no Bug"}): ${k.gone.length} elements`);
    lines.push("");
  }
  return lines.join("\n");
}

function writeBaseline(blocking, known) {
  const bugOf = (v) => known.find((k) => k.page === v.page && k.rule === v.rule && k.bug)?.bug ?? "";
  const list = blocking
    .map((v) => ({ page: v.page, width: v.width, rule: v.rule, impact: v.impact, bug: bugOf(v), targets: v.targets }))
    .sort((a, b) => a.page.localeCompare(b.page) || b.width - a.width || a.rule.localeCompare(b.rule));
  writeFileSync(BASELINE, JSON.stringify({ known: list }, null, 2) + "\n");
  return list.length;
}

/**
 * The new elements as baseline entries in at most 8 notice annotations of about 3.5 kB (sessions can
 * read annotations but not artifacts), so a baseline can be made from a CI-only scan.
 */
function candidates(fresh) {
  const rows = fresh.map((v) => ({ page: v.page, width: v.width, rule: v.rule, impact: v.impact, targets: v.unknown }));
  const chunks = [];
  let current = [];
  for (const row of rows) {
    if (current.length && JSON.stringify([...current, row]).length > 3500) {
      chunks.push(current);
      current = [];
    }
    current.push(row);
  }
  if (current.length) chunks.push(current);
  const shown = chunks.slice(0, 8);
  shown.forEach((chunk, i) => {
    const data = JSON.stringify(chunk).replace(/%/g, "%25").replace(/\r/g, "%0D").replace(/\n/g, "%0A");
    console.log(`::notice title=a11y baseline candidates ${i + 1}/${chunks.length}::${data}`);
  });
}

const server = originArg ? null : await serve();
const origin = originArg ? originArg.replace(/\/$/, "") : `http://127.0.0.1:${server.address().port}`;
try {
  const { results, errors } = await scan(origin);
  const known = existsSync(BASELINE) ? (JSON.parse(readFileSync(BASELINE, "utf8")).known ?? []) : [];
  const outcome = compare(results, errors, known);
  writeFileSync(
    reportPath,
    JSON.stringify({ widths: WIDTHS, tags: TAGS, pages: config.pages, errors, violations: results }, null, 2) + "\n",
  );
  const text = summary(results, errors, outcome);
  console.log(text);
  if (process.env.GITHUB_STEP_SUMMARY) appendFileSync(process.env.GITHUB_STEP_SUMMARY, text + "\n");
  for (const v of outcome.fresh) {
    console.log(
      `::error title=a11y ${v.rule}::${v.page} at ${v.width}px: ${v.help}; new elements: ${v.unknown.slice(0, 5).join(", ")} ${v.helpUrl}`,
    );
  }
  for (const e of errors) console.log(`::error title=a11y page::${e.page} at ${e.width}px: ${e.problem}`);
  if (updateBaseline) {
    if (errors.length) {
      console.log("::error title=a11y baseline::not written: some pages did not load");
      process.exitCode = 1;
    } else {
      const n = writeBaseline(outcome.blocking, known);
      console.log(`baseline: ${n} entries written to ${BASELINE}; give every entry its Bug`);
    }
  } else {
    for (const k of outcome.noBug) console.log(`::error title=a11y baseline::${k.page} ${k.width}px ${k.rule} has no Bug`);
    for (const k of outcome.fixed) {
      console.log(
        `::notice title=a11y fixed::${k.page} ${k.width}px ${k.rule}: ${k.gone.length} elements no longer fail; remove them from the baseline`,
      );
    }
    if (outcome.fresh.length) candidates(outcome.fresh);
    if (outcome.fresh.length || errors.length || outcome.noBug.length) process.exitCode = 1;
  }
} finally {
  server?.close();
}
