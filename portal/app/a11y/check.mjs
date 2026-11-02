// Accessibility check of data.zohelo.com (zohelo-data#2499, #2529): serves the portal build
// (dist, SPA fallback), runs axe-core (@axe-core/playwright, WCAG 2.0/2.1/2.2 A and AA rules) on the
// app at 1440 and 390 px, writes a job summary (counts by rule and page) and a JSON report, and fails
// on serious or critical violations that are not in the committed baseline (a11y/baseline.json).
// The R2 read API (/api, behind Cloudflare Access) is not served, so the scan sees the app shell and
// its empty and error states; the DuckDB engine loads from jsDelivr as in production.
//
//   node a11y/check.mjs [--root dist] [--origin http://...] [--report a11y-report.json] [--update-baseline]
//
// --update-baseline rewrites the baseline from today's serious and critical violations; a baseline
// entry is a page and a rule, and each one has a Bug that fixes it (the entry's `bug`).
import { AxeBuilder } from "@axe-core/playwright";
import {
  createReadStream,
  existsSync,
  readFileSync,
  statSync,
  writeFileSync,
  appendFileSync,
} from "node:fs";
import { createServer } from "node:http";
import { extname, join, normalize, resolve } from "node:path";
import { chromium } from "playwright";
import console from "node:console";
import process from "node:process";
import { URL } from "node:url";

const PAGES = ["/"];
// A new browser has no profile: create one so the scan sees the app (desktop: data explorer; mobile:
// home), then wait until DuckDB has started.
async function prepare(page) {
  const create = page.getByRole("dialog").getByRole("button", { name: "Create Profile" });
  await create.waitFor({ timeout: 90_000 });
  await page.getByRole("dialog").getByLabel("Name").fill("a11y");
  await create.click();
  await create.waitFor({ state: "hidden", timeout: 30_000 });
  await page.waitForFunction(() => !document.body.innerText.includes("Initializing DuckDB"), null, {
    timeout: 90_000,
  });
  await page.waitForLoadState("networkidle");
}
const WIDTHS = [1440, 390];
const TAGS = ["wcag2a", "wcag2aa", "wcag21a", "wcag21aa", "wcag22aa"];
const BLOCKING = new Set(["serious", "critical"]);
const BASELINE = new URL("./baseline.json", import.meta.url);

const args = process.argv.slice(2);
const opt = (name, fallback) => {
  const i = args.indexOf(name);
  return i >= 0 && args[i + 1] ? args[i + 1] : fallback;
};
const root = resolve(opt("--root", "dist"));
const originArg = opt("--origin", "");
const reportPath = opt("--report", "a11y-report.json");
const updateBaseline = args.includes("--update-baseline");

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
};

/** Static file for a URL path: the file, <path>/index.html or <path>.html; null when none. */
function fileFor(urlPath) {
  const clean = normalize(decodeURIComponent(urlPath.split("?")[0])).replace(/^(\.\.[/\\])+/, "");
  const base = join(root, clean);
  if (!base.startsWith(root)) return null;
  for (const candidate of [base, join(base, "index.html"), `${base}.html`]) {
    if (existsSync(candidate) && statSync(candidate).isFile()) return candidate;
  }
  return null;
}

function serve() {
  const server = createServer((req, res) => {
    if ((req.url ?? "").startsWith("/api/")) {
      res
        .writeHead(503, { "content-type": "application/json" })
        .end('{"error":"API not served in the accessibility check"}');
      return;
    }
    // Single-page app: unknown paths get index.html (the app shows its own not-found page).
    const path =
      fileFor(req.url ?? "/") ??
      (existsSync(join(root, "index.html")) ? join(root, "index.html") : null);
    const status = 200;
    if (!path) {
      res.writeHead(404).end("not found");
      return;
    }
    res.writeHead(status, { "content-type": TYPES[extname(path)] ?? "application/octet-stream" });
    createReadStream(path).pipe(res);
  });
  return new Promise((ok) => server.listen(0, "127.0.0.1", () => ok(server)));
}

function readBaseline() {
  if (!existsSync(BASELINE)) return [];
  return JSON.parse(readFileSync(BASELINE, "utf8")).known ?? [];
}

async function scan(origin) {
  const browser = await chromium.launch();
  const results = [];
  const errors = [];
  try {
    for (const width of WIDTHS) {
      const context = await browser.newContext({
        viewport: { width, height: 900 },
        reducedMotion: "reduce",
      });
      for (const path of PAGES) {
        const page = await context.newPage();
        try {
          const response = await page.goto(origin + path, {
            waitUntil: "networkidle",
            timeout: 30_000,
          });
          if (!response || response.status() !== 200) {
            errors.push(
              `${path} @${width}px: HTTP ${response ? response.status() : "no response"}`
            );
            continue;
          }
          await prepare(page);
          if (process.env.A11Y_DEBUG) console.log((await page.innerText("body")).slice(0, 300));
          const axe = await new AxeBuilder({ page }).withTags(TAGS).analyze();
          for (const v of axe.violations) {
            results.push({
              page: path,
              width,
              rule: v.id,
              impact: v.impact ?? "unknown",
              nodes: v.nodes.length,
              help: v.help,
              helpUrl: v.helpUrl,
              targets: v.nodes.slice(0, 10).map((n) => n.target.join(" ")),
            });
          }
        } catch (err) {
          errors.push(
            `${path} @${width}px: ${err instanceof Error ? err.message.split("\n")[0] : String(err)}`
          );
        } finally {
          await page.close();
        }
      }
      await context.close();
    }
  } finally {
    await browser.close();
  }
  return { results, errors };
}

function summarize(results, errors, known) {
  const isKnown = (v) => known.some((k) => k.page === v.page && k.rule === v.rule);
  const blocking = results.filter((v) => BLOCKING.has(v.impact));
  const fresh = blocking.filter((v) => !isKnown(v));
  const seen = new Set(blocking.map((v) => `${v.page} ${v.rule}`));
  const fixed = known.filter((k) => !seen.has(`${k.page} ${k.rule}`));

  const lines = ["## Accessibility (axe-core)", ""];
  lines.push(
    `${PAGES.length} pages × ${WIDTHS.map((w) => `${w} px`).join(", ")}; rules: ${TAGS.join(", ")}. ` +
      `Blocking: serious and critical not in the baseline.`,
    "",
    `**New blocking: ${fresh.length}** · known (baseline): ${blocking.length - fresh.length} · ` +
      `moderate/minor (reported only): ${results.length - blocking.length} · page errors: ${errors.length}`,
    ""
  );
  const byRule = new Map();
  for (const v of results) {
    const row = byRule.get(v.rule) ?? { impact: v.impact, pages: new Map(), nodes: 0 };
    row.nodes += v.nodes;
    row.pages.set(v.page, (row.pages.get(v.page) ?? 0) + v.nodes);
    byRule.set(v.rule, row);
  }
  if (byRule.size) {
    lines.push(
      "### By rule",
      "",
      "| rule | impact | elements (both widths) | pages |",
      "|---|---|---|---|"
    );
    for (const [rule, row] of [...byRule].sort((a, b) => b[1].nodes - a[1].nodes)) {
      lines.push(`| ${rule} | ${row.impact} | ${row.nodes} | ${row.pages.size} |`);
    }
    lines.push(
      "",
      "### By page",
      "",
      "| page | width | rule | impact | elements | status |",
      "|---|---|---|---|---|---|"
    );
    for (const v of [...results].sort(
      (a, b) => a.page.localeCompare(b.page) || b.width - a.width
    )) {
      const status = !BLOCKING.has(v.impact) ? "reported" : isKnown(v) ? "baseline" : "**new**";
      lines.push(
        `| \`${v.page}\` | ${v.width} | ${v.rule} | ${v.impact} | ${v.nodes} | ${status} |`
      );
    }
    lines.push("");
  } else {
    lines.push("No violations.", "");
  }
  if (fixed.length) {
    lines.push("### Baseline entries no longer seen (remove them from the baseline)", "");
    for (const k of fixed) lines.push(`- \`${k.page}\` ${k.rule}${k.bug ? ` (${k.bug})` : ""}`);
    lines.push("");
  }
  if (errors.length) {
    lines.push("### Page errors", "");
    for (const e of errors) lines.push(`- ${e}`);
    lines.push("");
  }
  return { fresh, fixed, blocking, text: lines.join("\n") };
}

const server = originArg ? null : await serve();
const origin = originArg
  ? originArg.replace(/\/$/, "")
  : `http://127.0.0.1:${server.address().port}`;
let outcome;
try {
  const { results, errors } = await scan(origin);
  const known = readBaseline();
  outcome = summarize(results, errors, known);
  writeFileSync(
    reportPath,
    JSON.stringify(
      { pages: PAGES, widths: WIDTHS, tags: TAGS, errors, violations: results },
      null,
      2
    ) + "\n"
  );
  if (updateBaseline) {
    const pairs = new Map();
    for (const v of outcome.blocking) {
      const prev = known.find((k) => k.page === v.page && k.rule === v.rule);
      pairs.set(`${v.page} ${v.rule}`, {
        page: v.page,
        rule: v.rule,
        impact: v.impact,
        bug: prev?.bug ?? "",
      });
    }
    const list = [...pairs.values()].sort(
      (a, b) => a.page.localeCompare(b.page) || a.rule.localeCompare(b.rule)
    );
    writeFileSync(BASELINE, JSON.stringify({ known: list }, null, 2) + "\n");
    console.log(`baseline: ${list.length} entries written`);
  }
  console.log(outcome.text);
  if (process.env.GITHUB_STEP_SUMMARY)
    appendFileSync(process.env.GITHUB_STEP_SUMMARY, outcome.text + "\n");
  for (const v of outcome.fresh) {
    console.log(
      `::error title=a11y ${v.rule}::${v.page} at ${v.width}px: ${v.help} (${v.nodes} elements) ${v.helpUrl}`
    );
  }
  for (const e of errors) console.log(`::error title=a11y page::${e}`);
  for (const k of outcome.fixed)
    console.log(`::notice title=a11y fixed::${k.page} ${k.rule}: remove it from the baseline`);
  if (!updateBaseline && (outcome.fresh.length || errors.length)) process.exitCode = 1;
} finally {
  server?.close();
}
