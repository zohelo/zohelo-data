# D32 · Chart rendering: build-time SVG for static charts, vega only where the reader re-configures

- **Status:** accepted (2026-11-03, Architect; zohelo-data#2550). Narrows D25; amends D4 for the explorer's vega
  specs; condition (1) of D31.
- **Products:** zohelo.com (all charts, first), the share card, zohelo-bi (dashboard, KPI and embed charts in a
  later roll-out step; the explorer keeps vega). data.zohelo.com has no `@zohelo/design` charts today.

## Context

D25 (28 Oct) renders every `@zohelo/design` visual through vega (Vega, Vega-Lite, vega-embed) in a React Router
`.client` module after hydration, because vega's Node build needs the optional `canvas` package and broke
pre-rendering. D31 (direction C) then fixed what the public charts are: lines without smoothing or fill, a target
band as a shaded range, a target rule, direct labels and a labelled end point, ranking bars, small multiples,
sparklines and the voivodeship choropleth; a table behind every chart; SVG text at 13 px or more; a 1200 × 630 share
card per KPI × period × language with the chart in it. None of these is re-configured by the reader on zohelo.com:
the site is pre-rendered (`ssr: false`, `prerender()`) and its data is fixed at build time.

Measured on zohelo `main` (2026-11-03, `bun run build`, gzip -9):

| Asset | Raw | Gzip |
|---|---|---|
| Vega chunk (`Chart-*.js`, vega + vega-lite + vega-embed) | 854 kB | 293 kB |
| Client entry (`entry.client-*.js`) | 220 kB | 68 kB |
| Prototype (source in the PR, zohelo-data#2566): 120-point monthly line + NBP band + target rule + labelled end point, 640 × 320, rendered by React to SVG | 3.1 kB | 1.1 kB |
| The same with its 120-row table | 7.7 kB | 1.8 kB |

The vega chunk is the largest asset of the site and the home page loads it (zohelo.com view, G2). Today's chart
features that the reader uses: hover tooltips with values in the page's number format (`visuals/locale.ts`;
zohelo#423 (open) keeps the series' decimals in them) and region names on the map. The vega layer in
`packages/design/src/visuals/` is 572 lines including tests (specs, theme, locale, `Chart`; about 400 of source).

**zohelo-bi today** draws every chart (dashboards, `/kpi`, `/embed`, explorer) with vega through its own
`packages/visuals` (exports `Chart`, `barSpec`, `lineSpec`, `config`; used by
`apps/portal/src/components/SeriesChart.tsx`); neither its root nor `apps/portal` depends on `@zohelo/design`, and the explorer
is imported statically (`apps/portal/src/App.tsx`), so the vega code is part of the app bundle. D4 moved chart code
to zohelo first; bi has not consumed it yet.

## Decision

1. **Static charts are React components in `@zohelo/design` that return plain SVG**, rendered with the page: at
   pre-render on zohelo.com, so the chart is in the HTML before any script runs, and, once zohelo-bi consumes
   `@zohelo/design` (decision 6), as ordinary React on the client there. One component per D31 chart kind: `LineChart` (with optional `band`, `rule`, end label, small multiples),
   `Sparkline`, `RankingBars`, `Choropleth`. Scales, ticks, curves and the projection use the small d3 modules
   (`d3-scale`, `d3-shape`, `d3-array`, `d3-geo`, `d3-time-format`) — no hand-written scale or projection maths.
   Time scales and formats are UTC (`scaleUtc`, `utcFormat`): the build runs in UTC and readers in Warsaw, and
   local-time scales would give other ticks and a hydration mismatch. Because the site hydrates (`ssr: false` with
   `prerender()`), the components and the d3 modules they import are in the client route chunks — small, and
   measured by the zohelo.com page-weight step (view G2). If that budget needs it, paths and ticks are computed in the
   route loader at pre-render (and the map ships simplified geometry), which keeps d3 and geometry out of the client.
   Label placement measures text with font metrics read from the bundled font files, not the DOM or canvas.
   Number and date text comes from `format.ts` only.
2. **One rows input, three outputs.** Each component takes the same rows and options that its table takes; the table
   behind the chart (D31) is rendered from those rows by `DataTable`, and the share card renders the same component.
3. **Accessible by construction.** The `<svg>` has `role="img"` and an `aria-label` that states the takeaway values
   (latest value, period, target where there is one); bars, region paths and the labelled end point carry a
   `<title>` with label and value in the page's format (mouse hover without extra code). `<title>` gives nothing on
   touch or to the keyboard, and D31 lines have no per-point marks, so the table behind the chart is the accessible
   and touch fallback; text is at least 13 px and
   uses the D31 tokens (colours by meaning, U+2212, `2,9%` / `2.9%`).
4. **Share card at build.** zohelo.com's build writes each 1200 × 630 card as SVG from the card component and
   rasterises it to PNG with `@resvg/resvg-js` (WASM/native, no browser); resvg does not resolve CSS `var()`, so the
   card is rendered with literal token values; fonts are the bundled Newsreader, Plus
   Jakarta Sans and Inter files.
5. **Vega stays only where the reader re-configures a chart**: the zohelo-bi explorer (`packages/visuals`, pivot of
   any KPI × dimension). Target: loaded with a dynamic import when the explorer opens; today it is a static import
   (a gap for the zohelo-bi view, #2565). A rich interaction that a static chart
   cannot give (brushing, zoom) is a new decision, not a quiet re-introduction of vega on zohelo.com.
6. **Distribution (D3) and roll-out order.** The components ship in `@zohelo/design`; zohelo.com uses them first
   (D31 step 4: tokens → `design-v*` Release → components). zohelo-bi adopts `@zohelo/design` through the `design-v*`
   Release in a later step and then moves its dashboard, KPI and embed charts to the SVG components; until then its
   vega charts stay, and work on them (for example the open PR zohelo-bi#347, Polish number and month formats) is the right
   interim work. The vega spec code for the explorer lives in zohelo-bi's `packages/visuals` alone (amends D4 for
   these specs); `@zohelo/design`'s `./visuals` entry stays exported until no zohelo route imports it, then goes
   (expand/contract, D6).

What stays of D25: pages carry their numbers in HTML; any chart that needs vega renders client-only after hydration
and never in a route's initial scripts. What D25 no longer says: that every chart is vega.

## Consequences

- zohelo.com chart pages lose the 293 kB gzip vega chunk; a chart costs about 1–2 kB of HTML, plus the chart
  components and their d3 modules in the client chunks (not measured yet; the G2 step measures it, and the loader
  route above removes d3 from the client if needed). The zohelo.com
  page-weight budget (view G2: no-chart pages ≤ 200 kB gzip) can hold for chart pages too, and the "chart chunk on
  demand" check applies only to the explorer in zohelo-bi.
- Charts are in the pre-rendered HTML: visible without JavaScript, in search snippets and in the share card from the
  same code, so the chart, its table and its card cannot disagree.
- Line charts lose today's per-point hover values: `<title>` covers bars, regions and the end point on mouse only.
  The table carries every exact value and is the touch and keyboard route. If readers need a crosshair, a small progressive-enhancement island can add it later; it must stay under a budget set then.
- We own about four chart components and their tests (snapshot of the SVG for fixed rows, axis and label rules,
  `aria-label` text). The vega spec builders for these kinds are deleted after the roll-out; the explorer's stay.
- Step 4 of the design process (D31 condition 2) can name the chart components: `LineChart`, `Sparkline`,
  `RankingBars`, `Choropleth`, `ShareCard`.
- resvg-js reads TTF/OTF; the `@fontsource` packages ship woff/woff2, and Polish text needs latin-ext glyphs. The
  share-card Task confirms the font files (or converts them at build) before it relies on the bundled fonts.
- Build time grows with the number of share cards (KPIs × periods × 2 languages); resvg renders a card in
  milliseconds, so this is measured in the share-card Task, not expected to matter.

## Alternatives considered

- **Vega everywhere (D25 as is).** One engine, specs exist. Rejected: 293 kB gzip on every chart page for charts
  that never change after the build, and the share card would need a second renderer anyway.
- **Vega `toSVG()` at build.** Keeps the specs and gives static SVG. Rejected: it puts vega back into the
  pre-render path where D25 recorded the `canvas` breakage; without `canvas` text widths are estimated, which fights
  D31's direct labels at 13 px; and D31's band, rule and end-label layout are custom marks either way.
- **A lighter client library (Observable Plot, uPlot, Chart.js).** Smaller than vega but still client JavaScript
  for static pictures; Plot can render at build only through a DOM shim (jsdom) in the build. Not needed for four
  fixed chart kinds.
- **visx (React + d3 primitives, renders SVG on the server).** Closest to this decision. Not chosen: our components
  need only the d3 modules visx itself wraps, and visx adds its own packages and API on top of them; if the chart
  set grows beyond the D31 kinds, visx is the first thing to reconsider.
