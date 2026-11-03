// data.zohelo.com settings for the accessibility check (a11y/a11y.mjs, the runner shared with zohelo and
// zohelo-bi; #2499). The portal build is served alone (SPA fallback): the R2 read API sits behind
// Cloudflare Access, so /api answers 503 and those responses are expected; the scan sees the app with
// its empty and error states. The DuckDB engine loads from jsDelivr as in production.
export default {
  // A11Y_ROOT: another build, e.g. one with the bundled DuckDB engines where jsDelivr is unreachable.
  root: process.env.A11Y_ROOT ?? "dist",
  spa: true,
  api: "/api/",
  ignoreResponses: ["/api/"],
  ready: "main",
  pages: [{ path: "/" }],
  // A new browser has no profile: create one so the scan sees the app (desktop: data explorer; mobile:
  // home), then wait until DuckDB has started.
  async prepare(page) {
    const create = page.getByRole("dialog").getByRole("button", { name: "Create Profile" });
    await create.waitFor({ timeout: 90_000 });
    await page.getByRole("dialog").getByLabel("Name").fill("a11y");
    await create.click();
    await create.waitFor({ state: "hidden", timeout: 30_000 });
    await page.waitForFunction(
      () => !document.body.innerText.includes("Initializing DuckDB"),
      null,
      {
        timeout: 90_000,
      }
    );
  },
};
