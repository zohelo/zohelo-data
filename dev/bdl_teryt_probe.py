"""Read-only probe of the BDL website territories page for one subgroup (default P3442):
territorial layouts, list sizes, visible buttons, and whether a background package
("Pobierz") is offered there. Then one export of the first CHUNK units of the first
layout through the result table, downloaded to the runner only (nothing written to R2)."""

from pathlib import Path
import subprocess
import sys
import tempfile
import traceback

subprocess.run([sys.executable, "-m", "pip", "install", "-q", "playwright==1.56.0"], check=True)

from ingestion.common.r2 import req  # noqa: E402
from ingestion.common.report import annotate  # noqa: E402
from ingestion.to_landing.gus_bdl import web_export as w  # noqa: E402

SID = sys.argv[1] if len(sys.argv) > 1 else "P3442"
CHUNK = int(sys.argv[2]) if len(sys.argv) > 2 else 250

LAYOUTS_JS = """() => {
  const visible = el => !!(el.offsetWidth || el.offsetHeight || el.getClientRects().length);
  const out = [];
  for (const el of document.querySelectorAll('select:not([multiple]), .RadComboBox[id]')) {
    if (!visible(el) || !el.id) continue;
    if (el.tagName === 'SELECT')
      out.push({id: el.id, kind: 'select', options: [...el.options].map(o => o.text.trim())});
    else { const b = window.$find?.(el.id); if (!b?.get_items) continue; const it = b.get_items();
      out.push({id: el.id, kind: 'combobox', selected: b.get_text?.(),
        options: Array.from({length: it.get_count()}, (_, i) => it.getItem(i).get_text().trim())}); }
  }
  return out;
}"""
LISTS_JS = """() => [...document.querySelectorAll('.RadListBox[id]')].map(el => {
  const b = window.$find?.(el.id); const it = b?.get_items?.();
  return {id: el.id, count: it ? it.get_count() : null,
          first: it && it.get_count() ? [it.getItem(0).get_text(), it.getItem(0).get_value()] : null};
})"""
BUTTONS_JS = """() => [...document.querySelectorAll('button, input[type=submit], a[id]')]
  .filter(e => e.offsetWidth || e.offsetHeight)
  .map(e => [e.id, (e.title || e.value || e.textContent || '').trim().slice(0, 40), String(e.className).slice(0, 40)])
  .slice(0, 60)"""

MENU_JS = """() => [...document.querySelectorAll('li, a, span')]
  .filter(e => (e.offsetWidth || e.offsetHeight) && /csv|xls|eksport|export|pobierz/i.test(e.textContent || ''))
  .map(e => [e.tagName, e.id, (e.textContent || '').trim().replace(/\\s+/g, ' ').slice(0, 60)]).slice(0, 20)"""

out = {"subgroup": SID}
try:
    from playwright.sync_api import sync_playwright

    w.ensure_browser()
    with sync_playwright() as pw, tempfile.TemporaryDirectory() as tmp:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_context(accept_downloads=True, locale="pl-PL").new_page()
        w.login(page, req("GUS_BDL_WEB_EMAIL"), req("GUS_BDL_WEB_PASSWORD"))
        api = w.Bdl()
        subject = api.get(f"/subjects/{SID}", lang="pl")
        group = subject["parentId"]
        category = api.get(f"/subjects/{group}", lang="pl")["parentId"]
        url = f"{w.WEB}/dane/podgrup/wymiary/{category[1:]}/{group[1:]}/{SID[1:]}"
        page.goto(url, wait_until="domcontentloaded", timeout=60_000)
        w.settle(page)
        for _ in range(20):
            ids = page.locator('[id$="_SelectAll"]').evaluate_all(
                "els => els.filter(e => (e.offsetWidth || e.offsetHeight) && !e.disabled).map(e => e.id)"
            )
            todo = sorted(set(ids) - set(out.get("clicked", [])))
            if not todo:
                break
            page.locator(f"#{todo[0]}").click()
            out.setdefault("clicked", []).append(todo[0])
            w.settle(page)
        out["information"] = w.information_count(page)
        out["dims_download_button"] = w.download_button(page) is not None
        w.enabled_next(page).click()
        page.wait_for_url(w.re.compile(r"/bdl/dane/podgrup/teryt"), timeout=30_000)
        w.settle(page)
        out["teryt_layouts"] = page.evaluate(LAYOUTS_JS)
        out["teryt_lists"] = page.evaluate(LISTS_JS)
        out["teryt_buttons"] = page.evaluate(BUTTONS_JS)
        out["teryt_download_button"] = w.download_button(page) is not None
        text = page.locator("body").inner_text()
        out["teryt_text"] = " ".join(text.split())[:200]
        out.pop("teryt_buttons")
        source = out["teryt_lists"][0]["id"]
        dest = out["teryt_lists"][1]["id"]
        page.evaluate(
            """([id, n]) => { const b = window.$find(id); b.clearSelection();
            const it = b.get_items();
            for (let i = 0; i < Math.min(n, it.get_count()); i++) it.getItem(i).select(); }""",
            [source, CHUNK],
        )
        page.locator("button.rlbTransferFrom:visible").first.click()
        w.settle(page)
        out["selected_after_transfer"] = page.evaluate(f"() => window.$find('{dest}').get_items().get_count()")
        out["teryt_download_button_after"] = w.download_button(page) is not None
        w.enabled_next(page).click()
        w.wait_for_result_table(page)
        out["table_url"] = page.url[-80:]
        out["table_text"] = " ".join(page.locator("body").inner_text().split())[:400]
        option = page.get_by_text(w.re.compile(r"CSV\s*[–-]\s*(tablica\s+)?relacyj", w.re.IGNORECASE)).first
        out["csv_option_visible_before"] = option.is_visible()
        if not option.is_visible():
            page.get_by_text(w.re.compile(r"^(Eksport|Export)$", w.re.IGNORECASE)).first.click()
            page.wait_for_timeout(1500)
        out["menu"] = page.evaluate(MENU_JS)
        out["csv_option_visible"] = option.is_visible()
        try:
            with page.expect_download(timeout=240_000) as info:
                option.click(no_wait_after=True)
        except Exception as exc:
            out["download_error"] = str(exc)[:300]
            out["after_url"] = page.url[-80:]
            out["after_text"] = " ".join(page.locator("body").inner_text().split())[:600]
            out["exports"] = [[e["ready"], e["text"][:30], e["row"][:150]] for e in w.exports(page)[:5]]
            raise
        path = Path(tmp) / "x.zip"
        info.value.save_as(path)
        import zipfile

        with zipfile.ZipFile(path) as z:
            out["zip"] = {
                "name": info.value.suggested_filename,
                "bytes": path.stat().st_size,
                "members": [(m.filename, m.file_size) for m in z.infolist()][:5],
            }
            first = z.infolist()[0]
            lines = z.read(first).decode("utf-8", "replace").splitlines()
            out["zip"]["rows"] = len(lines)
            out["zip"]["head"] = lines[:3]
        browser.close()
    annotate("notice", "bdl teryt probe", out)
except Exception:
    out["traceback"] = traceback.format_exc()[-600:]
    annotate("error", "bdl teryt probe", out)
    raise
