"""landing.extract "web packages" for GUS BDL: whole-subgroup packages from the website,
requested in one run and collected in a later one (owner decision 22 Oct 2026, #889).

The website builds the package of a subgroup with more than 3,500 values (variables ×
years) in the background, often over hours (P2017: ~3 h; P1875: overnight; test 21–22 Oct).
The account's package list is paged and each entry is either pending or downloadable. So a
run does two things, with one browser and one session:

1. collect: page through the whole list; download every ready package; the subgroup is read
   from the ZIP name (<area>_<number>_<kind>_<yyyymmddhhmmss>.zip); a package of a subgroup
   still missing from Landing, built after the subgroup's last update, is landed like any
   website export (web_export.land);
2. request: for every big subgroup still missing that has no package listed from the last
   REQUEST_AGAIN_DAYS days, select everything and ask for its package (once; a repeated
   request of one subgroup stays pending, 21 Oct).

Small subgroups (up to 3,500 values) have no package; they are not touched here.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
import math
from pathlib import Path
import re
import tempfile
import time
from typing import Any
from zoneinfo import ZoneInfo

from ingestion.common.r2 import req, s3
from ingestion.common.report import annotate, step_summary
from ingestion.to_landing.gus_bdl import web_export as w
from ingestion.to_landing.gus_bdl.api import Bdl
from ingestion.to_landing.gus_bdl.inspect import landing_versions

REQUEST_AGAIN_DAYS = 3
MAX_REQUESTS = 60  # per run; the site builds them over hours
ZIP_RE = re.compile(r"_(\d+)_[A-Z]+_(\d{14})\.zip$", re.IGNORECASE)
WARSAW = ZoneInfo("Europe/Warsaw")
ROWS_JS = """() => [...document.querySelectorAll(
    '[id*="DownloadedSubgroups_FilesList"] tr.rgRow, [id*="DownloadedSubgroups_FilesList"] tr.rgAltRow')]
  .map(tr => { const a = tr.querySelector('a[id$="_Export"]');
    return {id: a?.id || '', name: (a?.textContent || '').trim().replace(/\\s+/g, ' '),
            date: (tr.cells[1]?.innerText || '').trim(), ready: !!a && !/aspNetDisabled/.test(a.className)}; })"""
PAGER = '[id*="DownloadedSubgroups_FilesList"] .rgNumPart a'


def normal(name: str) -> str:
    return " ".join(name.split()).casefold()


def pages(page):
    """Yield each page of the account's package list (rows as dicts), the browser on that page."""
    w.exports(page)  # opens the start page with the list
    yield page.evaluate(ROWS_JS)
    for n in range(2, 500):
        w.check_time()
        link = page.locator(PAGER).filter(has_text=re.compile(rf"^{n}$"))
        if not link.count():
            more = page.locator(PAGER).filter(has_text="...")
            if not more.count():
                return
            more.last.evaluate("e => e.click()")  # the pager sits in a collapsed sidebar
            w.settle(page)
            link = page.locator(PAGER).filter(has_text=re.compile(rf"^{n}$"))
            if not link.count():
                return
        link.first.evaluate("e => e.click()")
        w.settle(page)
        yield page.evaluate(ROWS_JS)


def big_missing(api: Bdl, held: dict[str, str]) -> list[dict[str, Any]]:
    """Missing subgroups with more than MAX_INFORMATION values (they offer a package)."""
    out = []
    for subgroup in w.missing_subgroups(api, held):
        detail = api.get(f"/subjects/{subgroup['id']}", lang="pl")
        variables = api.get("/variables", **{"subject-id": subgroup["id"], "page-size": 1}).get("totalRecords") or 0
        values = variables * len(detail.get("years") or [])
        if values > w.MAX_INFORMATION:
            out.append({**subgroup, "subject": detail, "values": values})
    return out


def built_at(zip_name: str) -> datetime | None:
    match = ZIP_RE.search(zip_name)
    return datetime.strptime(match.group(2), "%Y%m%d%H%M%S").replace(tzinfo=WARSAW) if match else None


def collect(page, client, bucket, api, wanted: dict[str, dict], tmp: Path, summary: dict) -> set[str]:
    """Download every ready package on every page; land those of wanted subgroups. Returns
    the normalised names of packages listed in the last REQUEST_AGAIN_DAYS days."""
    recent: set[str] = set()
    since = (datetime.now(UTC) - timedelta(days=REQUEST_AGAIN_DAYS)).date().isoformat()
    by_name = {normal(s["name"]): sid for sid, s in wanted.items()}
    for rows in pages(page):
        for row in rows:
            if row["date"] >= since:
                recent.add(normal(row["name"]))
            sid = by_name.get(normal(row["name"]))
            if not row["ready"] or sid is None or sid in summary["landed_ids"]:
                continue  # only packages of subgroups still missing are downloaded
            subgroup = wanted[sid]
            try:
                with page.expect_download(timeout=600_000) as info:
                    page.locator(f"#{row['id']}").evaluate("e => e.click()")
                download = info.value
                name = download.suggested_filename
                match = ZIP_RE.search(name)
                if not match or f"P{match.group(1)}" != sid:
                    summary["skipped"].append([row["name"][:40], name, "ZIP name does not match"])
                    continue
                version = api.last_update(sid, subgroup["subject"])
                made = built_at(name)
                if version and made and made < version:
                    summary["skipped"].append([sid, name, "built before the subgroup's last update"])
                    continue
                path = tmp / f"{sid}.zip"
                download.save_as(path)
                if path.read_bytes()[:2] != b"PK":
                    raise RuntimeError("not a ZIP archive")
                megabytes = round(path.stat().st_size / 1e6, 1)
                result = {"path": path, "export_file_name": name, "information": subgroup["values"]}
                whole = {"kind": "download", "layout": None, "territories": ["all"], "dimensions": {}}
                w.land(client, bucket, subgroup, version, result, 1, 1, whole)
                summary["landed"].append([sid, megabytes, name])
                summary["landed_ids"].add(sid)
            except w.OutOfTime:
                raise
            except Exception as exc:  # one package's failure is recorded; the run goes on
                summary["failed"].append([sid, f"{type(exc).__name__}: {str(exc)[:160]}"])
    return recent


def request(page, todo: list[dict], deadline: float, summary: dict) -> None:
    """Ask for the package of each subgroup (one request each)."""
    for subgroup in todo[:MAX_REQUESTS]:
        if time.monotonic() > deadline:
            break
        try:
            w.select_dimensions(page, subgroup, [])
            button = w.download_button(page)
            if button is None:
                summary["no_package_control"].append(subgroup["id"])
                continue
            with page.expect_response(lambda r: r.request.method == "POST", timeout=360_000) as response:
                button.click(no_wait_after=True)
            if not response.value.ok:
                raise RuntimeError(f"HTTP {response.value.status}")
            summary["requested"].append(subgroup["id"])
        except w.OutOfTime:
            raise
        except Exception as exc:
            summary["failed"].append([subgroup["id"], f"request: {type(exc).__name__}: {str(exc)[:160]}"])
        time.sleep(w.PAUSE_SECONDS)


def main(budget_minutes: float = 50) -> int:
    """Collect ready packages, then request missing ones. Exit 0; 2 when the budget ended the run early."""
    started = time.monotonic()
    deadline = started + budget_minutes * 60
    w.HARD_DEADLINE = deadline + w.HARD_MARGIN_SECONDS
    client, bucket = s3(), req("R2_LANDING_BUCKET")
    api = Bdl()
    wanted = {s["id"]: s for s in big_missing(api, landing_versions(client, bucket))}
    summary: dict[str, Any] = {
        "big_missing": len(wanted),
        "landed": [],
        "landed_ids": set(),
        "requested": [],
        "already_listed": 0,
        "no_package_control": [],
        "skipped": [],
        "failed": [],
        "unfinished": False,
    }
    w.ensure_browser()
    from playwright.sync_api import sync_playwright

    with sync_playwright() as pw, tempfile.TemporaryDirectory() as tmp:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_context(accept_downloads=True, locale="pl-PL").new_page()
        page.set_default_timeout(120_000)
        w.login(page, req("GUS_BDL_WEB_EMAIL"), req("GUS_BDL_WEB_PASSWORD"))
        try:
            recent = collect(page, client, bucket, api, wanted, Path(tmp), summary)
            todo = [s for sid, s in wanted.items() if sid not in summary["landed_ids"]]
            fresh = [s for s in todo if normal(s["name"]) not in recent]
            summary["already_listed"] = len(todo) - len(fresh)
            request(page, fresh, deadline, summary)
        except w.OutOfTime:
            summary["unfinished"] = True
        browser.close()
    summary["landed_ids"] = sorted(summary["landed_ids"])
    summary["still_missing"] = len(wanted) - len(summary["landed_ids"])
    summary["minutes"] = math.ceil((time.monotonic() - started) / 60)
    summary["api_requests"] = api.requests
    annotate("error" if summary["failed"] else "notice", "gus_bdl web packages", summary)
    step_summary(
        "## gus_bdl · web packages\n\n"
        + "\n".join(f"- {k}: {len(v) if isinstance(v, list) else v}" for k, v in summary.items())
        + "\n"
    )
    return 2 if summary["unfinished"] else 0
