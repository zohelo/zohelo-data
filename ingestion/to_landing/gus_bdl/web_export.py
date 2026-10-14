"""landing.extract (website channel) for GUS BDL: subgroups missing from Landing.

BDL has no bulk download; the website's subgroup export ("CSV – tablica relacyjna" ZIP)
is the agreed workaround (owner decision 13 Oct 2026). One browser, one route, one
subgroup at a time with a pause, no IP rotation; login with GUS_BDL_WEB_EMAIL /
GUS_BDL_WEB_PASSWORD. This module handles subgroups whose full selection is at most
MAX_INFORMATION values (one file); larger subgroups are reported as deferred until the
splitter (one file per dimension × territory part) exists.

Landing layout and receipt match the existing web exports:
  01_landing/gus_bdl/snapshot/data/<P####>/<version_token>__<ingested>.zip (+ .receipt.json)
The version token is the subgroup's lastUpdate from the BDL API.
"""

from __future__ import annotations

from datetime import UTC, datetime
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time
from typing import Any

from ingestion.common.r2 import req, s3
from ingestion.common.report import annotate, step_summary
from ingestion.to_landing.gus_bdl.api import Bdl, token
from ingestion.to_landing.gus_bdl.inspect import DATA, landing_versions

WEB = "https://bdl.stat.gov.pl/bdl"
MAX_INFORMATION = 3500  # the website refuses larger selections in one export
PAUSE_SECONDS = 20  # between subgroups
INFO_RE = re.compile(r"Wybrano\s+([0-9\s]+)\s+informacji", re.IGNORECASE)
EXPORT_NAME_RE = re.compile(r"_([0-9]+)_CREL_([0-9]{14})\.zip$", re.IGNORECASE)


def ensure_browser() -> None:
    """Install Chromium for Playwright on a fresh runner (no-op when present)."""
    subprocess.run([sys.executable, "-m", "playwright", "install", "chromium"], check=True)


def missing_subgroups(api: Bdl, held: dict[str, str]) -> list[dict[str, Any]]:
    """Subgroups with no complete Landing version, with their export URL (K/G/P numbers)."""
    parents: dict[str, str] = {}
    out = []
    for subject in api.subgroups():
        if subject["id"] in held:
            continue
        group = subject["parentId"]
        if group not in parents:
            parents[group] = api.get(f"/subjects/{group}", lang="pl")["parentId"]
        category = parents[group]
        out.append(
            {
                "id": subject["id"],
                "name": subject.get("name", ""),
                "subject": subject,
                "url": f"{WEB}/dane/podgrup/wymiary/{category[1:]}/{group[1:]}/{subject['id'][1:]}",
            }
        )
    return out


def information_count(page) -> int | None:
    match = INFO_RE.search(page.locator("body").inner_text())
    return int(match.group(1).replace(" ", "").replace("\xa0", "")) if match else None


SETTLED_JS = """() => {
    const m = window.Sys?.WebForms?.PageRequestManager?.getInstance?.();
    return !m?.get_isInAsyncPostBack?.();
}"""


def settle(page) -> None:
    """Wait until the page's ASP.NET partial postback has finished."""
    page.wait_for_load_state("domcontentloaded")
    page.wait_for_function(SETTLED_JS, timeout=60_000)
    page.wait_for_timeout(300)
    page.wait_for_function(SETTLED_JS, timeout=60_000)


def note(page, trace: list, step: str) -> None:
    """Record where the browser is (URL, selection count, start of the visible text)."""
    text = page.locator("body").inner_text()
    trace.append(
        {
            "step": step,
            "url": page.url[-90:],
            "information": information_count(page),
            "text": " ".join(text.split())[:160],
        }
    )


def wait_for_result_table(page) -> None:
    """The result table page: URL /podgrup/tablica, or a visible Export / relational CSV control."""
    for _ in range(120):
        if "/bdl/dane/podgrup/tablica" in page.url:
            return
        for pattern in (r"^(Eksport|Export)$", r"CSV\s*[–-]\s*(tablica\s+)?relacyj"):
            if page.get_by_text(re.compile(pattern, re.IGNORECASE)).first.is_visible():
                return
        page.wait_for_timeout(1000)
    raise RuntimeError(f"result table not ready; at {page.url}")


def enabled_next(page):
    """The 'Dalej' button once the page's postback has finished."""
    for _ in range(40):
        for control_id in ("ctl00_ContentPlaceHolder_dalej1", "ctl00_ContentPlaceHolder_dalej2"):
            control = page.locator(f"#{control_id}")
            if control.count() and control.is_visible() and control.is_enabled():
                return control
        page.wait_for_timeout(500)
    raise RuntimeError("no enabled 'Dalej' button after the BDL postback")


def login(page, email: str, password: str) -> None:
    page.goto(f"{WEB}/logowanie", wait_until="domcontentloaded", timeout=45_000)
    page.locator("#ctl00_ContentPlaceHolder_Email").fill(email)
    page.locator("#ctl00_ContentPlaceHolder_Password").fill(password)
    page.locator("#ctl00_ContentPlaceHolder_SignIn").click()
    page.wait_for_timeout(1800)
    text = page.locator("body").inner_text()
    if "Gość" in text or "Użytkownik:" not in text:
        raise RuntimeError("BDL website login failed")


def export(page, subgroup: dict[str, Any], workdir: Path, trace: list) -> dict[str, Any]:
    """Select everything in one subgroup and download the relational CSV ZIP.

    Returns {"status": "downloaded", "path", "export_file_name", "information"} or
    {"status": "too_large" | "no_data", "information"}."""
    page.goto(subgroup["url"], wait_until="domcontentloaded", timeout=60_000)
    settle(page)
    note(page, trace, "dimensions page")
    clicked: set[str] = set()
    for _ in range(20):  # every dimension's "select all", one postback at a time
        ids = page.locator('[id$="_SelectAll"]').evaluate_all(
            "els => els.filter(e => (e.offsetWidth || e.offsetHeight) && !e.disabled).map(e => e.id)"
        )
        todo = sorted(i for i in ids if i not in clicked)
        if not todo:
            break
        page.locator(f"#{todo[0]}").click()
        clicked.add(todo[0])
        settle(page)
    note(page, trace, f"selected all in {len(clicked)} dimensions")
    information = information_count(page)
    if not information:
        return {"status": "no_data", "information": information}
    if information > MAX_INFORMATION:
        return {"status": "too_large", "information": information}
    enabled_next(page).click()
    page.wait_for_url(re.compile(r"/bdl/dane/podgrup/teryt"), timeout=30_000)
    settle(page)
    note(page, trace, "territories page")
    page.locator("#ctl00_ContentPlaceHolder_terytList_MenuButton").click()
    page.locator("li.rmItem").filter(has_text="Zaznacz wszystkie").first.locator(".rmLink").click()
    settle(page)
    page.locator("button.rlbTransferAllFrom").first.click()
    settle(page)
    note(page, trace, "territories selected")
    enabled_next(page).click()
    wait_for_result_table(page)
    note(page, trace, "result table")
    option = page.get_by_text(re.compile(r"CSV\s*[–-]\s*(tablica\s+)?relacyj", re.IGNORECASE)).first
    if not option.is_visible():
        page.get_by_text(re.compile(r"^(Eksport|Export)$", re.IGNORECASE)).first.click()
        page.wait_for_timeout(500)
    with page.expect_download(timeout=180_000) as download_info:
        option.click()
    download = download_info.value
    name = download.suggested_filename
    match = EXPORT_NAME_RE.search(name)
    if not match or match.group(1) != subgroup["id"][1:].lstrip("0"):
        raise RuntimeError(f"export file name {name!r} does not identify {subgroup['id']}")
    path = workdir / f"{subgroup['id']}.zip"
    download.save_as(path)
    if path.read_bytes()[:2] != b"PK":
        raise RuntimeError(f"export of {subgroup['id']} is not a ZIP archive")
    return {"status": "downloaded", "path": path, "export_file_name": name, "information": information}


def land(client, bucket: str, subgroup: dict[str, Any], version: datetime | None, result: dict[str, Any]) -> str:
    """Write the ZIP and its receipt to Landing; returns the data key."""
    body = result["path"].read_bytes()
    digest = sha256(body).hexdigest()
    now = datetime.now(UTC)
    version_token = token(version)
    key = f"{DATA}{subgroup['id']}/{version_token}__{now.strftime('%Y%m%dT%H%M%SZ')}.zip"
    client.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/zip", Metadata={"sha256": digest})
    receipt = {
        "schema_version": 2,
        "kind": "gus_bdl_landing",
        "source_id": "gus_bdl",
        "area": "data",
        "channel": "web_export",
        "subgroup_id": subgroup["id"],
        "sha256": digest,
        "size_bytes": len(body),
        "object": {"bucket": bucket, "key": key},
        "source_version": version.isoformat() if version else None,
        "version_token": version_token,
        "ingested_at": now.isoformat().replace("+00:00", "Z"),
        "export_file_name": result["export_file_name"],
        "selected_information": result["information"],
        "source_url": subgroup["url"],
        "code_sha": os.environ.get("GITHUB_SHA", ""),
        "part": {"index": 1, "total": 1},
    }
    client.put_object(
        Bucket=bucket,
        Key=key[: -len(".zip")] + ".receipt.json",
        ContentType="application/json",
        Body=json.dumps(receipt, indent=2, sort_keys=True).encode(),
    )
    return key


def main(limit: int = 20, budget_minutes: float = 50, only: list[str] | None = None) -> int:
    """Export up to `limit` missing subgroups within the time budget; exit 2 when work remains."""
    from playwright.sync_api import sync_playwright

    deadline = time.monotonic() + budget_minutes * 60
    client, bucket = s3(), req("R2_LANDING_BUCKET")
    api = Bdl()
    todo = missing_subgroups(api, landing_versions(client, bucket))
    if only:
        todo = [s for s in todo if s["id"] in only]
    summary: dict[str, Any] = {"missing": len(todo), "landed": [], "too_large": [], "no_data": [], "failed": []}
    ensure_browser()
    with sync_playwright() as pw, tempfile.TemporaryDirectory() as tmp:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_context(accept_downloads=True, locale="pl-PL").new_page()
        login(page, req("GUS_BDL_WEB_EMAIL"), req("GUS_BDL_WEB_PASSWORD"))
        for subgroup in todo[:limit]:
            if time.monotonic() > deadline:
                break
            trace: list = []
            try:
                version = api.last_update(subgroup["id"], subgroup["subject"])
                result = export(page, subgroup, Path(tmp), trace)
                if result["status"] == "downloaded":
                    land(client, bucket, subgroup, version, result)
                    summary["landed"].append([subgroup["id"], result["information"]])
                else:
                    summary[result["status"]].append([subgroup["id"], result["information"], trace])
            except Exception as exc:  # one subgroup's failure is recorded; the run goes on
                summary["failed"].append([subgroup["id"], f"{type(exc).__name__}: {str(exc)[:200]}", trace])
            time.sleep(PAUSE_SECONDS)
        browser.close()
    summary["remaining"] = len(todo) - len(summary["landed"])
    annotate("error" if summary["failed"] else "notice", "gus_bdl web export", summary)
    step_summary(
        "## gus_bdl · web export\n\n"
        + "\n".join(f"- {k}: {len(v) if isinstance(v, list) else v}" for k, v in summary.items())
        + "\n"
    )
    if summary["failed"]:
        return 1
    return 2 if summary["remaining"] else 0
