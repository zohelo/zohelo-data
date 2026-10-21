"""landing.extract (website channel) for GUS BDL: subgroups missing from Landing.

BDL has no bulk download; the website's subgroup export ("CSV – tablica relacyjna" ZIP)
is the agreed workaround (owner decision 13 Oct 2026). One browser, one route, one
subgroup at a time with a pause, no IP rotation; login with GUS_BDL_WEB_EMAIL /
GUS_BDL_WEB_PASSWORD. Routes per subgroup (export() has the rule): the website's background
package of the whole subgroup above MAX_INFORMATION values; otherwise the result table of
all territories; when the site cannot build it, territory parts per layout (part i of n in
the receipt, landed as they download, so a run resumes). Above MAX_INFORMATION without the
package control the subgroup is reported as too large (a dimension split is not built).

Landing layout and receipt match the existing web exports:
  01_landing/gus_bdl/snapshot/data/<P####>/<version_token>__<ingested>[__part-NNN].zip (+ .receipt.json)
The version token is the subgroup's lastUpdate from the BDL API.
"""

from __future__ import annotations

from collections import Counter
import contextlib
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


def ensure_browser() -> None:
    """Install Chromium for Playwright on a fresh runner (no-op when present)."""
    subprocess.run([sys.executable, "-m", "playwright", "install", "chromium"], check=True)


def entry(api: Bdl, subject: dict[str, Any], parents: dict[str, str]) -> dict[str, Any]:
    """A subgroup with its export URL (K/G/P numbers)."""
    group = subject["parentId"]
    if group not in parents:
        parents[group] = api.get(f"/subjects/{group}", lang="pl")["parentId"]
    category = parents[group]
    return {
        "id": subject["id"],
        "name": subject.get("name", ""),
        "subject": subject,
        "url": f"{WEB}/dane/podgrup/wymiary/{category[1:]}/{group[1:]}/{subject['id'][1:]}",
    }


def missing_subgroups(api: Bdl, held: dict[str, str], only: list[str] | None = None) -> list[dict[str, Any]]:
    """Subgroups with no complete Landing version (only: just these, without listing every subgroup)."""
    parents: dict[str, str] = {}
    subjects = [api.get(f"/subjects/{sid}", lang="pl") for sid in only] if only else api.subgroups()
    return [entry(api, subject, parents) for subject in subjects if subject["id"] not in held]


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
    """Until the result table shows its Export control (a table URL alone is not ready)."""
    from playwright.sync_api import TimeoutError as PlaywrightTimeout

    for _ in range(180):
        if "/bdl/errors/" in page.url:
            raise RuntimeError(f"BDL error page: {page.url}")
        with contextlib.suppress(PlaywrightTimeout):  # a long table postback; keep waiting
            settle(page)
        if page.get_by_text(re.compile(r"^(Eksport|Export)$", re.IGNORECASE)).first.is_visible():
            return
        page.wait_for_timeout(1000)
    raise RuntimeError(f"result table did not show its Export control; at {page.url}")


def save_download(download, subgroup: dict[str, Any], workdir: Path, stem: str = "") -> tuple[Path, str]:
    """Save a provider ZIP after checking its name identifies the subgroup and it is a ZIP."""
    name = download.suggested_filename
    number = subgroup["id"][1:]
    if not re.search(rf"(?:^|_){number}(?:_|\.)", name, re.IGNORECASE):
        raise RuntimeError(f"export file name {name!r} does not identify {subgroup['id']}")
    path = workdir / f"{stem or subgroup['id']}.zip"
    download.save_as(path)
    if path.read_bytes()[:2] != b"PK":
        raise RuntimeError(f"export of {subgroup['id']} is not a ZIP archive")
    return path, name


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


DOWNLOAD_IDS = ("ctl00_ContentPlaceHolder_download1", "ctl00_ContentPlaceHolder_download2")
EXPORTS_JS = """els => els.map(e => ({
    id: e.id, text: (e.textContent || '').trim().replace(/\\s+/g, ' '),
    ready: !/aspNetDisabled/i.test(typeof e.className === 'string' ? e.className : '') && !e.disabled
           && Boolean(e.getAttribute('href') || e.getAttribute('onclick')),
    row: (e.closest('tr')?.innerText || '').trim().replace(/\\s+/g, ' ').slice(0, 500)}))"""


def download_button(page):
    for control_id in DOWNLOAD_IDS:
        control = page.locator(f"#{control_id}")
        if control.count() and control.is_visible() and control.is_enabled():
            return control
    return None


def exports(page) -> list[dict[str, Any]]:
    """The account's generated packages listed on the start page."""
    page.goto(f"{WEB}/start", wait_until="domcontentloaded", timeout=30_000)
    settle(page)
    return page.locator('[id$="_Export"]').evaluate_all(EXPORTS_JS)


def fingerprint(entry: dict[str, Any]) -> str:
    return sha256(json.dumps([entry["text"], entry["row"]]).encode()).hexdigest()


def settled_exports(page) -> Counter:
    """How many of the account's packages carry each fingerprint, once none is still generating.

    Counted, not a set: two packages of one subgroup made on one day look the same (name and date)."""
    for _ in range(36):  # a package still generating would later be mistaken for ours
        before = exports(page)
        if all(e["ready"] for e in before):
            return Counter(fingerprint(e) for e in before)
        page.wait_for_timeout(5000)
    raise RuntimeError("earlier BDL packages did not finish generating")


def generated_export(page, subgroup: dict[str, Any], workdir: Path, trace: list, seen: Counter) -> tuple[Path, str]:
    """Request a background package for the current selection and download it when ready.

    The selection lives in the page session, so the export list is read before selecting."""
    button = download_button(page)
    note(page, trace, f"package requested ({sum(seen.values())} earlier packages)")
    with page.expect_response(lambda r: r.request.method == "POST", timeout=360_000) as response_info:
        button.click(no_wait_after=True)
    if not response_info.value.ok:
        raise RuntimeError(f"BDL package request returned HTTP {response_info.value.status}")
    number, name = subgroup["id"][1:], " ".join(subgroup["name"].split())[:40]

    def ours(e: dict[str, Any]) -> bool:
        return number in e["row"] + e["text"] or bool(name and name in e["row"])

    for _ in range(72):  # up to 6 minutes
        listed = exports(page)
        counts = Counter(fingerprint(e) for e in listed)
        # The list is newest first: a fingerprint listed more often than before is new in its first entries.
        new, taken = [], Counter()
        for e in listed:
            f = fingerprint(e)
            if taken[f] < counts[f] - seen[f]:
                taken[f] += 1
                if e["ready"] and ours(e):
                    new.append(e)
        if len(new) > 1:
            raise RuntimeError(f"several new packages match {subgroup['id']}")
        if new:
            note(page, trace, "package ready")
            with page.expect_download(timeout=180_000) as download_info:
                page.locator(f"#{new[0]['id']}").evaluate("e => e.click()")
            return save_download(download_info.value, subgroup, workdir)
        page.wait_for_timeout(5000)
    listed = exports(page)
    trace.append(
        {
            "step": "export list after waiting",
            "entries": [[e["ready"], e["text"][:40], e["row"][:200]] for e in listed[:8]],
        }
    )
    raise RuntimeError("BDL package was requested but did not appear in the export list")


CELL_BUDGET = int(os.environ.get("GUS_BDL_WEB_CELL_BUDGET", "80000"))  # information × units in one table
# 250 × 648 = 162k worked in the administrative layout, 400 × 648 failed (P3442, 21 Oct); the
# statistical layout's first 246 units (Poland, regions, subregions: dense) failed at 160k.
MAX_UNITS = 250
LAYOUT = "ctl00_ContentPlaceHolder_ukladTeryt"
UNITS = "ctl00_ContentPlaceHolder_terytList_UnitsList"
SELECTED = "ctl00_ContentPlaceHolder_terytList_SelectedUnits"


def select_dimensions(page, subgroup: dict[str, Any], trace: list) -> int | None:
    """Open the subgroup and select everything in every dimension; returns the information count."""
    page.goto(subgroup["url"], wait_until="domcontentloaded", timeout=60_000)
    settle(page)
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
    return information_count(page)


def to_territories(page) -> None:
    enabled_next(page).click()
    page.wait_for_url(re.compile(r"/bdl/dane/podgrup/teryt"), timeout=30_000)
    settle(page)


def layouts(page) -> list[str]:
    """The territorial layouts offered (statistical, administrative, functional)."""
    return page.evaluate(
        f"""() => {{ const it = window.$find('{LAYOUT}').get_items();
        return Array.from({{length: it.get_count()}}, (_, i) => it.getItem(i).get_text().trim()); }}"""
    )


def set_layout(page, label: str) -> None:
    current = lambda: page.evaluate(f"() => window.$find('{LAYOUT}').get_text().trim()")  # noqa: E731
    if current() == label:
        return
    page.locator(f"#{LAYOUT}_Arrow").click()
    page.locator(".rcbItem").filter(has_text=label).first.click()
    settle(page)
    page.wait_for_function(f"() => (window.$find('{UNITS}')?.get_items?.()?.get_count?.() || 0) > 0", timeout=45_000)
    if current() != label:
        raise RuntimeError(f"territorial layout {label!r} did not stay selected")


def unit_values(page) -> list[str]:
    return page.evaluate(
        f"""() => {{ const it = window.$find('{UNITS}').get_items();
        return Array.from({{length: it.get_count()}}, (_, i) => String(it.getItem(i).get_value())); }}"""
    )


def select_units(page, values: list[str]) -> None:
    """Move exactly these units to the selection."""
    page.evaluate(
        f"""values => {{ const b = window.$find('{UNITS}'); b.clearSelection();
        for (const v of values) {{ const item = b.findItemByValue(v);
            if (!item) throw new Error('unit ' + v + ' not listed'); item.select(); }} }}""",
        values,
    )
    page.locator("button.rlbTransferFrom:visible").first.click()
    settle(page)
    page.wait_for_function(
        f"n => (window.$find('{SELECTED}')?.get_items?.()?.get_count?.() || 0) === n", arg=len(values), timeout=45_000
    )


def select_all_units(page) -> None:
    page.locator("#ctl00_ContentPlaceHolder_terytList_MenuButton").click()
    page.locator("li.rmItem").filter(has_text="Zaznacz wszystkie").first.locator(".rmLink").click()
    settle(page)
    page.locator("button.rlbTransferAllFrom").first.click()
    settle(page)
    page.wait_for_function(f"() => (window.$find('{SELECTED}')?.get_items?.()?.get_count?.() || 0) > 0", timeout=45_000)


def table_export(page, subgroup: dict[str, Any], workdir: Path, trace: list, stem: str) -> tuple[Path, str]:
    """From the territories page: build the result table and download its relational CSV ZIP."""
    enabled_next(page).click()
    wait_for_result_table(page)
    note(page, trace, "result table")
    option = page.get_by_text(re.compile(r"CSV\s*[–-]\s*(tablica\s+)?relacyj", re.IGNORECASE)).first
    if not option.is_visible():
        page.get_by_text(re.compile(r"^(Eksport|Export)$", re.IGNORECASE)).first.click()
        option.wait_for(state="visible", timeout=15_000)
    with page.expect_download(timeout=240_000) as download_info:
        option.click(no_wait_after=True)
    return save_download(download_info.value, subgroup, workdir, stem)


def plan_parts(page, information: int) -> list[dict[str, Any]]:
    """Territory parts of a subgroup too large for one table: every layout's units in chunks
    of at most CELL_BUDGET / information units (overlapping layouts collapse in Silver)."""
    size = max(1, min(MAX_UNITS, CELL_BUDGET // max(1, information)))
    parts = []
    for label in layouts(page):
        set_layout(page, label)
        values = unit_values(page)
        parts += [{"layout": label, "territories": values[i : i + size]} for i in range(0, len(values), size)]
    return parts


def export(
    page,
    subgroup: dict[str, Any],
    workdir: Path,
    trace: list,
    landed: dict[int, set[int]],
    land_part,
    deadline: float,
    split_allowed: bool = True,
) -> dict[str, Any]:
    """Export one subgroup, landing each file as it is downloaded (land_part(result, index, total, selection)).

    Routes, as the September campaign: above MAX_INFORMATION values the website's background
    package ("Pobierz") of the whole subgroup; otherwise the result table of all territories;
    when the site cannot build that table (HTTP 500), territory parts per layout. `landed` maps
    a part total to the part indexes already in Landing for this version, so a run resumes.
    Without split_allowed a refused whole table is reported (needs_split) instead of split.
    Returns {"status": "downloaded" | "too_large" | "no_data" | "unfinished" | "needs_split",
    "information", "parts"}."""
    seen = settled_exports(page)
    information = select_dimensions(page, subgroup, trace)
    if not information:
        return {"status": "no_data", "information": information}
    whole = {"kind": "download", "layout": None, "territories": ["all"], "dimensions": {}}
    if download_button(page) is not None:
        path, name = generated_export(page, subgroup, workdir, trace, seen)
        land_part({"path": path, "export_file_name": name, "information": information}, 1, 1, whole)
        return {"status": "downloaded", "information": information, "parts": 1}
    if information > MAX_INFORMATION:
        return {"status": "too_large", "information": information}
    split = any(total > 1 for total in landed)
    if not split:
        to_territories(page)
        select_all_units(page)
        note(page, trace, "all territories selected")
        try:
            path, name = table_export(page, subgroup, workdir, trace, subgroup["id"])
        except RuntimeError as exc:
            if "BDL error page" not in str(exc):
                raise
            note(page, trace, "whole table refused")
            if not split_allowed:
                return {"status": "needs_split", "information": information}
        else:
            land_part({"path": path, "export_file_name": name, "information": information}, 1, 1, whole)
            return {"status": "downloaded", "information": information, "parts": 1}
        select_dimensions(page, subgroup, trace)
    to_territories(page)
    parts = plan_parts(page, information)
    total = len(parts)
    done = landed.get(total, set())
    note(page, trace, f"{total} territory parts, {len(done)} already landed")
    fresh = True  # the territories page of the current dimension selection, nothing selected yet
    for index, part in enumerate(parts, start=1):
        if index in done:
            continue
        if time.monotonic() > deadline:
            return {"status": "unfinished", "information": information, "parts": total}
        for attempt in (1, 2):  # the error page says the server may be overloaded: one retry after a pause
            if not fresh:
                select_dimensions(page, subgroup, trace)
                to_territories(page)
            set_layout(page, part["layout"])
            select_units(page, part["territories"])
            fresh = False
            try:
                path, name = table_export(page, subgroup, workdir, trace, f"{subgroup['id']}-{index:03d}")
                break
            except RuntimeError as exc:
                if attempt == 2 or "BDL error page" not in str(exc):
                    raise RuntimeError(f"part {index} of {total} ({part['layout']}): {exc}") from exc
                note(page, trace, f"part {index}: server error, retrying after a pause")
                page.wait_for_timeout(60_000)
        land_part(
            {"path": path, "export_file_name": name, "information": information},
            index,
            total,
            {"kind": "download", "layout": part["layout"], "territories": part["territories"], "dimensions": {}},
        )
        done.add(index)
    return {"status": "downloaded", "information": information, "parts": total}


def landed_parts(client, bucket: str, subgroup_id: str, version_token: str) -> dict[int, set[int]]:
    """Part indexes in Landing for this subgroup version, by announced part total."""
    out: dict[int, set[int]] = {}
    prefix = f"{DATA}{subgroup_id}/{version_token}__"
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for o in page.get("Contents", []):
            if o["Key"].endswith(".receipt.json"):
                part = json.loads(client.get_object(Bucket=bucket, Key=o["Key"])["Body"].read()).get("part") or {}
                out.setdefault(int(part.get("total", 1)), set()).add(int(part.get("index", 1)))
    return out


def land(
    client,
    bucket: str,
    subgroup: dict[str, Any],
    version: datetime | None,
    result: dict[str, Any],
    index: int = 1,
    total: int = 1,
    selection: dict[str, Any] | None = None,
) -> str:
    """Write one ZIP and its receipt to Landing; returns the data key."""
    body = result["path"].read_bytes()
    digest = sha256(body).hexdigest()
    now = datetime.now(UTC)
    version_token = token(version)
    suffix = f"__part-{index:03d}" if total > 1 else ""
    key = f"{DATA}{subgroup['id']}/{version_token}__{now.strftime('%Y%m%dT%H%M%SZ')}{suffix}.zip"
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
        "part": {"index": index, "total": total, "selection": selection or {}},
    }
    client.put_object(
        Bucket=bucket,
        Key=key[: -len(".zip")] + ".receipt.json",
        ContentType="application/json",
        Body=json.dumps(receipt, indent=2, sort_keys=True).encode(),
    )
    result["path"].unlink(missing_ok=True)
    return key


def main(limit: int = 20, budget_minutes: float = 50, only: list[str] | None = None, split: bool = False) -> int:
    """Export up to `limit` missing subgroups within the time budget; exit 2 when work remains."""
    from playwright.sync_api import sync_playwright

    deadline = time.monotonic() + budget_minutes * 60
    client, bucket = s3(), req("R2_LANDING_BUCKET")
    api = Bdl()
    todo = missing_subgroups(api, landing_versions(client, bucket), only)
    summary: dict[str, Any] = {
        "missing": len(todo),
        "landed": [],
        "too_large": [],
        "no_data": [],
        "unfinished": [],
        "needs_split": [],
        "failed": [],
    }
    ensure_browser()
    with sync_playwright() as pw, tempfile.TemporaryDirectory() as tmp:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_context(accept_downloads=True, locale="pl-PL").new_page()
        login(page, req("GUS_BDL_WEB_EMAIL"), req("GUS_BDL_WEB_PASSWORD"))
        for subgroup in todo[:limit]:
            if time.monotonic() > deadline:
                break
            trace: list = []
            began = time.monotonic()
            try:
                version = api.last_update(subgroup["id"], subgroup["subject"])
                landed = landed_parts(client, bucket, subgroup["id"], token(version))

                def land_part(result, index, total, selection, subgroup=subgroup, version=version):
                    land(client, bucket, subgroup, version, result, index, total, selection)

                result = export(page, subgroup, Path(tmp), trace, landed, land_part, deadline, split)
                if result["status"] == "downloaded":
                    seconds = round(time.monotonic() - began)
                    summary["landed"].append([subgroup["id"], result["information"], result["parts"], seconds])
                else:
                    summary[result["status"]].append([subgroup["id"], result["information"]])
            except Exception as exc:  # one subgroup's failure is recorded; the run goes on
                summary["failed"].append([subgroup["id"], f"{type(exc).__name__}: {str(exc)[:200]}", trace[-2:]])
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
