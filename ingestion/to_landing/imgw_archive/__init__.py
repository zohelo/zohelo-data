"""IMGW public observation archive Landing activities: daily synoptic station files (dobowe/synop).

https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/dobowe/synop/ is an Apache
directory index: five-year folders 1966_1970 … 1996_2000 (one ZIP per station and five years), year folders 2001 …
<last year> (one ZIP per station and year) and the current year (one ZIP per month, all stations). Each ZIP holds the
daily synop file s_d_* (and, until mid-2024, s_d_t_*). ``catalogue()`` lists the folders (one request per folder,
about 35 in total) and gives one entity per ZIP: ``synop_daily_<file name without _s.zip>``, e.g.
``synop_daily_2025_690`` (station 690, 2025), ``synop_daily_1991_1995_695``, ``synop_daily_2026_10`` (October 2026,
all stations).

Each ZIP is landed by the shared bulk_file pattern (exact bytes, receipt after the file, never overwritten, version =
Last-Modified). A file is downloaded when Landing has no copy (missing, newest years first) or when the index shows
it modified after the newest copy's version (stale: IMGW replaces corrected files, docs lista_zmian.txt); the index
time is the server's local time (Europe/Warsaw) to the minute. One request at a time with a pause; the run stops at
its time budget and the next run continues (missing first). Landing state comes from the receipt key names of one
listing (``<version>__<ingested>.receipt.json``), so no receipt is read to plan.

landing.inspect  catalogue vs Landing: missing / stale / current; no writes
landing.extract  download missing, then stale, within the budget
landing.verify   every receipt has its file (Landing or Archive); no requests to IMGW
"""

from __future__ import annotations

from datetime import UTC, datetime
import json
import re
import time
from typing import Any
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import requests

SOURCE = "imgw_archive"
ROOT_URL = "https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/dobowe/synop/"
LANDING_ROOT = "01_landing"  # a Dev trial points it at a zz_dev_ scratch prefix
USER_AGENT = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
TIMEOUT, RETRIES, PAUSE = 60, 4, 0.5
ROW = re.compile(  # one row of the Apache index table: link, "YYYY-MM-DD HH:MM", size
    r'<a href="([^"?/][^"]*)">[^<]*</a>\s*</td>\s*<td[^>]*>\s*(\d{4}-\d\d-\d\d \d\d:\d\d)\s*</td>'
    r"\s*<td[^>]*>\s*([^<]*)</td>"
)
FOLDER = re.compile(r"^(\d{4})(?:_(\d{4}))?/$")
ZIP = re.compile(r"^(\d{4}(?:_\d{4})?_\d{2,3})_s\.zip$")
WARSAW = ZoneInfo("Europe/Warsaw")


def _get(session: requests.Session, url: str) -> str:
    """One index page; bounded retries with backoff on network errors, 429 and 5xx; other statuses fail."""
    for attempt in range(RETRIES):
        time.sleep(PAUSE)
        try:
            response = session.get(url, timeout=TIMEOUT)
        except (requests.ConnectionError, requests.Timeout):
            response = None
        if response is not None and response.status_code == 200:
            return response.text
        if response is not None and response.status_code not in (429, 500, 502, 503, 504):
            raise RuntimeError(f"IMGW index {url}: HTTP {response.status_code}")
        time.sleep(min(120, 10 * 2**attempt))
    raise RuntimeError(f"IMGW index {url}: kept failing")


def _rows(html: str) -> list[tuple[str, str]]:
    return [(href, modified) for href, modified, _size in ROW.findall(html)]


def catalogue() -> list[dict[str, Any]]:
    """Every daily synop ZIP of the index as a bulk_file spec, newest period first, with ``listed`` (UTC stamp of
    the index time, to the minute). A folder without a parseable row fails: a changed index layout must not look
    like an empty archive."""
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    folders = [href for href, _ in _rows(_get(session, ROOT_URL)) if FOLDER.match(href)]
    if not folders:
        raise RuntimeError(f"IMGW index {ROOT_URL}: no year folders found (layout changed?)")
    specs = []
    for folder in folders:
        rows = _rows(_get(session, urljoin(ROOT_URL, folder)))
        found = [(ZIP.match(href), modified) for href, modified in rows]
        if not any(m for m, _ in found):
            raise RuntimeError(f"IMGW index {ROOT_URL}{folder}: no s_d ZIP found (layout changed?)")
        for match, modified in found:
            if not match:
                continue
            local = datetime.strptime(modified, "%Y-%m-%d %H:%M").replace(tzinfo=WARSAW)
            specs.append(
                {
                    "entity": f"synop_daily_{match.group(1)}",
                    "url": urljoin(ROOT_URL, folder + match.group(0)),
                    "format": "zip",
                    "version": "last_modified",
                    "folder": folder.rstrip("/"),
                    "listed": local.astimezone(UTC).strftime("%Y%m%dT%H%MZ"),
                }
            )
    return sorted(specs, key=lambda s: (s["folder"], s["entity"]), reverse=True)


def landing_state(client, bucket: str) -> dict[str, str]:
    """Entity -> version token of its newest landed copy, from the receipt key names of one listing."""
    newest: dict[str, tuple[str, str]] = {}
    prefix = f"{LANDING_ROOT}/{SOURCE}/snapshot/data/"
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            match = re.fullmatch(rf"{re.escape(prefix)}([a-z0-9_]+)/([0-9TZ]+)__([0-9TZ]+)\.receipt\.json", item["Key"])
            if match:
                entity, version, ingested = match.groups()
                if entity not in newest or ingested > newest[entity][1]:
                    newest[entity] = (version, ingested)
    return {entity: version for entity, (version, _) in newest.items()}


def classify(specs: list[dict[str, Any]], held: dict[str, str]) -> dict[str, list[dict[str, Any]]]:
    """missing (no copy), stale (index time after the newest copy's version, both UTC to the minute), current."""
    out: dict[str, list[dict[str, Any]]] = {"missing": [], "stale": [], "current": []}
    for spec in specs:
        version = held.get(spec["entity"])
        if version is None:
            out["missing"].append(spec)
        elif spec["listed"] > version[:13] + "Z":
            out["stale"].append(spec)
        else:
            out["current"].append(spec)
    return out


def _run(mode: str, p) -> int:
    from ingestion.common.r2 import req, s3
    from ingestion.common.report import annotate, step_summary
    from ingestion.to_landing.patterns import bulk_file

    if mode == "verify":
        client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
        entities = sorted(landing_state(client, bucket))
        return bulk_file.main(SOURCE, "verify", landing_root=LANDING_ROOT, specs=[{"entity": e} for e in entities])
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    specs = catalogue()
    state = classify(specs, landing_state(client, bucket))
    summary: dict[str, Any] = {
        "source": SOURCE,
        "mode": mode,
        "listed": len(specs),
        "folders": len({s["folder"] for s in specs}),
        "missing": len(state["missing"]),
        "stale": len(state["stale"]),
        "current": len(state["current"]),
        "missing_sample": [s["entity"] for s in state["missing"][:5]],
        "stale_sample": [s["entity"] for s in state["stale"][:5]],
    }
    status = 0
    if mode == "load":
        deadline = time.monotonic() + max(p.budget_minutes - 15, 5) * 60
        outcomes: dict[str, int] = {}
        failed = []
        for n, spec in enumerate(state["missing"] + state["stale"]):
            if time.monotonic() > deadline:
                summary["stopped_at_budget"] = n
                break
            time.sleep(PAUSE)
            try:
                result = bulk_file.load_file(client, bucket, SOURCE, spec, LANDING_ROOT)
            except (bulk_file.PermanentError, requests.RequestException, RuntimeError) as exc:
                failed.append({"entity": spec["entity"], "error": str(exc)[:200]})
                continue
            outcomes[result["outcome"]] = outcomes.get(result["outcome"], 0) + 1
        summary.update({"outcomes": outcomes, "failed": len(failed), "failed_sample": failed[:5]})
        status = 1 if failed else 0
    print(json.dumps(summary, sort_keys=True), flush=True)
    annotate("notice" if status == 0 else "error", f"{SOURCE} landing {mode}", summary)
    step_summary(f"## {SOURCE} · landing {mode}\n\n```json\n{json.dumps(summary, indent=2)}\n```\n")
    return status


ACTIVITIES = {
    "landing.inspect": lambda p: _run("inspect", p),
    "landing.extract": lambda p: _run("load", p),
    "landing.verify": lambda p: _run("verify", p),
}
