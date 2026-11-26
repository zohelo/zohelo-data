#!/usr/bin/env python3
"""PSE Reports API v2 Landing: one report for one business day per request, the response's exact bytes.

Route (docs/sources/pse_reports.md, register PL-ENV-019). ``GET https://api.raporty.pse.pl/api/<report>`` with
``$filter=business_date eq 'YYYY-MM-DD'`` answers ``{"value": [...]}``, one row per 15-minute period of the day, at
most 100 rows a page; a longer answer continues through ``nextLink`` (an ``$after`` cursor). A day has 96 periods
(92 and 100 on the DST days), so one page holds it: a nextLink is still followed and must lead to no rows, or the
day fails. Public, no key, no published limit, no rate-limit headers: one request at a time, PAUSE_SECONDS apart,
bounded retries on network errors, 429 and 5xx; any other status (403 included) fails the day at once, never worked
around. Measured from Actions (Dev probes 45790352597 and 45790863668, 26 Nov 2026): first day 2024-06-14 (rce-pln
has no rows for 2024-06-13; his-wlk-cal has one, its last period, which is left out); a repeated request returns
identical bytes; RCE of a day is published the day before, his-wlk-cal at about 02:52 UTC the next day.

Selection (#2870): ``rce-pln`` (market energy price RCE, PLN/MWh) and ``his-wlk-cal`` (power demand, wind and PV
generation and the other basic quantities of the national power system, MW). Other reports are left out.

Entity = report x business day, ``<report>_<YYYYMMDD>`` (``rce_pln_20240614``); version = the newest
``publication_ts_utc`` of the day's rows (``YYYYMMDDTHHMMSSZ``). Days up to yesterday (Europe/Warsaw) are asked: the
missing ones oldest first, then the newest RECHECK_DAYS days again (PSE re-publishes corrected values); an answer
whose bytes equal the newest landed copy is not written again. A day is landed only when it holds every period of
the local day (expected_periods) with distinct UTC times; an empty or short answer is counted and asked again on the
next run. The field set is not checked here (Landing keeps the bytes; Bronze rejects an unknown field set).

Layout (docs/landing.md): ``<root>/pse_reports/snapshot/data/<entity>/<version>__<ingested>.json`` + receipt.
Modes: inspect (window days vs Landing), load, verify.
"""

from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
import json
import os
import re
import time
from typing import Any
from zoneinfo import ZoneInfo

import requests

from ingestion.common.landing_write import USER_AGENT, put_new
from ingestion.common.r2 import archive_key, archived, get_with_backoff, req, s3
from ingestion.common.report import annotate, step_summary

SOURCE = "pse_reports"
API = "https://api.raporty.pse.pl/api"
REPORTS = {"rce_pln": "rce-pln", "his_wlk_cal": "his-wlk-cal"}  # entity prefix -> API report name
WINDOW_START = date(2024, 6, 14)
RECHECK_DAYS = 7
MAX_PAGES = 3
PAUSE_SECONDS = 1.2
TIMEOUT_SECONDS = (20, 120)
RETRY_DELAYS = (15, 60, 180)
WARSAW = ZoneInfo("Europe/Warsaw")
ENTITY_RE = re.compile(r"(rce_pln|his_wlk_cal)_(\d{8})")


def prefix(root: str) -> str:
    return f"{root.rstrip('/')}/{SOURCE}/snapshot/data/"


def entity_of(report: str, day: date) -> str:
    return f"{report}_{day:%Y%m%d}"


def parse_entity(entity: str) -> tuple[str, date] | None:
    match = ENTITY_RE.fullmatch(entity)
    return (match.group(1), datetime.strptime(match.group(2), "%Y%m%d").date()) if match else None


def expected_periods(day: date) -> int:
    """15-minute periods of the local (Europe/Warsaw) day: 96, 92 on the spring DST day, 100 on the autumn one."""
    start = datetime(day.year, day.month, day.day, tzinfo=WARSAW)
    nxt = day + timedelta(days=1)
    end = datetime(nxt.year, nxt.month, nxt.day, tzinfo=WARSAW)
    return int((end.timestamp() - start.timestamp()) // 900)


def last_day(now: datetime | None = None) -> date:
    """The newest business day asked for: yesterday in Warsaw (his-wlk-cal of a day is published the next night)."""
    return (now or datetime.now(UTC)).astimezone(WARSAW).date() - timedelta(days=1)


def window(last: date) -> list[date]:
    return [WINDOW_START + timedelta(days=i) for i in range((last - WINDOW_START).days + 1)]


def listing(client, bucket: str, root: str) -> tuple[dict[str, list[tuple[str, str]]], set[str]]:
    """Per entity: (ingested stamp, receipt key) oldest first; and the data keys present. Key names only, no reads."""
    receipts: dict[str, list[tuple[str, str]]] = {}
    data: set[str] = set()
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix(root)):
        for o in page.get("Contents", []):
            key = o["Key"]
            entity, name = key[len(prefix(root)) :].split("/", 1)
            if key.endswith(".receipt.json"):
                receipts.setdefault(entity, []).append((name.split("__", 1)[1].split(".", 1)[0], key))
            else:
                data.add(key)
    for rows in receipts.values():
        rows.sort()
    return receipts, data


def read_receipts(client, bucket: str, keys: list[str]) -> list[dict[str, Any]]:
    def read(key: str) -> dict[str, Any]:
        return {**json.loads(get_with_backoff(client, bucket, key)), "receipt_key": key}

    with ThreadPoolExecutor(max_workers=4) as pool:  # R2 throttles bursts of reads
        return list(pool.map(read, keys))


def held_entities(receipts: dict[str, list]) -> set[tuple[str, date]]:
    return {parsed for entity in receipts if (parsed := parse_entity(entity))}


def get(session: requests.Session, url: str, params: dict[str, Any] | None) -> bytes:
    """GET with bounded retries: network errors, 429 and 5xx back off; any other status fails at once."""
    for attempt in range(len(RETRY_DELAYS) + 1):
        time.sleep(PAUSE_SECONDS)
        try:
            response = session.get(url, params=params, timeout=TIMEOUT_SECONDS)
        except (requests.ConnectionError, requests.Timeout, requests.exceptions.ChunkedEncodingError):
            response = None
        if response is not None and response.status_code == 200:
            return response.content
        if response is not None and response.status_code not in (429, 500, 502, 503, 504):
            raise RuntimeError(f"PSE API {response.status_code} for {url} {params}: {response.text[:300]}")
        if attempt == len(RETRY_DELAYS):
            break
        time.sleep(RETRY_DELAYS[attempt])
    raise RuntimeError(f"PSE API kept failing for {url} {params}")


def answer_rows(body: bytes, where: str) -> tuple[list[dict[str, Any]], str | None]:
    try:
        document = json.loads(body)
    except ValueError as exc:
        raise RuntimeError(f"{where}: PSE API returned no JSON: {body[:200]!r}") from exc
    if (
        not isinstance(document, dict)
        or not isinstance(document.get("value"), list)
        or set(document) - {"value", "nextLink"}
    ):
        raise RuntimeError(f"{where}: unexpected answer shape: {body[:300]!r}")
    if not all(isinstance(row, dict) for row in document["value"]):
        raise RuntimeError(f"{where}: a row is not an object")
    return document["value"], document.get("nextLink")


def version_of(rows: list[dict[str, Any]], where: str) -> str:
    """The newest publication_ts_utc of the rows as YYYYMMDDTHHMMSSZ ('2026-11-14 12:59:12.065')."""
    stamps = [str(r.get("publication_ts_utc") or "") for r in rows]
    if not all(re.fullmatch(r"\d{4}-\d\d-\d\d \d\d:\d\d:\d\d(\.\d+)?", s) for s in stamps):
        raise RuntimeError(f"{where}: publication_ts_utc missing or not a timestamp")
    return datetime.strptime(max(stamps)[:19], "%Y-%m-%d %H:%M:%S").strftime("%Y%m%dT%H%M%SZ")


def fetch_day(session: requests.Session, report: str, day: date) -> tuple[str, bytes | None, dict[str, Any]]:
    """One report on one business day: ('ok', exact bytes, detail), or ('empty' | 'incomplete', None, detail)."""
    where = f"{report} {day}"
    body = get(session, f"{API}/{REPORTS[report]}", {"$filter": f"business_date eq '{day}'"})
    rows, next_link = answer_rows(body, where)
    pages = [{"sha256": sha256(body).hexdigest(), "size_bytes": len(body), "rows": len(rows)}]
    while next_link:  # a day fits one page of 100 rows; a further page must be empty
        if len(pages) >= MAX_PAGES:
            raise RuntimeError(f"{where}: more than {MAX_PAGES} pages")
        more, next_link = answer_rows(get(session, next_link, None), where)
        pages.append({"rows": len(more)})
        if more:
            raise RuntimeError(f"{where}: {len(more)} rows on page {len(pages)}; a day should fit one page")
    expected = expected_periods(day)
    detail = {"rows": len(rows), "expected_periods": expected, "pages": pages}
    if not rows:
        return "empty", None, detail
    if any(r.get("business_date") != f"{day}" for r in rows):
        raise RuntimeError(f"{where}: a row of another business_date in the answer")
    if len(rows) != expected or len({r.get("dtime_utc") for r in rows}) != expected:
        return "incomplete", None, detail
    detail["version_token"] = version_of(rows, where)
    return "ok", body, detail


def land_day(client, bucket: str, root: str, session, report: str, day: date, newest: dict[str, dict]) -> str:
    """Fetch, check and (when new or changed) write one entity with its receipt; returns the outcome."""
    status, body, detail = fetch_day(session, report, day)
    if body is None:
        return f"days_{status}"
    entity = entity_of(report, day)
    digest = sha256(body).hexdigest()
    before = newest.get(entity)
    if before and before["sha256"] == digest:
        return "files_unchanged"
    token = detail["version_token"]
    ingested = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    key = f"{prefix(root)}{entity}/{token}__{ingested}.json"
    put_new(client, bucket, key, body, "application/json")
    receipt = {
        "schema_version": 3,
        "kind": "pse_reports_landing",
        "source_id": SOURCE,
        "entity": entity,
        "area": "data",
        "content": "snapshot",
        "source_kind": "pse_reports_api_v2_json",
        "version_token": token,
        "version_basis": "max_publication_ts_utc",
        "ingested_at": ingested,
        "source_url": f"{API}/{REPORTS[report]}?$filter=business_date eq '{day}'",
        "report": REPORTS[report],
        "business_date": f"{day}",
        "rows": detail["rows"],
        "expected_periods": detail["expected_periods"],
        "pages": detail["pages"],
        "sha256": digest,
        "size_bytes": len(body),
        "object": {"bucket": bucket, "key": key},
        "replaces_sha256": before["sha256"] if before else None,
        "code_sha": os.environ.get("GITHUB_SHA", ""),
    }
    put_new(
        client,
        bucket,
        key[: -len(".json")] + ".receipt.json",
        json.dumps(receipt, indent=2, sort_keys=True).encode(),
        "application/json",
    )
    return "files_landed_changed" if before else "files_landed_new"


def extract(budget_minutes: float, landing_root: str = "01_landing", last: date | None = None) -> int:
    """landing.extract: missing entities oldest first, then the newest RECHECK_DAYS days again (within the budget)."""
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    last = last or last_day()
    receipts, _ = listing(client, bucket, landing_root)
    held = held_entities(receipts)
    days = window(last)
    wanted = [(r, d) for d in days for r in REPORTS]
    missing = [(r, d) for r, d in wanted if (r, d) not in held]
    recheck = [(r, d) for d in days[-RECHECK_DAYS:] for r in REPORTS if (r, d) in held]
    newest = {
        x["entity"]: x for x in read_receipts(client, bucket, [receipts[entity_of(r, d)][-1][1] for r, d in recheck])
    }
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
    deadline = time.monotonic() + budget_minutes * 60
    outcomes: Counter = Counter()
    not_landed: list[str] = []
    failed: list[dict[str, str]] = []
    for report, day in missing + recheck:
        if time.monotonic() > deadline:
            outcomes["not_started_time_budget"] += 1
            continue
        try:
            outcome = land_day(client, bucket, landing_root, session, report, day, newest)
        except RuntimeError as exc:  # one day failing must not stop the others; recorded, run ends red
            failed.append({"entity": entity_of(report, day), "error": str(exc)[:300]})
            continue
        outcomes[outcome] += 1
        if outcome.startswith("days_"):
            not_landed.append(f"{entity_of(report, day)}:{outcome[5:]}")
    summary = {
        "mode": "load",
        "window": [str(WINDOW_START), str(last)],
        "days_in_window": len(days),
        "missing_before": len(missing),
        "rechecked": len(recheck),
        **outcomes,
        "not_landed_sample": not_landed[:10],
        "failed": len(failed),
        "failed_sample": failed[:5],
    }
    status = 1 if failed else 2 if outcomes["not_started_time_budget"] else 0
    report_result("load", summary, "error" if failed else "warning" if not_landed else "notice")
    return status


def run_verify(client, bucket: str, root: str) -> dict[str, Any]:
    receipts, data = listing(client, bucket, root)
    stored = archived(client, bucket, SOURCE)
    problems = []
    held = read_receipts(client, bucket, [k for rows in receipts.values() for _, k in rows])
    for r in held:
        key = r["object"]["key"]
        if key not in data and stored.get(r["sha256"], {}).get("key") != archive_key(SOURCE, r["sha256"], key):
            problems.append(key)
    days = {day for _, day in held_entities(receipts)}
    return {
        "receipts": len(held),
        "entities": len(receipts),
        "days": len(days),
        "first_last_day": [str(min(days)) if days else None, str(max(days)) if days else None],
        "missing_data": len(problems),
        "missing_sample": problems[:10],
    }


def inspect(landing_root: str = "01_landing", last: date | None = None) -> int:
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    last = last or last_day()
    receipts, data = listing(client, bucket, landing_root)
    held = held_entities(receipts)
    days = window(last)
    missing = [entity_of(r, d) for d in days for r in REPORTS if (r, d) not in held]
    summary = {
        "mode": "inspect",
        "window": [str(WINDOW_START), str(last)],
        "entities_in_window": len(days) * len(REPORTS),
        "entities_current": len(days) * len(REPORTS) - len(missing),
        "entities_missing": len(missing),
        "missing_sample": missing[:10],
        "data_files_in_landing": len(data),
        "newest_day_held": str(max(d for _, d in held)) if held else None,
    }
    report_result("inspect", summary, "notice")
    return 0


def verify(landing_root: str = "01_landing") -> int:
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    summary = {"mode": "verify", **run_verify(client, bucket, landing_root)}
    status = 1 if summary["missing_data"] else 0
    report_result("verify", summary, "error" if status else "notice")
    return status


def report_result(mode: str, summary: dict[str, Any], level: str) -> None:
    print(json.dumps(summary, sort_keys=True, default=str))
    annotate(level, f"pse_reports landing {mode}", summary)
    step_summary(f"## pse_reports · landing {mode}\n\n```json\n{json.dumps(summary, indent=2, default=str)}\n```\n")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--mode", choices=("inspect", "load", "verify"), required=True)
    parser.add_argument("--budget-minutes", type=float, default=150)
    parser.add_argument("--landing-root", default="01_landing", help="Landing root (a zz_dev_ prefix in Dev trials)")
    args = parser.parse_args(argv)
    if args.mode == "inspect":
        return inspect(args.landing_root)
    if args.mode == "verify":
        return verify(args.landing_root)
    return extract(args.budget_minutes, args.landing_root)


if __name__ == "__main__":
    raise SystemExit(main())
