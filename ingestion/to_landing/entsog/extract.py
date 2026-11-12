#!/usr/bin/env python3
"""ENTSOG Transparency Platform Landing: daily physical flows of the declared operators, one gas month per request.

Route (docs/sources/entsog.md; register PL-ENV-021): the platform's public REST API, no key,
``/api/v1/operationaldatas?operatorKey=<op>&indicator=Physical%20Flow&periodType=day&timezone=WET&limit=-1
&from=<first day>&to=<last day>``. The API User Manual v3.0 asks for monthly ranges ("Monthly ranges are strongly
recommended"), a query runs at most 60 s, and failed requests need "reasonable back-off intervals" without rapid
retries; without ``limit=-1`` it returns 100 records. One month is about 1.4 MB and 1,500 records for GAZ-SYSTEM.
The API serves a rolling window of about five years (older days answer 404 "Data on the TP is archived on a rolling
basis ... check the TP Archives"); the TP Archives are not read.

Entity = operator x indicator x gas month (``physical_flow__<operatorKey>__<YYYY-MM>``). Version = the newest
``lastUpdateDateTime`` in the month's answer (UTC, ``YYYYMMDDTHHMMSSZ``). Two identical requests return identical
bytes (Dev probe 43320099570), so an answer is written only when its bytes differ from the newest landed copy.

Which months are requested (load):
  missing  months of the window without a landed copy, newest first (the backfill; resumable within the budget);
  open     the current month, and every month whose newest copy is not closed;
  rotation one closed month per run, chosen by the day number (each closed month is checked again about every
           N days for N closed months), because confirmed values are revised (January 2021 was last updated in
           February 2025).
A month is closed when it has ended, its copy holds every gas day of the month, and every record with a value is
``Confirmed`` (records with ``isNA`` have no value and no status; they count as final). The counts a decision needs
are kept in the receipt, so inspect makes no request.

Layout (docs/landing.md): ``<root>/entsog/snapshot/data/<entity>/<version>__<ingested>.json`` + ``.receipt.json``.
Polite access: one request at a time, a pause between requests, bounded retries with growing back-off on 429, 5xx
and timeouts, an identifying User-Agent. A month that still fails is recorded and the others are landed; the run
then exits 1. A 404 for a month before the window ("archived") is counted, not an error.
"""

from __future__ import annotations

import argparse
from calendar import monthrange
from collections import Counter
from datetime import UTC, date, datetime
from hashlib import sha256
import json
import os
import time
from typing import Any

import requests

from ingestion.common.r2 import archive_key, archived, req, s3
from ingestion.common.report import annotate, step_summary
from ingestion.to_landing.patterns import bulk_file
from ingestion.to_landing.world_bank_icp.extract import USER_AGENT, put_new

SOURCE = "entsog"
API = "https://transparency.entsog.eu/api/v1/operationaldatas"
# Operators loaded (register id per operator). The same code takes another operator by adding it here.
OPERATORS = {"PL-TSO-0002": "PL-ENV-021"}  # GAZ-SYSTEM S.A., Poland
INDICATORS = {"physical_flow": "Physical Flow"}  # entity slug -> API indicator name (case-sensitive)
FIRST_MONTH = (2021, 1)  # oldest gas month the API answered on 12 Nov 2026 (1 Jan 2020 answered 404, archived)
PAUSE_SECONDS = 5.0
TIMEOUT_SECONDS = 90  # the API stops a query after 60 s
BACKOFF_SECONDS = (30, 90, 240)  # waits before the 2nd, 3rd and 4th attempt
ARCHIVED_TEXT = "archived on a rolling basis"


def entity(slug: str, operator: str, month: tuple[int, int]) -> str:
    return f"{slug}__{operator}__{month[0]:04d}-{month[1]:02d}"


def months(today: date) -> list[tuple[int, int]]:
    """Gas months of the window, oldest first, up to the current month (UTC)."""
    out, (y, m) = [], FIRST_MONTH
    while (y, m) <= (today.year, today.month):
        out.append((y, m))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def url(indicator: str, operator: str, month: tuple[int, int]) -> str:
    last = monthrange(*month)[1]
    return (
        f"{API}?operatorKey={operator}&indicator={requests.utils.quote(indicator)}&periodType=day&timezone=WET"
        f"&limit=-1&from={month[0]:04d}-{month[1]:02d}-01&to={month[0]:04d}-{month[1]:02d}-{last:02d}"
    )


def fetch(session: requests.Session, address: str) -> tuple[int, bytes]:
    """GET with bounded retries: network errors, 429 and 5xx back off; 200 and 404 are answers."""
    for attempt in range(len(BACKOFF_SECONDS) + 1):
        if attempt:
            time.sleep(BACKOFF_SECONDS[attempt - 1])
        try:
            response = session.get(address, timeout=TIMEOUT_SECONDS)
        except (requests.ConnectionError, requests.Timeout):
            continue
        if response.status_code in (200, 404):
            return response.status_code, response.content
        if response.status_code not in (429, 500, 502, 503, 504):
            raise RuntimeError(f"ENTSOG API {response.status_code} for {address}: {response.text[:200]}")
    raise RuntimeError(f"ENTSOG API kept failing: {address}")


def profile(body: bytes, operator: str, indicator: str, month: tuple[int, int]) -> dict[str, Any]:
    """Counts of one answer (kept in the receipt) and its version; a malformed answer raises."""
    page = json.loads(body)
    records = page.get("operationaldatas") if isinstance(page, dict) else None
    if not isinstance(records, list) or not records:
        raise RuntimeError(f"no operationaldatas in the answer: {body[:200]!r}")
    if {r.get("operatorKey") for r in records} != {operator} or {r.get("indicator") for r in records} != {indicator}:
        raise RuntimeError("the answer holds another operator or indicator")
    stamps = [r["lastUpdateDateTime"] for r in records if r.get("lastUpdateDateTime")]
    newest = max(datetime.fromisoformat(s).astimezone(UTC) for s in stamps)
    with_value = [r for r in records if r.get("isNA") not in (1, True, "1") and r.get("value") not in (None, "")]
    return {
        "version_token": newest.strftime("%Y%m%dT%H%M%SZ"),
        "records": len(records),
        "records_with_value": len(with_value),
        "records_na": len(records) - len(with_value),
        "not_confirmed": sum(1 for r in with_value if r.get("flowStatus") != "Confirmed"),
        "gas_days": len({str(r.get("periodFrom"))[:10] for r in records}),
        "days_in_month": monthrange(*month)[1],
        "meta_count": (page.get("meta") or {}).get("count"),
        "meta_total": (page.get("meta") or {}).get("total"),
    }


def closed(receipt: dict[str, Any] | None, month: tuple[int, int], today: date) -> bool:
    if not receipt:
        return False
    detail = receipt.get("detail") or {}
    ended = month < (today.year, today.month)
    return ended and detail.get("gas_days") == detail.get("days_in_month") and detail.get("not_confirmed") == 0


def land(client, bucket: str, root: str, session, slug: str, operator: str, month, held: list) -> str:
    """One gas month; an answer equal to the newest landed copy is not written again."""
    time.sleep(PAUSE_SECONDS)
    address = url(INDICATORS[slug], operator, month)
    status, body = fetch(session, address)
    if status == 404:
        if ARCHIVED_TEXT in body.decode("utf-8", "replace"):
            return "archived_at_source"
        if month == (datetime.now(UTC).year, datetime.now(UTC).month):
            return "no_data_yet"  # the first hours of a month, before the first gas day is published
        raise RuntimeError(f"404 for {address}: {body[:200]!r}")
    detail = profile(body, operator, INDICATORS[slug], month)
    digest = sha256(body).hexdigest()
    before = held[-1] if held else None
    if before and before["sha256"] == digest:
        return "unchanged"
    name = entity(slug, operator, month)
    ingested = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    key = f"{bulk_file.prefix(SOURCE, name, root)}{detail['version_token']}__{ingested}.json"
    put_new(client, bucket, key, body, "application/json")
    receipt = {
        "schema_version": 3,
        "source": SOURCE,
        "source_id": SOURCE,
        "entity": name,
        "area": "data",
        "content": "snapshot",
        "version_token": detail["version_token"],
        "version_basis": "newest lastUpdateDateTime in the answer",
        "ingested_at": ingested,
        "source_url": address,
        "final_url": address,
        "sha256": digest,
        "size_bytes": len(body),
        "object": {"bucket": bucket, "key": key},
        "part": {"index": 1, "total": 1},
        "code_sha": os.environ.get("GITHUB_SHA", ""),
        "detail": {"operator": operator, "indicator": INDICATORS[slug], "month": f"{month[0]:04d}-{month[1]:02d}",
                   "replaces_sha256": before["sha256"] if before else None, **detail},
    }
    put_new(
        client,
        bucket,
        key[: -len(".json")] + ".receipt.json",
        json.dumps(receipt, indent=2, sort_keys=True).encode(),
        "application/json",
    )
    return "landed_changed" if before else "landed_new"


def plan(held: dict[str, list], today: date, wanted: set[str]) -> dict[str, list]:
    """Entities to request by reason: missing (newest first), open, rotation (one closed month)."""
    out: dict[str, list] = {"missing": [], "open": [], "rotation": [], "closed": []}
    for operator in OPERATORS:
        for slug in INDICATORS:
            for month in reversed(months(today)):
                name = entity(slug, operator, month)
                if wanted and name not in wanted and operator not in wanted:
                    continue
                rows = held.get(name) or []
                reason = "missing" if not rows else "closed" if closed(rows[-1], month, today) else "open"
                out[reason].append((slug, operator, month))
    if out["closed"]:
        out["rotation"].append(out["closed"][today.toordinal() % len(out["closed"])])
    return out


def landed_all(client, bucket: str, root: str) -> dict[str, list]:
    held: dict[str, list] = {}
    for r in bulk_file.landed(client, bucket, f"{root.rstrip('/')}/{SOURCE}/snapshot/data/"):
        held.setdefault(r["entity"], []).append(r)
    return held


def run_verify(client, bucket: str, root: str, held: dict[str, list]) -> dict[str, Any]:
    present = set()
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{root}/{SOURCE}/"):
        present |= {o["Key"] for o in page.get("Contents", []) if not o["Key"].endswith(".receipt.json")}
    stored = archived(client, bucket, SOURCE)
    problems = [
        r["object"]["key"]
        for rows in held.values()
        for r in rows
        if r["object"]["key"] not in present
        and stored.get(r["sha256"], {}).get("key") != archive_key(SOURCE, r["sha256"], r["object"]["key"])
    ]
    return {"receipts": sum(len(v) for v in held.values()), "missing_data": len(problems), "missing_sample": problems[:10]}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--mode", choices=("inspect", "load", "verify"), required=True)
    parser.add_argument("--filter", default="", help="comma separated entities or operator keys (default: all)")
    parser.add_argument("--budget-minutes", type=float, default=60)
    parser.add_argument("--landing-root", default="01_landing", help="Landing root (a zz_dev_ prefix in Dev trials)")
    parser.add_argument("--max-requests", type=int, default=0, help="stop after this many requests (0: no limit)")
    args = parser.parse_args(argv)
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    held = landed_all(client, bucket, args.landing_root)
    today = datetime.now(UTC).date()
    status = 0
    if args.mode == "verify":
        summary: dict[str, Any] = {"mode": "verify", **run_verify(client, bucket, args.landing_root, held)}
        status = 1 if summary["missing_data"] else 0
    else:
        todo = plan(held, today, {s.strip() for s in args.filter.split(",") if s.strip()})
        summary = {"mode": args.mode, "window": f"{FIRST_MONTH[0]}-{FIRST_MONTH[1]:02d}..{today:%Y-%m}",
                   **{k: len(v) for k, v in todo.items()}}
        summary["open_months"] = [entity(*t).rsplit("__", 1)[1] for t in todo["open"]][:12]
        if args.mode == "load":
            session = requests.Session()
            session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
            deadline = time.monotonic() + args.budget_minutes * 60
            outcomes: Counter = Counter()
            failed: list[dict[str, str]] = []
            requests_made = 0
            for slug, operator, month in todo["missing"] + todo["open"] + todo["rotation"]:
                if time.monotonic() > deadline or (args.max_requests and requests_made >= args.max_requests):
                    outcomes["not_started_budget"] += 1
                    continue
                requests_made += 1
                name = entity(slug, operator, month)
                try:
                    outcomes[land(client, bucket, args.landing_root, session, slug, operator, month, held.get(name, []))] += 1
                except (RuntimeError, ValueError) as exc:  # one month failing must not stop the others
                    failed.append({"entity": name, "error": str(exc)[:200]})
            summary.update(outcomes)
            summary["requests"] = requests_made
            summary["failed"] = failed[:5]
            status = 1 if failed else 0  # a budget stop is resumable: the next run continues with the missing months
    print(json.dumps(summary, sort_keys=True, default=str))
    level = "error" if status else "warning" if summary.get("not_started_budget") else "notice"
    annotate(level, f"entsog landing {args.mode}", summary)
    step_summary(f"## entsog · landing {args.mode}\n\n```json\n{json.dumps(summary, indent=2, default=str)}\n```\n")
    return status


if __name__ == "__main__":
    raise SystemExit(main())
