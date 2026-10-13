#!/usr/bin/env python3
"""NBP Landing: exchange-rate tables A, B, C and gold prices from the NBP Web API.

Route (docs/sources.md, NBP): the Web API (https://api.nbp.pl) is the documented machine
interface; one request may cover at most 93 days, so the entity is one table and one
calendar quarter (``A_2024Q1``, ``GOLD_2024Q1``). The yearly CSV/XLS archives on nbp.pl
are human exports and add only pre-2002 history (backlog).

Layout (docs/landing.md):
``01_landing/nbp/snapshot/data/<TABLE>_<YYYYQn>/<version>__<ingested>.json`` + receipt,
where version = the latest effective date in the response (``YYYYMMDDT000000Z``).

Modes:
  inspect  expected quarters vs Landing (missing / open / current); no downloads
  load     fetch missing quarters and re-fetch open ones (the quarter of today and any
           quarter that ended in the last 10 days); unchanged bytes are not written again
  verify   every receipt has its data file (in Landing or in the Archive)

Polite access: one request at a time, a pause between requests, retries with backoff
on 429/5xx, an identifying User-Agent.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from hashlib import sha256
import json
import os
import time
from typing import Any

import requests

from ingestion.common.r2 import archive_key, archived, get_with_backoff, req, s3
from ingestion.common.report import annotate, step_summary

SOURCE = "nbp"
PREFIX = "01_landing/nbp/snapshot/data/"
API = "https://api.nbp.pl/api"
TABLES = {"A": date(2002, 1, 2), "B": date(2002, 1, 2), "C": date(2002, 1, 2), "GOLD": date(2013, 1, 2)}
OPEN_DAYS = 10
PAUSE_SECONDS = 0.5
USER_AGENT = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"


def quarters(start: date, today: date) -> list[tuple[int, int]]:
    out, year, quarter = [], start.year, (start.month - 1) // 3 + 1
    while (year, quarter) <= (today.year, (today.month - 1) // 3 + 1):
        out.append((year, quarter))
        year, quarter = (year + 1, 1) if quarter == 4 else (year, quarter + 1)
    return out


def window(table: str, year: int, quarter: int, today: date) -> tuple[date, date]:
    first = date(year, 3 * quarter - 2, 1)
    last = (date(year + (quarter == 4), (3 * quarter) % 12 + 1, 1) - timedelta(days=1))
    return max(first, TABLES[table]), min(last, today)


def url(table: str, start: date, end: date) -> str:
    if table == "GOLD":
        return f"{API}/cenyzlota/{start.isoformat()}/{end.isoformat()}/?format=json"
    return f"{API}/exchangerates/tables/{table}/{start.isoformat()}/{end.isoformat()}/?format=json"


def expected(today: date) -> list[dict[str, Any]]:
    items = []
    for table, start in TABLES.items():
        for year, quarter in quarters(start, today):
            first, last = window(table, year, quarter, today)
            quarter_end = date(year + (quarter == 4), (3 * quarter) % 12 + 1, 1) - timedelta(days=1)
            items.append({"entity": f"{table}_{year}Q{quarter}", "table": table, "start": first, "end": last,
                          "open": quarter_end >= today - timedelta(days=OPEN_DAYS),
                          "url": url(table, first, last)})
    return items


def landed(client, bucket: str) -> dict[str, list[dict[str, Any]]]:
    """Receipts in Landing by entity, newest (version, ingested) last."""
    receipts = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=PREFIX):
        receipts += [o["Key"] for o in page.get("Contents", []) if o["Key"].endswith(".receipt.json")]
    out: dict[str, list[dict[str, Any]]] = {}
    for key in receipts:
        body = json.loads(get_with_backoff(client, bucket, key))
        out.setdefault(body["entity"], []).append({**body, "receipt_key": key})
    for rows in out.values():
        rows.sort(key=lambda r: (r["version_token"], r["ingested_at"]))
    return out


def fetch(session: requests.Session, address: str) -> tuple[int, bytes, dict[str, str]]:
    for attempt in range(6):
        try:
            response = session.get(address, timeout=60)
        except (requests.ConnectionError, requests.Timeout):
            response = None
        if response is not None and response.status_code in (200, 404):
            return response.status_code, response.content, dict(response.headers)
        if response is not None and response.status_code not in (429, 500, 502, 503, 504):
            raise RuntimeError(f"NBP API {response.status_code} for {address}: {response.text[:200]}")
        time.sleep(min(60, 5 * 2 ** attempt))
    raise RuntimeError(f"NBP API kept failing: {address}")


def version_of(table: str, body: bytes) -> str:
    rows = json.loads(body)
    dates = [r["data"] if table == "GOLD" else r["effectiveDate"] for r in rows]
    if not dates:
        raise RuntimeError("NBP response without rows")
    return max(dates).replace("-", "") + "T000000Z"


def put_new(client, bucket: str, key: str, body: bytes, content_type: str) -> None:
    """Never overwrite Landing: a key that exists is an error unless the bytes are equal."""
    try:
        head = client.head_object(Bucket=bucket, Key=key)
        if (head.get("Metadata") or {}).get("sha256") == sha256(body).hexdigest():
            return
        raise RuntimeError(f"Landing key exists with different bytes: {key}")
    except client.exceptions.ClientError:
        pass
    client.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type,
                      Metadata={"sha256": sha256(body).hexdigest()})
    if (client.head_object(Bucket=bucket, Key=key).get("Metadata") or {}).get("sha256") != sha256(body).hexdigest():
        raise RuntimeError(f"Landing write could not be verified: {key}")


def run_load(client, bucket: str, items: list[dict[str, Any]], held: dict[str, list], budget: float) -> Counter:
    deadline = time.monotonic() + budget * 60
    outcomes: Counter = Counter()
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
    code_sha = os.environ.get("GITHUB_SHA", "")
    for item in items:
        if item["entity"] in held and not item["open"]:
            outcomes["already_landed"] += 1
            continue
        if time.monotonic() > deadline:
            outcomes["not_started_time_budget"] += 1
            continue
        time.sleep(PAUSE_SECONDS)
        status, body, headers = fetch(session, item["url"])
        if status == 404:
            outcomes["no_data_in_window"] += 1
            continue
        digest = sha256(body).hexdigest()
        previous = (held.get(item["entity"]) or [None])[-1]
        if previous and previous["sha256"] == digest:
            outcomes["unchanged"] += 1
            continue
        version = version_of(item["table"], body)
        ingested = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        key = f"{PREFIX}{item['entity']}/{version}__{ingested}.json"
        put_new(client, bucket, key, body, "application/json")
        receipt = {"schema_version": 2, "kind": "nbp_landing", "source_id": SOURCE, "entity": item["entity"],
                   "table": item["table"], "content": "snapshot", "area": "data", "source_kind": "nbp_api_json",
                   "window": {"start": item["start"].isoformat(), "end": item["end"].isoformat()},
                   "version_token": version, "ingested_at": ingested, "source_url": item["url"],
                   "sha256": digest, "size_bytes": len(body), "object": {"bucket": bucket, "key": key},
                   "http": {"status": status, "content_type": headers.get("Content-Type"),
                            "last_modified": headers.get("Last-Modified"), "etag": headers.get("ETag")},
                   "replaces_sha256": previous["sha256"] if previous else None, "code_sha": code_sha}
        put_new(client, bucket, key[: -len(".json")] + ".receipt.json",
                json.dumps(receipt, indent=2, sort_keys=True).encode(), "application/json")
        outcomes["landed_new" if not previous else "landed_changed"] += 1
    return outcomes


def run_verify(client, bucket: str, held: dict[str, list]) -> dict[str, Any]:
    present = set()
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=PREFIX):
        present |= {o["Key"] for o in page.get("Contents", []) if not o["Key"].endswith(".receipt.json")}
    stored = archived(client, bucket, SOURCE)
    problems = []
    for rows in held.values():
        for r in rows:
            key = r["object"]["key"]
            if key not in present and stored.get(r["sha256"], {}).get("key") != archive_key(SOURCE, r["sha256"], key):
                problems.append(key)
    receipted = {r["object"]["key"] for rows in held.values() for r in rows}
    return {"receipts": sum(len(v) for v in held.values()), "missing_data": len(problems),
            "missing_sample": problems[:10], "data_without_receipt": sorted(present - receipted)[:10]}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--mode", choices=("inspect", "load", "verify"), required=True)
    parser.add_argument("--filter", default="", help="comma separated tables (A,B,C,GOLD)")
    parser.add_argument("--budget-minutes", type=float, default=300)
    args = parser.parse_args(argv)
    client, bucket = s3(), req("R2_LANDING_BUCKET")
    today = datetime.now(timezone.utc).date()
    tables = {t.strip().upper() for t in args.filter.split(",") if t.strip()}
    items = [i for i in expected(today) if not tables or i["table"] in tables]
    held = landed(client, bucket)

    if args.mode == "verify":
        summary = {"mode": "verify", **run_verify(client, bucket, held)}
        status = 1 if summary["missing_data"] else 0
    else:
        missing = [i["entity"] for i in items if i["entity"] not in held]
        summary = {"mode": args.mode, "expected": len(items), "landed_entities": len(held),
                   "missing": len(missing), "missing_sample": missing[:10],
                   "open": [i["entity"] for i in items if i["open"]]}
        status = 0
        if args.mode == "load":
            outcomes = run_load(client, bucket, items, held, args.budget_minutes)
            summary.update(outcomes)
            status = 2 if outcomes["not_started_time_budget"] else 0
    print(json.dumps(summary, sort_keys=True, default=str))
    annotate("notice", f"nbp landing {args.mode}", summary)
    step_summary(f"## nbp · landing {args.mode}\n\n```json\n{json.dumps(summary, indent=2, default=str)}\n```\n")
    return status


if __name__ == "__main__":
    raise SystemExit(main())
