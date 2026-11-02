#!/usr/bin/env python3
"""World Bank ICP Landing: the ICP 2021 database (source 90) from the World Bank API v2.

Route (docs/sources.md, World Bank ICP): DataBank publishes no bulk file for ICP, so the
entity is one series of source 90 (48 series, about 1.2 million observations in all) for every
economy and year: ``/v2/sources/90/country/all/series/<id>/time/all?format=json&per_page=50000``.
A series larger than one page lands one file per page (``series_<id>_p<n>``); usually one.
Version = the source's ``lastupdated`` date (``YYYYMMDDT000000Z``), so a series is fetched
again only when the World Bank updates the database.

Layout (docs/landing.md):
``<root>/world_bank_icp/snapshot/data/series_<id>[_p<n>]/<version>__<ingested>.json`` + receipt.

Modes:
  inspect  series of the source vs Landing (missing / stale / current); two small requests
  load     fetch missing, then stale series; unchanged bytes are not written again
  verify   every receipt has its data file (in Landing or in the Archive)

Polite access: one request at a time, a pause between requests, bounded retries with backoff
on 429/5xx and timeouts, an identifying User-Agent.
"""

from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from hashlib import sha256
import json
import os
import time
from typing import Any

import requests

from ingestion.common.r2 import archive_key, archived, get_with_backoff, req, s3
from ingestion.common.report import annotate, step_summary

SOURCE = "world_bank_icp"
SOURCE_ID = 90
API = "https://api.worldbank.org/v2"
PER_PAGE = 50000
PAUSE_SECONDS = 1.0
TIMEOUT_SECONDS = 180
RETRIES = 5
USER_AGENT = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"


def prefix(root: str) -> str:
    return f"{root.rstrip('/')}/{SOURCE}/snapshot/data/"


def fetch(session: requests.Session, address: str) -> tuple[bytes, dict[str, str]]:
    """GET with bounded retries: network errors, 429 and 5xx back off; other statuses fail at once."""
    for attempt in range(RETRIES):
        try:
            response = session.get(address, timeout=TIMEOUT_SECONDS)
        except (requests.ConnectionError, requests.Timeout):
            response = None
        if response is not None and response.status_code == 200:
            return response.content, dict(response.headers)
        if response is not None and response.status_code not in (429, 500, 502, 503, 504):
            raise RuntimeError(f"World Bank API {response.status_code} for {address}: {response.text[:200]}")
        time.sleep(min(120, 5 * 2**attempt))
    raise RuntimeError(f"World Bank API kept failing: {address}")


def as_json(body: bytes, address: str) -> Any:
    """The API answers errors with HTTP 200 and an XML or JSON message: fail loudly on those."""
    try:
        parsed = json.loads(body)
    except ValueError as exc:
        raise RuntimeError(f"World Bank API returned no JSON for {address}: {body[:200]!r}") from exc
    if isinstance(parsed, list) and parsed and isinstance(parsed[0], dict) and "message" in parsed[0]:
        raise RuntimeError(f"World Bank API error for {address}: {json.dumps(parsed[0])[:300]}")
    return parsed


def catalogue(session: requests.Session) -> tuple[str, list[str]]:
    """The source's version (lastupdated) and its series ids."""
    address = f"{API}/sources/{SOURCE_ID}?format=json"
    meta = as_json(fetch(session, address)[0], address)
    version = meta[1][0]["lastupdated"].replace("-", "") + "T000000Z"
    time.sleep(PAUSE_SECONDS)
    address = f"{API}/sources/{SOURCE_ID}/series?format=json&per_page=1000"
    listing = as_json(fetch(session, address)[0], address)
    if int(listing["total"]) > 1000:
        raise RuntimeError(f"ICP lists {listing['total']} series, more than one listing page")
    series = [v["id"] for concept in listing["source"][0]["concept"] for v in concept["variable"]]
    if len(series) != int(listing["total"]):
        raise RuntimeError(f"ICP series listing has {len(series)} ids, total says {listing['total']}")
    return version, series


def series_url(series_id: str, page: int) -> str:
    return (
        f"{API}/sources/{SOURCE_ID}/country/all/series/{series_id}/time/all?format=json&per_page={PER_PAGE}&page={page}"
    )


def landed(client, bucket: str, root: str) -> dict[str, list[dict[str, Any]]]:
    """Receipts in Landing by series id, newest (version, ingested) last."""
    receipts = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix(root)):
        receipts += [o["Key"] for o in page.get("Contents", []) if o["Key"].endswith(".receipt.json")]

    def read(key: str) -> dict[str, Any]:
        return {**json.loads(get_with_backoff(client, bucket, key)), "receipt_key": key}

    out: dict[str, list[dict[str, Any]]] = {}
    with ThreadPoolExecutor(max_workers=8) as pool:  # R2 throttles larger bursts of reads
        for body in pool.map(read, receipts):
            out.setdefault(body["series_id"], []).append(body)
    for rows in out.values():
        rows.sort(key=lambda r: (r["version_token"], r["ingested_at"], r["page"]))
    return out


def state(rows: list[dict[str, Any]] | None, version: str) -> str:
    if not rows:
        return "missing"
    newest = rows[-1]["version_token"]
    complete = {r["page"] for r in rows if r["version_token"] == newest} == set(range(1, rows[-1]["pages"] + 1))
    return "current" if newest >= version and complete else "stale"


def put_new(client, bucket: str, key: str, body: bytes, content_type: str) -> None:
    """Never overwrite Landing: a key that exists is an error unless the bytes are equal."""
    digest = sha256(body).hexdigest()
    try:
        head = client.head_object(Bucket=bucket, Key=key)
        if (head.get("Metadata") or {}).get("sha256") == digest:
            return
        raise RuntimeError(f"Landing key exists with different bytes: {key}")
    except client.exceptions.ClientError:
        pass
    client.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type, Metadata={"sha256": digest})
    if (client.head_object(Bucket=bucket, Key=key).get("Metadata") or {}).get("sha256") != digest:
        raise RuntimeError(f"Landing write could not be verified: {key}")


def land_series(
    client, bucket: str, root: str, session: requests.Session, series_id: str, version: str, held: list | None
) -> Counter:
    """Every page of one series; a page whose bytes equal the newest landed copy is skipped."""
    outcomes: Counter = Counter()
    previous = {r["page"]: r for r in held or []}  # newest receipt per page (rows are sorted)
    page, pages = 1, 1
    while page <= pages:
        time.sleep(PAUSE_SECONDS)
        address = series_url(series_id, page)
        body, headers = fetch(session, address)
        parsed = as_json(body, address)
        pages, total = int(parsed["pages"]), int(parsed["total"])
        digest = sha256(body).hexdigest()
        before = previous.get(page)
        if before and before["sha256"] == digest:
            outcomes["unchanged_pages"] += 1
            page += 1
            continue
        entity = f"series_{series_id}" if pages == 1 else f"series_{series_id}_p{page}"
        ingested = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        key = f"{prefix(root)}{entity}/{version}__{ingested}.json"
        put_new(client, bucket, key, body, "application/json")
        receipt = {
            "schema_version": 2,
            "kind": "world_bank_icp_landing",
            "source_id": SOURCE,
            "entity": entity,
            "series_id": series_id,
            "page": page,
            "pages": pages,
            "observations": total,
            "content": "snapshot",
            "area": "data",
            "source_kind": "world_bank_api_v2_json",
            "version_token": version,
            "ingested_at": ingested,
            "source_url": address,
            "sha256": digest,
            "size_bytes": len(body),
            "object": {"bucket": bucket, "key": key},
            "http": {"content_type": headers.get("Content-Type"), "last_modified": headers.get("Last-Modified")},
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
        outcomes["landed_new_pages" if not before else "landed_changed_pages"] += 1
        outcomes["bytes"] += len(body)
        page += 1
    return outcomes


def run_verify(client, bucket: str, root: str, held: dict[str, list]) -> dict[str, Any]:
    present = set()
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix(root)):
        present |= {o["Key"] for o in page.get("Contents", []) if not o["Key"].endswith(".receipt.json")}
    stored = archived(client, bucket, SOURCE)
    problems = []
    for rows in held.values():
        for r in rows:
            key = r["object"]["key"]
            if key not in present and stored.get(r["sha256"], {}).get("key") != archive_key(SOURCE, r["sha256"], key):
                problems.append(key)
    receipted = {r["object"]["key"] for rows in held.values() for r in rows}
    return {
        "receipts": sum(len(v) for v in held.values()),
        "missing_data": len(problems),
        "missing_sample": problems[:10],
        "data_without_receipt": sorted(present - receipted)[:10],
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--mode", choices=("inspect", "load", "verify"), required=True)
    parser.add_argument("--filter", default="", help="comma separated series ids")
    parser.add_argument("--budget-minutes", type=float, default=300)
    parser.add_argument("--landing-root", default="01_landing", help="Landing root (a zz_dev_ prefix in Dev trials)")
    args = parser.parse_args(argv)
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    held = landed(client, bucket, args.landing_root)

    if args.mode == "verify":
        summary: dict[str, Any] = {"mode": "verify", **run_verify(client, bucket, args.landing_root, held)}
        status = 1 if summary["missing_data"] else 0
    else:
        session = requests.Session()
        session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
        version, series = catalogue(session)
        wanted = {s.strip() for s in args.filter.split(",") if s.strip()}
        series = [s for s in series if not wanted or s in wanted]
        states = {s: state(held.get(s), version) for s in series}
        counts = Counter(states.values())
        summary = {
            "mode": args.mode,
            "source_version": version,
            "series": len(series),
            **{k: counts.get(k, 0) for k in ("current", "stale", "missing")},
            "missing_sample": [s for s, v in states.items() if v == "missing"][:10],
        }
        status = 0
        if args.mode == "load":
            deadline = time.monotonic() + args.budget_minutes * 60
            outcomes: Counter = Counter()
            order = [s for s in series if states[s] == "missing"] + [s for s in series if states[s] == "stale"]
            for series_id in order:
                if time.monotonic() > deadline:
                    outcomes["not_started_time_budget"] += 1
                    continue
                outcomes.update(
                    land_series(client, bucket, args.landing_root, session, series_id, version, held.get(series_id))
                )
                outcomes["series_done"] += 1
            summary.update(outcomes)
            status = 2 if outcomes["not_started_time_budget"] else 0
    print(json.dumps(summary, sort_keys=True, default=str))
    annotate("notice", f"world_bank_icp landing {args.mode}", summary)
    step_summary(
        f"## world_bank_icp · landing {args.mode}\n\n```json\n{json.dumps(summary, indent=2, default=str)}\n```\n"
    )
    return status


if __name__ == "__main__":
    raise SystemExit(main())
