#!/usr/bin/env python3
"""NFZ waiting times (API Terminy Leczenia v1.4) Landing: a monthly snapshot of every queue, by case and voivodeship.

Route (docs/sources/nfz_waiting_times.md; register PLS-015). NFZ publishes no bulk file of the waiting-times
register; the API ``https://apinfz.nfz.gov.pl/app-itl-api-pcus/queues`` (version 1.4, live since 10 July 2026)
answers the queues of one case (1 stable, 2 urgent) and one voivodeship (NFZ codes 01-16) at most 25 records a
page. The API keeps no history: what it serves is the current state (the forecast time to the benefit as at
``date-situation-as-at``, the month-end waiting-list counts of the month in ``update``). Each run therefore takes a
dated snapshot; nothing is backfilled.

Entity = one case and voivodeship in one snapshot month: ``queues_<YYYYMM>_case<c>_<pp>`` (32 a month), so every
month is its own entity and its newest copy stays in Bronze while the next month is added (the latest-version
rule of #822 keeps the newest read of each month). A month's entities are read once; a later run in the same
month reads only the entities still missing (resumable after the time budget or a failure).

One Landing file per entity: a ZIP whose members are the exact answer bodies (``page_<nnnn>.json``), in page
order, so the entity is complete or not there. The API lists queues in display order (by forecast time), which
moves while the entity is read; a read whose record count differs from ``meta.count`` or that holds a queue id
twice is read again (at most READS times; still inconsistent, the entity fails and the next run reads it again);
the receipt records the counts and reads of the copy landed.

Polite access (terms v1.3 section III: at most 10 requests per second per IP): one request at a time,
PAUSE_SECONDS between requests, an identifying User-Agent, timeouts, bounded retries with backoff on 429, 5xx and
timeouts (Retry-After honoured); FAIL_STREAK entities failing in a row stop the run (the API is down: never
hammered). Time-budgeted: no entity is started in the last MARGIN_MINUTES.

Layout (docs/landing.md): ``<root>/nfz_waiting_times/snapshot/data/<entity>/<version>__<ingested>.zip`` + receipt;
version = the time the entity's read started (the API gives no update time; ``meta.date-modified`` is the time of
the request).

Modes: inspect (this month's entities vs Landing: missing / current; no requests), load (missing entities),
verify (every receipt has its data file, in Landing or the Archive).
"""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import UTC, datetime
from hashlib import sha256
import io
import json
import os
import time
from typing import Any
import zipfile

import requests

from ingestion.common.r2 import archive_key, archived, get_with_backoff, req, s3
from ingestion.common.report import annotate, step_summary
from ingestion.to_landing.world_bank_icp.extract import USER_AGENT, put_new

SOURCE = "nfz_waiting_times"
API = "https://apinfz.nfz.gov.pl/app-itl-api-pcus"
QUERY = "format=json&api-version=1.4"
CASES = (1, 2)  # 1 stable, 2 urgent (swagger v1.4, parameter case)
PROVINCES = tuple(f"{n:02d}" for n in range(1, 17))  # NFZ voivodeship codes 01 dolnośląskie ... 16 zachodniopomorskie
PAGE_LIMIT = 25  # the API's maximum (swagger: limit 1-25)
PAUSE_SECONDS = 0.3
TIMEOUT_SECONDS = 90
RETRY_DELAYS = (5, 15, 45, 120)  # at most about 3 minutes per request
READS = 3  # reads of one entity until its records match meta.count with no queue id twice
FAIL_STREAK = 3
MARGIN_MINUTES = 10


class Unavailable(RuntimeError):
    """The API kept failing (5xx, timeouts) after the bounded retries."""


def prefix(root: str) -> str:
    return f"{root.rstrip('/')}/{SOURCE}/snapshot/data/"


def entity_of(month: str, case: int, province: str) -> str:
    return f"queues_{month}_case{case}_{province}"


def entities(month: str) -> list[tuple[str, int, str]]:
    return [(entity_of(month, c, p), c, p) for c in CASES for p in PROVINCES]


def page_url(case: int, province: str, page: int) -> str:
    return f"{API}/queues?case={case}&province={province}&page={page}&limit={PAGE_LIMIT}&{QUERY}"


def fetch(session: requests.Session, address: str) -> tuple[bytes, Any]:
    """GET one answer: (exact body, parsed JSON). 429, 5xx and network errors are retried with backoff; another
    status fails at once; still failing → Unavailable."""
    last = ""
    for attempt, delay in enumerate((0, *RETRY_DELAYS)):
        time.sleep(PAUSE_SECONDS + delay)
        try:
            response = session.get(address, timeout=TIMEOUT_SECONDS)
        except (requests.ConnectionError, requests.Timeout) as exc:
            last = type(exc).__name__
            continue
        if response.status_code == 200:
            try:
                return response.content, response.json()
            except ValueError as exc:
                raise RuntimeError(f"NFZ API answered no JSON for {address}: {response.content[:200]!r}") from exc
        last = f"HTTP {response.status_code} {response.text[:160]}"
        if response.status_code not in (429, 500, 502, 503, 504):
            raise RuntimeError(f"NFZ API {last} for {address}")
        retry_after = response.headers.get("Retry-After", "")
        if retry_after.isdigit() and attempt < len(RETRY_DELAYS):
            time.sleep(min(600, int(retry_after)))
    raise Unavailable(f"NFZ API kept failing ({last}) for {address}")


def read_entity(session: requests.Session, case: int, province: str) -> tuple[list[bytes], dict[str, Any]]:
    """Every page of one case and voivodeship: (exact bodies, counts)."""
    bodies, ids, count, last_count = [], [], None, None
    page, pages = 1, 1
    while page <= pages:
        body, parsed = fetch(session, page_url(case, province, page))
        meta, data = parsed.get("meta") or {}, parsed.get("data")
        if not isinstance(data, list) or not isinstance(meta.get("count"), int):
            raise RuntimeError(f"case {case} province {province} page {page}: no data list or meta.count")
        if count is None:
            count = meta["count"]
            pages = max(1, -(-count // PAGE_LIMIT))
        last_count = meta["count"]
        bodies.append(body)
        ids += [str(item.get("id")) for item in data]
        page += 1
    repeated = sum(n - 1 for n in Counter(ids).values() if n > 1)
    return bodies, {
        "count": count,
        "count_last_page": last_count,
        "pages": len(bodies),
        "records": len(ids),
        "distinct_ids": len(set(ids)),
        "repeated_ids": repeated,
        "consistent": count == last_count == len(ids) and repeated == 0,
    }


def bundle(bodies: list[bytes]) -> bytes:
    """A ZIP of the exact answer bodies, page order; fixed member dates so equal answers give equal bytes."""
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as archive:
        for n, body in enumerate(bodies, start=1):
            info = zipfile.ZipInfo(f"page_{n:04d}.json", date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, body)
    return out.getvalue()


def landed(client, bucket: str, root: str) -> dict[str, list[dict[str, Any]]]:
    """Receipts in Landing by entity, newest ingest last."""
    out: dict[str, list[dict[str, Any]]] = {}
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix(root)):
        for o in page.get("Contents", []):
            if o["Key"].endswith(".receipt.json"):
                body = {**json.loads(get_with_backoff(client, bucket, o["Key"])), "receipt_key": o["Key"]}
                out.setdefault(body["entity"], []).append(body)
    for rows in out.values():
        rows.sort(key=lambda r: r["ingested_at"])
    return out


def land(client, bucket: str, root: str, session, entity: str, case: int, province: str) -> dict[str, Any]:
    version = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    reads = 0
    while True:
        reads += 1
        bodies, counts = read_entity(session, case, province)
        if counts["consistent"]:
            break
        if reads >= READS:  # never lands a read with a queue twice or missing; the next run reads it again
            raise RuntimeError(f"{entity}: still inconsistent after {reads} reads: {json.dumps(counts)}")
    counts["reads"] = reads
    body = bundle(bodies)
    digest = sha256(body).hexdigest()
    ingested = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    key = f"{prefix(root)}{entity}/{version}__{ingested}.zip"
    put_new(client, bucket, key, body, "application/zip")
    receipt = {
        "schema_version": 2,
        "kind": "nfz_waiting_times_landing",
        "source_id": SOURCE,
        "entity": entity,
        "case": case,
        "province": province,
        "content": "snapshot",
        "area": "data",
        "source_kind": "nfz_itl_api_v1_4_json_pages_zip",
        "version_token": version,
        "version_basis": "read started (the API gives no update time)",
        "ingested_at": ingested,
        "source_url": page_url(case, province, 1),
        "sha256": digest,
        "size_bytes": len(body),
        "object": {"bucket": bucket, "key": key},
        "detail": counts,
        "code_sha": os.environ.get("GITHUB_SHA", ""),
    }
    put_new(
        client,
        bucket,
        key[: -len(".zip")] + ".receipt.json",
        json.dumps(receipt, indent=2, sort_keys=True).encode(),
        "application/json",
    )
    return counts


def run_verify(client, bucket: str, root: str, held: dict[str, list]) -> dict[str, Any]:
    present = set()
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix(root)):
        present |= {o["Key"] for o in page.get("Contents", []) if not o["Key"].endswith(".receipt.json")}
    stored = archived(client, bucket, SOURCE)
    problems = [
        r["object"]["key"]
        for rows in held.values()
        for r in rows
        if r["object"]["key"] not in present
        and stored.get(r["sha256"], {}).get("key") != archive_key(SOURCE, r["sha256"], r["object"]["key"])
    ]
    return {
        "receipts": sum(len(v) for v in held.values()),
        "missing_data": len(problems),
        "missing_sample": problems[:10],
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--mode", choices=("inspect", "load", "verify"), required=True)
    parser.add_argument("--filter", default="", help="comma separated '<case>_<province>' (e.g. 1_07); default all")
    parser.add_argument("--budget-minutes", type=float, default=300)
    parser.add_argument("--landing-root", default="01_landing", help="Landing root (a zz_dev_ prefix in Dev trials)")
    parser.add_argument("--month", default="", help="snapshot month YYYYMM (default: the current month, UTC)")
    args = parser.parse_args(argv)
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    held = landed(client, bucket, args.landing_root)
    if args.mode == "verify":
        summary: dict[str, Any] = {"mode": "verify", **run_verify(client, bucket, args.landing_root, held)}
        status = 1 if summary["missing_data"] else 0
    else:
        month = args.month or datetime.now(UTC).strftime("%Y%m")
        wanted = {s.strip() for s in args.filter.split(",") if s.strip()}
        todo = [(e, c, p) for e, c, p in entities(month) if not wanted or f"{c}_{p}" in wanted]
        missing = [t for t in todo if not held.get(t[0])]
        summary = {
            "mode": args.mode,
            "snapshot_month": month,
            "entities": len(todo),
            "current": len(todo) - len(missing),
            "missing": len(missing),
            "months_held": len({e.split("_")[1] for e in held}),
        }
        status = 0
        if args.mode == "load":
            session = requests.Session()
            session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
            deadline = time.monotonic() + (args.budget_minutes - MARGIN_MINUTES) * 60
            outcomes: Counter = Counter()
            failed: list[dict[str, str]] = []
            streak = 0
            records, pages = 0, 0
            for entity, case, province in missing:
                if time.monotonic() > deadline or streak >= FAIL_STREAK:
                    outcomes["not_started_time_budget" if streak < FAIL_STREAK else "not_started_api_down"] += 1
                    continue
                try:
                    counts = land(client, bucket, args.landing_root, session, entity, case, province)
                except (RuntimeError, requests.RequestException) as exc:  # one entity failing must not stop the others
                    failed.append({"entity": entity, "error": str(exc)[:200]})
                    streak += 1
                    continue
                streak = 0
                outcomes["landed"] += 1
                records += counts["records"]
                pages += counts["pages"]
            summary.update(outcomes)
            summary.update({"records": records, "pages": pages, "failed": failed[:5]})
            status = 1 if failed else 2 if outcomes["not_started_time_budget"] else 0
    print(json.dumps(summary, sort_keys=True, default=str))
    annotate("error" if status == 1 else "notice", f"{SOURCE} landing {args.mode}", summary)
    step_summary(f"## {SOURCE} · landing {args.mode}\n\n```json\n{json.dumps(summary, indent=2, default=str)}\n```\n")
    return status


if __name__ == "__main__":
    raise SystemExit(main())
