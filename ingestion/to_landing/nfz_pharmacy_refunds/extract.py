#!/usr/bin/env python3
"""NFZ pharmacy refunds Landing: monthly reimbursement totals of the NFZ statistics API (register PLS-013).

Route (docs/sources/nfz_pharmacy_refunds.md): NFZ publishes no bulk file; the API ``app-stat-api-ra`` (v1.0) is
the source. Entity = one area (Poland unfiltered, or one of the 16 voivodeships by NFZ code ``province=01..16``) x
one ATC selection (no filter, or one of the 14 anatomical main groups, requested by the group name of ``/atc``):
every page of ``/monthly-provisions`` for it (25 rows a page, the API's maximum), one Landing file per page,
``<version>__<ingested>__part-<n>.json``, all parts of one download sharing ``ingested``. Version = ``end-date`` of
``/available-periods`` (``YYYYMMDDT000000Z``): a run whose version is held makes that one request and nothing else.
Every answer carries the request time in ``meta.date-modified``, so a new download is compared by the SHA-256 of
its ``data`` rows (``data_sha256`` in the receipts), not by its bytes; equal rows are not written again.

Guards before anything is written: every page answers the filters asked (``links.self`` echoes the province code
and the ATC name), the page numbers and ``meta.count`` agree, the rows equal ``meta.count`` and (year, month) is unique;
otherwise the entity fails.

Layout (docs/landing.md):
``<root>/nfz_pharmacy_refunds/snapshot/data/<area>_<atc>/<version>__<ingested>__part-<n>.json`` + one receipt per
part, written after the part. An entity counts as landed only when every part of its newest download has a receipt.
Modes: inspect (missing / stale / current, one request), load (missing first, then stale; time-budgeted), verify
(every receipt has its file, in Landing or the Archive). Polite access: one request at a time, a pause of about
1 s, bounded retries with backoff on 429/5xx/timeouts (the ICP extractor's ``fetch``; an HTTP 500 ``Maintenance``
answer and the 502 of a slow query are such cases); two entities failing in a row stop the run. A new query of a
large selection takes about 10 s at the API (Dev probe 43317668158), so a full load takes hours, not minutes.
"""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import UTC, datetime
from hashlib import sha256
import json
import os
import time
from typing import Any
from urllib.parse import parse_qs, urlencode, urlsplit

from botocore.exceptions import ClientError
import requests

from ingestion.common.r2 import archive_key, archived, get_with_backoff, req, s3
from ingestion.common.report import annotate, step_summary
from ingestion.to_landing.world_bank_icp.extract import USER_AGENT, fetch, put_new

SOURCE = "nfz_pharmacy_refunds"
API = "https://api.nfz.gov.pl/app-stat-api-ra"
LIMIT = 25  # the API's documented maximum page size
PAUSE_SECONDS = 1.0
AREAS = ["PL"] + [f"{i:02d}" for i in range(1, 17)]  # PL = no province filter; NFZ voivodeship codes 01..16
ATC = ["A", "B", "C", "D", "G", "H", "J", "L", "M", "N", "P", "R", "S", "V"]  # the 14 groups of /atc
TOTAL = "T"  # entity suffix of the selection without an ATC filter
FIELDS = {"year", "month", "quantity", "refund", "donation", "contribution-of-patient", "value"}


def entities() -> list[str]:
    return [f"{area}_{atc}" for area in AREAS for atc in [TOTAL, *ATC]]


def prefix(root: str) -> str:
    return f"{root.rstrip('/')}/{SOURCE}/snapshot/data/"


def get_json(session: requests.Session, path: str, **params: Any) -> tuple[bytes, Any, str]:
    time.sleep(PAUSE_SECONDS)
    address = f"{API}/{path}?{urlencode({**params, 'format': 'json', 'api-version': '1.0'})}"
    body, _ = fetch(session, address)
    try:
        parsed = json.loads(body)
    except ValueError as exc:
        raise RuntimeError(f"NFZ API returned no JSON for {address}: {body[:200]!r}") from exc
    if isinstance(parsed, dict) and parsed.get("errors"):
        raise RuntimeError(f"NFZ API error for {address}: {json.dumps(parsed['errors'])[:300]}")
    return body, parsed, address


def version(session: requests.Session) -> str:
    """The source's version: end-date of /available-periods as YYYYMMDDT000000Z."""
    _, periods, _ = get_json(session, "available-periods")
    end = str((periods.get("data") or periods)["end-date"])[:10]
    return datetime.strptime(end, "%Y-%m-%d").strftime("%Y%m%dT000000Z")


def atc_names(session: requests.Session) -> dict[str, str]:
    """ATC group code -> the name the data resources take as filter; fails when the groups are not the declared 14."""
    _, listing, address = get_json(session, "atc", page=1, limit=LIMIT)
    names = {item["attributes"]["code"]: item["attributes"]["name"] for item in listing["data"]}
    if sorted(names) != sorted(ATC) or int(listing["meta"]["count"]) != len(ATC):
        raise RuntimeError(f"NFZ /atc lists {sorted(names)}, declared {ATC}: {address}")
    return names


def put(client, bucket: str, key: str, body: bytes) -> None:
    """put_new (never overwrite) with bounded retries: R2 can answer a write with 503 ServiceUnavailable ("reduce
    your concurrent request rate for the same object", Dev trial 43312973452) after the client's own retries."""
    for attempt in range(5):
        try:
            return put_new(client, bucket, key, body, "application/json")
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") not in ("ServiceUnavailable", "SlowDown") or attempt == 4:
                raise
            time.sleep(10 * 2**attempt)


def landed(client, bucket: str, root: str) -> dict[str, list[dict[str, Any]]]:
    """Receipts in Landing by entity, oldest download first."""
    out: dict[str, list[dict[str, Any]]] = {}
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix(root)):
        for o in page.get("Contents", []):
            if o["Key"].endswith(".receipt.json"):
                body = {**json.loads(get_with_backoff(client, bucket, o["Key"])), "receipt_key": o["Key"]}
                out.setdefault(body["entity"], []).append(body)
    for rows in out.values():
        rows.sort(key=lambda r: (r["ingested_at"], r["part"]["index"]))
    return out


def newest_complete(rows: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    """The receipts of the newest download whose parts are all there, else []."""
    downloads: dict[str, list[dict[str, Any]]] = {}
    for r in rows or []:
        downloads.setdefault(r["ingested_at"], []).append(r)
    for stamp in sorted(downloads, reverse=True):
        parts = downloads[stamp]
        if sorted(p["part"]["index"] for p in parts) == list(range(1, parts[0]["part"]["total"] + 1)):
            return parts
    return []


def state(rows: list[dict[str, Any]] | None, token: str) -> str:
    held = newest_complete(rows)
    if not held:
        return "missing"
    return "current" if held[0]["version_token"] >= token else "stale"


def filters(entity: str, names: dict[str, str]) -> dict[str, str]:
    area, atc = entity.split("_")
    out = {} if area == "PL" else {"province": area}
    return out if atc == TOTAL else {**out, "atc": names[atc]}


def download(session: requests.Session, entity: str, asked: dict[str, str]) -> list[tuple[bytes, str]]:
    """Every page of the entity, checked; (body, address) per page."""
    pages, rows, count, total = [], [], None, 1
    page = 1
    while page <= total:
        body, parsed, address = get_json(session, "monthly-provisions", **asked, page=page, limit=LIMIT)
        meta = parsed["meta"]
        echoed = {k: v[0] for k, v in parse_qs(urlsplit((parsed.get("links") or {}).get("self") or "").query).items()}
        # the link echoes the filters (UTF-8 percent-encoded, Dev probe 43317668158); an unknown province code is
        # answered, not refused, so the echo is the proof that the filter was applied
        if (echoed.get("province"), echoed.get("atc")) != (asked.get("province"), asked.get("atc")) or int(
            meta["page"]
        ) != page:
            raise RuntimeError(f"{entity}: page {page} answers {echoed} page {meta['page']}, asked {asked}")
        if count is None:
            count = int(meta["count"])
            total = max(1, -(-count // LIMIT))
        elif int(meta["count"]) != count:
            raise RuntimeError(f"{entity}: meta.count changed between pages ({count}, {meta['count']})")
        if any(set(r) != FIELDS for r in parsed["data"]):
            raise RuntimeError(f"{entity}: unknown record layout {sorted(set().union(*map(set, parsed['data'])))}")
        rows += parsed["data"]
        pages.append((body, address))
        page += 1
    keys = {(r["year"], r["month"]) for r in rows}
    if len(rows) != count or len(keys) != count:
        raise RuntimeError(f"{entity}: {len(rows)} rows, {len(keys)} distinct months, meta.count {count}")
    return pages


def data_digest(pages: list[tuple[bytes, str]]) -> str:
    rows = sorted((r for body, _ in pages for r in json.loads(body)["data"]), key=lambda r: (r["year"], r["month"]))
    return sha256(json.dumps(rows, sort_keys=True).encode()).hexdigest()


def land_entity(client, bucket, root, session, entity, token, names, held) -> str:
    pages = download(session, entity, filters(entity, names))
    digest = data_digest(pages)
    before = newest_complete(held)
    if before and before[0]["data_sha256"] == digest:
        return "unchanged_data"
    ingested = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    area, atc = entity.split("_")
    for index, (body, address) in enumerate(pages, start=1):
        key = f"{prefix(root)}{entity}/{token}__{ingested}__part-{index}.json"
        put(client, bucket, key, body)
        receipt = {
            "schema_version": 2,
            "kind": "nfz_pharmacy_refunds_landing",
            "source_id": SOURCE,
            "entity": entity,
            "area_code": area,
            "atc_code": "_T" if atc == TOTAL else atc,
            "atc_name": names.get(atc),
            "content": "snapshot",
            "area": "data",
            "source_kind": "nfz_api_json",
            "version_token": token,
            "ingested_at": ingested,
            "source_url": address,
            "sha256": sha256(body).hexdigest(),
            "size_bytes": len(body),
            "object": {"bucket": bucket, "key": key},
            "part": {"index": index, "total": len(pages)},
            "data_sha256": digest,
            "replaces_data_sha256": before[0]["data_sha256"] if before else None,
            "code_sha": os.environ.get("GITHUB_SHA", ""),
        }
        receipt_body = json.dumps(receipt, indent=2, sort_keys=True).encode()
        put(client, bucket, key[: -len(".json")] + ".receipt.json", receipt_body)
    return "landed_changed" if before else "landed_new"


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
    incomplete = sorted(e for e in entities() if held.get(e) and not newest_complete(held[e]))
    return {
        "receipts": sum(len(v) for v in held.values()),
        "missing_data": len(problems),
        "missing_sample": problems[:10],
        "entities_without_complete_download": incomplete[:10],
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--mode", choices=("inspect", "load", "verify"), required=True)
    parser.add_argument("--filter", default="", help="comma separated entities, e.g. PL_T,07_C (default: all 255)")
    parser.add_argument("--budget-minutes", type=float, default=120)
    parser.add_argument("--landing-root", default="01_landing", help="Landing root (a zz_dev_ prefix in Dev trials)")
    args = parser.parse_args(argv)
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    held = landed(client, bucket, args.landing_root)
    status = 0
    if args.mode == "verify":
        summary: dict[str, Any] = {"mode": "verify", **run_verify(client, bucket, args.landing_root, held)}
        status = 1 if summary["missing_data"] else 0
    else:
        session = requests.Session()
        session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
        token = version(session)
        wanted = {s.strip() for s in args.filter.split(",") if s.strip()}
        todo = [e for e in entities() if not wanted or e in wanted]
        states = {e: state(held.get(e), token) for e in todo}
        summary = {"mode": args.mode, "source_version": token, "entities": len(todo), **Counter(states.values())}
        order = [e for e in todo if states[e] == "missing"] + [e for e in todo if states[e] == "stale"]
        if args.mode == "load" and order:
            deadline = time.monotonic() + args.budget_minutes * 60
            names = atc_names(session)
            outcomes: Counter = Counter()
            failed: list[dict[str, str]] = []
            streak = 0  # entities failed in a row; two stop the run (API down or in maintenance)
            for entity in order:
                if time.monotonic() > deadline or streak >= 2:
                    outcomes["not_started"] += 1
                    continue
                try:
                    outcome = land_entity(
                        client, bucket, args.landing_root, session, entity, token, names, held.get(entity)
                    )
                    outcomes[outcome] += 1
                    streak = 0
                except (RuntimeError, KeyError, TypeError, ValueError) as exc:  # recorded; the run ends red
                    failed.append({"entity": entity, "error": str(exc)[:200]})
                    streak += 1
            summary.update(outcomes)
            summary["failed"] = failed[:5]
            status = 1 if failed else 2 if outcomes["not_started"] else 0
    print(json.dumps(summary, sort_keys=True, default=str))
    annotate("error" if status == 1 else "notice", f"{SOURCE} landing {args.mode}", summary)
    step_summary(f"## {SOURCE} · landing {args.mode}\n\n```json\n{json.dumps(summary, indent=2, default=str)}\n```\n")
    return status


if __name__ == "__main__":
    raise SystemExit(main())
