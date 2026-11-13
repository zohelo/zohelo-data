#!/usr/bin/env python3
"""TED notices (Poland) Landing: the TED Search API answers for notices of Polish buyers, one publication day at a time.

Route (docs/sources/ted_notices.md, register PLG-023). The Search API (``POST /v3/notices/search``, anonymous) answers
an expert query with chosen fields of the published notices; the query here is ``buyer-country=POL AND
publication-date=<day>``. A day is a closed set (a published notice is never changed; a change is a new notice
with its own publication number), so each day is fetched once and only the newest RECHECK_DAYS days are asked
again. The window starts at WINDOW_START (the smallest valuable selection: two calendar years of eForms notices);
older notices are left out.

Entity = one page of one publication day, ``day_<YYYYMMDD>_p<n>`` (PAGE_SIZE notices a page, ordered by the API),
so every Landing file is the exact bytes of one answer and the newest file of its entity (the shared Archive,
retention and Bronze checks expect one newest file per entity). All pages of a day are fetched in the same run and
checked together (page sizes, the day's total, no publication number twice) before any is written; a day whose
pages disagree is not written. Version = the publication day; a page whose bytes equal its newest landed copy is
not written again. Days without notices land their (empty) answer once, so they are not asked again.

Layout (docs/landing.md): ``<root>/ted_notices/snapshot/data/day_<YYYYMMDD>_p<n>/<YYYYMMDD>T000000Z__<ingested>.json``
+ receipt. Modes: inspect (window days vs Landing), load, verify. Polite access: one request at a time, a pause
between requests, bounded retries with backoff on 429/5xx and timeouts, an identifying User-Agent, the documented
page size. Only the declared FIELDS are asked for: no contact points, persons, addresses, e-mails or winners.
"""

from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
import json
import math
import os
import re
import time
from typing import Any

import requests

from ingestion.common.r2 import archive_key, archived, get_with_backoff, req, s3
from ingestion.common.report import annotate, step_summary
from ingestion.to_landing.patterns.bulk_file import USER_AGENT

SOURCE = "ted_notices"
API = "https://api.ted.europa.eu/v3/notices/search"
QUERY = "buyer-country=POL AND publication-date={day}"
WINDOW_START = date(2025, 1, 1)
RECHECK_DAYS = 3
PAGE_SIZE = 250
PAUSE_SECONDS = 1.5
TIMEOUT_SECONDS = (20, 120)
RETRY_DELAYS = (15, 30, 60, 120, 240)
# Fields asked for (14 x PAGE_SIZE stays below the API's 10,000 fields a page). Organisation-level only: the buyer's
# name, country, NUTS code (BT-507) and legal type; no contact point, person, address, e-mail, telephone or winner.
FIELDS = [
    "publication-number",
    "notice-type",
    "form-type",
    "publication-date",
    "dispatch-date",
    "contract-nature",
    "procedure-type",
    "classification-cpv",
    "buyer-country",
    "buyer-country-sub",
    "buyer-name",
    "buyer-legal-type",
    "estimated-value-proc",
    "estimated-value-cur-proc",
]
ENTITY_RE = re.compile(r"day_(\d{8})_p(\d+)")
THROTTLED = {"ServiceUnavailable", "SlowDown", "TooManyRequests", "429", "503", "InternalError"}


def prefix(root: str) -> str:
    return f"{root.rstrip('/')}/{SOURCE}/snapshot/data/"


def entity_of(day: date, page: int) -> str:
    return f"day_{day:%Y%m%d}_p{page}"


def last_day(now: datetime | None = None) -> date:
    """The newest publication day asked for: yesterday (UTC), so a day is complete when it is first fetched."""
    return (now or datetime.now(UTC)).date() - timedelta(days=1)


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


def days_held(receipts: dict[str, list]) -> set[date]:
    out = set()
    for entity in receipts:
        match = ENTITY_RE.fullmatch(entity)
        if match:
            out.add(datetime.strptime(match.group(1), "%Y%m%d").date())
    return out


def post(session: requests.Session, body: dict[str, Any]) -> bytes:
    """POST with bounded retries: network errors, 429 and 5xx back off; other statuses fail at once."""
    for attempt in range(len(RETRY_DELAYS) + 1):
        time.sleep(PAUSE_SECONDS)
        try:
            response = session.post(API, json=body, timeout=TIMEOUT_SECONDS)
        except (requests.ConnectionError, requests.Timeout):
            response = None
        if response is not None and response.status_code == 200:
            return response.content
        if response is not None and response.status_code not in (429, 500, 502, 503, 504):
            raise RuntimeError(f"TED API {response.status_code} for {body['query']}: {response.text[:300]}")
        if attempt == len(RETRY_DELAYS):
            break
        time.sleep(RETRY_DELAYS[attempt])
    raise RuntimeError(f"TED API kept failing for {body['query']} page {body['page']}")


def request_body(day: date, page: int) -> dict[str, Any]:
    return {
        "query": QUERY.format(day=f"{day:%Y%m%d}"),
        "fields": FIELDS,
        "page": page,
        "limit": PAGE_SIZE,
        "scope": "ALL",
        "paginationMode": "PAGE_NUMBER",
        "checkQuerySyntax": False,
    }


def parse(body: bytes, day: date, page: int) -> tuple[int, list[str]]:
    """(total notices of the day, publication numbers of this page); a malformed answer fails loudly."""
    try:
        answer = json.loads(body)
    except ValueError as exc:
        raise RuntimeError(f"TED API returned no JSON for {day} page {page}: {body[:200]!r}") from exc
    if not isinstance(answer, dict) or not isinstance(answer.get("notices"), list):
        raise RuntimeError(f"TED API answer without notices for {day} page {page}: {body[:300]!r}")
    total = answer.get("totalNoticeCount")
    if type(total) is not int:
        raise RuntimeError(f"TED API answer without totalNoticeCount for {day} page {page}")
    if answer.get("timedOut"):
        raise RuntimeError(f"TED API search timed out for {day} page {page}")
    numbers = [str(n.get("publication-number")) for n in answer["notices"]]
    return total, numbers


def fetch_day(session: requests.Session, day: date) -> list[bytes]:
    """Every page of one day, checked together: page sizes, the total, each publication number once."""
    first = post(session, request_body(day, 1))
    total, numbers = parse(first, day, 1)
    pages = [first]
    for page in range(2, max(1, math.ceil(total / PAGE_SIZE)) + 1):
        body = post(session, request_body(day, page))
        page_total, page_numbers = parse(body, day, page)
        if page_total != total:
            raise RuntimeError(f"{day}: total changed between pages ({total} then {page_total})")
        pages.append(body)
        numbers += page_numbers
    if len(numbers) != total or len(set(numbers)) != total:
        found = f"{len(numbers)} notices ({len(set(numbers))} distinct) on {len(pages)} pages"
        raise RuntimeError(f"{day}: {found}, total {total}")
    return pages


def put_new(client, bucket: str, key: str, body: bytes, content_type: str) -> None:
    """Never overwrite Landing: a key that exists is an error unless the bytes are equal. R2 throttling is retried."""
    digest = sha256(body).hexdigest()

    def attempt(operation):
        for wait in (2, 5, 15, 30, 60, None):
            try:
                return operation()
            except client.exceptions.ClientError as exc:
                if str(exc.response.get("Error", {}).get("Code", "")) not in THROTTLED or wait is None:
                    raise
                time.sleep(wait)
        return None

    try:
        head = attempt(lambda: client.head_object(Bucket=bucket, Key=key))
        if (head.get("Metadata") or {}).get("sha256") == digest:
            return
        raise RuntimeError(f"Landing key exists with different bytes: {key}")
    except client.exceptions.ClientError as exc:  # only "not found" means free to write
        if exc.response.get("Error", {}).get("Code") not in ("404", "NoSuchKey", "NotFound"):
            raise
    meta = {"sha256": digest}
    attempt(lambda: client.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type, Metadata=meta))
    head = attempt(lambda: client.head_object(Bucket=bucket, Key=key))
    if (head.get("Metadata") or {}).get("sha256") != digest:
        raise RuntimeError(f"Landing write could not be verified: {key}")


def land_day(client, bucket: str, root: str, session, day: date, newest: dict[str, dict]) -> Counter:
    """One day: every page fetched and checked, then each page whose bytes differ from its newest copy written."""
    outcomes: Counter = Counter()
    pages = fetch_day(session, day)
    beyond = [e for e in newest if e.startswith(f"day_{day:%Y%m%d}_p") and int(e.rsplit("_p", 1)[1]) > len(pages)]
    if beyond:  # a closed day never loses notices; Landing is never deleted, so this needs a person
        raise RuntimeError(f"{day}: now {len(pages)} pages, Landing holds {sorted(beyond)}")
    ingested = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    token = f"{day:%Y%m%d}T000000Z"
    total = parse(pages[0], day, 1)[0]
    for number, body in enumerate(pages, start=1):
        entity = entity_of(day, number)
        digest = sha256(body).hexdigest()
        before = newest.get(entity)
        if before and before["sha256"] == digest:
            outcomes["pages_unchanged"] += 1
            continue
        key = f"{prefix(root)}{entity}/{token}__{ingested}.json"
        put_new(client, bucket, key, body, "application/json")
        receipt = {
            "schema_version": 3,
            "kind": "ted_notices_landing",
            "source_id": SOURCE,
            "entity": entity,
            "area": "data",
            "content": "snapshot",
            "source_kind": "ted_search_api_v3_json",
            "version_token": token,
            "version_basis": "publication_day",
            "ingested_at": ingested,
            "source_url": API,
            "request": request_body(day, number),
            "publication_day": f"{day:%Y-%m-%d}",
            "part": {"index": number, "total": len(pages)},
            "day_total_notices": total,
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
        outcomes["pages_landed_changed" if before else "pages_landed_new"] += 1
    outcomes["notices_seen"] += total
    return outcomes


def run_verify(client, bucket: str, root: str) -> dict[str, Any]:
    receipts, data = listing(client, bucket, root)
    stored = archived(client, bucket, SOURCE)
    problems = []
    held = read_receipts(client, bucket, [k for rows in receipts.values() for _, k in rows])
    for r in held:
        key = r["object"]["key"]
        if key not in data and stored.get(r["sha256"], {}).get("key") != archive_key(SOURCE, r["sha256"], key):
            problems.append(key)
    days = days_held(receipts)
    return {
        "receipts": len(held),
        "entities": len(receipts),
        "days": len(days),
        "first_last_day": [str(min(days)) if days else None, str(max(days)) if days else None],
        "missing_data": len(problems),
        "missing_sample": problems[:10],
    }


def extract(budget_minutes: float, landing_root: str = "01_landing", last: date | None = None) -> int:
    """landing.extract: missing days oldest first, then the newest RECHECK_DAYS days again (within the budget)."""
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    last = last or last_day()
    receipts, _ = listing(client, bucket, landing_root)
    held = days_held(receipts)
    days = window(last)
    missing = [d for d in days if d not in held]
    recheck = [d for d in days[-RECHECK_DAYS:] if d in held]
    newest_keys = [rows[-1][1] for e, rows in receipts.items() if ENTITY_RE.fullmatch(e) and
                   datetime.strptime(ENTITY_RE.fullmatch(e).group(1), "%Y%m%d").date() in set(recheck)]  # fmt: skip
    newest = {r["entity"]: r for r in read_receipts(client, bucket, newest_keys)}
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
    deadline = time.monotonic() + budget_minutes * 60
    outcomes: Counter = Counter()
    failed: list[dict[str, str]] = []
    for day in missing + recheck:
        if time.monotonic() > deadline:
            outcomes["days_not_started_time_budget"] += 1
            continue
        try:
            outcomes.update(land_day(client, bucket, landing_root, session, day, newest))
            outcomes["days_done"] += 1
        except RuntimeError as exc:  # one day failing must not stop the others; recorded, run ends red
            failed.append({"day": str(day), "error": str(exc)[:300]})
    summary = {
        "mode": "load",
        "window": [str(WINDOW_START), str(last)],
        "days_in_window": len(days),
        "days_missing_before": len(missing),
        "days_rechecked": len(recheck),
        **outcomes,
        "failed_days": len(failed),
        "failed_sample": failed[:5],
    }
    status = 1 if failed else 2 if outcomes["days_not_started_time_budget"] else 0
    report("load", summary, status)
    return status


def inspect(landing_root: str = "01_landing", last: date | None = None) -> int:
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    last = last or last_day()
    receipts, data = listing(client, bucket, landing_root)
    held = days_held(receipts)
    days = window(last)
    summary = {
        "mode": "inspect",
        "window": [str(WINDOW_START), str(last)],
        "days_in_window": len(days),
        "days_current": len([d for d in days if d in held]),
        "days_missing": len([d for d in days if d not in held]),
        "entities": len(receipts),
        "data_files_in_landing": len(data),
        "newest_day_held": str(max(held)) if held else None,
    }
    report("inspect", summary, 0)
    return 0


def verify(landing_root: str = "01_landing") -> int:
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    summary = {"mode": "verify", **run_verify(client, bucket, landing_root)}
    status = 1 if summary["missing_data"] else 0
    report("verify", summary, status)
    return status


def report(mode: str, summary: dict[str, Any], status: int) -> None:
    print(json.dumps(summary, sort_keys=True, default=str))
    annotate("error" if status == 1 else "notice", f"ted_notices landing {mode}", summary)
    step_summary(f"## ted_notices · landing {mode}\n\n```json\n{json.dumps(summary, indent=2, default=str)}\n```\n")


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
