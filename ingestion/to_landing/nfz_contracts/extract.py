#!/usr/bin/env python3
"""NFZ contracts API Landing: the agreements NFZ branches signed, per contract year and branch.

Route (docs/sources/nfz_contracts.md; register PLS-014). NFZ publishes no bulk file of its contract register; the
API ``https://api.nfz.gov.pl/app-umw-api/`` (version 1.2) lists agreements only for one year and one branch at a
time (``/agreements?year=<y>&branch=<nn>``, both required), at most 25 a page. Entity = one year and branch,
``agreements_<year>_<branch>``: every page of that list. The deeper levels (an agreement's plans by contract
product, a plan's months, one request per agreement or plan) and the dictionaries are not landed (scope in the
source docs).

One Landing file per entity and copy: a ZIP whose members are the exact answer bodies (``p<page>.json``), so a
year and branch is complete or not there. The receipt lists every member with its request URL and SHA-256 and
the count the API reported. A copy is complete only when the pages hold ``meta.count`` agreements with distinct
ids; otherwise the entity is read once more, then recorded as failed.

Version: the API gives no update date for a list (``meta.date-modified`` is the time of the request, so the bytes
of an unchanged answer differ on every request). A copy is landed only when the digest of the pages' ``data``
members differs from the newest landed copy; its version token is its download time. Which entities are read:
missing ones (newest year first), then the open years (the current contract year and the one before, which NFZ
still amends: OPEN_YEARS) in every run, and any other year whose newest copy is older than REREAD_DAYS.

Polite access: one request at a time, PAUSE_SECONDS between requests (the terms allow 10 per second per IP), an
identifying User-Agent, timeouts, bounded retries with backoff on 429, 5xx and timeouts, Retry-After honoured;
FAIL_STREAK failing entities in a row stop the run (the API is down: never hammered). Time-budgeted: no entity is
started in the last MARGIN_MINUTES; the next run continues from Landing.

Layout (docs/landing.md): ``<root>/nfz_contracts/snapshot/data/agreements_<year>_<branch>/<version>__<ingested>.zip``
+ receipt.
"""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import UTC, datetime, timedelta
from hashlib import sha256
import io
import json
import os
import time
from typing import Any
import zipfile
from zoneinfo import ZoneInfo

import requests

from ingestion.common.r2 import archive_key, archived, get_with_backoff, req, s3
from ingestion.common.report import annotate, step_summary
from ingestion.to_landing.world_bank_icp.extract import USER_AGENT, put_new

SOURCE = "nfz_contracts"
API = "https://api.nfz.gov.pl/app-umw-api"
QUERY = "format=json&api-version=1.2"
FIRST_YEAR = 2019  # scope: contract years from FIRST_YEAR (docs/sources/nfz_contracts.md)
BRANCHES = tuple(f"{n:02d}" for n in range(1, 17))  # the 16 regional branches, one per voivodeship
PAGE_LIMIT = 25  # the API's maximum
PAUSE_SECONDS = 0.3
TIMEOUT_SECONDS = 60
RETRY_DELAYS = (5, 15, 45, 120)  # at most about 3 minutes per request
FAIL_STREAK = 3
OPEN_YEARS = 2  # the current contract year and the one before are read again in every run
REREAD_DAYS = 365  # an older year is read again when its newest copy is older than this
MARGIN_MINUTES = 10


class Unavailable(RuntimeError):
    """The API kept failing (5xx, timeouts) after the bounded retries."""


def prefix(root: str) -> str:
    return f"{root.rstrip('/')}/{SOURCE}/snapshot/data/"


def entity_of(year: int, branch: str) -> str:
    return f"agreements_{year}_{branch}"


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


def years(session: requests.Session) -> tuple[int, int]:
    """The contract years the API serves (``/available-years``)."""
    _, parsed = fetch(session, f"{API}/available-years?{QUERY}")
    return int(parsed["start-year"]), int(parsed["end-year"])


def read_entity(session: requests.Session, year: int, branch: str) -> list[tuple[str, str, bytes, Any]]:
    """Every page of one year and branch: [(member, url, body, parsed)]; fails unless the pages hold meta.count
    agreements with distinct ids."""
    out, page = [], 1
    while True:
        url = f"{API}/agreements?year={year}&branch={branch}&page={page}&limit={PAGE_LIMIT}&{QUERY}"
        body, parsed = fetch(session, url)
        out.append((f"p{page}.json", url, body, parsed))
        count = int(parsed["meta"]["count"])
        if page * PAGE_LIMIT >= count:
            break
        page += 1
    ids = [a["id"] for _, _, _, p in out for a in p["data"]["agreements"]]
    if len(ids) != count or len(set(ids)) != count:
        raise ValueError(f"{entity_of(year, branch)}: {len(ids)} agreements ({len(set(ids))} ids), meta.count {count}")
    return out


def digest(answers: list[tuple[str, str, bytes, Any]]) -> str:
    """Digest of the pages' data members (the meta block carries the request time)."""
    data = [parsed["data"] for _, _, _, parsed in answers]
    return sha256(json.dumps(data, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()


def bundle(answers: list[tuple[str, str, bytes, Any]]) -> bytes:
    """ZIP of the exact bodies (fixed member dates, so equal answers give equal bytes)."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for member, _url, body, _parsed in answers:
            archive.writestr(zipfile.ZipInfo(member, date_time=(2000, 1, 1, 0, 0, 0)), body, zipfile.ZIP_DEFLATED)
    return buffer.getvalue()


def landed(client, bucket: str, root: str) -> dict[str, list[dict[str, Any]]]:
    """Receipts in Landing by entity, oldest landed first."""
    out: dict[str, list[dict[str, Any]]] = {}
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix(root)):
        for o in page.get("Contents", []):
            if o["Key"].endswith(".receipt.json"):
                body = {**json.loads(get_with_backoff(client, bucket, o["Key"])), "receipt_key": o["Key"]}
                out.setdefault(body["entity"], []).append(body)
    for rows in out.values():
        rows.sort(key=lambda r: r["ingested_at"])
    return out


def wanted(first: int, last: int, held: dict[str, list], now: datetime) -> dict[str, str]:
    """State of every entity in scope: missing, open (read again every run), stale (older than REREAD_DAYS) or
    current. The current contract year is the Warsaw calendar year, capped by the API's end year."""
    current = min(last, now.astimezone(ZoneInfo("Europe/Warsaw")).year)
    states = {}
    for year in range(max(first, FIRST_YEAR), last + 1):
        for branch in BRANCHES:
            entity = entity_of(year, branch)
            rows = held.get(entity)
            if not rows:
                states[entity] = "missing"
            elif year > current - OPEN_YEARS:
                states[entity] = "open"
            else:
                newest = datetime.strptime(rows[-1]["ingested_at"], "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
                states[entity] = "stale" if now - newest > timedelta(days=REREAD_DAYS) else "current"
    return states


def land_entity(client, bucket: str, root: str, session, entity: str, held: list | None) -> str:
    """One year and branch; a copy whose data equal the newest landed copy is not written again."""
    _, year, branch = entity.split("_")
    try:
        answers = read_entity(session, int(year), branch)
    except ValueError:  # the list changed while it was paged: read it once more
        answers = read_entity(session, int(year), branch)
    data_digest = digest(answers)
    before = held[-1] if held else None
    if before and before.get("data_sha256") == data_digest:
        return "unchanged"
    body = bundle(answers)
    ingested = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    key = f"{prefix(root)}{entity}/{ingested}__{ingested}.zip"
    put_new(client, bucket, key, body, "application/zip")
    receipt = {
        "schema_version": 2,
        "kind": "nfz_contracts_landing",
        "source_id": SOURCE,
        "entity": entity,
        "year": int(year),
        "branch": branch,
        "agreements": int(answers[0][3]["meta"]["count"]),
        "content": "snapshot",
        "area": "data",
        "source_kind": "nfz_umw_api_json_pages_zip",
        "version_token": ingested,
        "version_basis": "download time (the API gives no update date)",
        "ingested_at": ingested,
        "source_url": answers[0][1],
        "sha256": sha256(body).hexdigest(),
        "size_bytes": len(body),
        "data_sha256": data_digest,
        "members": [
            {"member": m, "url": u, "sha256": sha256(b).hexdigest(), "size_bytes": len(b)} for m, u, b, _ in answers
        ],
        "object": {"bucket": bucket, "key": key},
        "replaces_sha256": before["sha256"] if before else None,
        "code_sha": os.environ.get("GITHUB_SHA", ""),
    }
    put_new(
        client,
        bucket,
        key[: -len(".zip")] + ".receipt.json",
        json.dumps(receipt, indent=2, sort_keys=True).encode(),
        "application/json",
    )
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
    return {
        "receipts": sum(len(v) for v in held.values()),
        "missing_data": len(problems),
        "missing_sample": problems[:10],
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--mode", choices=("inspect", "load", "verify"), required=True)
    parser.add_argument("--filter", default="", help="comma separated entities (agreements_<year>_<branch>)")
    parser.add_argument("--budget-minutes", type=float, default=300)
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
        first, last = years(session)
        if first > FIRST_YEAR:
            raise RuntimeError(f"the API serves {first}-{last}, the scope starts in {FIRST_YEAR}")
        states = wanted(first, last, held, datetime.now(UTC))
        chosen = {e.strip() for e in args.filter.split(",") if e.strip()}
        unknown = chosen - set(states)
        if unknown:
            raise RuntimeError(f"not in scope: {sorted(unknown)}")
        if chosen:
            states = {e: s for e, s in states.items() if e in chosen}
        summary = {
            "mode": args.mode,
            "api_years": f"{first}-{last}",
            "entities": len(states),
            **Counter(states.values()),
        }
        if args.mode == "load":
            deadline = time.monotonic() + (args.budget_minutes - MARGIN_MINUTES) * 60
            order = sorted((e for e, s in states.items() if s == "missing"), reverse=True)
            order += sorted((e for e, s in states.items() if s in ("open", "stale")), reverse=True)
            outcomes: Counter = Counter()
            failed: list[dict[str, str]] = []
            streak = 0
            for entity in order:
                if time.monotonic() > deadline or streak >= FAIL_STREAK:
                    outcomes["not_started_time_budget" if streak < FAIL_STREAK else "not_started_api_down"] += 1
                    continue
                try:
                    outcomes[land_entity(client, bucket, args.landing_root, session, entity, held.get(entity))] += 1
                    streak = 0
                except (RuntimeError, ValueError, KeyError) as exc:  # recorded; the others are still landed
                    failed.append({"entity": entity, "error": str(exc)[:200]})
                    streak += 1
            summary.update(outcomes)
            summary["failed"] = len(failed)
            summary["failed_sample"] = failed[:5]
            status = 1 if failed else 2 if outcomes["not_started_time_budget"] else 0
    print(json.dumps(summary, sort_keys=True, default=str))
    annotate("error" if status == 1 else "notice", f"nfz_contracts landing {args.mode}", summary)
    step_summary(
        f"## nfz_contracts · landing {args.mode}\n\n```json\n{json.dumps(summary, indent=2, default=str)}\n```\n"
    )
    return status


if __name__ == "__main__":
    raise SystemExit(main())
