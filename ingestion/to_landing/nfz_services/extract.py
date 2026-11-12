#!/usr/bin/env python3
"""NFZ statistics API, services (JGP) Landing: the benefit dictionaries and, per benefit, the basic-data tables.

Route (docs/sources/nfz_services.md; register PLS-012). NFZ publishes no bulk file of these statistics; the API
``https://api.nfz.gov.pl/app-stat-api-jgp/`` (version 1.1) answers one table per request, at most 25 rows a page.
Entities:

- ``benefits_<catalog>``: the dictionary of one catalog (1a JGP groups, 1b, 1c, 1d, 1w), every page of
  ``/benefits?catalog=<c>``;
- ``<catalog>_<code>``: one benefit: its ``/index-of-tables`` answer and, for every year it lists, the
  ``basic-data`` table for Poland (``branch=false``) and by NFZ regional branch (``branch=true``), every page.
  Other table types (sex, age, admission and discharge type, products, histograms, ICD-9, ICD-10) and the
  hospital-type splits are not loaded.

One Landing file per entity and copy: a ZIP whose members are the exact answer bodies (``index.json``,
``basic_<table id>_<pl|branch>_p<page>.json``), so a benefit is complete or not there and Bronze, the Archive and
the latest-version rules see one file per entity. The receipt lists every member with its request URL, SHA-256 and
the digest of its ``data`` member.

Version: the API gives no update date (``meta.date-modified`` is the time of the request, so the bytes of an
unchanged answer differ on every request). A copy is landed only when the digest of the answers' ``data`` members
differs from the newest landed copy; its version token is its download time. Which held benefits are asked again:
the canary benefits (CANARY) in every run, a quarter of the others by a stable hash per ISO week (each benefit at
least every four weekly runs), and all of them in a run whose canary index changed (a new year was published).
An unchanged benefit costs its one index request. A fifty-second of the benefits is read whole each week (every
benefit once a year), so a revision published under unchanged table ids is landed too.

Polite access: one request at a time, PAUSE_SECONDS between requests (the terms allow 10 per second per IP),
an identifying User-Agent, timeouts, bounded retries with backoff on 429, 5xx (also the API's
``"error-result": "Maintenance"`` 500) and timeouts, Retry-After honoured; a benefit that still fails is recorded
and the run goes on; FAIL_STREAK failures in a row stop the run (the API is down: never hammered). Time-budgeted
(missing benefits first, the canaries among them first, then the checked ones; no benefit is started in the last
MARGIN_MINUTES); the next run continues from Landing.

Layout (docs/landing.md): ``<root>/nfz_services/snapshot/data/<entity>/<version>__<ingested>.zip`` + receipt.
"""

from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from hashlib import sha256
import io
import json
import os
import re
import time
from typing import Any
from urllib.parse import quote
import zipfile

import requests

from ingestion.common.r2 import archive_key, archived, get_with_backoff, req, s3
from ingestion.common.report import annotate, step_summary
from ingestion.to_landing.world_bank_icp.extract import USER_AGENT, put_new

SOURCE = "nfz_services"
API = "https://api.nfz.gov.pl/app-stat-api-jgp"
QUERY = "format=json&api-version=1.1"
CATALOGS = ("1a", "1b", "1c", "1d", "1w")
PAGE_LIMIT = 25  # the API's maximum
PAUSE_SECONDS = 0.7
TIMEOUT_SECONDS = 60
RETRY_DELAYS = (5, 15, 45, 120)  # at most about 3 minutes per request
FAIL_STREAK = 3  # benefits failing in a row: the API is down, the run stops asking
SHARES = 4  # each held benefit's index is asked once every SHARES weekly runs
REREAD = 52  # and all its tables once every REREAD weekly runs (a revision under unchanged table ids)
MARGIN_MINUTES = 10  # no benefit is started in the last minutes of the budget
# Benefits asked in every run (entity names): the JGP groups the KPIs read (kpi_measure.csv rows of nfz_services)
# and one benefit each of the radiotherapy (1d) and highly specialised (1w) catalogs. A changed canary index means a
# release (a new year): every held benefit is asked in that run.
CANARY = (
    "1a_5.51.01.0013001",  # N01 childbirth
    "1a_5.51.01.0008013",  # H13 primary total hip replacement
    "1a_5.51.01.0008015",  # H15 primary total knee replacement
    "1a_5.51.01.0001048",  # A48 stroke, stroke unit > 7 days
    "1d_5.07.01.0000061",  # 3D real-time brachytherapy
    "1w_03.4603.033.02",  # heart valve implantation
)
BASIC = "basic-data"  # the resource of the general-data tables
GENERAL = "general-data"


class Unavailable(RuntimeError):
    """The API kept failing (5xx, Maintenance, timeouts) after the bounded retries."""


def prefix(root: str) -> str:
    return f"{root.rstrip('/')}/{SOURCE}/snapshot/data/"


def entity_of(catalog: str, code: str) -> str:
    """Landing folder of a benefit: catalog and code, characters outside [A-Za-z0-9.-] as '_'."""
    return f"{catalog}_{re.sub(r'[^A-Za-z0-9.-]', '_', code)}"


def digest(value: Any) -> str:
    return sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()


def fetch(session: requests.Session, address: str) -> tuple[bytes, Any]:
    """GET one answer: (exact body, parsed JSON). 429, 5xx (the API's Maintenance 500 included) and network errors
    are retried with backoff; another status fails at once; still failing → Unavailable."""
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


def pages(session: requests.Session, address: str) -> list[tuple[str, bytes, Any]]:
    """Every page of a paged answer: [(url, body, parsed)]; a page count from ``meta.count`` and the limit."""
    out, page = [], 1
    while True:
        url = f"{address}&page={page}&limit={PAGE_LIMIT}&{QUERY}"
        body, parsed = fetch(session, url)
        out.append((url, body, parsed))
        count = int((parsed.get("meta") or {}).get("count") or 0)
        if page * PAGE_LIMIT >= count or not (parsed.get("links") or {}).get("next"):
            return out
        page += 1


def dictionary(session: requests.Session, catalog: str) -> tuple[list[tuple[str, str, bytes, Any]], list[dict]]:
    """The catalog's dictionary: ([(member, url, body, parsed)], [{code, name}]); fails on a count mismatch."""
    answers = pages(session, f"{API}/benefits?catalog={catalog}")
    items = [i for _, _, parsed in answers for i in (parsed.get("data") or [])]
    count = int((answers[0][2].get("meta") or {}).get("count") or 0)
    if len(items) != count or any(not isinstance(i, dict) or not i.get("code") for i in items):
        raise RuntimeError(f"benefits {catalog}: {len(items)} entries read, meta.count {count}")
    return [(f"benefits_p{n}.json", url, body, parsed) for n, (url, body, parsed) in enumerate(answers, 1)], items


def index_url(catalog: str, item: dict[str, str]) -> str:
    """The index of a benefit: ``name`` must be the dictionary name exactly (the code answers ``data: null``)."""
    return f"{API}/index-of-tables?catalog={catalog}&name={quote(item['name'], safe='')}&{QUERY}"


def basic_tables(index: Any) -> list[tuple[int, str, str | None, str | None]]:
    """(year, table id, period from, period to) of every basic-data table (type ``general-data``) the index lists:
    one per year, and per part-year period where a benefit changed within a year (``periods``, from and to set)."""
    data = index.get("data") if isinstance(index, dict) else None
    if data is None:
        return []
    found = []
    for year in (data.get("attributes") or {}).get("years") or []:
        for table in year.get("tables") or []:
            if table.get("type") == GENERAL:
                found.append((int(year["year"]), str(table["id"]), None, None))
        for period in year.get("periods") or []:
            for table in period.get("tables") or []:
                if table.get("type") == GENERAL:
                    found.append((int(year["year"]), str(table["id"]), period["date-from"], period["date-to"]))
    return sorted(found, key=lambda t: (t[0], t[2] or "", t[1]))


def benefit(session: requests.Session, catalog: str, item: dict[str, str], index: tuple | None = None) -> list:
    """The benefit's answers [(member, url, body, parsed)]: the index (``index`` = an answer already fetched) and
    every page of each basic-data table it lists, for Poland and by branch."""
    if index is None:
        address = index_url(catalog, item)
        index = (address, *fetch(session, address))
    address, body, parsed = index
    answers = [("index.json", address, body, parsed)]
    served = ((parsed.get("data") or {}).get("attributes") or {}).get("product-code")
    if served is not None and served != item["code"]:
        # two dictionary entries share a name and the index (asked by name) serves the other product: only the
        # index is landed, Bronze records the file as rejected with this reason
        return answers
    for _year, table_id, _start, _end in basic_tables(parsed):
        for scope, branch in (("pl", "false"), ("branch", "true")):
            base = f"{API}/{BASIC}/{quote(table_id, safe='')}?branch={branch}&hospitalType=false"
            for n, (url, page_body, page) in enumerate(pages(session, base), 1):
                answers.append((f"basic_{table_id}_{scope}_p{n}.json", url, page_body, page))
    return answers


def bundle(answers: list[tuple[str, str, bytes, Any]]) -> bytes:
    """ZIP of the exact bodies (fixed member dates, so equal answers give equal bytes)."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for member, _url, body, _parsed in answers:
            archive.writestr(zipfile.ZipInfo(member, date_time=(1980, 1, 1, 0, 0, 0)), body, zipfile.ZIP_DEFLATED)
    return buffer.getvalue()


def landed(client, bucket: str, root: str) -> dict[str, list[dict[str, Any]]]:
    """Receipts in Landing by entity, oldest landed first."""
    keys = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix(root)):
        keys += [o["Key"] for o in page.get("Contents", []) if o["Key"].endswith(".receipt.json")]

    def read(key: str) -> dict[str, Any]:
        return {**json.loads(get_with_backoff(client, bucket, key)), "receipt_key": key}

    out: dict[str, list[dict[str, Any]]] = {}
    with ThreadPoolExecutor(max_workers=8) as pool:  # R2 throttles larger bursts of reads
        for body in pool.map(read, keys):
            out.setdefault(body["entity"], []).append(body)
    for rows in out.values():
        rows.sort(key=lambda r: r["ingested_at"])
    return out


def land(client, bucket: str, root: str, entity: str, answers: list, held: list | None, extra: dict) -> str:
    """Land the entity's answers unless their data equals the newest landed copy: landed_new, landed_changed or
    unchanged."""
    data_digest = digest([[member, parsed.get("data")] for member, _url, _body, parsed in answers])
    before = held[-1] if held else None
    if before and before.get("data_digest") == data_digest:
        return "unchanged"
    body = bundle(answers)
    ingested = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    key = f"{prefix(root)}{entity}/{ingested}__{ingested}.zip"
    put_new(client, bucket, key, body, "application/zip")
    receipt = {
        "schema_version": 2,
        "kind": "nfz_services_landing",
        "source_id": SOURCE,
        "entity": entity,
        "content": "snapshot",
        "area": "data",
        "source_kind": "nfz_jgp_api_1.1_json_zip",
        "version_token": ingested,
        "version_basis": "download time of a copy whose data differs from the newest landed copy",
        "ingested_at": ingested,
        "source_url": answers[0][1],
        "sha256": sha256(body).hexdigest(),
        "size_bytes": len(body),
        "object": {"bucket": bucket, "key": key},
        "data_digest": data_digest,
        "parts": [
            {
                "member": member,
                "url": url,
                "sha256": sha256(raw).hexdigest(),
                "size_bytes": len(raw),
                "data_digest": digest(parsed.get("data")),
            }
            for member, url, raw, parsed in answers
        ],
        **extra,
        "replaces_sha256": before["sha256"] if before else None,
        "code_sha": os.environ.get("GITHUB_SHA", ""),
    }
    put_new(
        client,
        bucket,
        key[: -len(".zip")] + ".receipt.json",
        json.dumps(receipt, ensure_ascii=False, indent=2, sort_keys=True).encode(),
        "application/json",
    )
    return "landed_changed" if before else "landed_new"


def slot(entity: str, week: int, every: int) -> bool:
    """True in one of ``every`` weekly runs for this entity (a stable hash of its name)."""
    return int(sha256(entity.encode()).hexdigest(), 16) % every == week % every


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
    receipted = {r["object"]["key"] for rows in held.values() for r in rows}
    return {
        "entities": len(held),
        "receipts": sum(len(v) for v in held.values()),
        "missing_data": len(problems),
        "missing_sample": problems[:10],
        "data_without_receipt": sorted(present - receipted)[:10],
    }


def wanted(filter_text: str, catalog: str, entity: str) -> bool:
    """--filter: comma separated catalogs (1d) or entities (1a_N01); empty = everything."""
    terms = {t.strip() for t in filter_text.split(",") if t.strip()}
    return not terms or catalog in terms or entity in terms


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--mode", choices=("inspect", "load", "verify"), required=True)
    parser.add_argument("--filter", default="", help="comma separated catalogs or entities (default: all)")
    parser.add_argument("--budget-minutes", type=float, default=300)
    parser.add_argument("--landing-root", default="01_landing", help="Landing root (a zz_dev_ prefix in Dev trials)")
    args = parser.parse_args(argv)
    deadline = time.monotonic() + max(args.budget_minutes - MARGIN_MINUTES, args.budget_minutes / 2) * 60
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    held = landed(client, bucket, args.landing_root)
    if args.mode == "verify":
        summary: dict[str, Any] = {"mode": "verify", **run_verify(client, bucket, args.landing_root, held)}
        return report(summary, 1 if summary["missing_data"] else 0)

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
    outcomes: Counter = Counter()
    failed: list[dict[str, str]] = []
    benefits: list[tuple[str, dict[str, str]]] = []
    for catalog in CATALOGS:
        answers, items = dictionary(session, catalog)
        outcomes[f"dictionary_{catalog}"] = len(items)
        benefits += [(catalog, i) for i in items]
        if args.mode == "load":
            entity = f"benefits_{catalog}"
            outcomes[
                f"dictionary_{land(client, bucket, args.landing_root, entity, answers, held.get(entity), {})}"
            ] += 1
    todo = [(c, i, entity_of(c, i["code"])) for c, i in benefits if wanted(args.filter, c, entity_of(c, i["code"]))]
    if len({e for _, _, e in todo}) != len(todo):
        raise RuntimeError("two dictionary entries map to one Landing entity")
    week = datetime.now(UTC).isocalendar().week
    # missing benefits, the canaries (the KPI groups) first, so the first run of the initial load has them
    missing = sorted((t for t in todo if t[2] not in held), key=lambda t: t[2] not in CANARY)
    canary = [t for t in todo if t[2] in held and t[2] in CANARY]
    others = [t for t in todo if t[2] in held and t[2] not in CANARY]
    share = [t for t in others if slot(t[2], week, SHARES)]
    reread = {t[2] for t in others if slot(t[2], week, REREAD)}
    summary: dict[str, Any] = {
        "mode": args.mode,
        "benefits": len(todo),
        "missing": len(missing),
        "held": len(todo) - len(missing),
        "missing_sample": [e for _, _, e in missing[:10]],
        "canary": len(canary),
        "share_this_week": len(share),
        "reread_this_week": len(reread),
    }
    state = {"streak": 0, "stopped": False}

    def work(group: list, name: str) -> int:
        """Ask each benefit of the group; returns how many held benefits had a changed index."""
        changed = 0
        for position, (catalog, item, entity) in enumerate(group):
            if state["stopped"] or time.monotonic() > deadline:
                outcomes["not_started_api_failing" if state["stopped"] else "not_started_time_budget"] += (
                    len(group) - position
                )
                return changed
            before = held.get(entity)
            try:
                index = None
                if before and entity not in reread:  # an unchanged index costs this one request
                    address = index_url(catalog, item)
                    index = (address, *fetch(session, address))
                    if digest(index[2].get("data")) == before[-1].get("index_digest"):
                        outcomes["checked_current"] += 1
                        state["streak"] = 0
                        continue
                    changed += 1
                if args.mode == "inspect":
                    outcomes["stale" if before else "missing_seen"] += 1
                    state["streak"] = 0
                    continue
                answers = benefit(session, catalog, item, index)
            except (Unavailable, RuntimeError) as exc:  # recorded; the run goes on, red; the next run retries
                failed.append({"entity": entity, "group": name, "error": str(exc)[:200]})
                state["streak"] += 1
                state["stopped"] = state["streak"] >= FAIL_STREAK
                continue
            state["streak"] = 0
            extra = {
                "catalog": catalog,
                "benefit_code": item["code"],
                "benefit_name": item.get("name"),
                "index_digest": digest(answers[0][3].get("data")),
                "basic_tables": [list(t) for t in basic_tables(answers[0][3])],
            }
            outcomes[land(client, bucket, args.landing_root, entity, answers, before, extra)] += 1
            outcomes["benefit_requests"] += len(answers)
        return changed

    if args.mode == "load":
        work(missing, "missing")
    if work(canary, "canary"):  # a canary index changed: a release; every held benefit is asked in this run
        summary["release_detected"] = True
        share = others
    if args.mode == "load":
        work(share + [t for t in others if t[2] in reread and t not in share], "share")
    summary.update(outcomes)
    summary["failed"] = len(failed)
    summary["failed_sample"] = failed[:5]
    left = outcomes["not_started_time_budget"]
    return report(summary, 1 if failed else 2 if left else 0)


def report(summary: dict[str, Any], status: int) -> int:
    print(json.dumps(summary, sort_keys=True, default=str))
    annotate("error" if status == 1 else "notice", f"{SOURCE} landing {summary['mode']}", summary)
    step_summary(
        f"## {SOURCE} · landing {summary['mode']}\n\n```json\n{json.dumps(summary, indent=2, default=str)}\n```\n"
    )
    return status


if __name__ == "__main__":
    raise SystemExit(main())
