#!/usr/bin/env python3
"""GUS DBW (Dziedzinowe Bazy Wiedzy) Landing: the website's bulk ZIP files.

Route (docs/sources.md, GUS DBW): the DBW website publishes every indicator as bulk CSV ZIP
files, one per indicator × cross-section × presentation × periodicity (large ones also per
year), listed with their last update date by the website's own application endpoints:

  /api_app/wsk/getIndicatorsTree               every area → group → indicator
  /api_app/getAggregatesById?id=<n>&czy_pl=true  an indicator's cross-sections: name, unit,
                                                  years, last update (YYYY-MM-DD), its ZIP files
  /bulk_new/<file>.zip                          the ZIP (one semicolon CSV)

That is a bulk feature, so it is the route (extraction order: bulk first). The API
(api-dbw.stat.gov.pl, 5,000 requests per 12 h) stays for dictionaries.

Entity = one ZIP file (its name without .zip, e.g. ``1000_156_93_4_pl``); version = the last
update date of its cross-section (``YYYYMMDDT000000Z``). Layout (docs/landing.md):
``01_landing/gus_dbw/snapshot/data/<entity>/<version>__<ingested>.zip`` + receipt.

Modes:
  inspect  last listings (cached) vs Landing (current / stale / missing); one website request
  load     renew listings older than 20 h (oldest first, at most half the budget), then download
           missing and stale files
  verify   every receipt has its data file (in Landing or in the Archive)

Polite access: one request at a time with a pause, retries with backoff on 429/5xx, an
identifying User-Agent.
"""

from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import contextlib
from datetime import UTC, datetime
from hashlib import sha256
import json
import os
import re
import time
from typing import Any

import requests

from ingestion.common.r2 import archive_key, archived, get_with_backoff, req, s3
from ingestion.common.report import annotate, step_summary

SOURCE = "gus_dbw"
PREFIX = "01_landing/gus_dbw/snapshot/data/"
WEB = "https://dbw.stat.gov.pl"
PAUSE_SECONDS = 0.3
USER_AGENT = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
FILE_RE = re.compile(r"^[0-9]+(_[0-9]+)+_pl\.zip$")
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class NotFound(RuntimeError):
    """404: the website lists an indicator or file it does not serve."""


class Fetched:
    """A finished response: body bytes, headers and the seconds it took."""

    def __init__(self, content: bytes, headers, seconds: float):  # headers: case-insensitive mapping
        self.content, self.headers, self.seconds = content, headers, seconds

    def json(self) -> Any:
        return json.loads(self.content)


TIMING: Counter = Counter()  # requests, retries, seconds, slowest: reported by every mode


DEADLINE = float("inf")  # the run's hard deadline (main sets it): no request outlives it


def get(session: requests.Session, address: str, limit: float = 300, **params: Any) -> Fetched:
    """GET with retries; each attempt is streamed in small chunks and abandoned after ``limit``
    seconds or at the run's deadline, so a server that trickles bytes cannot hold the run (a
    4-hour trial stalled that way, 24 Oct). Every failure is a RuntimeError (NotFound for 404)."""
    for attempt in range(6):
        started, status, text = time.monotonic(), None, ""
        cap = min(limit, DEADLINE - started)
        if cap <= 0:
            raise RuntimeError(f"run deadline reached before {address}")
        try:
            with session.get(address, params=params or None, timeout=(30, 60), stream=True) as response:
                status = response.status_code
                if status == 200:
                    chunks = []
                    for chunk in response.iter_content(64 << 10):
                        chunks.append(chunk)
                        if time.monotonic() - started > cap:
                            raise requests.Timeout(f"slower than {cap:.0f} s")
                    seconds = time.monotonic() - started
                    TIMING["requests"] += 1
                    TIMING["seconds"] += round(seconds)
                    TIMING["slowest"] = max(TIMING["slowest"], round(seconds))
                    return Fetched(b"".join(chunks), response.headers, seconds)
                text = response.text[:200]
        except (requests.ConnectionError, requests.Timeout, requests.exceptions.ChunkedEncodingError):
            status = None
        except requests.RequestException as exc:  # redirects, decoding: not worth a retry
            raise RuntimeError(f"DBW website request failed for {address}: {exc}") from exc
        if status == 404:
            raise NotFound(f"DBW website 404 for {address} {params or ''}")
        if status is not None and status not in (429, 500, 502, 503, 504):
            raise RuntimeError(f"DBW website {status} for {address}: {text}")
        TIMING["retries"] += 1
        pause = min(60, 5 * 2**attempt)
        if time.monotonic() + pause > DEADLINE:
            break
        time.sleep(pause)
    raise RuntimeError(f"DBW website kept failing: {address}")


def indicators(tree: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Every indicator of the tree (id, name, path of area and group names)."""
    out, stack = [], [(node, []) for node in tree]
    while stack:
        node, path = stack.pop()
        if node.get("type") == "INDICATOR" and node.get("indicator_id") is not None:
            out.append({"indicator_id": int(node["indicator_id"]), "name": node.get("name"), "path": path})
        for child in node.get("children") or []:
            stack.append((child, [*path, node.get("name")]))
    unique = {i["indicator_id"]: i for i in out}  # an indicator can sit under several groups
    return sorted(unique.values(), key=lambda i: i["indicator_id"])


def files_of(indicator: dict[str, Any], body: dict[str, Any]) -> list[dict[str, Any]]:
    """The ZIP files of one indicator with their cross-section's facts and last update."""
    out = []
    for row in (body.get("data") or {}).get("table", {}).get("rows") or []:
        values = [c.get("value") for c in row if isinstance(c, dict) and "value" in c]
        section = next((c.get("section_id") for c in row if isinstance(c, dict) and "section_id" in c), None)
        updated = next((v for v in values if isinstance(v, str) and DATE_RE.fullmatch(v)), None)
        for cell in row:
            for f in (cell.get("files") if isinstance(cell, dict) else None) or []:
                name = f.get("filename") or ""
                if not FILE_RE.fullmatch(name) or not updated:
                    continue
                out.append(
                    {
                        "entity": name[: -len(".zip")],
                        "file": name,
                        "indicator_id": indicator["indicator_id"],
                        "indicator_name": indicator["name"],
                        "section_id": section,
                        "section_name": values[1] if len(values) > 1 else None,
                        "periodicity": values[2] if len(values) > 2 else None,
                        "unit": values[3] if len(values) > 3 else None,
                        "years": values[4] if len(values) > 4 else None,
                        "updated": updated,
                        "version": updated.replace("-", "") + "T000000Z",
                    }
                )
    return out


INVENTORY_KEY = "_control/gus_dbw/inventory.json"  # lakehouse bucket: each indicator's last listing
FRESH_HOURS = 20  # a listing younger than this is reused: one listing of every indicator a day


def read_inventory(client) -> dict[str, Any]:
    try:
        body = client.get_object(Bucket=req("STORAGE_LAKEHOUSE_BUCKET_NAME"), Key=INVENTORY_KEY)["Body"].read()
    except client.exceptions.NoSuchKey:
        return {"indicators": {}}
    return json.loads(body)


def write_inventory(client, cache: dict[str, Any]) -> None:
    body = json.dumps(cache, ensure_ascii=False, separators=(",", ":")).encode()
    client.put_object(
        Bucket=req("STORAGE_LAKEHOUSE_BUCKET_NAME"), Key=INVENTORY_KEY, Body=body, ContentType="application/json"
    )


def inventory(
    session: requests.Session,
    only: set[int] | None,
    deadline: float,
    cache: dict[str, Any] | None = None,
    fresh_hours: float = FRESH_HOURS,
    save=None,
    cache_only: bool = False,
) -> tuple[list[dict], dict]:
    """(files listed by the website, counts). The tree is one request; each indicator one more,
    unless ``cache`` holds a listing of it younger than ``fresh_hours`` (the website takes 2-5 s
    an indicator: ~1,550 indicators are 1-2 h). The oldest listings are renewed first, so a
    listing cut by the deadline continues with the rest next run. ``save(cache)`` is called every
    100 renewed listings."""
    cache = cache if cache is not None else {"indicators": {}}
    held = cache.setdefault("indicators", {})
    tree = get(session, f"{WEB}/api_app/wsk/getIndicatorsTree").json()
    found, stats = [], Counter()
    now = datetime.now(UTC)
    todo = [i for i in indicators(tree) if not only or i["indicator_id"] in only]
    todo.sort(key=lambda i: (held.get(str(i["indicator_id"])) or {}).get("listed_at", ""))
    renewed = 0
    for ind in todo:
        entry = held.get(str(ind["indicator_id"]))
        age = (now - datetime.fromisoformat(entry["listed_at"])).total_seconds() / 3600 if entry else None
        if entry and age is not None and age < fresh_hours:
            stats["indicators_from_cache"] += 1
            found += entry["files"]
            continue
        if cache_only:  # inspect and shard jobs never list the website
            stats["indicators_not_listed_yet"] += 1
            continue
        if age is not None and age > 48:
            stats["indicators_listing_older_than_48h"] += 1
        if time.monotonic() > deadline:
            stats["indicators_not_listed_time_budget"] += 1
            if entry:  # an older listing still says what exists; its files are not lost
                found += entry["files"]
            continue
        time.sleep(PAUSE_SECONDS)
        try:
            body = get(session, f"{WEB}/api_app/getAggregatesById", id=ind["indicator_id"], czy_pl="true").json()
        except NotFound:  # in the tree, but nothing published for it (seen 24 Oct 2026)
            stats["indicators_not_found"] += 1
            held[str(ind["indicator_id"])] = {"listed_at": datetime.now(UTC).isoformat(), "files": []}
            continue
        except (RuntimeError, ValueError) as exc:  # one indicator's failure is counted; the run goes on
            stats["indicators_failed"] += 1
            stats.setdefault("indicators_failed_sample", []).append(f"{ind['indicator_id']}: {str(exc)[:120]}")
            if entry:
                found += entry["files"]
            continue
        files = files_of(ind, body)
        held[str(ind["indicator_id"])] = {"listed_at": datetime.now(UTC).isoformat(), "files": files}
        stats["indicators"] += 1
        stats["indicators_without_files"] += not files
        found += files
        renewed += 1
        if save and renewed % 100 == 0:
            save(cache)
    if save and renewed:
        save(cache)
    unique: dict[str, dict] = {}
    for f in found:  # a file listed twice keeps its newest date
        if f["entity"] not in unique or f["version"] > unique[f["entity"]]["version"]:
            unique[f["entity"]] = f
    if "indicators_failed_sample" in stats:
        stats["indicators_failed_sample"] = stats["indicators_failed_sample"][:5]
    return sorted(unique.values(), key=lambda f: f["entity"]), dict(stats)


def landed(client, bucket: str) -> dict[str, list[dict[str, Any]]]:
    """Receipts in Landing by entity, newest (version, ingested) last."""
    keys = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=PREFIX):
        keys += [o["Key"] for o in page.get("Contents", []) if o["Key"].endswith(".receipt.json")]

    def read(key: str) -> dict[str, Any]:
        return {**json.loads(get_with_backoff(client, bucket, key)), "receipt_key": key}

    out: dict[str, list[dict[str, Any]]] = {}
    with ThreadPoolExecutor(max_workers=8) as pool:  # R2 throttles larger bursts of reads
        for body in pool.map(read, keys):
            out.setdefault(body["entity"], []).append(body)
    for rows in out.values():
        rows.sort(key=lambda r: (r["version_token"], r["ingested_at"]))
    return out


def classify(files: list[dict], held: dict[str, list]) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = {"current": [], "stale": [], "missing": []}
    for f in files:
        rows = held.get(f["entity"])
        if not rows:
            out["missing"].append(f)
        elif rows[-1]["version_token"] < f["version"]:
            out["stale"].append(f)
        else:
            out["current"].append(f)
    return out


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


def run_load(client, bucket, session, todo: list[dict], held: dict[str, list], deadline: float) -> Counter:
    outcomes: Counter = Counter()
    failures: list[str] = []
    code_sha = os.environ.get("GITHUB_SHA", "")
    for f in todo:
        if time.monotonic() > deadline:
            outcomes["not_started_time_budget"] += 1
            continue
        time.sleep(PAUSE_SECONDS)
        address = f"{WEB}/bulk_new/{f['file']}"
        try:
            response = get(session, address, limit=900)
        except NotFound:  # listed but not served: counted, no alert (as listed indicators without data)
            outcomes["files_not_served"] += 1
            continue
        except RuntimeError as exc:  # one file's failure is counted; the next run tries it again
            outcomes["failed_download"] += 1
            failures.append(f"{f['entity']}: {str(exc)[:120]}")
            continue
        body = response.content
        if body[:2] != b"PK":
            outcomes["not_a_zip"] += 1
            failures.append(f"{f['entity']}: not a ZIP")
            continue
        digest = sha256(body).hexdigest()
        previous = (held.get(f["entity"]) or [None])[-1]
        # A new date with the same bytes still lands as the new version (else it would look
        # stale on every run); the Archive keeps the bytes once and Bronze sees the same recipe.
        ingested = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        key = f"{PREFIX}{f['entity']}/{f['version']}__{ingested}.zip"
        receipt = {
            "schema_version": 2,
            "kind": "gus_dbw_landing",
            "source_id": SOURCE,
            "entity": f["entity"],
            "content": "snapshot",
            "area": "data",
            "source_kind": "dbw_bulk_zip_csv",
            "indicator_id": f["indicator_id"],
            "indicator_name": f["indicator_name"],
            "section_id": f["section_id"],
            "section_name": f["section_name"],
            "periodicity": f["periodicity"],
            "unit": f["unit"],
            "years": f["years"],
            "source_updated": f["updated"],
            "version_token": f["version"],
            "ingested_at": ingested,
            "source_url": address,
            "sha256": digest,
            "size_bytes": len(body),
            "object": {"bucket": bucket, "key": key},
            "http": {
                "content_type": response.headers.get("Content-Type"),
                "last_modified": response.headers.get("Last-Modified"),
                "etag": response.headers.get("ETag"),
            },
            "replaces_sha256": previous["sha256"] if previous else None,
            "code_sha": code_sha,
        }
        try:  # data first, then its receipt; a failed receipt takes the data object back out
            put_new(client, bucket, key, body, "application/zip")
            try:
                put_new(
                    client,
                    bucket,
                    key[: -len(".zip")] + ".receipt.json",
                    json.dumps(receipt, ensure_ascii=False, indent=2, sort_keys=True).encode(),
                    "application/json",
                )
            except Exception:
                with contextlib.suppress(Exception):
                    client.delete_object(Bucket=bucket, Key=key)
                raise
        except Exception as exc:  # storage trouble for one file: counted, the next run tries again
            outcomes["failed_write"] += 1
            failures.append(f"{f['entity']}: write {type(exc).__name__}: {str(exc)[:100]}")
            continue
        same = previous and previous["sha256"] == digest
        outcomes["landed_new" if not previous else "landed_same_bytes" if same else "landed_changed"] += 1
    if failures:
        outcomes["failures_sample"] = failures[:10]  # type: ignore[assignment]
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
    return {
        "receipts": sum(len(v) for v in held.values()),
        "missing_data": len(problems),
        "missing_sample": problems[:10],
        "data_without_receipt": sorted(present - receipted)[:10],
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--mode", choices=("inspect", "load", "verify"), required=True)
    parser.add_argument("--filter", default="", help="comma separated indicator ids")
    parser.add_argument("--budget-minutes", type=float, default=300)
    parser.add_argument("--shard", default="0/1", help="job i of n: downloads only its share of the files")
    args = parser.parse_args(argv)
    global DEADLINE
    deadline = time.monotonic() + args.budget_minutes * 60
    DEADLINE = deadline + 10 * 60  # no request outlives the budget by more than this
    shard, shards = (int(v) for v in args.shard.split("/"))
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    held = landed(client, bucket)
    if args.mode == "verify":
        summary = {"mode": "verify", **run_verify(client, bucket, held)}
        status = 1 if summary["missing_data"] else 0
    else:
        only = {int(x) for x in re.split(r"[,\s]+", args.filter) if x.strip().isdigit()} or None
        session = requests.Session()
        session.headers.update({"User-Agent": USER_AGENT})
        cache = read_inventory(client)
        if args.mode == "inspect" or shard:  # from the last listings: only load job 0 lists the website
            files, stats = inventory(session, only, deadline, cache, fresh_hours=float("inf"), cache_only=True)
        else:  # listing gets at most half of the budget; downloads get the rest
            listing_deadline = min(deadline, time.monotonic() + args.budget_minutes * 30)
            files, stats = inventory(session, only, listing_deadline, cache, save=lambda c: write_inventory(client, c))
        groups = classify(files, held)
        summary = {
            "mode": args.mode,
            "files_listed": len(files),
            "landed_entities": len(held),
            **{k: len(v) for k, v in groups.items()},
            "stale_sample": [f["entity"] for f in groups["stale"][:10]],
            "missing_sample": [f["entity"] for f in groups["missing"][:10]],
            **stats,
        }
        # inspect only reports: an unfinished listing is a warning, never a reason to skip the extract
        # a listing cut by its budget continues next run (oldest listings first); one that falls
        # two days behind is a warning the owner sees (exit 2)
        status = 2 if args.mode == "load" and stats.get("indicators_listing_older_than_48h") else 0
        if args.mode == "load":
            todo = [
                f
                for f in groups["missing"] + groups["stale"]
                if int(sha256(f["entity"].encode()).hexdigest(), 16) % shards == shard
            ]
            outcomes = run_load(client, bucket, session, todo, held, deadline)
            summary.update(outcomes)
            status = 2 if outcomes["not_started_time_budget"] or status else 0
        failed = ("failed_download", "not_a_zip", "failed_write")
        if args.mode == "load" and (stats.get("indicators_failed") or any(summary.get(k) for k in failed)):
            status = 1  # the rest was done; the failed indicators or files are retried next run
    summary["timing"] = dict(TIMING)
    print(json.dumps(summary, sort_keys=True, default=str))
    annotate("error" if status == 1 else "notice", f"gus_dbw landing {args.mode}", summary)
    step_summary(f"## gus_dbw · landing {args.mode}\n\n```json\n{json.dumps(summary, indent=2, default=str)}\n```\n")
    return status


if __name__ == "__main__":
    raise SystemExit(main())
