#!/usr/bin/env python3
"""IRENASTAT Landing: IRENA's electricity statistics by country, technology, data type (installed capacity in MW,
generation in GWh), grid connection and year, from the IRENASTAT PxWeb API, as one bundle of exact API answers.

Route (docs/sources/irena_stat.md; register GL-035). IRENASTAT publishes no bulk file; its PxWeb API v1
``https://pxweb.irena.org/api/v1/en/IRENASTAT`` lists folders and tables (GET) and answers data to POST queries
(JSON-stat 2.0). The host's robots.txt answers 404 (no rules); it is read before every run, and a refusal of any
kind stops the run (www.irena.org refuses runners and is never used). Table ids carry the release
(``Country_ELECSTAT_2026_H2_PX.px``), so the entity is the table without its release:

* ``country_elecstat``: ``Country_ELECSTAT_<year>_H<n>_PX.px`` of the folder "Power Capacity and Generation"
  (224 countries and areas x 26 technologies x 2 data types x 3 grid connections x 26 years = 908,544 cells in the
  2026 H2 release, Dev probe 45866615504). When the folder holds several releases the newest one is read.

Version = ``<year>_H<n>_<table updated>`` (the listing's ``updated``; the JSON-stat answer stamps 9999-12-31); a
table whose version equals the newest landed copy is not read again (one listing request), and a read whose bytes
equal the newest copy writes nothing.

The API publishes no cell limit (``?config`` answers 404), so the table is queried in parts of whole countries of at
most MAX_CELLS cells (19 queries for 224 countries); PxWeb answers a query above its limit with HTTP 403, and then the
parts are halved (never below one country, which the probe answered with 4,056 cells). A 403 on a listing or a
one-country query, and any other refusal, stops the run. Each answer must be a JSON-stat dataset holding exactly the
countries asked for. The Landing file is a ZIP of the exact bodies, ``metadata.json`` (the table's variables) and
``data_<nnn>.json`` per part, with fixed member dates, so equal answers give equal bytes; a read that fails part-way
lands nothing (restart unit: the table).

Polite access: one request at a time, PAUSE_SECONDS apart, an identifying User-Agent, timeouts, bounded retries with
backoff on 429, 5xx and network errors.

Layout (docs/landing.md): ``<root>/irena_stat/snapshot/data/<entity>/<version>__<ingested>.zip`` + receipt.
Modes: inspect (listing vs Landing: current / stale / missing), load (read and land when changed), verify (every
receipt has its data file, in Landing or the Archive).
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
from hashlib import sha256
import io
import json
import os
import re
import time
from typing import Any
import urllib.parse
import zipfile

import requests

from ingestion.common.r2 import archive_key, archived, req, s3
from ingestion.common.report import annotate, step_summary
from ingestion.to_landing.patterns import bulk_file
from ingestion.to_landing.world_bank_icp.extract import USER_AGENT, put_new

SOURCE = "irena_stat"
HOST = "https://pxweb.irena.org"
API = f"{HOST}/api/v1/en/IRENASTAT"
FOLDER = "Power Capacity and Generation"
TABLES = {"country_elecstat": re.compile(r"Country_ELECSTAT_(\d{4})_H([12])_PX\.px")}
AREA = "Country/area"  # the variable the parts are split by
MAX_CELLS = 50_000
PAUSE_SECONDS = 2.0
TIMEOUT = (30, 300)
RETRY_DELAYS = (30, 60, 120)
MARGIN_MINUTES = 5


class TooLarge(RuntimeError):
    """PxWeb refused a query above its cell limit (HTTP 403 to a POST)."""


def quoted(path: str) -> str:
    return "/".join(urllib.parse.quote(p) for p in path.split("/"))


def robots_allow(session: requests.Session) -> None:
    """Stop unless robots.txt is absent (404) or allows the API (RFC 9309)."""
    import urllib.robotparser

    response = session.get(f"{HOST}/robots.txt", timeout=TIMEOUT)
    if response.status_code == 404:
        return
    if response.status_code != 200:
        raise RuntimeError(f"robots.txt of {HOST} answered HTTP {response.status_code}: not read")
    parser = urllib.robotparser.RobotFileParser()
    parser.parse(response.text.splitlines())
    if not parser.can_fetch(USER_AGENT, API):
        raise RuntimeError(f"robots.txt of {HOST} disallows {API}")


def call(session: requests.Session, url: str, deadline: float, query: dict | None = None) -> bytes:
    """GET (or POST a query): the exact body. 429, 5xx and network errors are retried with backoff; a 403 to a
    query is TooLarge; any other status fails at once."""
    last = ""
    for delay in (0, *RETRY_DELAYS):
        if time.monotonic() + delay + PAUSE_SECONDS > deadline:
            break
        time.sleep(delay + PAUSE_SECONDS)
        try:
            if query is None:
                response = session.get(url, timeout=TIMEOUT)
            else:
                response = session.post(url, json=query, timeout=TIMEOUT)
        except requests.RequestException as exc:
            last = type(exc).__name__
            continue
        if response.status_code == 200:
            return response.content
        last = f"HTTP {response.status_code} {response.text[:160]}"
        if response.status_code == 403 and query is not None:
            raise TooLarge(url)
        if response.status_code not in (429, 500, 502, 503, 504):
            raise RuntimeError(f"IRENASTAT {last} for {url}")
    raise RuntimeError(f"IRENASTAT kept failing or the time budget ran out ({last}) for {url}")


def listing(session: requests.Session, deadline: float) -> dict[str, dict[str, Any]]:
    """The newest release of each table in TABLES: entity → table id, release, updated and version."""
    items = json.loads(call(session, f"{API}/{quoted(FOLDER)}", deadline))
    found: dict[str, dict[str, Any]] = {}
    for item in items:
        for entity, pattern in TABLES.items():
            match = pattern.fullmatch(str(item.get("id", "")))
            if not match or item.get("type") != "t":
                continue
            release = f"{match.group(1)}_H{match.group(2)}"
            updated = datetime.fromisoformat(str(item["updated"])).strftime("%Y%m%dT%H%M%S")
            table = {"table_id": item["id"], "release": release, "updated": updated, "title": item.get("text", "")}
            table["version"] = f"{release}_{updated}"
            if entity not in found or table["version"] > found[entity]["version"]:
                found[entity] = table
    missing = sorted(set(TABLES) - set(found))
    if missing:
        raise RuntimeError(f"no table for {missing} in {FOLDER!r}: {[i.get('id') for i in items][:10]}")
    return found


def query(variables: list[dict[str, Any]], areas: list[str]) -> dict[str, Any]:
    """Every value of every variable, the areas given of AREA."""
    selections = []
    for v in variables:
        if v["code"] == AREA:
            selections.append({"code": v["code"], "selection": {"filter": "item", "values": areas}})
        else:
            selections.append({"code": v["code"], "selection": {"filter": "all", "values": ["*"]}})
    return {"query": selections, "response": {"format": "json-stat2"}}


def answer_areas(body: bytes) -> list[str]:
    """The AREA codes a JSON-stat 2.0 answer holds (fails on anything else)."""
    data = json.loads(body)
    if data.get("class") != "dataset" or AREA not in (data.get("id") or []) or "value" not in data:
        raise RuntimeError(f"not a JSON-stat dataset with {AREA!r}: {body[:200]!r}")
    index = data["dimension"][AREA]["category"]["index"]
    return list(index) if isinstance(index, list) else sorted(index, key=index.get)


def read(session: requests.Session, table: dict[str, Any], deadline: float) -> tuple[list[tuple[str, bytes]], dict]:
    """The table's metadata and every part: (member name, exact body) in read order, and counts."""
    url = f"{API}/{quoted(FOLDER)}/{quoted(table['table_id'])}"
    meta_body = call(session, url, deadline)
    variables = json.loads(meta_body)["variables"]
    by_code = {v["code"]: v for v in variables}
    if AREA not in by_code:
        raise RuntimeError(f"{table['table_id']}: no variable {AREA!r} ({list(by_code)})")
    areas = list(by_code[AREA]["values"])
    cells = 1
    for v in variables:
        cells *= len(v["values"])
    per_area = cells // len(areas)
    size = max(1, MAX_CELLS // per_area)
    members: list[tuple[str, bytes]] = [("metadata.json", meta_body)]
    start, halvings = 0, 0
    while start < len(areas):
        chunk = areas[start : start + size]
        try:
            body = call(session, url, deadline, query(variables, chunk))
        except TooLarge:
            if size == 1:
                raise RuntimeError(
                    f"{table['table_id']}: a one-area query ({chunk[0]}) was refused (HTTP 403)"
                ) from None
            size, halvings = max(1, size // 2), halvings + 1
            continue
        got = answer_areas(body)
        if sorted(got) != sorted(chunk):
            raise RuntimeError(
                f"{table['table_id']}: asked {chunk[:3]}… ({len(chunk)}), answered {got[:3]}… ({len(got)})"
            )
        members.append((f"data_{len(members):03d}.json", body))
        start += len(chunk)
    counts = {
        "areas": len(areas),
        "cells": cells,
        "parts": len(members) - 1,
        "areas_per_part": size,
        "halvings": halvings,
        "variables": [v["code"] for v in variables],
    }
    return members, counts


def bundle(members: list[tuple[str, bytes]]) -> bytes:
    """A ZIP of the exact answer bodies; fixed member dates so equal answers give equal bytes."""
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, body in members:
            info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, body)
    return out.getvalue()


def landed(client, bucket: str, root: str, entity: str) -> list[dict[str, Any]]:
    """Receipts of the entity, oldest landed first."""
    return bulk_file.landed(client, bucket, bulk_file.prefix(SOURCE, entity, root))


def state(held: list[dict[str, Any]], table: dict[str, Any]) -> str:
    if not held:
        return "missing"
    return "current" if held[-1]["version_token"] >= table["version"] else "stale"


def land(client, bucket: str, root: str, session, entity: str, table: dict, held: list, deadline: float) -> dict:
    members, counts = read(session, table, deadline)
    body = bundle(members)
    digest = sha256(body).hexdigest()
    counts.update({"sha256": digest, "size_bytes": len(body)})
    if held and held[-1]["sha256"] == digest:
        return {**counts, "outcome": "unchanged"}
    ingested = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    key = f"{bulk_file.prefix(SOURCE, entity, root)}{table['version']}__{ingested}.zip"
    put_new(client, bucket, key, body, "application/zip")
    receipt = {
        "schema_version": 2,
        "kind": "irena_stat_landing",
        "source_id": SOURCE,
        "entity": entity,
        "table_id": table["table_id"],
        "release": table["release"],
        "title": table["title"],
        "content": "snapshot",
        "area": "data",
        "source_kind": "pxweb_api_v1_json_stat2_zip",
        "version_token": table["version"],
        "version_basis": "release in the table id, then the table's updated time in the API listing",
        "ingested_at": ingested,
        "source_url": f"{API}/{quoted(FOLDER)}/{quoted(table['table_id'])}",
        "sha256": digest,
        "size_bytes": len(body),
        "object": {"bucket": bucket, "key": key},
        "replaces_sha256": held[-1]["sha256"] if held else None,
        "detail": {k: v for k, v in counts.items() if k not in ("sha256", "size_bytes")},
        "code_sha": os.environ.get("GITHUB_SHA", ""),
    }
    put_new(
        client,
        bucket,
        key[: -len(".zip")] + ".receipt.json",
        json.dumps(receipt, indent=2, sort_keys=True).encode(),
        "application/json",
    )
    return {**counts, "outcome": "landed", "key": key}


def run_verify(client, bucket: str, root: str, held: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    present = set()
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{root}/{SOURCE}/"):
        present |= {o["Key"] for o in page.get("Contents", []) if not o["Key"].endswith(".receipt.json")}
    receipts = [r for rows in held.values() for r in rows]
    stored = archived(client, bucket, SOURCE) if any(r["object"]["key"] not in present for r in receipts) else {}
    problems = [
        r["object"]["key"]
        for r in receipts
        if r["object"]["key"] not in present
        and stored.get(r["sha256"], {}).get("key") != archive_key(SOURCE, r["sha256"], r["object"]["key"])
    ]
    return {"receipts": len(receipts), "missing_data": len(problems), "missing_sample": problems[:5]}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--mode", choices=("inspect", "load", "verify"), required=True)
    parser.add_argument("--budget-minutes", type=float, default=60)
    parser.add_argument("--landing-root", default="01_landing", help="Landing root (a zz_dev_ prefix in Dev trials)")
    args = parser.parse_args(argv)
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    held = {entity: landed(client, bucket, args.landing_root, entity) for entity in TABLES}
    summary: dict[str, Any] = {
        "mode": args.mode,
        "copies_held": {e: len(rows) for e, rows in held.items()},
        "newest": {e: rows[-1]["version_token"] if rows else None for e, rows in held.items()},
    }
    status = 0
    deadline = time.monotonic() + (args.budget_minutes - MARGIN_MINUTES) * 60
    try:
        if args.mode == "verify":
            summary.update(run_verify(client, bucket, args.landing_root, held))
            status = 1 if summary["missing_data"] else 0
        else:
            session = requests.Session()
            session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
            robots_allow(session)
            tables = listing(session, deadline)
            states = {e: state(held[e], t) for e, t in tables.items()}
            summary.update({"published": {e: t["version"] for e, t in tables.items()}, "states": states})
            if args.mode == "load":
                for entity, table in tables.items():
                    if states[entity] == "current":
                        summary[entity] = {"outcome": "current"}
                        continue
                    summary[entity] = land(
                        client, bucket, args.landing_root, session, entity, table, held[entity], deadline
                    )
    except (RuntimeError, requests.RequestException, KeyError, ValueError, TypeError) as exc:
        summary.update({"outcome": "failed", "error": str(exc)[:300]})
        status = 1
    print(json.dumps(summary, sort_keys=True, default=str))
    annotate("error" if status else "notice", f"{SOURCE} landing {args.mode}", summary)
    step_summary(f"## {SOURCE} · landing {args.mode}\n\n```json\n{json.dumps(summary, indent=2, default=str)}\n```\n")
    return status


if __name__ == "__main__":
    raise SystemExit(main())
