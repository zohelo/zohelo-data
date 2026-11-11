#!/usr/bin/env python3
"""UNECE Landing: the tables of the UNECE Statistical Database (PXWeb) economic statistics, from its PXWeb API.

Route (docs/sources.md, UNECE): the database publishes no bulk files (the PX files are not served) and its
PXWeb API v1 answers data only to POST queries, so this small extractor replaces the bulk_file pattern. The
entity is one table: every table under the folders in FOLDERS (``20-ME``: macroeconomic overview, national
accounts, labour force and wages, price indices, industrial production, external relations, globalization),
listed by the API with its ``updated`` time. A table is queried whole (every value of every variable,
``json-stat2``); a table above the API's cell limit (HTTP 403) is queried per value of its first variable and
lands each part as its own entity ``<table>_p<n>``. Version = the table's ``updated`` time; a file whose bytes equal the
newest landed copy is not written again.

Layout (docs/landing.md):
``<root>/unece/snapshot/data/<table>[_p<n>]/<version>__<ingested>.json`` + receipt.

Modes: inspect (tables vs Landing: missing / stale / current), load (missing first, then stale), verify (every
receipt has its data file in Landing or the Archive). Polite access: one request at a time, a pause between
requests (the API answers bursts with 429), bounded retries with backoff, an identifying User-Agent.
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

import requests

from ingestion.common.r2 import archive_key, archived, get_with_backoff, req, s3
from ingestion.common.report import annotate, step_summary

SOURCE = "unece"
API = "https://w3.unece.org/PXWeb2015/api/v1/en/STAT"
FOLDERS = ["20-ME"]  # economic statistics; walked recursively
PAUSE_SECONDS = 4.0
TIMEOUT_SECONDS = 300
RETRIES = 5
USER_AGENT = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"


class TooLarge(RuntimeError):
    """The API refused a query above its cell limit."""


def prefix(root: str) -> str:
    return f"{root.rstrip('/')}/{SOURCE}/snapshot/data/"


def call(session: requests.Session, address: str, query: dict | None = None) -> bytes:
    """GET (or POST a query) with a pause first and bounded retries on network errors, 429 and 5xx."""
    for attempt in range(RETRIES):
        time.sleep(PAUSE_SECONDS)
        try:
            if query is None:
                response = session.get(address, timeout=TIMEOUT_SECONDS)
            else:
                response = session.post(address, json=query, timeout=TIMEOUT_SECONDS)
        except (requests.ConnectionError, requests.Timeout):
            response = None
        if response is not None and response.status_code == 200:
            return response.content
        if response is not None and response.status_code == 403 and query is not None:
            raise TooLarge(address)
        if response is not None and response.status_code not in (429, 500, 502, 503, 504):
            raise RuntimeError(f"UNECE PXWeb {response.status_code} for {address}: {response.text[:200]}")
        time.sleep(min(180, 15 * 2**attempt))
    raise RuntimeError(f"UNECE PXWeb kept failing: {address}")


def catalogue(session: requests.Session) -> dict[str, dict[str, str]]:
    """Every table under FOLDERS: entity (table id without .px) → path and version."""
    tables: dict[str, dict[str, str]] = {}
    todo = list(FOLDERS)
    while todo:
        folder = todo.pop(0)
        for item in json.loads(call(session, f"{API}/{folder}")):
            if item["type"] == "l":
                todo.append(f"{folder}/{item['id']}")
            elif item["type"] == "t":
                entity = item["id"].removesuffix(".px")
                updated = datetime.fromisoformat(item["updated"]).strftime("%Y%m%dT%H%M%SZ")
                tables[entity] = {"path": f"{folder}/{item['id']}", "version": updated, "title": item["text"].strip()}
    return tables


def table_variables(session: requests.Session, path: str) -> list[dict[str, Any]]:
    """The table's variables (code and value codes) from its metadata."""
    meta = json.loads(call(session, f"{API}/{path}"))
    return [{"code": v["code"], "values": v["values"]} for v in meta["variables"]]


def query(variables: list[dict[str, Any]], first: str | None = None) -> dict[str, Any]:
    """Every value of every variable; with ``first``, only that value of the first variable."""
    selections = [{"code": v["code"], "selection": {"filter": "all", "values": ["*"]}} for v in variables]
    if first is not None:
        selections[0]["selection"] = {"filter": "item", "values": [first]}
    return {"query": selections, "response": {"format": "json-stat2"}}


def landed(client, bucket: str, root: str) -> dict[str, list[dict[str, Any]]]:
    """Receipts in Landing by entity, newest (version, ingested, part) last."""
    keys = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix(root)):
        keys += [o["Key"] for o in page.get("Contents", []) if o["Key"].endswith(".receipt.json")]
    out: dict[str, list[dict[str, Any]]] = {}
    for key in keys:
        body = json.loads(get_with_backoff(client, bucket, key))
        out.setdefault(body["entity"], []).append(body)
    for rows in out.values():
        rows.sort(key=lambda r: r["ingested_at"])
    return out


def state(held: dict[str, list[dict[str, Any]]], entity: str, version: str) -> str:
    """missing / stale / current of a table, whole (<table>) or in parts (<table>_p1 …)."""
    rows = held.get(entity) or held.get(f"{entity}_p1")
    if not rows:
        return "missing"
    return "current" if rows[-1]["version_token"] >= version else "stale"


def put_new(client, bucket: str, key: str, body: bytes) -> None:
    """Never overwrite Landing: a key that exists is an error unless the bytes are equal."""
    digest = sha256(body).hexdigest()
    try:
        head = client.head_object(Bucket=bucket, Key=key)
        if (head.get("Metadata") or {}).get("sha256") == digest:
            return
        raise RuntimeError(f"Landing key exists with different bytes: {key}")
    except client.exceptions.ClientError as exc:  # only "not found" means free to write
        if exc.response.get("Error", {}).get("Code") not in ("404", "NoSuchKey", "NotFound"):
            raise
    client.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json", Metadata={"sha256": digest})
    if (client.head_object(Bucket=bucket, Key=key).get("Metadata") or {}).get("sha256") != digest:
        raise RuntimeError(f"Landing write could not be verified: {key}")


def land_table(client, bucket: str, root: str, session, entity: str, table: dict, held: dict) -> Counter:
    """One table (whole, or per value of its first variable as entities <table>_p<n>); a file whose bytes
    equal the newest landed copy of its entity is not written again."""
    outcomes: Counter = Counter()
    address = f"{API}/{table['path']}"
    variables = table_variables(session, table["path"])
    try:
        parts = [(entity, call(session, address, query(variables)))]
    except TooLarge:
        values = variables[0]["values"]
        parts = [(f"{entity}_p{n}", call(session, address, query(variables, v))) for n, v in enumerate(values, 1)]
    for name, body in parts:
        json.loads(body)  # a JSON-stat answer, or fail before writing
        digest = sha256(body).hexdigest()
        before = (held.get(name) or [None])[-1]
        if before and before["sha256"] == digest:
            outcomes["unchanged_files"] += 1
            continue
        ingested = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        key = f"{prefix(root)}{name}/{table['version']}__{ingested}.json"
        put_new(client, bucket, key, body)
        receipt = {
            "schema_version": 1,
            "kind": "unece_landing",
            "source_id": SOURCE,
            "entity": name,
            "table": entity,
            "title": table["title"],
            "parts": len(parts),
            "content": "snapshot",
            "area": "data",
            "source_kind": "pxweb_api_v1_json_stat2",
            "version_token": table["version"],
            "version_basis": "table updated",
            "ingested_at": ingested,
            "source_url": address,
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
        )
        outcomes["files_landed"] += 1
        outcomes["bytes"] += len(body)
    outcomes["tables_done"] += 1
    return outcomes


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
    parser.add_argument("--filter", default="", help="comma separated table ids (without .px)")
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
        tables = catalogue(session)
        wanted = {s.strip() for s in args.filter.split(",") if s.strip()}
        tables = {k: v for k, v in tables.items() if not wanted or k in wanted}
        states = {k: state(held, k, v["version"]) for k, v in tables.items()}
        counts = Counter(states.values())
        summary = {
            "mode": args.mode,
            "tables": len(tables),
            **{k: counts.get(k, 0) for k in ("current", "stale", "missing")},
        }
        status = 0
        if args.mode == "load":
            deadline = time.monotonic() + args.budget_minutes * 60
            outcomes: Counter = Counter()
            order = [k for k in tables if states[k] == "missing"] + [k for k in tables if states[k] == "stale"]
            for entity in order:
                if time.monotonic() > deadline:
                    outcomes["not_started_time_budget"] += 1
                    continue
                outcomes.update(land_table(client, bucket, args.landing_root, session, entity, tables[entity], held))
            summary.update(outcomes)
            status = 2 if outcomes["not_started_time_budget"] else 0
    print(json.dumps(summary, sort_keys=True, default=str))
    annotate("notice", f"unece landing {args.mode}", summary)
    step_summary(f"## unece · landing {args.mode}\n\n```json\n{json.dumps(summary, indent=2, default=str)}\n```\n")
    return status


if __name__ == "__main__":
    raise SystemExit(main())
