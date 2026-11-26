#!/usr/bin/env python3
"""GUS STRATEG Landing: the indicators of the Strategy for Responsible Development (SOR) with their observed values
for the EU and its countries and for the voivodeships, as one bundle of exact API answers.

Route (docs/sources/gus_strateg.md; register PLS-003). STRATEG offers no bulk file; its REST API
``https://strateg.stat.gov.pl/api/`` (spec ``https://strateg.stat.gov.pl/strateg_en.json``; the host
api-strateg.stat.gov.pl named in older pages does not resolve) answers, for the strategy 78 (SOR, 2017-2030):

- ``strategies-programmes/78`` (and ``?lang=en``): the SOR indicator list, one indicator dimension each, with the
  unit, the objective and the strategy's base, intermediate and target values (targets are not observations);
- ``indicators/<id>``: the indicator's territorial levels and years;
- ``indicators/<id>/dimensions/<dimension>?level_id=<L>``: the observed values, ``{"real_values": {year:
  {territory code: value}}}``. Level 0 = the EU aggregates and the EU countries (Poland among them), level 2 = the
  voivodeships. The other levels (3 regions NUTS 2, 4-6 powiats and gminas, M and O) are not read.

Entity = the whole selection, ``sor`` (one strategy, about 190 answers, under 1 MB): one Landing file, a ZIP of the
exact answer bodies with fixed member dates, so the same answers give the same bytes. The API gives no update time:
version = the time the read started, and a read whose bytes equal the newest landed copy writes nothing. A read
that fails part-way lands nothing (the next run reads it again).

Polite access (spec: anonymous users at most 5 requests a second and 100 per 15 minutes): one request at a time,
PAUSE_SECONDS between requests (at most 95 per 15 minutes), an identifying User-Agent, timeouts, bounded retries
with backoff on 429, 5xx, timeouts and refused connections (Retry-After honoured). About 32 minutes a read.

Layout (docs/landing.md): ``<root>/gus_strateg/snapshot/data/sor/<version>__<ingested>.zip`` + receipt.
Modes: inspect (newest copy held, no requests), load (read and land when changed), verify (every receipt has its
data file, in Landing or the Archive).
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
from hashlib import sha256
import io
import json
import os
import time
from typing import Any
import zipfile

import requests

from ingestion.common.r2 import archive_key, archived, req, s3
from ingestion.common.report import annotate, step_summary
from ingestion.to_landing.patterns import bulk_file
from ingestion.to_landing.world_bank_icp.extract import USER_AGENT, put_new

SOURCE = "gus_strateg"
ENTITY = "sor"
STRATEGY = 78  # Strategia na rzecz Odpowiedzialnego Rozwoju (SOR), 2017-2030
API = "https://strateg.stat.gov.pl/api/"
LEVELS = ("0", "2")  # EU and countries; voivodeships
PAUSE_SECONDS = 9.5  # 100 requests per 15 minutes for anonymous users: at most 95
TIMEOUT = (30, 120)
RETRY_DELAYS = (30, 60, 120, 300)
MARGIN_MINUTES = 5


def strategy_paths() -> list[tuple[str, str]]:
    return [
        (f"strategy_{STRATEGY}_pl.json", f"strategies-programmes/{STRATEGY}"),
        (f"strategy_{STRATEGY}_en.json", f"strategies-programmes/{STRATEGY}?lang=en"),
    ]


def fetch(session: requests.Session, path: str, deadline: float) -> tuple[bytes, Any]:
    """GET one answer: (exact body, parsed JSON). 429, 5xx and network errors are retried with backoff; another
    status fails at once."""
    last = ""
    for attempt, delay in enumerate((0, *RETRY_DELAYS)):
        if time.monotonic() + delay > deadline:
            break
        time.sleep(PAUSE_SECONDS + delay)
        try:
            response = session.get(API + path, timeout=TIMEOUT)
        except requests.RequestException as exc:  # connect timeouts happen (Dev probe 45745965411)
            last = type(exc).__name__
            continue
        if response.status_code == 200:
            try:
                return response.content, response.json()
            except ValueError as exc:
                raise RuntimeError(f"STRATEG answered no JSON for {path}: {response.content[:200]!r}") from exc
        last = f"HTTP {response.status_code} {response.text[:160]}"
        if response.status_code not in (429, 500, 502, 503, 504):
            raise RuntimeError(f"STRATEG {last} for {path}")
        retry_after = response.headers.get("Retry-After", "")
        if retry_after.isdigit() and attempt < len(RETRY_DELAYS):
            time.sleep(min(900, int(retry_after)))
    raise RuntimeError(f"STRATEG kept failing ({last}) for {path}")


def indicator_dimensions(strategy: Any) -> list[tuple[int, int]]:
    """(indicator, dimension) pairs of the strategy answer, in answer order, each once."""
    if not isinstance(strategy, list):
        raise RuntimeError(f"strategy {STRATEGY}: not a list")
    pairs: list[tuple[int, int]] = []
    for group in strategy:
        for item in group.get("indicators") or []:
            pair = (int(item["indicator_id"]), int(item["dimension_id"]))
            if pair not in pairs:
                pairs.append(pair)
    if not pairs:
        raise RuntimeError(f"strategy {STRATEGY}: no indicators")
    return pairs


def read(session: requests.Session, deadline: float) -> tuple[list[tuple[str, bytes]], dict[str, Any]]:
    """Every answer of the selection: (member name, exact body) in read order, and counts."""
    members: list[tuple[str, bytes]] = []
    parsed = {}
    for name, path in strategy_paths():
        body, parsed[name] = fetch(session, path, deadline)
        members.append((name, body))
    pairs = indicator_dimensions(parsed[f"strategy_{STRATEGY}_pl.json"])
    counts: dict[str, Any] = {"indicator_dimensions": len(pairs), "answers_by_level": dict.fromkeys(LEVELS, 0)}
    for indicator, dimension in pairs:
        body, info = fetch(session, f"indicators/{indicator}", deadline)
        members.append((f"indicator_{indicator}.json", body))
        levels = set((info.get("territory_levels") or {}) if isinstance(info, dict) else {})
        for level in LEVELS:
            if level not in levels:
                continue
            body, _ = fetch(session, f"indicators/{indicator}/dimensions/{dimension}?level_id={level}", deadline)
            members.append((f"data_{indicator}_{dimension}_level{level}.json", body))
            counts["answers_by_level"][level] += 1
    counts["answers"] = len(members)
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


def landed(client, bucket: str, root: str) -> list[dict[str, Any]]:
    """Receipts of the entity, oldest landed first."""
    return bulk_file.landed(client, bucket, bulk_file.prefix(SOURCE, ENTITY, root))


def land(client, bucket: str, root: str, held: list[dict[str, Any]], deadline: float) -> dict[str, Any]:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
    version = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    members, counts = read(session, deadline)
    body = bundle(members)
    digest = sha256(body).hexdigest()
    counts.update({"sha256": digest, "size_bytes": len(body)})
    if held and held[-1]["sha256"] == digest:
        return {**counts, "outcome": "unchanged"}
    ingested = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    key = f"{bulk_file.prefix(SOURCE, ENTITY, root)}{version}__{ingested}.zip"
    put_new(client, bucket, key, body, "application/zip")
    receipt = {
        "schema_version": 2,
        "kind": "gus_strateg_landing",
        "source_id": SOURCE,
        "entity": ENTITY,
        "content": "snapshot",
        "area": "data",
        "source_kind": "strateg_api_json_answers_zip",
        "version_token": version,
        "version_basis": "read started (the API gives no update time)",
        "ingested_at": ingested,
        "source_url": f"{API}strategies-programmes/{STRATEGY}",
        "sha256": digest,
        "size_bytes": len(body),
        "object": {"bucket": bucket, "key": key},
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


def run_verify(client, bucket: str, root: str, held: list[dict[str, Any]]) -> dict[str, Any]:
    present = set()
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=bulk_file.prefix(SOURCE, ENTITY, root)
    ):
        present |= {o["Key"] for o in page.get("Contents", []) if not o["Key"].endswith(".receipt.json")}
    stored = archived(client, bucket, SOURCE) if any(r["object"]["key"] not in present for r in held) else {}
    problems = [
        r["object"]["key"]
        for r in held
        if r["object"]["key"] not in present
        and stored.get(r["sha256"], {}).get("key") != archive_key(SOURCE, r["sha256"], r["object"]["key"])
    ]
    return {"receipts": len(held), "missing_data": len(problems), "missing_sample": problems[:5]}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--mode", choices=("inspect", "load", "verify"), required=True)
    parser.add_argument("--budget-minutes", type=float, default=120)
    parser.add_argument("--landing-root", default="01_landing", help="Landing root (a zz_dev_ prefix in Dev trials)")
    args = parser.parse_args(argv)
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    held = landed(client, bucket, args.landing_root)
    summary: dict[str, Any] = {
        "mode": args.mode,
        "entity": ENTITY,
        "copies_held": len(held),
        "newest": held[-1]["version_token"] if held else None,
    }
    status = 0
    if args.mode == "verify":
        summary.update(run_verify(client, bucket, args.landing_root, held))
        status = 1 if summary["missing_data"] else 0
    elif args.mode == "load":
        deadline = time.monotonic() + (args.budget_minutes - MARGIN_MINUTES) * 60
        try:
            summary.update(land(client, bucket, args.landing_root, held, deadline))
        except (RuntimeError, requests.RequestException, KeyError, ValueError, TypeError) as exc:
            summary.update({"outcome": "failed", "error": str(exc)[:300]})
            status = 1
    print(json.dumps(summary, sort_keys=True, default=str))
    annotate("error" if status else "notice", f"{SOURCE} landing {args.mode}", summary)
    step_summary(f"## {SOURCE} · landing {args.mode}\n\n```json\n{json.dumps(summary, indent=2, default=str)}\n```\n")
    return status


if __name__ == "__main__":
    raise SystemExit(main())
