#!/usr/bin/env python3
"""CCKP ERA5 Landing: the World Bank Climate Change Knowledge Portal's ERA5 0.25-degree country and voivodeship
series of mean air temperature (tas) and precipitation (pr), monthly and annual, as one bundle of exact API answers.

Route (docs/sources/cckp_era5.md; register GL-036). CCKP serves ERA5 reanalysis aggregated by the World Bank to
countries and first-level sub-national units through the documented download API
``https://datacatalogapi.worldbank.org/ddhxext/v3/cckp/<dataset id>/<geocodes>?_format=json`` (the same ids as
cckpapi.worldbank.org, whose robots.txt answers HTTP 500, which RFC 9309 reads as a full disallow; this host's
robots.txt answers 404, no rules). The dataset id carries variable, aggregation and period:
``era5-x0.25_timeseries_<var>_timeseries_<agg>_<first>-<last>_mean_historical_era5_x0.25_mean``. Answers are
``{"metadata": {"status": "success", ...}, "data": {<geocode>: {<period>: <value>}}}``; an unknown id or code
answers HTTP 200 with an empty ``data``.

One read (about 11 requests, one at a time, PAUSE_SECONDS apart):

1. the period: the newest last year from the current year down to FIRST_KNOWN_LAST for which the annual tas and pr
   answers for POL are not empty (the World Bank publishes a new id per extension, 1950-2024 then 1950-2025; the
   old id kept answering on 26 Nov 2026, so the newest one that answers wins);
2. per variable and aggregation: ``all_countries`` and Poland's 16 first-level units in one request (codes joined by
   commas). The codes are stated in POL_ADM1 (and, with their TERC codes, in warehouse/macros/sources/cckp_era5.sql):
   they come from CCKP's geonames.json (download page, "Spatial Unit's Names and Codes"; names checked in Dev probe
   45810479823 and trial 45826698285). That file is on climateknowledgeportal.worldbank.org, whose robots.txt answered
   HTTP 403 to two of three Dev runners (a refusal, not worked around), so it is not read at run time; an answer
   without one of the 16 codes fails the read.

Entity = the whole selection, ``era5``: one Landing file, a ZIP of the exact answer bodies with fixed member
dates, so the same answers give the same bytes. Version = ``<period>_<read started>`` (the API gives no update time;
the period sorts first, so a newer period is a newer version). A read whose bytes equal the newest landed copy
writes nothing; a read that fails part-way lands nothing. Only reanalysis (historical ERA5) ids are read; CMIP6
projections are not observations and are never requested.

Polite access: robots.txt of the API host read first (404 = no rules; any other failure stops the read), one request
at a time, an identifying User-Agent, timeouts, bounded retries with backoff on 429, 5xx and network errors, a
Retry-After honoured (the World Bank catalogue API answered 429 once in the research probe).

Layout (docs/landing.md): ``<root>/cckp_era5/snapshot/data/era5/<version>__<ingested>.zip`` + receipt.
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
import urllib.robotparser
import zipfile

import requests

from ingestion.common.r2 import archive_key, archived, req, s3
from ingestion.common.report import annotate, step_summary
from ingestion.to_landing.patterns import bulk_file
from ingestion.to_landing.world_bank_icp.extract import USER_AGENT, put_new

SOURCE = "cckp_era5"
ENTITY = "era5"
API = "https://datacatalogapi.worldbank.org/ddhxext/v3/cckp/"
VARIABLES = ("tas", "pr")  # mean air temperature (degC), precipitation total (mm)
AGGREGATIONS = ("monthly", "annual")
FIRST_YEAR = 1950
FIRST_KNOWN_LAST = 2024  # the oldest period id seen (1950-2024); 1950-2025 answered on 26 Nov 2026
COUNTRY = "POL"  # first-level units read for this country
# Poland's first-level units in CCKP (geonames.json, Dev probe 45810479823): Dolnoslaskie ... Zachodnio-Pomorskie
POL_ADM1 = tuple(f"POL.{n}" for n in range(1982370, 1982386))
PAUSE_SECONDS = 5.0
TIMEOUT = (30, 180)
RETRY_DELAYS = (30, 60, 120)
MARGIN_MINUTES = 5


def dataset_id(variable: str, aggregation: str, period: str) -> str:
    return f"era5-x0.25_timeseries_{variable}_timeseries_{aggregation}_{period}_mean_historical_era5_x0.25_mean"


def member_name(variable: str, aggregation: str, area: str) -> str:
    return f"{variable}_{aggregation}_{area}.json"


def robots_allow(session: requests.Session, url: str) -> None:
    """Stop unless the host's robots.txt allows the URL (404 = no rules; RFC 9309)."""
    host = "/".join(url.split("/")[:3])
    response = session.get(f"{host}/robots.txt", timeout=TIMEOUT)
    if response.status_code == 404:
        return
    if response.status_code != 200:
        raise RuntimeError(f"robots.txt of {host} answered HTTP {response.status_code}: not read")
    parser = urllib.robotparser.RobotFileParser()
    parser.parse(response.text.splitlines())
    if not parser.can_fetch(USER_AGENT, url):
        raise RuntimeError(f"robots.txt of {host} disallows {url}")


def fetch(session: requests.Session, url: str, deadline: float) -> tuple[bytes, Any]:
    """GET one answer: (exact body, parsed JSON). 429, 5xx and network errors are retried with backoff (Retry-After
    honoured); another status fails at once."""
    last = ""
    for attempt, delay in enumerate((0, *RETRY_DELAYS)):
        if time.monotonic() + delay + PAUSE_SECONDS > deadline:
            break
        time.sleep(delay + PAUSE_SECONDS)
        try:
            response = session.get(url, timeout=TIMEOUT)
        except requests.RequestException as exc:
            last = type(exc).__name__
            continue
        if response.status_code == 200:
            try:
                return response.content, response.json()
            except ValueError as exc:
                raise RuntimeError(f"CCKP answered no JSON for {url}: {response.content[:200]!r}") from exc
        last = f"HTTP {response.status_code} {response.text[:160]}"
        if response.status_code not in (429, 500, 502, 503, 504):
            raise RuntimeError(f"CCKP {last} for {url}")
        retry_after = response.headers.get("Retry-After", "")
        if retry_after.isdigit() and attempt < len(RETRY_DELAYS):
            time.sleep(min(300, int(retry_after)))
    raise RuntimeError(f"CCKP kept failing ({last}) for {url}")


def data_of(answer: Any, url: str) -> dict[str, Any]:
    """The ``data`` object of a successful answer ({} when CCKP has no such id or code)."""
    if not isinstance(answer, dict) or (answer.get("metadata") or {}).get("status") != "success":
        raise RuntimeError(f"CCKP answer is not a success: {str(answer)[:200]} ({url})")
    data = answer.get("data")
    if data in (None, [], {}):
        return {}
    if not isinstance(data, dict):
        raise RuntimeError(f"CCKP data is not an object ({url})")
    return data


def newest_period(session: requests.Session, deadline: float, year: int) -> tuple[str, list[int]]:
    """The newest ``<FIRST_YEAR>-<last>`` whose annual tas and pr answers for POL are not empty, and the last years
    tried (newest first)."""
    tried = []
    for last in range(year, FIRST_KNOWN_LAST - 1, -1):
        tried.append(last)
        period = f"{FIRST_YEAR}-{last}"
        answers = []
        for variable in VARIABLES:
            url = f"{API}{dataset_id(variable, 'annual', period)}/{COUNTRY}?_format=json"
            answers.append(bool(data_of(fetch(session, url, deadline)[1], url)))
            if not answers[-1]:
                break
        if all(answers) and len(answers) == len(VARIABLES):
            return period, tried
    raise RuntimeError(f"no ERA5 period id answered for {COUNTRY} (last years tried: {tried})")


def read(session: requests.Session, deadline: float) -> tuple[list[tuple[str, bytes]], dict[str, Any]]:
    """Every answer of the selection: (member name, exact body) in read order, and counts."""
    robots_allow(session, API)
    members: list[tuple[str, bytes]] = []
    period, tried = newest_period(session, deadline, datetime.now(UTC).year)
    counts: dict[str, Any] = {"period": period, "last_years_tried": tried}
    for variable in VARIABLES:
        for aggregation in AGGREGATIONS:
            for area, geocodes in (("all_countries", "all_countries"), (f"{COUNTRY}_adm1", ",".join(POL_ADM1))):
                url = f"{API}{dataset_id(variable, aggregation, period)}/{geocodes}?_format=json"
                body, answer = fetch(session, url, deadline)
                data = data_of(answer, url)
                if not data:
                    raise RuntimeError(f"empty answer for {variable} {aggregation} {area} {period}")
                if area != "all_countries" and set(data) != set(POL_ADM1):
                    raise RuntimeError(f"{variable} {aggregation}: first-level codes answered {sorted(data)[:5]}")
                members.append((member_name(variable, aggregation, area), body))
                counts[f"{variable}_{aggregation}_{area}"] = len(data)
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
    session.headers.update({"User-Agent": USER_AGENT})
    started = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    members, counts = read(session, deadline)
    version = f"{counts['period']}_{started}"
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
        "kind": "cckp_era5_landing",
        "source_id": SOURCE,
        "entity": ENTITY,
        "content": "snapshot",
        "area": "data",
        "source_kind": "cckp_api_json_answers_zip",
        "version_token": version,
        "version_basis": "period of the dataset id, then the time the read started (the API gives no update time)",
        "ingested_at": ingested,
        "source_url": f"{API}{dataset_id('tas', 'monthly', counts['period'])}/all_countries?_format=json",
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
    parser.add_argument("--budget-minutes", type=float, default=60)
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
