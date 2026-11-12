#!/usr/bin/env python3
"""World Bank GEM Landing: selected Global Economic Monitor series (API source 15) for the EU27 members.

Route (docs/sources.md, World Bank GEM; decision of 12 Nov 2026: the World Bank API like world_bank_icp). The GEM
bulk file on DataBank (GemDataEXTR.zip) was last modified in March 2024 while the source was updated on
2026-09-08, so it is stale; the API serves the current data. A request for every economy (``country/all``) or for
several series at once does not finish (HTTP 524 after 300 s, Dev probes 43270092150, 43275104514, 43278796642),
while one series for the 27 EU members answers in about 70 s (18,225 records, 4 MB). Entity = one declared series
for the EU27 members, every period (annual, quarterly and monthly codes in one answer):
``/v2/sources/15/country/<EU27 ISO3>/series/<id>/time/all?format=json&per_page=50000``. Version = the source's
``lastupdated`` date, so a series is fetched again only when the World Bank updates GEM (two small catalogue
requests otherwise).

Layout (docs/landing.md): ``<root>/world_bank_gem/snapshot/data/series_<id>/<version>__<ingested>.json`` + receipt.

Modes: inspect (declared series vs Landing: missing / stale / current), load (missing, then stale; unchanged bytes
are not written again), verify (every receipt has its data file, in Landing or the Archive). Polite access as the
ICP extractor (its fetch: one request at a time, a pause, bounded retries with backoff on 429/5xx and timeouts, an
identifying User-Agent). A series that still fails is recorded and the others are landed; the run then exits 1.
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
from ingestion.to_landing.world_bank_icp.extract import USER_AGENT, as_json, fetch, put_new

SOURCE = "world_bank_gem"
SOURCE_ID = 15
API = "https://api.worldbank.org/v2"
PER_PAGE = 50000
PAUSE_SECONDS = 2.0
# The series built into Gold (gold.build_world_bank_gem, macro poc_world_bank_gem_series); ids as in the API.
SERIES = [
    "CPTOTSAXNZGY",  # CPI price, % y-o-y (name: "not seas. adj."; definition: seasonally adjusted)
    "CPTOTSAXN",  # CPI price index, seasonally adjusted
    "CORESA",  # core CPI index, seasonally adjusted
    "IPTOTSAKD",  # industrial production, constant US$, seasonally adjusted
    "UNEMPSA_",  # unemployment rate, %, seasonally adjusted
    "RETSALESSA",  # retail sales volume index, seasonally adjusted
    "DPANUSSPB",  # exchange rate, LCU per USD, period average
    "NYGDPMKTPSAKD",  # GDP, constant 2010 US$, millions, seasonally adjusted
]
EU27 = "AUT;BEL;BGR;CYP;CZE;DEU;DNK;EST;ESP;FIN;FRA;GRC;HRV;HUN;IRL;ITA;LTU;LUX;LVA;MLT;NLD;POL;PRT;ROU;SWE;SVN;SVK"


def prefix(root: str) -> str:
    return f"{root.rstrip('/')}/{SOURCE}/snapshot/data/"


def version(session: requests.Session) -> tuple[str, list[str]]:
    """The source's lastupdated date and the series it lists (to fail loudly on a renamed series)."""
    address = f"{API}/sources/{SOURCE_ID}?format=json"
    meta = as_json(fetch(session, address)[0], address)
    token = meta[1][0]["lastupdated"].replace("-", "") + "T000000Z"
    time.sleep(PAUSE_SECONDS)
    address = f"{API}/sources/{SOURCE_ID}/series?format=json&per_page=1000"
    listing = as_json(fetch(session, address)[0], address)
    listed = [v["id"] for concept in listing["source"][0]["concept"] for v in concept["variable"]]
    return token, listed


def series_url(series_id: str) -> str:
    return f"{API}/sources/{SOURCE_ID}/country/{EU27}/series/{series_id}/time/all?format=json&per_page={PER_PAGE}"


def landed(client, bucket: str, root: str) -> dict[str, list[dict[str, Any]]]:
    """Receipts in Landing by series id, newest (version, ingested) last."""
    out: dict[str, list[dict[str, Any]]] = {}
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix(root)):
        for o in page.get("Contents", []):
            if o["Key"].endswith(".receipt.json"):
                body = {**json.loads(get_with_backoff(client, bucket, o["Key"])), "receipt_key": o["Key"]}
                out.setdefault(body["series_id"], []).append(body)
    for rows in out.values():
        rows.sort(key=lambda r: (r["version_token"], r["ingested_at"]))
    return out


def state(rows: list[dict[str, Any]] | None, token: str) -> str:
    if not rows:
        return "missing"
    return "current" if rows[-1]["version_token"] >= token else "stale"


def land_series(client, bucket: str, root: str, session, series_id: str, token: str, held: list | None) -> str:
    """One series; bytes equal to the newest landed copy are not written again."""
    time.sleep(PAUSE_SECONDS)
    address = series_url(series_id)
    body, headers = fetch(session, address)
    parsed = as_json(body, address)
    if int(parsed["pages"]) != 1:
        raise RuntimeError(f"series {series_id}: {parsed['pages']} pages, expected one")
    digest = sha256(body).hexdigest()
    before = held[-1] if held else None
    if before and before["sha256"] == digest:
        return "unchanged"
    entity = f"series_{series_id}"
    ingested = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    key = f"{prefix(root)}{entity}/{token}__{ingested}.json"
    put_new(client, bucket, key, body, "application/json")
    receipt = {
        "schema_version": 2,
        "kind": "world_bank_gem_landing",
        "source_id": SOURCE,
        "entity": entity,
        "series_id": series_id,
        "observations": int(parsed["total"]),
        "content": "snapshot",
        "area": "data",
        "source_kind": "world_bank_api_v2_json",
        "version_token": token,
        "ingested_at": ingested,
        "source_url": address,
        "sha256": digest,
        "size_bytes": len(body),
        "object": {"bucket": bucket, "key": key},
        "http": {"content_type": headers.get("Content-Type"), "last_modified": headers.get("Last-Modified")},
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
    parser.add_argument("--filter", default="", help="comma separated series ids (default: every declared series)")
    parser.add_argument("--budget-minutes", type=float, default=120)
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
        token, listed = version(session)
        unknown = [s for s in SERIES if s not in listed]
        if unknown:
            raise RuntimeError(f"GEM no longer lists the declared series {unknown}")
        wanted = {s.strip() for s in args.filter.split(",") if s.strip()}
        series = [s for s in SERIES if not wanted or s in wanted]
        states = {s: state(held.get(s), token) for s in series}
        counts = Counter(states.values())
        summary = {"mode": args.mode, "source_version": token, "series": len(series), **counts}
        status = 0
        if args.mode == "load":
            deadline = time.monotonic() + args.budget_minutes * 60
            outcomes: Counter = Counter()
            failed: list[dict[str, str]] = []
            order = [s for s in series if states[s] == "missing"] + [s for s in series if states[s] == "stale"]
            for series_id in order:
                if time.monotonic() > deadline:
                    outcomes["not_started_time_budget"] += 1
                    continue
                try:
                    outcomes[
                        land_series(client, bucket, args.landing_root, session, series_id, token, held.get(series_id))
                    ] += 1
                except RuntimeError as exc:  # one series failing must not stop the others; recorded, run ends red
                    failed.append({"series": series_id, "error": str(exc)[:200]})
            summary.update(outcomes)
            summary["failed_series"] = failed[:5]
            status = 1 if failed else 2 if outcomes["not_started_time_budget"] else 0
    print(json.dumps(summary, sort_keys=True, default=str))
    annotate("error" if status == 1 else "notice", f"world_bank_gem landing {args.mode}", summary)
    step_summary(
        f"## world_bank_gem · landing {args.mode}\n\n```json\n{json.dumps(summary, indent=2, default=str)}\n```\n"
    )
    return status


if __name__ == "__main__":
    raise SystemExit(main())
