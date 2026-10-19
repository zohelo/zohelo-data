#!/usr/bin/env python3
"""GUS TERYT Landing: full files TERC, SIMC, ULIC and WMRODZ from the public eTERYT page.

Route (docs/sources.md, TERYT): the public "pliki pełne" page of eTERYT serves each
catalogue as a ZIP (XML + CSV) through an ASP.NET form postback; no account and no
browser are needed (the SOAP service TERYT ws1 needs an account). One page load and
one request per file, one at a time with a pause.

Entity = catalogue variant: TERC_Urzedowy, TERC_Adresowy, SIMC_Urzedowy, SIMC_Adresowy,
SIMC_Statystyczny, ULIC_Urzedowy, ULIC_Adresowy, WMRODZ. Version = the catalogue state
date inside the file (``<catalog … date="YYYY-MM-DD">``); TERC/SIMC change on 1 January
or on legal changes, ULIC daily. A file is written only when its version is newer than
the newest landed one.

Layout: ``01_landing/gus_teryt/snapshot/data/<entity>/<version>__<ingested>.zip`` + receipt.
Modes: inspect (Landing versions per entity, no requests to eTERYT), load, verify.
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
import zipfile

import requests

from ingestion.common.r2 import archive_key, archived, get_with_backoff, req, s3
from ingestion.common.report import annotate, step_summary

SOURCE = "gus_teryt"
PREFIX = "01_landing/gus_teryt/snapshot/data/"
PAGE = (
    "https://eteryt.stat.gov.pl/eTeryt/rejestr_teryt/udostepnianie_danych/baza_teryt/"
    "uzytkownicy_indywidualni/pobieranie/pliki_pelne.aspx"
)
CONTROLS = {
    "TERC_Urzedowy": "ctl00$body$BTERCUrzedowyPobierz",
    "TERC_Adresowy": "ctl00$body$BTERCAdresowyPobierz",
    "SIMC_Urzedowy": "ctl00$body$BSIMCUrzedowyPobierz",
    "SIMC_Adresowy": "ctl00$body$BSIMCAdresowyPobierz",
    "SIMC_Statystyczny": "ctl00$body$BSIMCStatystycznyPobierz",
    "ULIC_Urzedowy": "ctl00$body$BULICUrzedowyPobierz",
    "ULIC_Adresowy": "ctl00$body$BULICAdresowyPobierz",
    "WMRODZ": "ctl00$body$BRodzMiejPobierz",
}
PAUSE_SECONDS = 2
USER_AGENT = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"


def landed(client, bucket: str) -> dict[str, list[dict[str, Any]]]:
    receipts = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=PREFIX):
        receipts += [o["Key"] for o in page.get("Contents", []) if o["Key"].endswith(".receipt.json")]

    def read(key: str) -> dict[str, Any]:
        return {**json.loads(get_with_backoff(client, bucket, key)), "receipt_key": key}

    out: dict[str, list[dict[str, Any]]] = {}
    with ThreadPoolExecutor(max_workers=8) as pool:
        for body in pool.map(read, receipts):
            out.setdefault(body["entity"], []).append(body)
    for rows in out.values():
        rows.sort(key=lambda r: (r["version_token"], r["ingested_at"]))
    return out


def catalogue_date(body: bytes) -> str:
    """The catalogue state date from the XML member (``<catalog … date="YYYY-MM-DD">``)."""
    archive = zipfile.ZipFile(io.BytesIO(body))
    xml = [i for i in archive.infolist() if i.filename.lower().endswith(".xml")]
    if len(xml) != 1 or not any(i.filename.lower().endswith(".csv") for i in archive.infolist()):
        raise RuntimeError(f"unexpected TERYT ZIP members: {[i.filename for i in archive.infolist()]}")
    with archive.open(xml[0]) as handle:
        match = re.search(rb'<catalog[^>]*\sdate="(\d{4}-\d{2}-\d{2})"', handle.read(2000))
    if not match:
        raise RuntimeError("TERYT XML without catalogue date")
    return match.group(1).decode().replace("-", "") + "T000000Z"


def put_new(client, bucket: str, key: str, body: bytes, content_type: str) -> None:
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


def request(session: requests.Session, method: str, data: dict | None = None) -> requests.Response:
    last = ""
    for attempt in range(5):
        try:
            response = session.request(method, PAGE, data=data, timeout=180)
            if response.status_code == 200:
                return response
            if response.status_code not in (429, 500, 502, 503, 504):
                raise RuntimeError(f"eTERYT {method} HTTP {response.status_code}")
            last = f"HTTP {response.status_code}"
        except (requests.ConnectionError, requests.Timeout) as error:
            last = f"{type(error).__name__}: {error}"[:300]
        time.sleep(10 * 2**attempt)
    raise RuntimeError(f"eTERYT {method} kept failing after 5 attempts; last: {last}")


def run_load(client, bucket: str, entities: list[str], held: dict[str, list]) -> Counter:
    outcomes: Counter = Counter()
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    html = request(session, "GET").text
    fields = dict(re.findall(r'<input[^>]+name="(__[A-Z]+)"[^>]+value="([^"]*)"', html))
    if "__VIEWSTATE" not in fields:
        raise RuntimeError("eTERYT page without form state; the page changed")
    # A catalogue whose download button is gone is skipped with a warning, so one page change does
    # not stop the others (#843: SIMC_Statystyczny became a generate-on-demand button in Oct 2026).
    absent = [e for e in entities if CONTROLS[e].split("$")[-1] not in html]
    if absent:
        annotate(
            "warning",
            "gus_teryt eTERYT page changed",
            {"not_landed": absent, "page_controls": sorted(set(re.findall(r"ctl00\$body\$\w+", html)))[:20]},
        )
        if len(absent) == len(entities):
            raise RuntimeError(f"eTERYT page has none of the download controls for {absent}; the page changed")
    outcomes["not_on_page"] = len(absent)
    for entity in (e for e in entities if e not in absent):
        time.sleep(PAUSE_SECONDS)
        response = request(session, "POST", {**fields, "__EVENTTARGET": CONTROLS[entity], "__EVENTARGUMENT": ""})
        body = response.content
        if body[:2] != b"PK":
            raise RuntimeError(f"eTERYT returned no ZIP for {entity}: {response.headers.get('Content-Type')}")
        version = catalogue_date(body)
        previous = (held.get(entity) or [None])[-1]
        if previous and previous["version_token"] >= version:
            outcomes["unchanged_version"] += 1
            continue
        digest = sha256(body).hexdigest()
        ingested = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        key = f"{PREFIX}{entity}/{version}__{ingested}.zip"
        put_new(client, bucket, key, body, "application/zip")
        receipt = {
            "schema_version": 2,
            "kind": "gus_teryt_landing",
            "source_id": SOURCE,
            "entity": entity,
            "content": "snapshot",
            "area": "data",
            "source_kind": "eteryt_full_file_zip",
            "version_token": version,
            "ingested_at": ingested,
            "source_url": PAGE,
            "form_control": CONTROLS[entity],
            "sha256": digest,
            "size_bytes": len(body),
            "object": {"bucket": bucket, "key": key},
            "http": {
                "content_type": response.headers.get("Content-Type"),
                "content_disposition": response.headers.get("Content-Disposition"),
            },
            "replaces_sha256": previous["sha256"] if previous else None,
            "code_sha": os.environ.get("GITHUB_SHA", ""),
        }
        put_new(
            client,
            bucket,
            key[: -len(".zip")] + ".receipt.json",
            json.dumps(receipt, indent=2, sort_keys=True).encode(),
            "application/json",
        )
        outcomes["landed_new" if not previous else "landed_newer_version"] += 1
    return outcomes


def run_verify(client, bucket: str, held: dict[str, list]) -> dict[str, Any]:
    present = set()
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=PREFIX):
        present |= {o["Key"] for o in page.get("Contents", []) if not o["Key"].endswith(".receipt.json")}
    stored = archived(client, bucket, SOURCE)
    missing = [
        r["object"]["key"]
        for rows in held.values()
        for r in rows
        if r["object"]["key"] not in present
        and stored.get(r["sha256"], {}).get("key") != archive_key(SOURCE, r["sha256"], r["object"]["key"])
    ]
    receipted = {r["object"]["key"] for rows in held.values() for r in rows}
    return {
        "receipts": len(receipted),
        "missing_data": len(missing),
        "missing_sample": missing[:10],
        "data_without_receipt": sorted(present - receipted)[:10],
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--mode", choices=("inspect", "load", "verify"), required=True)
    parser.add_argument("--filter", default="", help="comma separated entities, e.g. TERC_Urzedowy")
    args = parser.parse_args(argv)
    client, bucket = s3(), req("R2_LANDING_BUCKET")
    wanted = {e.strip() for e in args.filter.split(",") if e.strip()}
    unknown = wanted - set(CONTROLS)
    if unknown:
        raise SystemExit(f"unknown TERYT entities: {sorted(unknown)}")
    entities = [e for e in CONTROLS if not wanted or e in wanted]
    held = landed(client, bucket)
    if args.mode == "verify":
        summary = {"mode": "verify", **run_verify(client, bucket, held)}
        status = 1 if summary["missing_data"] else 0
    else:
        summary = {
            "mode": args.mode,
            "entities": len(entities),
            "landed_versions": {e: held[e][-1]["version_token"] for e in entities if e in held},
            "missing": [e for e in entities if e not in held],
        }
        status = 0
        if args.mode == "load":
            summary.update(run_load(client, bucket, entities, held))
    print(json.dumps(summary, sort_keys=True))
    annotate("notice", f"gus_teryt landing {args.mode}", summary)
    step_summary(f"## gus_teryt · landing {args.mode}\n\n```json\n{json.dumps(summary, indent=2)}\n```\n")
    return status


if __name__ == "__main__":
    raise SystemExit(main())
