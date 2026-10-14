#!/usr/bin/env python3
"""GUGiK PRG Landing: the national administrative-boundaries package.

Route (docs/sources.md, PRG): one national ZIP of shapefiles,
https://opendata.geoportal.gov.pl/prg/granice/00_jednostki_administracyjne.zip (~377 MB):
A00 country, A01 voivodeships, A02 powiats, A03 gminas, A05 cadastral units, A06
cadastral precincts. GUGiK states the data is free for any purpose. The server sends no
Last-Modified or ETag, so the version is the download time, and a download is written
only when its SHA-256 differs from the newest landed file (PRG states 1 January; a few
corrections a year).

Entity: ``jednostki_administracyjne``. Layout:
``01_landing/gugik_prg/snapshot/data/jednostki_administracyjne/<version>__<ingested>.zip`` + receipt.
Modes: inspect (Landing state, no download), load, verify.
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
from hashlib import sha256
import json
import os
from pathlib import Path
import tempfile
import time
from typing import Any

import requests

from ingestion.common.r2 import archive_key, archived, get_with_backoff, req, s3
from ingestion.common.report import annotate, step_summary

SOURCE = "gugik_prg"
ENTITY = "jednostki_administracyjne"
PREFIX = f"01_landing/gugik_prg/snapshot/data/{ENTITY}/"
URL = "https://opendata.geoportal.gov.pl/prg/granice/00_jednostki_administracyjne.zip"
USER_AGENT = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"


def landed(client, bucket: str) -> list[dict[str, Any]]:
    keys = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=PREFIX):
        keys += [o["Key"] for o in page.get("Contents", []) if o["Key"].endswith(".receipt.json")]
    rows = [{**json.loads(get_with_backoff(client, bucket, k)), "receipt_key": k} for k in keys]
    return sorted(rows, key=lambda r: (r["version_token"], r["ingested_at"]))


def download(path: Path) -> tuple[str, int, dict[str, str]]:
    for attempt in range(4):
        digest, size = sha256(), 0
        try:
            with requests.get(URL, headers={"User-Agent": USER_AGENT}, stream=True, timeout=(30, 300)) as response:
                if response.status_code != 200:
                    raise RuntimeError(f"PRG download {response.status_code}")
                expected = int(response.headers.get("Content-Length") or 0)
                with path.open("wb") as handle:
                    for block in response.iter_content(8 << 20):
                        handle.write(block)
                        digest.update(block)
                        size += len(block)
                if expected and size != expected:
                    raise RuntimeError(f"PRG download truncated: {size} of {expected} bytes")
                if path.read_bytes()[:2] != b"PK":
                    raise RuntimeError("PRG download is not a ZIP")
                return digest.hexdigest(), size, dict(response.headers)
        except (requests.ConnectionError, requests.Timeout, RuntimeError) as exc:
            if attempt == 3:
                raise
            print(json.dumps({"status": "retry", "error": str(exc)[:200]}), flush=True)
            time.sleep(60 * (attempt + 1))
    raise RuntimeError("unreachable")


def run_load(client, bucket: str, held: list[dict[str, Any]]) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="zohelo-prg-") as temporary:
        path = Path(temporary) / "prg.zip"
        started = time.monotonic()
        digest, size, headers = download(path)
        seconds = round(time.monotonic() - started)
        if held and held[-1]["sha256"] == digest:
            return {"outcome": "unchanged", "sha256": digest, "download_seconds": seconds}
        ingested = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        key = f"{PREFIX}{ingested}__{ingested}.zip"
        client.upload_file(
            str(path), bucket, key, ExtraArgs={"ContentType": "application/zip", "Metadata": {"sha256": digest}}
        )
        head = client.head_object(Bucket=bucket, Key=key)
        if head["ContentLength"] != size or (head.get("Metadata") or {}).get("sha256") != digest:
            raise RuntimeError(f"Landing write could not be verified: {key}")
    receipt = {
        "schema_version": 2,
        "kind": "gugik_prg_landing",
        "source_id": SOURCE,
        "entity": ENTITY,
        "content": "snapshot",
        "area": "data",
        "source_kind": "prg_shapefile_zip",
        "version_token": ingested,
        "version_basis": "download time (server sends no validators)",
        "ingested_at": ingested,
        "source_url": URL,
        "sha256": digest,
        "size_bytes": size,
        "object": {"bucket": bucket, "key": key},
        "http": {"content_type": headers.get("Content-Type"), "content_length": headers.get("Content-Length")},
        "replaces_sha256": held[-1]["sha256"] if held else None,
        "code_sha": os.environ.get("GITHUB_SHA", ""),
    }
    client.put_object(
        Bucket=bucket,
        Key=key[: -len(".zip")] + ".receipt.json",
        ContentType="application/json",
        Body=json.dumps(receipt, indent=2, sort_keys=True).encode(),
    )
    return {
        "outcome": "landed_new" if not held else "landed_changed",
        "sha256": digest,
        "size_mb": round(size / 1e6, 1),
        "download_seconds": seconds,
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--mode", choices=("inspect", "load", "verify"), required=True)
    args = parser.parse_args(argv)
    client, bucket = s3(), req("R2_LANDING_BUCKET")
    held = landed(client, bucket)
    summary: dict[str, Any] = {
        "mode": args.mode,
        "landed_versions": len(held),
        "newest": held[-1]["version_token"] if held else None,
    }
    status = 0
    if args.mode == "load":
        summary.update(run_load(client, bucket, held))
    elif args.mode == "verify":
        present = set()
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=PREFIX):
            present |= {o["Key"] for o in page.get("Contents", []) if not o["Key"].endswith(".receipt.json")}
        stored = archived(client, bucket, SOURCE)
        missing = [
            r["object"]["key"]
            for r in held
            if r["object"]["key"] not in present
            and stored.get(r["sha256"], {}).get("key") != archive_key(SOURCE, r["sha256"], r["object"]["key"])
        ]
        summary.update({"missing_data": len(missing), "missing_sample": missing[:5]})
        status = 1 if missing else 0
    print(json.dumps(summary, sort_keys=True))
    annotate("notice", f"gugik_prg landing {args.mode}", summary)
    step_summary(f"## gugik_prg · landing {args.mode}\n\n```json\n{json.dumps(summary, indent=2)}\n```\n")
    return status


if __name__ == "__main__":
    raise SystemExit(main())
