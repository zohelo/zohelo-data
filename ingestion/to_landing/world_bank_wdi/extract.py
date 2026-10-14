#!/usr/bin/env python3
"""World Bank WDI Landing: the bulk CSV archive.

Route (docs/sources.md, WDI): the stable publisher URL
https://databank.worldbank.org/data/download/WDI_CSV.zip (~270 MB; redirects to the
download host). One entity, ``WDI_CSV``; version = the response's Last-Modified (UTC), or
the download time when absent. The archive is written only when its SHA-256 differs from
the newest landed one, after the ZIP and every member have been checked. CC BY 4.0.

Layout: ``01_landing/world_bank_wdi/snapshot/data/WDI_CSV/<version>__<ingested>.zip`` + receipt.
Modes: inspect (Landing state and the server's Last-Modified via HEAD), load, verify.
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from hashlib import sha256
import json
import os
from pathlib import Path
import tempfile
import time
from typing import Any
import zipfile

import requests

from ingestion.common.r2 import archive_key, archived, get_with_backoff, req, s3
from ingestion.common.report import annotate, step_summary

SOURCE = "world_bank_wdi"
ENTITY = "WDI_CSV"
PREFIX = f"01_landing/world_bank_wdi/snapshot/data/{ENTITY}/"
URL = "https://databank.worldbank.org/data/download/WDI_CSV.zip"
USER_AGENT = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"


def landed(client, bucket: str) -> list[dict[str, Any]]:
    keys = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=PREFIX):
        keys += [o["Key"] for o in page.get("Contents", []) if o["Key"].endswith(".receipt.json")]
    rows = [{**json.loads(get_with_backoff(client, bucket, k)), "receipt_key": k} for k in keys]
    return sorted(rows, key=lambda r: (r["version_token"], r["ingested_at"]))


def version_from(headers: dict[str, str], fallback: str) -> str:
    try:
        return parsedate_to_datetime(headers["Last-Modified"]).astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")
    except (KeyError, TypeError, ValueError):
        return fallback


def download(path: Path) -> tuple[str, int, dict[str, str], str]:
    for attempt in range(4):
        digest, size = sha256(), 0
        try:
            with requests.get(
                URL, headers={"User-Agent": USER_AGENT}, stream=True, timeout=(30, 300), allow_redirects=True
            ) as response:
                if response.status_code != 200:
                    raise RuntimeError(f"WDI download {response.status_code}")
                with path.open("wb") as handle:
                    for block in response.iter_content(8 << 20):
                        handle.write(block)
                        digest.update(block)
                        size += len(block)
                expected = int(response.headers.get("Content-Length") or 0)
                if expected and size != expected:
                    raise RuntimeError(f"WDI download truncated: {size} of {expected}")
                bad = zipfile.ZipFile(path).testzip()
                if bad:
                    raise RuntimeError(f"WDI ZIP member failed its CRC: {bad}")
                return digest.hexdigest(), size, dict(response.headers), response.url
        except (requests.ConnectionError, requests.Timeout, RuntimeError, zipfile.BadZipFile) as exc:
            if attempt == 3:
                raise
            print(json.dumps({"status": "retry", "error": str(exc)[:200]}), flush=True)
            time.sleep(60 * (attempt + 1))
    raise RuntimeError("unreachable")


def run_load(client, bucket: str, held: list[dict[str, Any]]) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="zohelo-wdi-") as temporary:
        path = Path(temporary) / "wdi.zip"
        digest, size, headers, final_url = download(path)
        if held and held[-1]["sha256"] == digest:
            return {"outcome": "unchanged", "sha256": digest}
        members = [(i.filename, i.file_size) for i in zipfile.ZipFile(path).infolist()]
        ingested = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        version = version_from(headers, ingested)
        key = f"{PREFIX}{version}__{ingested}.zip"
        client.upload_file(
            str(path), bucket, key, ExtraArgs={"ContentType": "application/zip", "Metadata": {"sha256": digest}}
        )
        head = client.head_object(Bucket=bucket, Key=key)
        if head["ContentLength"] != size or (head.get("Metadata") or {}).get("sha256") != digest:
            raise RuntimeError(f"Landing write could not be verified: {key}")
    receipt = {
        "schema_version": 2,
        "kind": "world_bank_wdi_landing",
        "source_id": SOURCE,
        "entity": ENTITY,
        "content": "snapshot",
        "area": "data",
        "source_kind": "wdi_csv_zip",
        "version_token": version,
        "ingested_at": ingested,
        "source_url": URL,
        "final_url": final_url,
        "sha256": digest,
        "size_bytes": size,
        "object": {"bucket": bucket, "key": key},
        "members": members,
        "http": {
            "last_modified": headers.get("Last-Modified"),
            "etag": headers.get("ETag"),
            "content_type": headers.get("Content-Type"),
        },
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
        "version": version,
        "members": [m for m, _ in members],
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
    if args.mode == "inspect":
        head = requests.head(URL, headers={"User-Agent": USER_AGENT}, allow_redirects=True, timeout=60)
        summary["server_version"] = version_from(dict(head.headers), "unknown")
        summary["state"] = (
            "missing" if not held else "stale" if summary["server_version"] > held[-1]["version_token"] else "current"
        )
    elif args.mode == "load":
        summary.update(run_load(client, bucket, held))
    else:
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
    annotate("notice", f"world_bank_wdi landing {args.mode}", summary)
    step_summary(f"## world_bank_wdi · landing {args.mode}\n\n```json\n{json.dumps(summary, indent=2)}\n```\n")
    return status


if __name__ == "__main__":
    raise SystemExit(main())
