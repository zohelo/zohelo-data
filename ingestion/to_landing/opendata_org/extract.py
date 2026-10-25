#!/usr/bin/env python3
"""OpenData.org (BrightQuery) Landing: the Senzing-ready full snapshot from the public bucket.

Route (docs/sources.md, OpenData): BrightQuery publishes OpenData.org snapshots in the public
Google Cloud Storage bucket ``bq-public-datastore`` under ``opendata/ODO-SENZING-<date>/full/``
as one ZIP (2,496 newline-delimited Senzing JSON files: Organization/, PeopleBusiness/,
Locations/). The bucket listing (GCS JSON API, no account) gives each object's size, MD5 and
update time, so a run lands the newest full snapshot only when it is newer than Landing.

Entity = ``senzing_full``; version = the object's update time (``YYYYMMDDTHHMMSSZ``). Layout
(docs/landing.md): ``01_landing/opendata_org/snapshot/data/senzing_full/<version>__<ingested>.zip``
+ receipt. The download (~21 GB) streams to the runner's disk, is checked against the bucket's
MD5 and size, and is uploaded with its SHA-256.

Modes: inspect (newest snapshot vs Landing), load, verify (every receipt has its data file).
"""

from __future__ import annotations

import argparse
import base64
from datetime import UTC, datetime
from hashlib import md5, sha256
import json
import os
from pathlib import Path
import re
import tempfile
import time
from typing import Any

import requests

from ingestion.common.r2 import archive_key, archived, get_with_backoff, req, s3
from ingestion.common.report import annotate, step_summary

SOURCE = "opendata_org"
ENTITY = "senzing_full"
PREFIX = f"01_landing/{SOURCE}/snapshot/data/{ENTITY}/"
LISTING = "https://storage.googleapis.com/storage/v1/b/bq-public-datastore/o"
DOWNLOAD = "https://storage.googleapis.com/bq-public-datastore/"
FULL_RE = re.compile(r"^opendata/ODO-SENZING-[^/]+/full/[^/]+\.zip$")
OLD_COPY = f"01_landing/{SOURCE}/ODO_SENZING_20260305.zip"  # Drive-era copy of the 5 Mar 2026 snapshot
USER_AGENT = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"


def newest(session: requests.Session) -> dict[str, Any] | None:
    """The newest full Senzing snapshot in the public bucket (name, size, md5, updated)."""
    items, token = [], None
    while True:
        params = {"prefix": "opendata/", "fields": "items(name,size,md5Hash,updated),nextPageToken"}
        if token:
            params["pageToken"] = token
        response = session.get(LISTING, params=params, timeout=60)
        response.raise_for_status()
        body = response.json()
        items += body.get("items", [])
        token = body.get("nextPageToken")
        if not token:
            break
    full = [i for i in items if FULL_RE.fullmatch(i["name"])]
    return max(full, key=lambda i: i["updated"]) if full else None


def version_of(item: dict[str, Any]) -> str:
    return datetime.fromisoformat(item["updated"].replace("Z", "+00:00")).strftime("%Y%m%dT%H%M%SZ")


def landed(client, bucket: str) -> list[dict[str, Any]]:
    keys = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=PREFIX):
        keys += [o["Key"] for o in page.get("Contents", []) if o["Key"].endswith(".receipt.json")]
    rows = [{**json.loads(get_with_backoff(client, bucket, k)), "receipt_key": k} for k in keys]
    return sorted(rows, key=lambda r: (r["version_token"], r["ingested_at"]))


def adopt(client, bucket: str, item: dict[str, Any]) -> tuple[str, int, str] | None:
    """The snapshot already in R2 (Drive-era copy), read once inside R2 for its SHA-256 and MD5;
    returns (sha256, size, source key) when it is byte-identical to the bucket's file, else None.
    Saves downloading 21 GB from the publisher again."""
    from ingestion.common.r2 import raw_chunks

    try:
        head = client.head_object(Bucket=bucket, Key=OLD_COPY)
    except client.exceptions.ClientError:
        return None
    if head["ContentLength"] != int(item["size"]) or not item["name"].endswith(OLD_COPY.rsplit("/", 1)[1]):
        return None
    digest, check, size = sha256(), md5(), 0
    for block in raw_chunks(client, bucket, OLD_COPY):
        digest.update(block)
        check.update(block)
        size += len(block)
    if size != int(item["size"]) or base64.b64encode(check.digest()).decode() != item["md5Hash"]:
        return None
    return digest.hexdigest(), size, OLD_COPY


def load(client, bucket: str, session: requests.Session, item: dict[str, Any], held: list[dict]) -> dict[str, Any]:
    url = DOWNLOAD + item["name"]
    version = version_of(item)
    started = time.monotonic()
    adopted = adopt(client, bucket, item)
    if adopted:  # server-side copy inside R2 into the agreed layout; nothing downloaded
        sha, size, origin = adopted
        ingested = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        key = f"{PREFIX}{version}__{ingested}.zip"
        client.copy(
            {"Bucket": bucket, "Key": origin},
            bucket,
            key,
            ExtraArgs={"MetadataDirective": "REPLACE", "Metadata": {"sha256": sha}, "ContentType": "application/zip"},
        )
        head = client.head_object(Bucket=bucket, Key=key)
        if head["ContentLength"] != size or (head.get("Metadata") or {}).get("sha256") != sha:
            raise RuntimeError(f"Landing copy could not be verified: {key}")
        return write_receipt(client, bucket, item, url, version, ingested, sha, size, key, held, started, origin)
    with tempfile.TemporaryDirectory(prefix="zohelo-opendata-") as tmp:
        path = Path(tmp) / "snapshot.zip"
        digest, check, size = sha256(), md5(), 0
        for attempt in range(4):
            try:
                digest, check, size = sha256(), md5(), 0
                with session.get(url, stream=True, timeout=(30, 300)) as response:
                    response.raise_for_status()
                    with path.open("wb") as handle:
                        for block in response.iter_content(16 << 20):
                            digest.update(block)
                            check.update(block)
                            size += len(block)
                            handle.write(block)
                break
            except (requests.ConnectionError, requests.Timeout, requests.exceptions.ChunkedEncodingError):
                if attempt == 3:
                    raise
                time.sleep(60 * (attempt + 1))
        if size != int(item["size"]) or base64.b64encode(check.digest()).decode() != item["md5Hash"]:
            raise RuntimeError(f"download does not match the bucket's size/MD5: {item['name']}")
        sha = digest.hexdigest()
        ingested = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        key = f"{PREFIX}{version}__{ingested}.zip"
        client.upload_file(
            str(path), bucket, key, ExtraArgs={"ContentType": "application/zip", "Metadata": {"sha256": sha}}
        )
        head = client.head_object(Bucket=bucket, Key=key)
        if head["ContentLength"] != size or (head.get("Metadata") or {}).get("sha256") != sha:
            raise RuntimeError(f"Landing write could not be verified: {key}")
    return write_receipt(client, bucket, item, url, version, ingested, sha, size, key, held, started, None)


def write_receipt(client, bucket, item, url, version, ingested, sha, size, key, held, started, origin) -> dict:
    previous = held[-1] if held else None
    receipt = {
        "schema_version": 2,
        "kind": f"{SOURCE}_landing",
        "source_id": SOURCE,
        "entity": ENTITY,
        "content": "snapshot",
        "area": "data",
        "source_kind": "senzing_jsonl_zip",
        "source_object": item["name"],
        "source_updated": item["updated"],
        "source_md5": item["md5Hash"],
        "version_token": version,
        "ingested_at": ingested,
        "source_url": url,
        "sha256": sha,
        "size_bytes": size,
        "object": {"bucket": bucket, "key": key},
        "replaces_sha256": previous["sha256"] if previous else None,
        "copied_from": origin,  # the R2 copy that was byte-identical to the bucket's file (MD5 checked)
        "code_sha": os.environ.get("GITHUB_SHA", ""),
    }
    client.put_object(
        Bucket=bucket,
        Key=key[: -len(".zip")] + ".receipt.json",
        ContentType="application/json",
        Body=json.dumps(receipt, indent=2, sort_keys=True).encode(),
    )
    return {
        "landed": item["name"],
        "gb": round(size / 1e9, 2),
        "from": origin or "publisher bucket",
        "minutes": round((time.monotonic() - started) / 60, 1),
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
        "newest_landed": held[-1]["version_token"] if held else None,
    }
    status = 0
    if args.mode == "verify":
        present = set()
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=PREFIX):
            present |= {o["Key"] for o in page.get("Contents", [])}
        stored = archived(client, bucket, SOURCE)
        missing = [
            r["object"]["key"]
            for r in held
            if r["object"]["key"] not in present
            and stored.get(r["sha256"], {}).get("key") != archive_key(SOURCE, r["sha256"], r["object"]["key"])
        ]
        summary["missing_data"] = missing
        status = 1 if missing else 0
    else:
        session = requests.Session()
        session.headers.update({"User-Agent": USER_AGENT})
        item = newest(session)
        if item is None:
            raise RuntimeError("no full Senzing snapshot listed in the public bucket")
        summary.update(
            {
                "source_newest": item["name"],
                "source_version": version_of(item),
                "source_gb": round(int(item["size"]) / 1e9, 2),
            }
        )
        current = bool(held) and held[-1]["version_token"] >= version_of(item)
        summary["state"] = "current" if current else ("stale" if held else "missing")
        if args.mode == "load" and not current:
            summary.update(load(client, bucket, session, item, held))
    print(json.dumps(summary, default=str))
    annotate("error" if status else "notice", f"{SOURCE} landing {args.mode}", summary)
    step_summary(f"## {SOURCE} · landing {args.mode}\n\n```json\n{json.dumps(summary, indent=2, default=str)}\n```\n")
    return status


if __name__ == "__main__":
    raise SystemExit(main())
