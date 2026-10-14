"""bulk_file pattern: sources whose entities are complete files at fixed URLs.

Declared in ``ingestion/sources/<source>.yaml`` (docs/landing.md, "Common ingestion
framework")::

    extraction:
      kind: bulk_file
      bulk_file:
        files:
          - entity: WDI_CSV
            url: https://databank.worldbank.org/data/download/WDI_CSV.zip
            format: zip              # zip | gzip | raw
            version: last_modified   # last_modified (fallback: download time) | ingested

Each file lands as ``01_landing/<source>/snapshot/data/<entity>/<version>__<ingested>.<ext>``
with a receipt, only when its SHA-256 differs from the newest landed copy. The download
streams to disk, is checked by format, uploaded and verified by size and SHA-256. Modes:
inspect (Landing vs the server's Last-Modified), load, verify (every receipt has its file
in Landing or the Archive).
"""

from __future__ import annotations

from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
import gzip
from hashlib import sha256
import json
import os
from pathlib import Path
import tempfile
import time
from typing import Any
import zipfile

import requests
import yaml

from ingestion.common.r2 import archive_key, archived, get_with_backoff, req, s3
from ingestion.common.report import annotate, step_summary

ROOT = Path(__file__).resolve().parents[3]
USER_AGENT = "zohelo-data/1.0 (+https://github.com/zohelo/zohelo-data; public statistics platform)"
EXTENSIONS = {"zip": "zip", "gzip": "gz", "raw": "bin"}
CONTENT_TYPES = {"zip": "application/zip", "gzip": "application/gzip", "raw": "application/octet-stream"}
RETRIES = 4


def declaration(source: str) -> list[dict[str, Any]]:
    """The declared files of a bulk_file source."""
    config = yaml.safe_load((ROOT / "ingestion" / "sources" / f"{source}.yaml").read_text())
    extraction = config.get("extraction") or {}
    if extraction.get("kind") != "bulk_file":
        raise ValueError(f"{source}: extraction.kind is not bulk_file")
    return list(extraction["bulk_file"]["files"])


def prefix(source: str, entity: str, landing_root: str = "01_landing") -> str:
    return f"{landing_root}/{source}/snapshot/data/{entity}/"


def landed(client, bucket: str, folder: str) -> list[dict[str, Any]]:
    """Receipts under a prefix, oldest version first."""
    keys = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=folder):
        keys += [o["Key"] for o in page.get("Contents", []) if o["Key"].endswith(".receipt.json")]
    rows = [{**json.loads(get_with_backoff(client, bucket, k)), "receipt_key": k} for k in keys]
    return sorted(rows, key=lambda r: (r["version_token"], r["ingested_at"]))


def stamp(moment: datetime) -> str:
    return moment.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")


def server_version(headers: dict[str, str]) -> str | None:
    try:
        return stamp(parsedate_to_datetime(headers["Last-Modified"]))
    except (KeyError, TypeError, ValueError):
        return None


def check_format(path: Path, fmt: str) -> dict[str, Any]:
    """Raise when the file is not a valid instance of its declared format; return details."""
    if fmt == "zip":
        with zipfile.ZipFile(path) as archive:
            bad = archive.testzip()
            if bad:
                raise RuntimeError(f"ZIP member failed its CRC: {bad}")
            return {"members": [(i.filename, i.file_size) for i in archive.infolist()]}
    if fmt == "gzip":
        with gzip.open(path) as handle:
            while handle.read(8 << 20):
                pass
    return {}


def download(url: str, path: Path) -> tuple[str, int, dict[str, str], str]:
    """Stream to disk with retries; returns (sha256, size, headers, final URL)."""
    for attempt in range(RETRIES):
        digest, size = sha256(), 0
        try:
            with requests.get(
                url, headers={"User-Agent": USER_AGENT}, stream=True, timeout=(30, 300), allow_redirects=True
            ) as response:
                if response.status_code != 200:
                    raise RuntimeError(f"download {response.status_code}")
                with path.open("wb") as handle:
                    for block in response.iter_content(8 << 20):
                        handle.write(block)
                        digest.update(block)
                        size += len(block)
                expected = int(response.headers.get("Content-Length") or 0)
                if expected and size != expected:
                    raise RuntimeError(f"download truncated: {size} of {expected} bytes")
                return digest.hexdigest(), size, dict(response.headers), response.url
        except (requests.ConnectionError, requests.Timeout, RuntimeError) as exc:
            if attempt == RETRIES - 1:
                raise
            print(json.dumps({"status": "retry", "url": url, "error": str(exc)[:200]}), flush=True)
            time.sleep(60 * (attempt + 1))
    raise RuntimeError("unreachable")


def load_file(client, bucket: str, source: str, spec: dict[str, Any], landing_root: str) -> dict[str, Any]:
    fmt = spec.get("format", "raw")
    folder = prefix(source, spec["entity"], landing_root)
    held = landed(client, bucket, folder)
    with tempfile.TemporaryDirectory(prefix=f"zohelo-{source}-") as temporary:
        path = Path(temporary) / f"file.{EXTENSIONS[fmt]}"
        started = time.monotonic()
        digest, size, headers, final_url = download(spec["url"], path)
        seconds = round(time.monotonic() - started)
        detail = check_format(path, fmt)
        if held and held[-1]["sha256"] == digest:
            return {"entity": spec["entity"], "outcome": "unchanged", "sha256": digest, "download_seconds": seconds}
        ingested = stamp(datetime.now(UTC))
        basis = spec.get("version", "last_modified")
        version = (server_version(headers) if basis == "last_modified" else None) or ingested
        key = f"{folder}{version}__{ingested}.{EXTENSIONS[fmt]}"
        client.upload_file(
            str(path), bucket, key, ExtraArgs={"ContentType": CONTENT_TYPES[fmt], "Metadata": {"sha256": digest}}
        )
        head = client.head_object(Bucket=bucket, Key=key)
        if head["ContentLength"] != size or (head.get("Metadata") or {}).get("sha256") != digest:
            raise RuntimeError(f"Landing write could not be verified: {key}")
    receipt = {
        "schema_version": 3,
        "kind": f"{source}_landing",
        "source_id": source,
        "entity": spec["entity"],
        "content": "snapshot",
        "area": "data",
        "version_token": version,
        "version_basis": "Last-Modified" if version != ingested else "download time",
        "ingested_at": ingested,
        "source_url": spec["url"],
        "final_url": final_url,
        "sha256": digest,
        "size_bytes": size,
        "object": {"bucket": bucket, "key": key},
        "replaces_sha256": held[-1]["sha256"] if held else None,
        "code_sha": os.environ.get("GITHUB_SHA", ""),
        "detail": {
            "pattern": "bulk_file",
            "format": fmt,
            **detail,
            "http": {
                "last_modified": headers.get("Last-Modified"),
                "etag": headers.get("ETag"),
                "content_type": headers.get("Content-Type"),
            },
        },
    }
    client.put_object(
        Bucket=bucket,
        Key=key.rsplit(".", 1)[0] + ".receipt.json",
        ContentType="application/json",
        Body=json.dumps(receipt, indent=2, sort_keys=True).encode(),
    )
    return {
        "entity": spec["entity"],
        "outcome": "landed_new" if not held else "landed_changed",
        "sha256": digest,
        "size_mb": round(size / 1e6, 1),
        "version": version,
        "download_seconds": seconds,
    }


def main(source: str, mode: str, landing_root: str = "01_landing") -> int:
    """Run one mode for every declared file of a bulk_file source; exit 1 on missing data."""
    client, bucket = s3(), req("R2_LANDING_BUCKET")
    results, status = [], 0
    for spec in declaration(source):
        folder = prefix(source, spec["entity"], landing_root)
        if mode == "load":
            results.append(load_file(client, bucket, source, spec, landing_root))
            continue
        held = landed(client, bucket, folder)
        row: dict[str, Any] = {
            "entity": spec["entity"],
            "landed_versions": len(held),
            "newest": held[-1]["version_token"] if held else None,
        }
        if mode == "inspect":
            head = requests.head(spec["url"], headers={"User-Agent": USER_AGENT}, allow_redirects=True, timeout=60)
            row["server_version"] = server_version(dict(head.headers))
            row["state"] = (
                "missing"
                if not held
                else "stale"
                if (row["server_version"] or "") > held[-1]["version_token"]
                else "current"
            )
        else:
            present = set()
            for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=folder):
                present |= {o["Key"] for o in page.get("Contents", []) if not o["Key"].endswith(".receipt.json")}
            stored = archived(client, bucket, source)
            missing = [
                r["object"]["key"]
                for r in held
                if r["object"]["key"] not in present
                and stored.get(r["sha256"], {}).get("key") != archive_key(source, r["sha256"], r["object"]["key"])
            ]
            row.update({"missing_data": len(missing), "missing_sample": missing[:5]})
            status = max(status, 1 if missing else 0)
        results.append(row)
    summary = {"source": source, "mode": mode, "files": results}
    print(json.dumps(summary, sort_keys=True))
    annotate("notice" if status == 0 else "error", f"{source} landing {mode}", summary)
    step_summary(f"## {source} · landing {mode}\n\n```json\n{json.dumps(summary, indent=2)}\n```\n")
    return status
