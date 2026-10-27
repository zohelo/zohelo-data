"""Eurostat structure metadata into Landing: the four bulk SDMX 2.1 structure messages.

Route (verified 14 Oct 2026 from a runner): the SDMX REST API returns every artefact of
a kind in one message — dataflows (~38 MB, names and update annotations of all
datasets), data structures (dimensions and their code lists), code lists (~175 MB) and
concept schemes (~38 MB). Four requests replace one ~3.5 MB "descendants" message per
dataset (15 GB for half the datasets in the legacy campaign).

Entities ESTAT_ALL_DATAFLOW, ESTAT_ALL_DATASTRUCTURE, ESTAT_ALL_CODELIST,
ESTAT_ALL_CONCEPTSCHEME under ``01_landing/eurostat/snapshot/metadata/<entity>/``,
``<ingested>__<ingested>.structure.xml`` + receipt (``source_kind``
eurostat_structure_sdmx, read by ingestion/to_bronze/eurostat/structure.py). A message
is written only when its SHA-256 differs from the newest landed one. Nothing is
overwritten. Run by Eurostat ``landing: extract`` with filter ``structure``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from hashlib import sha256
import json
import os
from pathlib import Path
import tempfile
import time
from typing import Any

import requests

from ingestion.common.r2 import get_with_backoff, req, s3
from ingestion.common.report import annotate, step_summary

PREFIX = "01_landing/eurostat/snapshot/metadata/"
BASE = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1"
USER_AGENT = "zohelo-data-eurostat-structure/1 (polite; github.com/zohelo/zohelo-data)"
MESSAGES = {
    "ESTAT_ALL_DATAFLOW": f"{BASE}/dataflow/ESTAT/all/latest",
    "ESTAT_ALL_DATASTRUCTURE": f"{BASE}/datastructure/ESTAT/all/latest",
    "ESTAT_ALL_CODELIST": f"{BASE}/codelist/ESTAT/all/latest",
    "ESTAT_ALL_CONCEPTSCHEME": f"{BASE}/conceptscheme/ESTAT/all/latest",
}
MAX_BYTES = 2 << 30


def newest_sha(client, bucket: str, entity: str) -> str | None:
    keys = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{PREFIX}{entity}/"):
        keys += [o["Key"] for o in page.get("Contents", []) if o["Key"].endswith(".receipt.json")]
    if not keys:
        return None
    return json.loads(get_with_backoff(client, bucket, sorted(keys)[-1]))["sha256"]


def download(url: str, path: Path) -> tuple[str, int, dict[str, str]]:
    for attempt in range(4):
        digest, size = sha256(), 0
        try:
            with requests.get(url, headers={"User-Agent": USER_AGENT}, stream=True, timeout=(30, 900)) as r:
                if r.status_code != 200:
                    raise RuntimeError(f"HTTP {r.status_code}")
                with path.open("wb") as handle:
                    for block in r.iter_content(8 << 20):
                        size += len(block)
                        if size > MAX_BYTES:
                            raise RuntimeError("structure message larger than 2 GB")
                        handle.write(block)
                        digest.update(block)
                with path.open("rb") as handle:
                    head = handle.read(512)
                tail_size = min(size, 256)
                with path.open("rb") as handle:
                    handle.seek(size - tail_size)
                    tail = handle.read()
                if b"<m:Structure" not in head and b":Structure" not in head:
                    raise RuntimeError("response is not an SDMX structure message")
                if b"Structure>" not in tail:
                    raise RuntimeError("structure message is truncated")
                return digest.hexdigest(), size, dict(r.headers)
        except (requests.ConnectionError, requests.Timeout, RuntimeError) as exc:
            if attempt == 3:
                raise
            print(json.dumps({"status": "retry", "url": url, "error": str(exc)[:200]}), flush=True)
            time.sleep(60 * (attempt + 1))
    raise RuntimeError("unreachable")


def main() -> int:
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    results: dict[str, Any] = {}
    for entity, url in MESSAGES.items():
        held = newest_sha(client, bucket, entity)
        with tempfile.TemporaryDirectory(prefix="zohelo-eurostat-structure-") as temporary:
            path = Path(temporary) / "message.xml"
            digest, size, headers = download(url, path)
            if digest == held:
                results[entity] = {"outcome": "unchanged"}
                continue
            ingested = datetime.now(UTC)
            token = ingested.strftime("%Y%m%dT%H%M%SZ")
            key = f"{PREFIX}{entity}/{token}__{token}.structure.xml"
            client.upload_file(
                str(path),
                bucket,
                key,
                ExtraArgs={"ContentType": "application/vnd.sdmx.structure+xml", "Metadata": {"sha256": digest}},
            )
            head = client.head_object(Bucket=bucket, Key=key)
            if head["ContentLength"] != size or (head.get("Metadata") or {}).get("sha256") != digest:
                raise RuntimeError(f"Landing write could not be verified: {key}")
        receipt = {
            "schema_version": 2,
            "kind": "eurostat_landing",
            "source_id": "eurostat",
            "dataset_code": entity,
            "part": None,
            "content": "snapshot",
            "area": "metadata",
            "source_kind": "eurostat_structure_sdmx",
            "eurostat_version": None,
            "version_token": token,
            "ingested_at": ingested.isoformat(),
            "source_url": url,
            "sha256": digest,
            "size_bytes": size,
            "object": {"bucket": bucket, "key": key},
            "replaces_sha256": held,
            "http": {
                "content_type": headers.get("Content-Type"),
                "etag": headers.get("ETag"),
                "last_modified": headers.get("Last-Modified"),
            },
            "code_sha": os.environ.get("GITHUB_SHA", ""),
        }
        client.put_object(
            Bucket=bucket,
            Key=key[: -len(".structure.xml")] + ".receipt.json",
            ContentType="application/json",
            Body=json.dumps(receipt, indent=2, sort_keys=True).encode(),
        )
        results[entity] = {"outcome": "landed", "mb": round(size / 1e6, 1)}
        time.sleep(5)
    annotate("notice", "eurostat structure landing", results)
    step_summary(f"## eurostat · structure landing\n\n```json\n{json.dumps(results, indent=2)}\n```\n")
    return 0
