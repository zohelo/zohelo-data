"""Shared R2 access for ingestion: S3 client, R2 SQL queries and Landing listings."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import PurePosixPath
import re
from typing import Any

import boto3
from botocore.config import Config
import requests

SHA_RE = re.compile(r"^[0-9a-f]{64}$")
CONTENT_ADDRESSED_RE = re.compile(r"^(raw|response)-([0-9a-f]{64})\.bin$")
SNAPSHOT = "snapshot/"
OLD_EUROSTAT_DATA = "01_landing/eurostat/bulk/data/"   # 11-12 Oct layout, until retired


def req(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"missing environment variable: {name}")
    return value


def s3():
    return boto3.client(
        "s3", endpoint_url=req("R2_S3_ENDPOINT").rstrip("/"), region_name="auto",
        aws_access_key_id=req("CLOUDFLARE_R2_ACCESS_KEY_ID"),
        aws_secret_access_key=req("CLOUDFLARE_R2_SECRET_ACCESS_KEY"),
        config=Config(signature_version="s3v4", retries={"max_attempts": 8, "mode": "standard"}))


def sql(query: str) -> list[dict[str, Any]]:
    """Run one R2 SQL query and return its rows as dicts."""
    url = (f"https://api.sql.cloudflarestorage.com/api/v1/accounts/"
           f"{req('CLOUDFLARE_ACCOUNT_ID')}/r2-sql/query/{req('R2_LAKEHOUSE_BUCKET')}")
    response = requests.post(url, json={"query": query}, timeout=180, headers={
        "Authorization": f"Bearer {req('R2_DATA_CATALOG_TOKEN')}", "Content-Type": "application/json"})
    if response.status_code != 200:
        raise RuntimeError(f"R2 SQL {response.status_code}: {response.text[:500]}")
    payload = response.json()
    if payload.get("success") is False:
        raise RuntimeError(f"R2 SQL failed: {json.dumps(payload)[:800]}")
    candidates = []
    if isinstance(payload.get("result"), list):
        candidates.extend(payload["result"])
    elif isinstance(payload.get("result"), dict):
        candidates.append(payload["result"])
    candidates.append(payload)
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        value = candidate.get("results")
        if isinstance(value, list) and (not value or isinstance(value[0], dict)):
            return value
        if isinstance(value, dict):
            columns, rows = value.get("columns"), value.get("rows")
            if isinstance(columns, list) and isinstance(rows, list):
                return [dict(zip(columns, row)) for row in rows]
        rows = candidate.get("rows")
        if isinstance(rows, list) and (not rows or isinstance(rows[0], dict)):
            return rows
    raise RuntimeError(f"unsupported R2 SQL response: {json.dumps(payload)[:1000]}")


def sha_column(query: str, column: str) -> set[str]:
    values = set()
    for row in sql(query):
        value = row.get(column)
        if value is None:
            continue
        if not isinstance(value, str) or not SHA_RE.fullmatch(value):
            raise RuntimeError(f"invalid {column} value from R2 SQL: {value!r}")
        values.add(value)
    return values


def head_sha256(client, bucket: str, keys: list[str]) -> dict[str, str]:
    """SHA-256 recorded in each object's metadata at write time."""
    def head(key: str) -> tuple[str, str]:
        digest = str((client.head_object(Bucket=bucket, Key=key).get("Metadata") or {}).get("sha256", ""))
        if not SHA_RE.fullmatch(digest):
            raise RuntimeError(f"Landing object has no SHA-256 metadata: {key}")
        return key, digest
    with ThreadPoolExecutor(max_workers=16) as pool:
        return dict(pool.map(head, keys))


def landing_objects(client, bucket: str, source: str) -> dict[str, dict[str, Any]]:
    """Landing files of one source by SHA-256.

    Agreed layout (docs/landing.md): 01_landing/<source>/snapshot/<area>/<entity>/<version>__<ingested>.<ext>,
    digest from object metadata. Earlier layouts still present are included: content-addressed
    raw-/response-<sha>.bin names and Eurostat's 11-12 Oct data/ layout. When the same bytes
    exist in several layouts, the agreed-layout key wins.
    """
    prefix = f"01_landing/{source}/"
    snapshot, metadata_keyed, result = [], [], {}
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            key, size = str(item["Key"]), int(item.get("Size", 0))
            if size <= 0 or key.endswith("/") or key.endswith(".receipt.json"):
                continue
            rest = key[len(prefix):]
            if rest.startswith(SNAPSHOT):
                snapshot.append({"key": key, "size": size})
            elif key.startswith(OLD_EUROSTAT_DATA):
                metadata_keyed.append({"key": key, "size": size})
            else:
                match = CONTENT_ADDRESSED_RE.fullmatch(PurePosixPath(key).name)
                if not match:
                    raise RuntimeError(f"unexpected Landing object: {key}")
                kind, digest = match.groups()
                result.setdefault(digest, {"key": key, "size": size, "kind": kind,
                                           "layout": "content_addressed", "sha256": digest})
    digests = head_sha256(client, bucket, [i["key"] for i in snapshot + metadata_keyed])
    for item in metadata_keyed:
        digest = digests[item["key"]]
        result.setdefault(digest, {**item, "kind": "raw", "layout": "eurostat_data_v1", "sha256": digest})
    for item in snapshot:
        digest = digests[item["key"]]
        area, entity, name = item["key"][len(prefix) + len(SNAPSHOT):].split("/", 2)
        token, _, rest = name.partition("__")
        ingested = rest.split(".", 1)[0].split("__", 1)[0]
        held = result.get(digest)
        if held is not None and int(held["size"]) != item["size"]:
            raise RuntimeError(f"same SHA-256 with different sizes in Landing: {digest}")
        # Identical bytes can sit under several entities or versions; keep every snapshot path.
        paths = (held or {}).get("snapshot_paths", []) + [
            {"key": item["key"], "area": area, "entity": entity, "version": token, "ingested": ingested}]
        result[digest] = {**item, "kind": "raw" if area == "data" else "metadata", "layout": "snapshot",
                          "area": area, "entity": entity, "version": token, "ingested": ingested,
                          "sha256": digest, "snapshot_paths": paths}
    return result


def latest_snapshot(meta: dict[str, dict[str, Any]], area: str = "data") -> set[str]:
    """Digests of the latest version (then latest ingest) of every snapshot entity."""
    latest: dict[str, tuple[tuple[str, str], str]] = {}
    for digest, item in meta.items():
        for path in item.get("snapshot_paths", []):
            if path["area"] != area:
                continue
            order = (path["version"], path["ingested"])
            if path["entity"] not in latest or order > latest[path["entity"]][0]:
                latest[path["entity"]] = (order, digest)
    return {digest for _, digest in latest.values()}
