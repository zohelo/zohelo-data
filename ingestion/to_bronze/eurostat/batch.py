#!/usr/bin/env python3
"""One Eurostat Bronze batch: Landing data files → bronze.eurostat_full_observations.

Reads the Landing layout of docs/landing.md
(``01_landing/eurostat/snapshot/data/<CODE>/<version>__<ingested>.tsv.gz`` + receipt) and
loads the latest version of every dataset. Each Landing file becomes one Parquet file
``02_bronze/eurostat/observations/<dataset_id>/<raw_sha256>.parquet`` registered in the
Iceberg table (partitioned by dataset_id), so the table's file list is the checkpoint.
Files the decoder rejects are recorded in bronze.eurostat_rejected_files and skipped on
later runs (ops/retry_rejected clears them). Never calls Eurostat, never touches Landing.
Prints one JSON line with "operation": "eurostat-bronze-batch" as its result.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import shutil
import tempfile
import time
from typing import Any

import pyarrow as pa
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import LongType, NestedField, StringType, TimestamptzType
import requests

from ingestion.common.r2 import catalog, req, s3
from ingestion.to_bronze.eurostat.decode import (
    BRONZE_COLUMNS, COLUMN_NAMES, EurostatBulkDecodeError, decode_landing_distribution)

TABLE_ID = ("bronze", "eurostat_full_observations")
REJECTED_ID = ("bronze", "eurostat_rejected_files")
OUTPUT_PREFIX = "02_bronze/eurostat/observations/"
LANDING_DATA_PREFIX = "01_landing/eurostat/snapshot/data/"
SHA_RE = re.compile(r"^[0-9a-f]{64}$")
RETRY_DELAYS = (2, 5, 10, 20, 40)
TYPES = {"VARCHAR": StringType(), "BIGINT": LongType(), "TIMESTAMPTZ": TimestamptzType()}
TABLE_PROPERTIES = {
    "write.parquet.compression-codec": "zstd",
    "write.metadata.metrics.column.dataset_id": "full",
    "write.metadata.metrics.column.raw_sha256": "full",
}
REJECTED_SCHEMA = Schema(
    NestedField(1, "dataset_id", StringType(), required=False),
    NestedField(2, "raw_sha256", StringType(), required=False),
    NestedField(3, "landing_key", StringType(), required=False),
    NestedField(4, "error", StringType(), required=False),
    NestedField(5, "recorded_at", TimestamptzType(), required=False),
    NestedField(6, "code_sha", StringType(), required=False),
)


def transient(exc: Exception | None) -> bool:
    seen = set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        if isinstance(exc, (requests.ConnectionError, requests.Timeout)):
            return True
        if isinstance(exc, requests.HTTPError):
            return exc.response is not None and exc.response.status_code in {429, 500, 502, 503, 504}
        exc = exc.__cause__
    return False


def retry(operation, label: str):
    """Retry transient catalog/storage reads (never use for an append)."""
    for attempt in range(len(RETRY_DELAYS) + 1):
        try:
            return operation()
        except Exception as exc:
            if not transient(exc) or attempt == len(RETRY_DELAYS):
                raise
            print(json.dumps({"status": "transient_retry", "operation": label, "attempt": attempt + 1}), flush=True)
            time.sleep(RETRY_DELAYS[attempt])


def observations_table(cat):
    """Load the Bronze table, creating it (lean schema, partitioned by dataset_id) if absent."""
    try:
        return cat.load_table(TABLE_ID)
    except NoSuchTableError:
        schema = Schema(*[NestedField(i, name, TYPES[kind], required=False)
                          for i, (name, kind) in enumerate(BRONZE_COLUMNS, start=1)])
        spec = PartitionSpec(PartitionField(source_id=1, field_id=1000,
                                            transform=IdentityTransform(), name="dataset_id"))
        table = cat.create_table(TABLE_ID, schema=schema, partition_spec=spec, properties=TABLE_PROPERTIES)
        with table.transaction() as tx:
            tx.set_properties(**{"schema.name-mapping.default":
                                 table.schema().name_mapping.model_dump_json()})
        return cat.load_table(TABLE_ID)


def rejected_table(cat):
    try:
        return cat.load_table(REJECTED_ID)
    except NoSuchTableError:
        return cat.create_table(REJECTED_ID, schema=REJECTED_SCHEMA)


def loaded_hashes(table) -> set[str]:
    """Raw SHA-256 of every Landing file already in Bronze (from the file names)."""
    if table.current_snapshot() is None:
        return set()
    paths = table.inspect.files().column("file_path").to_pylist()
    return {Path(p).stem for p in paths if SHA_RE.fullmatch(Path(p).stem)}


def rejected_hashes(table) -> set[str]:
    if table.current_snapshot() is None:
        return set()
    return set(table.scan(selected_fields=("raw_sha256",)).to_arrow().column("raw_sha256").to_pylist())


def landing_candidates(client, landing_bucket: str) -> list[dict[str, Any]]:
    """The latest Landing version of every dataset (highest source version, then ingest)."""
    keys = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=landing_bucket, Prefix=LANDING_DATA_PREFIX):
        keys += [item["Key"] for item in page.get("Contents", []) if item["Key"].endswith(".receipt.json")]

    def read(key: str) -> dict[str, Any]:
        body = retry(lambda: client.get_object(Bucket=landing_bucket, Key=key)["Body"].read(), "read_receipt")
        receipt = json.loads(body)
        sha, size, obj = receipt.get("sha256"), receipt.get("size_bytes"), receipt.get("object") or {}
        if (receipt.get("schema_version") != 2 or receipt.get("kind") != "eurostat_landing"
                or receipt.get("area") != "data" or not isinstance(sha, str) or not SHA_RE.fullmatch(sha)
                or type(size) is not int or size <= 0 or obj.get("bucket") != landing_bucket
                or not str(obj.get("key", "")).startswith(LANDING_DATA_PREFIX)):
            raise RuntimeError(f"invalid Eurostat Landing receipt: {key}")
        return {"raw_sha256": sha, "size": size, "receipt_key": key, "receipt_bytes": body,
                "data_key": obj["key"], "dataset_id": receipt.get("dataset_code"),
                "order": (str(receipt.get("version_token") or ""), str(receipt.get("ingested_at") or ""))}

    with ThreadPoolExecutor(max_workers=16) as pool:
        receipts = list(pool.map(read, sorted(keys)))
    latest: dict[str, dict[str, Any]] = {}
    for item in receipts:
        held = latest.get(item["dataset_id"])
        if held is None or item["order"] > held["order"]:
            latest[item["dataset_id"]] = item
    return sorted(latest.values(), key=lambda item: item["dataset_id"])


def download(client, bucket: str, item: dict[str, Any], destination: Path) -> None:
    if shutil.disk_usage(destination.parent).free < item["size"] * 6 + (2 << 30):
        raise RuntimeError("runner disk headroom is insufficient for Eurostat decode")
    body = client.get_object(Bucket=bucket, Key=item["data_key"])["Body"]
    digest, size = sha256(), 0
    try:
        with destination.open("xb") as handle:
            for block in iter(lambda: body.read(8 << 20), b""):
                size += len(block)
                digest.update(block)
                handle.write(block)
    finally:
        body.close()
    if size != item["size"] or digest.hexdigest() != item["raw_sha256"]:
        raise RuntimeError(f"Eurostat Landing bytes do not match their receipt: {item['data_key']}")


def upload(client, bucket: str, item: dict[str, Any], path: Path) -> str:
    """Upload once; an existing object must be byte-identical (same SHA-256 metadata)."""
    digest = sha256(path.read_bytes()).hexdigest()
    key = f"{OUTPUT_PREFIX}{item['dataset_id']}/{item['raw_sha256']}.parquet"
    try:
        head = client.head_object(Bucket=bucket, Key=key)
        if (head.get("Metadata") or {}).get("sha256") != digest:
            client.delete_object(Bucket=bucket, Key=key)    # unregistered leftover of a failed run
            raise FileNotFoundError
    except Exception:
        client.upload_file(str(path), bucket, key, ExtraArgs={
            "ContentType": "application/vnd.apache.parquet",
            "Metadata": {"sha256": digest, "raw-sha256": item["raw_sha256"]}})
        if (client.head_object(Bucket=bucket, Key=key).get("Metadata") or {}).get("sha256") != digest:
            raise RuntimeError(f"Bronze upload verification failed: {key}")
    return f"s3://{bucket}/{key}"


def commit(cat, uris: list[str]) -> None:
    """Register the files; a lost commit response is re-checked, never blindly resent."""
    for attempt in range(len(RETRY_DELAYS) + 1):
        table = retry(lambda: cat.load_table(TABLE_ID), "reload_table")
        present = set(table.inspect.files().column("file_path").to_pylist()) if table.current_snapshot() else set()
        missing = [u for u in uris if u not in present]
        if not missing:
            return
        try:
            with table.transaction() as tx:
                tx.add_files(missing, snapshot_properties={"zohelo.operation": "eurostat-bronze-batch"},
                             check_duplicate_files=False)
            return
        except Exception as exc:
            if not transient(exc) or attempt == len(RETRY_DELAYS):
                raise
            time.sleep(RETRY_DELAYS[attempt])


def record_rejections(cat, rejected: list[dict[str, Any]]) -> None:
    if not rejected:
        return
    rows = pa.Table.from_pylist(rejected, schema=REJECTED_SCHEMA.as_arrow())
    rejected_table(cat).append(rows)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-files", type=int, default=64)
    parser.add_argument("--session-seconds", type=int, default=900)
    args = parser.parse_args(argv)
    started = time.monotonic()
    client, cat = s3(), retry(catalog, "catalog")
    bucket, landing_bucket = req("R2_LAKEHOUSE_BUCKET"), req("R2_LANDING_BUCKET")
    table = retry(lambda: observations_table(cat), "load_table")
    if [f.name for f in table.schema().fields] != list(COLUMN_NAMES):
        raise RuntimeError("bronze.eurostat_full_observations schema differs from the decoder")
    done = loaded_hashes(table) | rejected_hashes(rejected_table(cat))
    candidates = [c for c in landing_candidates(client, landing_bucket) if c["raw_sha256"] not in done]
    candidates.sort(key=lambda c: (c["size"], c["raw_sha256"]))
    selected = candidates[: args.max_files]

    prepared, rejected = [], []
    with tempfile.TemporaryDirectory(prefix="zohelo-eurostat-bronze-") as temporary:
        for index, item in enumerate(selected):
            if (prepared or rejected) and time.monotonic() - started >= args.session_seconds:
                break
            work = Path(temporary) / f"{index:03d}"
            work.mkdir()
            raw, receipt, output = work / "data.tsv.gz", work / "receipt.json", work / "out.parquet"
            receipt.write_bytes(item["receipt_bytes"])
            download(client, landing_bucket, item, raw)
            try:
                report = decode_landing_distribution(
                    raw, receipt, output, receipt_uri=f"r2://{landing_bucket}/{item['receipt_key']}",
                    raw_uri=f"r2://{landing_bucket}/{item['data_key']}")
            except EurostatBulkDecodeError as exc:
                rejected.append({"dataset_id": item["dataset_id"], "raw_sha256": item["raw_sha256"],
                                 "landing_key": item["data_key"], "error": str(exc)[:1000],
                                 "recorded_at": datetime.now(timezone.utc),
                                 "code_sha": os.environ.get("GITHUB_SHA", "unknown")})
                print(json.dumps({"status": "rejected", **{k: str(v) for k, v in rejected[-1].items()}}), flush=True)
                shutil.rmtree(work, ignore_errors=True)
                continue
            uri = upload(client, bucket, item, output)
            prepared.append({"uri": uri, "rows": report["observation_cells"], "bytes": report["output_bytes"],
                             "dataset_id": item["dataset_id"]})
            print(json.dumps({"status": "prepared", **prepared[-1]}), flush=True)
            shutil.rmtree(work, ignore_errors=True)

    if prepared:
        commit(cat, [p["uri"] for p in prepared])
        after = loaded_hashes(retry(lambda: cat.load_table(TABLE_ID), "reload_table"))
        lost = [p["uri"] for p in prepared if Path(p["uri"]).stem not in after]
        if lost:
            raise RuntimeError(f"Bronze commit lacks prepared files: {lost[:3]}")
    record_rejections(cat, rejected)
    print(json.dumps({
        "operation": "eurostat-bronze-batch", "result": "pass",
        "processed": len(prepared) + len(rejected), "appended_files": len(prepared),
        "rows_added": sum(p["rows"] for p in prepared), "bytes_added": sum(p["bytes"] for p in prepared),
        "failed": len(rejected), "failed_datasets": [r["dataset_id"] for r in rejected],
        "missing_distinct_raw_sha256": len(candidates) - len(prepared) - len(rejected),
        "elapsed_seconds": round(time.monotonic() - started, 1),
    }, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
