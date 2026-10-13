#!/usr/bin/env python3
"""One BDL Bronze batch: Landing subgroup exports → bronze.bdl_subgroup_observations.

Loads every part of the newest complete version of each subgroup in
01_landing/gus_bdl/snapshot/data/<P####>/ (receipts announce part.total). One Parquet file
per Landing file: 02_bronze/gus_bdl/subgroup_observations/<P####>/<raw_sha256>.parquet,
partitioned by subgroup_id; the table's file list is the checkpoint. Rejections go to
bronze.bdl_rejected_files. Reads a moved file from the Archive. Prints one JSON result line.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import shutil
import tempfile
import time
from typing import Any

import pyarrow as pa
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.io.pyarrow import parquet_file_to_data_file
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import IntegerType, LongType, NestedField, StringType, TimestamptzType

from ingestion.common.r2 import archive_key, catalog, get_with_backoff, raw_chunks, req, s3
from ingestion.to_bronze.eurostat.batch import REJECTED_SCHEMA, RETRY_DELAYS, SHA_RE, retry, transient
from ingestion.to_bronze.gus_bdl.decode import BRONZE_COLUMNS, COLUMN_NAMES, BdlDecodeError, decode_web_export

TABLE_ID = ("bronze", "bdl_subgroup_observations")
REJECTED_ID = ("bronze", "bdl_rejected_files")
DATA = "01_landing/gus_bdl/snapshot/data/"
OUTPUT_PREFIX = "02_bronze/gus_bdl/subgroup_observations/"
TYPES = {"VARCHAR": StringType(), "BIGINT": LongType(), "INTEGER": IntegerType(), "TIMESTAMPTZ": TimestamptzType()}
STATS_PROPERTIES = {"write.metadata.metrics.column.subgroup_id": "full",
                    "write.metadata.metrics.column.raw_sha256": "full"}


def observations_table(cat):
    try:
        return cat.load_table(TABLE_ID)
    except NoSuchTableError:
        schema = Schema(*[NestedField(i, n, TYPES[k], required=False) for i, (n, k) in enumerate(BRONZE_COLUMNS, start=1)])
        spec = PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="subgroup_id"))
        table = cat.create_table(TABLE_ID, schema=schema, partition_spec=spec)
        with table.transaction() as tx:
            tx.set_properties(**{"schema.name-mapping.default": table.schema().name_mapping.model_dump_json()})
        return cat.load_table(TABLE_ID)


def rejected_table(cat):
    try:
        return cat.load_table(REJECTED_ID)
    except NoSuchTableError:
        return cat.create_table(REJECTED_ID, schema=REJECTED_SCHEMA)


def file_hashes(table) -> set[str]:
    if table.current_snapshot() is None:
        return set()
    return {Path(p).stem for p in table.inspect.files().column("file_path").to_pylist() if SHA_RE.fullmatch(Path(p).stem)}


def rejected_hashes(table) -> set[str]:
    if table.current_snapshot() is None:
        return set()
    return set(table.scan(selected_fields=("raw_sha256",)).to_arrow().column("raw_sha256").to_pylist())


def candidates(client, bucket: str) -> list[dict[str, Any]]:
    """Every part of the newest complete version per subgroup."""
    keys = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=DATA):
        keys += [o["Key"] for o in page.get("Contents", []) if o["Key"].endswith(".receipt.json")]

    def read(key: str) -> dict:
        body = get_with_backoff(client, bucket, key)
        return {**json.loads(body), "_receipt_key": key, "_receipt_bytes": body}

    with ThreadPoolExecutor(max_workers=8) as pool:
        receipts = list(pool.map(read, keys))
    versions: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for r in receipts:
        if r.get("kind") == "gus_bdl_landing" and r.get("area") == "data":
            versions[(r["subgroup_id"], r["version_token"])].append(r)
    newest: dict[str, tuple[str, list[dict]]] = {}
    for (subgroup, version), parts in versions.items():
        total = int((parts[0].get("part") or {}).get("total", 1))
        if len({(p.get("part") or {}).get("index", 1) for p in parts}) < total:
            continue                                        # incomplete version: never loaded
        if subgroup not in newest or version > newest[subgroup][0]:
            newest[subgroup] = (version, parts)
    return [p for _, parts in newest.values() for p in parts]


def download(client, bucket: str, receipt: dict, destination: Path) -> None:
    key = receipt["object"]["key"]
    try:
        client.head_object(Bucket=bucket, Key=key)
    except client.exceptions.ClientError:
        key = archive_key("gus_bdl", receipt["sha256"], key)
    with destination.open("xb") as handle:
        for block in raw_chunks(client, bucket, key):
            handle.write(block)


def upload(client, bucket: str, receipt: dict, path: Path) -> str:
    digest = sha256(path.read_bytes()).hexdigest()
    key = f"{OUTPUT_PREFIX}{receipt['subgroup_id']}/{receipt['sha256']}.parquet"
    client.upload_file(str(path), bucket, key, ExtraArgs={
        "ContentType": "application/vnd.apache.parquet", "Metadata": {"sha256": digest, "raw-sha256": receipt["sha256"]}})
    return f"s3://{bucket}/{key}"


def commit(cat, uris: list[str]) -> None:
    for attempt in range(len(RETRY_DELAYS) + 1):
        table = retry(lambda: cat.load_table(TABLE_ID), "reload_table")
        present = set(table.inspect.files().column("file_path").to_pylist()) if table.current_snapshot() else set()
        missing = [u for u in uris if u not in present]
        if not missing:
            return
        metadata = table.metadata.model_copy(update={"properties": {**table.metadata.properties, **STATS_PROPERTIES}})
        files = [parquet_file_to_data_file(table.io, metadata, u) for u in missing]
        try:
            with table.transaction() as tx:
                with tx.update_snapshot(snapshot_properties={"zohelo.operation": "bdl-bronze-batch"}).fast_append() as app:
                    for f in files:
                        app.append_data_file(f)
            return
        except Exception as exc:
            if not transient(exc) or attempt == len(RETRY_DELAYS):
                raise
            time.sleep(RETRY_DELAYS[attempt])


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-files", type=int, default=256)
    parser.add_argument("--session-seconds", type=int, default=2700)
    args = parser.parse_args(argv)
    started = time.monotonic()
    client, cat = s3(), retry(catalog, "catalog")
    bucket, landing = req("R2_LAKEHOUSE_BUCKET"), req("R2_LANDING_BUCKET")
    table = retry(lambda: observations_table(cat), "load_table")
    if [f.name for f in table.schema().fields] != list(COLUMN_NAMES):
        raise RuntimeError("bronze.bdl_subgroup_observations schema differs from the decoder")
    done = file_hashes(table) | rejected_hashes(rejected_table(cat))
    todo = sorted((c for c in candidates(client, landing) if c["sha256"] not in done), key=lambda c: c["size_bytes"])
    prepared, rejected = [], []
    with tempfile.TemporaryDirectory(prefix="zohelo-bdl-bronze-") as tmp:
        for index, receipt in enumerate(todo[: args.max_files]):
            if (prepared or rejected) and time.monotonic() - started >= args.session_seconds:
                break
            work = Path(tmp) / f"{index:04d}"
            work.mkdir()
            raw, output = work / "export.zip", work / "out.parquet"
            download(client, landing, receipt, raw)
            try:
                report = decode_web_export(raw, receipt, output)
            except BdlDecodeError as exc:
                rejected.append({"dataset_id": receipt["subgroup_id"], "raw_sha256": receipt["sha256"],
                                 "landing_key": receipt["object"]["key"], "error": str(exc)[:1000],
                                 "recorded_at": datetime.now(timezone.utc),
                                 "code_sha": os.environ.get("GITHUB_SHA", "unknown")})
                shutil.rmtree(work, ignore_errors=True)
                continue
            prepared.append({"uri": upload(client, bucket, receipt, output), "rows": report["rows"],
                             "bytes": report["output_bytes"]})
            shutil.rmtree(work, ignore_errors=True)
    if prepared:
        commit(cat, [p["uri"] for p in prepared])
        after = file_hashes(retry(lambda: cat.load_table(TABLE_ID), "reload_table"))
        lost = [p["uri"] for p in prepared if Path(p["uri"]).stem not in after]
        if lost:
            raise RuntimeError(f"Bronze commit lacks prepared files: {lost[:3]}")
    if rejected:
        rejected_table(cat).append(pa.Table.from_pylist(rejected, schema=REJECTED_SCHEMA.as_arrow()))
    print(json.dumps({"operation": "bdl-bronze-batch", "result": "pass",
                      "processed": len(prepared) + len(rejected), "appended_files": len(prepared),
                      "rows_added": sum(p["rows"] for p in prepared), "bytes_added": sum(p["bytes"] for p in prepared),
                      "failed": len(rejected), "failed_datasets": [r["dataset_id"] for r in rejected],
                      "missing_distinct_raw_sha256": len(todo) - len(prepared) - len(rejected),
                      "elapsed_seconds": round(time.monotonic() - started, 1)}, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
