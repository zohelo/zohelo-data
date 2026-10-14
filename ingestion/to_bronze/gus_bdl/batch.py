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
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import shutil
import tempfile
import time
from typing import Any

from ingestion.common import bronze_files as bf
from ingestion.common.r2 import catalog, get_with_backoff, req, s3
from ingestion.to_bronze.gus_bdl.decode import BRONZE_COLUMNS, BdlDecodeError, decode_web_export

TABLE_ID = ("bronze", "bdl_subgroup_observations")
REJECTED_ID = ("bronze", "bdl_rejected_files")
ENTITY_COLUMN = "dataset_id"  # rejected-files table predates the shared "entity" name
DATA = "01_landing/gus_bdl/snapshot/data/"
OUTPUT_PREFIX = "02_bronze/gus_bdl/subgroup_observations/"
STATS_COLUMNS = ["subgroup_id", "raw_sha256"]


def rejected_hashes(table) -> set[str]:
    """Raw SHA-256 of every Landing file recorded as rejected."""
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
            continue  # incomplete version: never loaded
        if subgroup not in newest or version > newest[subgroup][0]:
            newest[subgroup] = (version, parts)
    return [p for _, parts in newest.values() for p in parts]


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-files", type=int, default=256)
    parser.add_argument("--session-seconds", type=int, default=2700)
    args = parser.parse_args(argv)
    started = time.monotonic()
    client, cat = s3(), bf.retry(catalog, "catalog")
    bucket, landing = req("R2_LAKEHOUSE_BUCKET"), req("R2_LANDING_BUCKET")
    table = bf.retry(lambda: bf.ensure_table(cat, TABLE_ID, list(BRONZE_COLUMNS), "subgroup_id"), "load_table")
    done = bf.loaded_hashes(table) | rejected_hashes(bf.rejected_table(cat, REJECTED_ID, ENTITY_COLUMN))
    todo = sorted((c for c in candidates(client, landing) if c["sha256"] not in done), key=lambda c: c["size_bytes"])
    prepared, rejected = [], []
    with tempfile.TemporaryDirectory(prefix="zohelo-bdl-bronze-") as tmp:
        for index, receipt in enumerate(todo[: args.max_files]):
            if (prepared or rejected) and time.monotonic() - started >= args.session_seconds:
                break
            work = Path(tmp) / f"{index:04d}"
            work.mkdir()
            raw, output = work / "export.zip", work / "out.parquet"
            bf.download(
                client, landing, "gus_bdl", receipt["object"]["key"], receipt["sha256"], receipt.get("size_bytes"), raw
            )
            try:
                report = decode_web_export(raw, receipt, output)
            except BdlDecodeError as exc:
                rejected.append(
                    {
                        "entity": receipt["subgroup_id"],
                        "raw_sha256": receipt["sha256"],
                        "landing_key": receipt["object"]["key"],
                        "error": str(exc)[:1000],
                        "recorded_at": datetime.now(UTC),
                        "code_sha": os.environ.get("GITHUB_SHA", "unknown"),
                    }
                )
                shutil.rmtree(work, ignore_errors=True)
                continue
            key = f"{OUTPUT_PREFIX}{receipt['subgroup_id']}/{receipt['sha256']}.parquet"
            prepared.append(
                {
                    "uri": bf.upload(client, bucket, key, output, receipt["sha256"]),
                    "rows": report["rows"],
                    "bytes": report["output_bytes"],
                }
            )
            shutil.rmtree(work, ignore_errors=True)
    if prepared:
        bf.commit(cat, TABLE_ID, [p["uri"] for p in prepared], STATS_COLUMNS, "bdl-bronze-batch")
        after = bf.loaded_hashes(bf.retry(lambda: cat.load_table(TABLE_ID), "reload_table"))
        lost = [p["uri"] for p in prepared if Path(p["uri"]).stem not in after]
        if lost:
            raise RuntimeError(f"Bronze commit lacks prepared files: {lost[:3]}")
    bf.record_rejections(cat, REJECTED_ID, rejected, ENTITY_COLUMN)
    print(
        json.dumps(
            {
                "operation": "bdl-bronze-batch",
                "result": "pass",
                "processed": len(prepared) + len(rejected),
                "appended_files": len(prepared),
                "rows_added": sum(p["rows"] for p in prepared),
                "bytes_added": sum(p["bytes"] for p in prepared),
                "failed": len(rejected),
                "failed_datasets": [r["entity"] for r in rejected],
                "missing_distinct_raw_sha256": len(todo) - len(prepared) - len(rejected),
                "elapsed_seconds": round(time.monotonic() - started, 1),
            },
            sort_keys=True,
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
