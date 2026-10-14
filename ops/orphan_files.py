#!/usr/bin/env python3
"""Find (and with --delete remove) table data files no Iceberg snapshot references.

R2 Data Catalog maintenance expires snapshots but never removes orphan files: files
left by failed commits, by tables that were dropped or replaced, or by earlier loaders.
Everything under the layer folders (02_bronze/, 03_silver/, 04_gold/) and the catalog's
own default table location (__r2_data_catalog/, where dbt-built tables live) of the
lakehouse bucket is scanned, including folders no table points to any more. A file counts as
referenced when ANY snapshot of ANY table in ANY namespace references it (zero-copy
tables share files). Only Parquet files older than --min-age-hours are candidates (an
in-flight load may not have committed yet). Without --delete nothing is written.
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime, timedelta
import json
import os
from pathlib import Path
from urllib.parse import urlsplit

from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate

LAYERS = ("02_bronze/", "03_silver/", "04_gold/", "__r2_data_catalog/")


def referenced_files(cat) -> set[str]:
    """Object keys of every data file referenced by any snapshot of any table."""
    keys: set[str] = set()
    for namespace in cat.list_namespaces():
        for identifier in cat.list_tables(namespace):
            table = cat.load_table(identifier)
            for path in table.inspect.all_files().column("file_path").to_pylist():
                keys.add(urlsplit(path).path.lstrip("/"))
    return keys


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--min-age-hours", type=float, default=48)
    parser.add_argument("--delete", action="store_true")
    args = parser.parse_args(argv)
    client, bucket = s3(), req("R2_LAKEHOUSE_BUCKET")
    keys = referenced_files(catalog())
    cutoff = datetime.now(UTC) - timedelta(hours=args.min_age_hours)

    orphans, young = [], 0
    for prefix in LAYERS:
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["Key"] in keys or not obj["Key"].endswith(".parquet"):
                    continue
                if obj["LastModified"] > cutoff:
                    young += 1
                    continue
                orphans.append({"key": obj["Key"], "bytes": int(obj["Size"])})

    deleted = 0
    if args.delete:
        for start in range(0, len(orphans), 1000):
            batch = orphans[start : start + 1000]
            response = client.delete_objects(
                Bucket=bucket, Delete={"Objects": [{"Key": o["key"]} for o in batch], "Quiet": True}
            )
            if response.get("Errors"):
                raise RuntimeError(f"delete failed: {response['Errors'][:3]}")
            deleted += len(batch)
    by_folder: dict[str, list[int]] = {}
    for orphan in orphans:
        folder = "/".join(orphan["key"].split("/")[:3])
        by_folder.setdefault(folder, [0, 0])
        by_folder[folder][0] += 1
        by_folder[folder][1] += orphan["bytes"]
    result = {
        "referenced_files": len(keys),
        "orphans": len(orphans),
        "orphan_gb": round(sum(o["bytes"] for o in orphans) / 1e9, 3),
        "skipped_younger_than_min_age": young,
        "deleted": deleted,
        "top_folders": dict(sorted(by_folder.items(), key=lambda kv: -kv[1][1])[:15]),
    }
    annotate("notice", "orphan files", result)
    output = Path(os.environ.get("OPS_OUTPUT_DIR", ".")) / "orphan-files.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps({**result, "orphans": orphans}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
