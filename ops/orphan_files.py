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
import traceback
from urllib.parse import urlsplit

from ingestion.common.bronze_files import retry
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate

LAYERS = ("02_bronze/", "03_silver/", "04_gold/", "__r2_data_catalog/")


MIN_DELETE_AGE_HOURS = 24
MAX_ORPHAN_SHARE = 0.5  # refuse --delete when orphans exceed this share of a prefix's bytes


def namespaces(cat, parent: tuple = ()) -> list[tuple]:
    """Every namespace, nested ones included."""
    found = []
    listed = retry(lambda: cat.list_namespaces(parent) if parent else cat.list_namespaces(), "list namespaces")
    for ns in listed:
        found += [ns, *namespaces(cat, ns)]
    return found


def referenced_files(cat) -> tuple[set[str], dict[str, str]]:
    """Object keys of every data file referenced by any snapshot of any table, and each
    table's folder (bucket key prefix) → table name."""
    keys: set[str] = set()
    folders: dict[str, str] = {}
    for namespace in namespaces(cat):
        # Catalog reads are retried: the scan takes ~45 min and the catalog closes idle
        # connections, which once failed the approved --delete run before it deleted (#865).
        for identifier in retry(lambda namespace=namespace: cat.list_tables(namespace), f"list {namespace}"):
            table = retry(lambda identifier=identifier: cat.load_table(identifier), f"load {identifier}")
            folders[urlsplit(table.location()).path.strip("/")] = ".".join(identifier)
            # Manifests read directly: table.inspect.all_files() decodes column bounds and
            # fails on bounds truncated inside a UTF-8 character (Polish names, pyiceberg 0.12).
            seen: set[str] = set()
            for snapshot in table.snapshots():
                for manifest in snapshot.manifests(table.io):
                    if manifest.manifest_path in seen:
                        continue
                    seen.add(manifest.manifest_path)
                    for entry in manifest.fetch_manifest_entry(table.io, discard_deleted=False):
                        keys.add(urlsplit(entry.data_file.file_path).path.lstrip("/"))
    return keys, folders


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--min-age-hours", type=float, default=48)
    # Raise only for an approved clean-up that is expected to orphan most of a prefix (#835).
    parser.add_argument("--max-share", type=float, default=MAX_ORPHAN_SHARE)
    parser.add_argument("--delete", action="store_true")
    args = parser.parse_args(argv)
    if args.delete and args.min_age_hours < MIN_DELETE_AGE_HOURS:
        parser.error(f"--delete needs --min-age-hours >= {MIN_DELETE_AGE_HOURS}")
    client, bucket = s3(), req("R2_LAKEHOUSE_BUCKET")
    keys, folders = referenced_files(catalog())
    cutoff = datetime.now(UTC) - timedelta(hours=args.min_age_hours)

    orphans, young = [], 0
    scanned: dict[str, int] = dict.fromkeys(LAYERS, 0)
    for prefix in LAYERS:
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                scanned[prefix] += int(obj["Size"])
                if obj["Key"] in keys or not obj["Key"].endswith(".parquet"):
                    continue
                if obj["LastModified"] > cutoff:
                    young += 1
                    continue
                orphans.append({"key": obj["Key"], "bytes": int(obj["Size"])})

    orphan_share = {
        prefix: round(sum(o["bytes"] for o in orphans if o["key"].startswith(prefix)) / scanned[prefix], 3)
        for prefix in LAYERS
        if scanned[prefix]
    }
    deleted = 0
    if args.delete and any(share > args.max_share for share in orphan_share.values()):
        # A share this high suggests the catalog scan missed tables, not real orphans.
        raise RuntimeError(f"refusing to delete: orphan share per prefix {orphan_share} above {args.max_share}")
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
        live = [name for path, name in folders.items() if path.startswith(folder) or folder.startswith(path)]
        folder = f"{folder} ({', '.join(live[:3]) if live else 'no live table: dropped'})"
        by_folder.setdefault(folder, [0, 0])
        by_folder[folder][0] += 1
        by_folder[folder][1] += orphan["bytes"]
    result = {
        "referenced_files": len(keys),
        "orphans": len(orphans),
        "orphan_gb": round(sum(o["bytes"] for o in orphans) / 1e9, 3),
        "skipped_younger_than_min_age": young,
        "orphan_share_by_prefix": orphan_share,
        "deleted": deleted,
        "top_folders": dict(sorted(by_folder.items(), key=lambda kv: -kv[1][1])[:15]),
    }
    annotate("notice", "orphan files", result)
    output = Path(os.environ.get("OPS_OUTPUT_DIR", ".")) / "orphan-files.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps({**result, "orphans": orphans}, indent=2))
    return 0


def run(argv=None) -> int:
    """main() with any failure published as an annotation (nothing silent)."""
    try:
        return main(argv)
    except Exception:
        annotate("error", "orphan files failed", {"traceback": traceback.format_exc()[-3000:]})
        raise


if __name__ == "__main__":
    raise SystemExit(run())
