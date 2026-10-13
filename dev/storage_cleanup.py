#!/usr/bin/env python3
"""Lakehouse clean-up (dev, deleted before merge).

--mode plan  : list every object under the layer and control prefixes, mark the ones
               any Iceberg snapshot references, summarise the rest by folder with
               content samples. Read-only.
--mode apply : delete unreferenced objects older than --min-age-hours under the given
               --delete-prefix values (owner-approved one-off), never a referenced file
               and never anything under --keep-prefix.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
from urllib.parse import urlsplit

from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate

OUT = Path(os.environ.get("DEV_OUTPUT_DIR", "."))
SCAN = ["02_bronze/", "03_silver/", "04_gold/", "05_archive/", "06_control/", "_drive_versions/"]


def referenced(cat) -> set[str]:
    keys = set()
    for ns in cat.list_namespaces():
        for ident in cat.list_tables(ns):
            table = cat.load_table(ident)
            for path in table.inspect.all_files().column("file_path").to_pylist():
                keys.add(urlsplit(path).path.lstrip("/"))
    return keys


def main(argv=None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["plan", "apply"], default="plan")
    p.add_argument("--min-age-hours", type=float, default=48)
    p.add_argument("--delete-prefix", nargs="*", default=[])
    p.add_argument("--keep-prefix", nargs="*", default=[])
    p.add_argument("--drop-table", nargs="*", default=[], help="namespace.table, dropped first (apply only)")
    args = p.parse_args(argv)
    client, bucket = s3(), req("R2_LAKEHOUSE_BUCKET")
    cat = catalog()
    dropped = []
    if args.mode == "apply":
        for name in args.drop_table:
            ns, table = name.split(".")
            if cat.table_exists((ns, table)):
                cat.drop_table((ns, table))
                dropped.append(name)
    refs = referenced(cat)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=args.min_age_hours)

    summary = defaultdict(lambda: [0, 0, 0, 0])  # ref n, ref bytes, unref n, unref bytes
    samples: dict[str, list[str]] = {}
    to_delete = []
    for prefix in SCAN:
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key, size = obj["Key"], int(obj["Size"])
                folder = "/".join(key.split("/")[:3])
                row = summary[folder]
                if key in refs:
                    row[0] += 1
                    row[1] += size
                    continue
                row[2] += 1
                row[3] += size
                if args.mode == "plan" and key.startswith("06_control/") and size > 1000 \
                        and len(samples.setdefault(folder, [])) < 1:
                    body = client.get_object(Bucket=bucket, Key=key, Range="bytes=0-299")["Body"].read()
                    samples[folder].append(key[-70:] + " :: " + body.decode("utf-8", "replace"))
                if args.mode == "apply" and obj["LastModified"] < cutoff \
                        and any(key.startswith(d) for d in args.delete_prefix) \
                        and not any(key.startswith(k) for k in args.keep_prefix):
                    to_delete.append({"Key": key, "size": size})

    deleted = 0
    for start in range(0, len(to_delete), 1000):
        batch = to_delete[start:start + 1000]
        resp = client.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": o["Key"]} for o in batch], "Quiet": True})
        if resp.get("Errors"):
            raise RuntimeError(f"delete errors: {resp['Errors'][:3]}")
        deleted += len(batch)

    table = {k: [v[0], round(v[1] / 1e9, 3), v[2], round(v[3] / 1e9, 3)]
             for k, v in sorted(summary.items(), key=lambda kv: -(kv[1][1] + kv[1][3]))}
    (OUT / "cleanup.json").write_text(json.dumps({"folders": table, "samples": samples}, indent=2))
    annotate("notice", "totals", {
        "mode": args.mode, "dropped_tables": dropped, "referenced_files": len(refs),
        "unreferenced_gb": round(sum(v[3] for v in summary.values()) / 1e9, 2),
        "deleted": deleted, "deleted_gb": round(sum(o["size"] for o in to_delete) / 1e9, 2),
        "format": "folder: [referenced n, GB, unreferenced n, GB]"})
    chunk = {}
    for k, v in list(table.items())[:80]:
        if chunk and len(json.dumps({**chunk, k: v})) > 3700:
            annotate("notice", "folders", chunk)
            chunk = {}
        chunk[k] = v
    if chunk:
        annotate("notice", "folders", chunk)
    if args.mode == "plan":
        chunk = {}
        for k, v in samples.items():
            v = [s[:420] for s in v]
            if chunk and len(json.dumps({**chunk, k: v})) > 3700:
                annotate("notice", "samples", chunk)
                chunk = {}
            chunk[k] = v
        if chunk:
            annotate("notice", "samples", chunk)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
