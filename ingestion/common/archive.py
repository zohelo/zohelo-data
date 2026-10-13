#!/usr/bin/env python3
"""landing.archive: move Landing files that Bronze has loaded into the Archive.

For every Landing file whose SHA-256 is proven in Bronze, every superseded version (a
newer version of the entity exists; retention decides how long it is kept), and — if
the source's ``archive_policy.without_bronze`` lists an area such as ``metadata`` —
every file of that area, this:

1. writes the content-addressed Archive copy ``05_archive/<source>/raw/<sha256>``:
   ``.bin`` = the exact bytes for already-compressed formats (server-side copy),
   ``.zst`` = a zstd-compressed copy for uncompressed formats (XML, CSV, JSON …);
2. reads the Archive copy back and checks the original SHA-256 and size;
3. only then deletes the Landing data file (``archive_policy.move: true``). The receipt
   stays in Landing as the pointer; ops/restore_from_archive puts a file back.

Idempotent and resumable: an existing, verified Archive copy is reused. Stops starting
new files when the time budget is used up (exit 2; the next run continues).

Supported proofs: Eurostat (Bronze file per Landing SHA-256); World Bank WDI (the bulk ZIP
whose SHA-256 equals silver.wdi_observations.archive_sha256).
"""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from hashlib import sha256
import json
import tempfile
import threading
import time
from typing import Any

from ingestion.common.r2 import archive_key, landing_objects, latest_snapshot, raw_chunks, req, s3, sha_column, sources
from ingestion.common.report import annotate, step_summary

SOURCES = ("eurostat", "world_bank_wdi")


def proven_for(source: str, meta: dict[str, dict[str, Any]]) -> set[str]:
    if source == "eurostat":
        from ingestion.common.r2 import catalog
        from ingestion.to_bronze.eurostat.batch import TABLE_ID, loaded_hashes
        return loaded_hashes(catalog().load_table(TABLE_ID))
    bulk = {digest for digest, item in meta.items() if "/bulk/" in item["key"]}
    modeled = sha_column("SELECT DISTINCT archive_sha256 FROM silver.wdi_observations "
                         "WHERE archive_sha256 IS NOT NULL;", "archive_sha256")
    if len(bulk) != 1 or modeled != bulk:
        raise RuntimeError(f"WDI bulk/model lineage mismatch: landing={sorted(bulk)} modeled={sorted(modeled)}")
    return bulk


def verified(client, bucket: str, key: str, digest: str, size: int) -> bool:
    observed, length = sha256(), 0
    for block in raw_chunks(client, bucket, key):
        observed.update(block)
        length += len(block)
    return observed.hexdigest() == digest and length == size


def ensure_archive(client, bucket: str, source: str, item: dict[str, Any]) -> str:
    """Write (or reuse) the verified Archive copy; returns 'reused' or 'copied'."""
    target = archive_key(source, item["sha256"], item["key"])
    try:
        client.head_object(Bucket=bucket, Key=target)
        if verified(client, bucket, target, item["sha256"], int(item["size"])):
            return "reused"
        client.delete_object(Bucket=bucket, Key=target)      # a broken copy is never kept
    except client.exceptions.ClientError:
        pass
    metadata = {"source-sha256": item["sha256"], "source-size": str(item["size"]),
                "source-key": item["key"][-900:]}
    if target.endswith(".bin"):
        client.copy_object(Bucket=bucket, Key=target, CopySource={"Bucket": bucket, "Key": item["key"]},
                           MetadataDirective="REPLACE", Metadata=metadata,
                           ContentType="application/octet-stream")
    else:
        import zstandard
        with tempfile.TemporaryFile() as handle:
            with zstandard.ZstdCompressor(level=12, threads=-1).stream_writer(handle, closefd=False) as writer:
                for block in raw_chunks(client, bucket, item["key"]):
                    writer.write(block)
            handle.seek(0)
            client.upload_fileobj(handle, bucket, target, ExtraArgs={
                "Metadata": {**metadata, "compression": "zstd"}, "ContentType": "application/zstd"})
    if not verified(client, bucket, target, item["sha256"], int(item["size"])):
        client.delete_object(Bucket=bucket, Key=target)
        raise RuntimeError(f"Archive readback mismatch: {item['key']}")
    return "copied"


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", choices=SOURCES, required=True)
    parser.add_argument("--budget-minutes", type=float, default=100)
    args = parser.parse_args(argv)
    deadline = time.monotonic() + args.budget_minutes * 60
    policy = (sources().get(args.source) or {}).get("archive_policy") or {}
    move = bool(policy.get("move"))
    without_bronze = set(policy.get("without_bronze") or [])
    client, bucket = s3(), req("R2_LANDING_BUCKET")

    meta = landing_objects(client, bucket, args.source)
    proven = proven_for(args.source, meta)
    if not proven <= set(meta):
        raise RuntimeError(f"Bronze provenance without Landing receipt or file: {len(proven - set(meta))}")
    # Superseded: no path of these bytes is the newest version of its entity any more.
    newest = latest_snapshot(meta, "data") | latest_snapshot(meta, "metadata")
    superseded = {d for d, item in meta.items() if item.get("snapshot_paths") and d not in newest}
    selected = [item for digest, item in meta.items() if item.get("present") and
                (digest in proven or digest in superseded or item.get("area") in without_bronze)]
    selected.sort(key=lambda item: item["size"])

    outcomes: Counter = Counter()
    moved_bytes = [0]
    lock = threading.Lock()

    def process(item: dict[str, Any]) -> None:
        if time.monotonic() > deadline:
            with lock:
                outcomes["not_started_time_budget"] += 1
            return
        result = ensure_archive(client, bucket, args.source, item)
        removed = 0
        if move:
            # Every Landing path holding these bytes is covered by the one Archive copy.
            for path in item.get("snapshot_paths") or [{"key": item["key"], "present": True}]:
                if path.get("present"):
                    client.delete_object(Bucket=bucket, Key=path["key"])
                    removed += 1
        with lock:
            outcomes[f"archive_{result}"] += 1
            outcomes["landing_files_deleted"] += removed
            moved_bytes[0] += int(item["size"]) if removed else 0

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(process, selected))
    summary = {"source": args.source, "move": move, "proven_in_bronze": len(proven),
               "superseded_versions": len(superseded),
               "without_bronze_areas": sorted(without_bronze), "selected": len(selected),
               "selected_gb": round(sum(int(i["size"]) for i in selected) / 1e9, 2),
               "landing_gb_freed": round(moved_bytes[0] / 1e9, 2), **outcomes}
    print(json.dumps(summary, sort_keys=True))
    annotate("notice", f"{args.source} landing archive", summary)
    step_summary(f"## {args.source} · landing archive\n\n```json\n{json.dumps(summary, indent=2)}\n```\n")
    return 2 if outcomes["not_started_time_budget"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
