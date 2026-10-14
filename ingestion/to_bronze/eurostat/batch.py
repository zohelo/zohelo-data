#!/usr/bin/env python3
"""One Eurostat Bronze batch: Landing data files → bronze.eurostat_full_observations.

Reads the Landing layout of docs/landing.md
(``01_landing/eurostat/snapshot/data/<CODE>/<version>__<ingested>.tsv.gz`` + receipt) and
loads the latest version of every dataset. Each Landing file becomes one Parquet file
``02_bronze/eurostat/observations/<dataset_id>/<raw_sha256>.parquet`` registered in the
Iceberg table (partitioned by dataset_id), so the table's file list is the checkpoint.
Files the decoder rejects are recorded in bronze.eurostat_rejected_files and skipped on
later runs (ops/retry_rejected clears them). Never calls Eurostat, never touches Landing;
reads a file from the Archive when landing.archive has already moved it.
Prints one JSON line with "operation": "eurostat-bronze-batch" as its result.
"""

from __future__ import annotations

import argparse
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
from ingestion.common.r2 import catalog, req, s3
from ingestion.to_bronze.eurostat.decode import BRONZE_COLUMNS, EurostatBulkDecodeError, decode_landing_distribution

TABLE_ID = ("bronze", "eurostat_full_observations")
REJECTED_ID = ("bronze", "eurostat_rejected_files")
ENTITY_COLUMN = "dataset_id"  # rejected-files table predates the shared "entity" name
OUTPUT_PREFIX = "02_bronze/eurostat/observations/"
LANDING_DATA_PREFIX = "01_landing/eurostat/snapshot/data/"
STATS_COLUMNS = ["dataset_id", "raw_sha256"]  # full bounds: partition value and pruning


def loaded_hashes(table) -> set[str]:
    """Raw SHA-256 of every Landing file already in Bronze (from the file names)."""
    return bf.loaded_hashes(table)


def rejected_hashes(table) -> set[str]:
    """Raw SHA-256 of every Landing file recorded as rejected."""
    if table.current_snapshot() is None:
        return set()
    return set(table.scan(selected_fields=("raw_sha256",)).to_arrow().column("raw_sha256").to_pylist())


def landing_candidates(client, landing_bucket: str) -> list[dict[str, Any]]:
    """The latest Landing version of every dataset (highest source version, then ingest)."""
    keys = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=landing_bucket, Prefix=LANDING_DATA_PREFIX):
        keys += [item["Key"] for item in page.get("Contents", []) if item["Key"].endswith(".receipt.json")]

    def read(key: str) -> dict[str, Any]:
        body = bf.retry(lambda: client.get_object(Bucket=landing_bucket, Key=key)["Body"].read(), "read_receipt")
        receipt = json.loads(body)
        sha, size, obj = receipt.get("sha256"), receipt.get("size_bytes"), receipt.get("object") or {}
        if (
            receipt.get("schema_version") != 2
            or receipt.get("kind") != "eurostat_landing"
            or receipt.get("area") != "data"
            or not isinstance(sha, str)
            or not bf.SHA_RE.fullmatch(sha)
            or type(size) is not int
            or size <= 0
            or obj.get("bucket") != landing_bucket
            or not str(obj.get("key", "")).startswith(LANDING_DATA_PREFIX)
        ):
            raise RuntimeError(f"invalid Eurostat Landing receipt: {key}")
        return {
            "raw_sha256": sha,
            "size": size,
            "receipt_key": key,
            "receipt_bytes": body,
            "data_key": obj["key"],
            "dataset_id": receipt.get("dataset_code"),
            "order": (str(receipt.get("version_token") or ""), str(receipt.get("ingested_at") or "")),
        }

    with ThreadPoolExecutor(max_workers=8) as pool:  # R2 throttles bursts of reads
        receipts = list(pool.map(read, sorted(keys)))
    latest: dict[str, dict[str, Any]] = {}
    for item in receipts:
        held = latest.get(item["dataset_id"])
        if held is None or item["order"] > held["order"]:
            latest[item["dataset_id"]] = item
    return sorted(latest.values(), key=lambda item: item["dataset_id"])


def download(client, bucket: str, item: dict[str, Any], destination: Path) -> None:
    """The Landing file's exact bytes (Landing or Archive), verified against the receipt."""
    if shutil.disk_usage(destination.parent).free < item["size"] * 6 + (2 << 30):
        raise RuntimeError("runner disk headroom is insufficient for Eurostat decode")
    bf.download(client, bucket, "eurostat", item["data_key"], item["raw_sha256"], item["size"], destination)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-files", type=int, default=64)
    parser.add_argument("--session-seconds", type=int, default=900)
    args = parser.parse_args(argv)
    started = time.monotonic()
    client, cat = s3(), bf.retry(catalog, "catalog")
    bucket, landing_bucket = req("R2_LAKEHOUSE_BUCKET"), req("R2_LANDING_BUCKET")
    table = bf.retry(lambda: bf.ensure_table(cat, TABLE_ID, list(BRONZE_COLUMNS), "dataset_id"), "load_table")
    done = loaded_hashes(table) | rejected_hashes(bf.rejected_table(cat, REJECTED_ID, ENTITY_COLUMN))
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
                    raw,
                    receipt,
                    output,
                    receipt_uri=f"r2://{landing_bucket}/{item['receipt_key']}",
                    raw_uri=f"r2://{landing_bucket}/{item['data_key']}",
                )
            except EurostatBulkDecodeError as exc:
                rejected.append(
                    {
                        "entity": item["dataset_id"],
                        "raw_sha256": item["raw_sha256"],
                        "landing_key": item["data_key"],
                        "error": str(exc)[:1000],
                        "recorded_at": datetime.now(UTC),
                        "code_sha": os.environ.get("GITHUB_SHA", "unknown"),
                    }
                )
                print(json.dumps({"status": "rejected", **{k: str(v) for k, v in rejected[-1].items()}}), flush=True)
                shutil.rmtree(work, ignore_errors=True)
                continue
            uri = bf.upload(
                client,
                bucket,
                f"{OUTPUT_PREFIX}{item['dataset_id']}/{item['raw_sha256']}.parquet",
                output,
                item["raw_sha256"],
            )
            prepared.append(
                {
                    "uri": uri,
                    "rows": report["observation_cells"],
                    "bytes": report["output_bytes"],
                    "dataset_id": item["dataset_id"],
                }
            )
            print(json.dumps({"status": "prepared", **prepared[-1]}), flush=True)
            shutil.rmtree(work, ignore_errors=True)

    if prepared:
        bf.commit(cat, TABLE_ID, [p["uri"] for p in prepared], STATS_COLUMNS, "eurostat-bronze-batch")
        after = loaded_hashes(bf.retry(lambda: cat.load_table(TABLE_ID), "reload_table"))
        lost = [p["uri"] for p in prepared if Path(p["uri"]).stem not in after]
        if lost:
            raise RuntimeError(f"Bronze commit lacks prepared files: {lost[:3]}")
    bf.record_rejections(cat, REJECTED_ID, rejected, ENTITY_COLUMN)
    print(
        json.dumps(
            {
                "operation": "eurostat-bronze-batch",
                "result": "pass",
                "processed": len(prepared) + len(rejected),
                "appended_files": len(prepared),
                "rows_added": sum(p["rows"] for p in prepared),
                "bytes_added": sum(p["bytes"] for p in prepared),
                "failed": len(rejected),
                "failed_datasets": [r["entity"] for r in rejected],
                "missing_distinct_raw_sha256": len(candidates) - len(prepared) - len(rejected),
                "elapsed_seconds": round(time.monotonic() - started, 1),
            },
            sort_keys=True,
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
