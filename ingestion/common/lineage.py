#!/usr/bin/env python3
"""Read-only exact Landing-to-Iceberg hash membership (bronze.verify).

This script never archives, deletes, rewrites pointers, or mutates Iceberg metadata.
Landing identities (SHA-256 from object metadata or content-addressed names) are compared
with Bronze provenance: the Bronze file list (one file per Landing SHA-256) and the
rejections table of every source in bronze_files.SOURCE_TABLES, read from the catalog.
The expectation is the latest version of every entity in the agreed Landing layout;
older versions are reported as superseded (Eurostat). Landing files whose rows a Bronze
table holds twice (#803) are reported per table; while the known NBP duplicates await their
clean-up this is a warning, not a failure.
"""

from __future__ import annotations

import argparse
import json
from typing import Any

from ingestion.common.r2 import landing_objects, latest_snapshot, req, s3
from ingestion.common.report import annotate

SOURCES = ("eurostat", "gugik_prg", "gus_teryt", "nbp", "world_bank_wdi")


def summarize(name: str, landing_set: set[str], proven: set[str], meta: dict[str, dict[str, Any]]) -> dict[str, Any]:
    represented = landing_set & proven
    unrepresented = landing_set - proven
    missing_raw = proven - landing_set
    return {
        "name": name,
        "landing_sha256": len(landing_set),
        "provenance_sha256": len(proven),
        "represented_landing_sha256": len(represented),
        "unrepresented_landing_sha256": len(unrepresented),
        "provenance_without_landing_sha256": len(missing_raw),
        "represented_bytes": sum(int(meta[d]["size"]) for d in represented),
        "unrepresented_bytes": sum(int(meta[d]["size"]) for d in unrepresented),
        "represented_sample": sorted(represented)[:12],
        "unrepresented_sample": sorted(unrepresented)[:12],
        "provenance_without_landing_sample": sorted(missing_raw)[:12],
    }


def duplicates(duplicated: dict[str, dict[str, set[str]]]) -> dict[str, Any]:
    """Per Bronze table: Landing files loaded twice, and files only in several compacted files."""
    return {
        table: {kind: {"count": len(hashes), "sample": sorted(hashes)[:3]} for kind, hashes in found.items() if hashes}
        for table, found in duplicated.items()
        if any(found.values())
    }


def inspect_eurostat(meta: dict[str, dict[str, Any]]) -> dict[str, Any]:
    from ingestion.common.bronze_files import bronze_hashes

    data = {d for d, i in meta.items() if i["kind"] == "raw"}
    latest = latest_snapshot(meta)
    superseded = {d for d in data if meta[d]["layout"] == "snapshot"} - latest
    full, rejected, duplicated = bronze_hashes("eurostat")
    unloaded = latest - full
    return {
        "latest": summarize("latest", latest, full, meta),
        "latest_rejected_with_reason": len(unloaded & rejected),
        "latest_unexplained": len(unloaded - rejected),
        "latest_unexplained_sample": sorted(meta[d]["key"] for d in unloaded - rejected)[:10],
        "superseded_versions": len(superseded),
        "superseded_in_bronze": len(superseded & full),
        "all_provenance_recoverable": full <= set(meta),
        "bronze_duplicates": duplicates(duplicated),
    }


def inspect_snapshot(meta: dict[str, dict[str, Any]], source: str) -> dict[str, Any]:
    from ingestion.common.bronze_files import bronze_hashes

    latest = latest_snapshot(meta)
    loaded, rejected, duplicated = bronze_hashes(source)
    unloaded = latest - loaded
    return {
        "latest": summarize("latest", latest, loaded, meta),
        "latest_rejected_with_reason": len(unloaded & rejected),
        "latest_unexplained": len(unloaded - rejected),
        "latest_unexplained_sample": sorted(meta[d]["key"] for d in unloaded - rejected)[:10],
        "all_provenance_recoverable": loaded <= set(meta),
        "bronze_duplicates": duplicates(duplicated),
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", choices=SOURCES, required=True)
    args = parser.parse_args(argv)
    client = s3()
    bucket = req("R2_LANDING_BUCKET")
    meta = landing_objects(client, bucket, args.source)
    result = inspect_eurostat(meta) if args.source == "eurostat" else inspect_snapshot(meta, args.source)
    summary = {
        "result": "pass",
        "operation": "inspect-exact-landing-iceberg-hash-membership",
        "source": args.source,
        "landing_objects": len(meta),
        "landing_bytes": sum(int(item["size"]) for item in meta.values()),
        "writes_performed": False,
        **result,
    }
    print(json.dumps(summary, sort_keys=True))
    twice = any("loaded_twice" in found for found in summary["bronze_duplicates"].values())
    annotate("warning" if twice else "notice", f"{args.source} Landing to Bronze lineage", summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
