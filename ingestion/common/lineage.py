#!/usr/bin/env python3
"""Read-only exact Landing-to-Iceberg hash membership (bronze.verify).

This script never archives, deletes, rewrites pointers, or mutates Iceberg metadata.
Landing identities (SHA-256 from object metadata or content-addressed names) are compared
with Bronze provenance: the Bronze file list (one file per Landing SHA-256) and the
rejections table of every source in bronze_files.SOURCE_TABLES, read from the catalog.
The expectation is the latest version of every entity in the agreed Landing layout;
older versions are reported as superseded (Eurostat). The latest-only gate (#845) uses the
rule the prune after a load applies (``latest.keep_digests``), computed from the hashes read
once for the membership check (#851); ``superseded_in_bronze`` is for information only.
Landing files whose rows a Bronze table holds twice (#803) get their own annotation, a warning
while the known NBP duplicates await their clean-up.
"""

from __future__ import annotations

import argparse
import json
from typing import Any

from ingestion.common.r2 import landing_objects, latest_snapshot, req, s3
from ingestion.common.report import annotate

SOURCES = ("gugik_prg", "gus_teryt", "nbp", "world_bank_wdi")  # Eurostat, GUS BDL, GLEIF: entities.verify (#875)


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


def inspect_snapshot(meta: dict[str, dict[str, Any]], loaded: set[str], rejected: set[str]) -> dict[str, Any]:
    data = {d for d, i in meta.items() if i.get("kind") == "raw"}
    latest = latest_snapshot(meta)
    superseded = {d for d in data if meta[d].get("layout") == "snapshot"} - latest
    unloaded = latest - loaded
    return {
        "latest": summarize("latest", latest, loaded, meta),
        "latest_rejected_with_reason": len(unloaded & rejected),
        "latest_unexplained": len(unloaded - rejected),
        "latest_unexplained_sample": sorted(meta[d]["key"] for d in unloaded - rejected)[:10],
        "superseded_versions": len(superseded),
        "superseded_in_bronze": len(superseded & loaded),
        "all_provenance_recoverable": loaded <= set(meta),
    }


def check_latest_only(meta: dict[str, dict[str, Any]], held: dict[str, set[str]]) -> tuple[bool, dict[str, Any]]:
    """Every Bronze table holds only the latest version of every entity (policy #822/#845).

    Same rule as the prune after a load (``latest.keep_digests``), computed from the Landing
    receipts and table hashes this step already read: no second pass over the tables (#851)."""
    from ingestion.common.latest import keep_digests

    known = {d for d, item in meta.items() if item.get("snapshot_paths")}
    keep = keep_digests(meta)
    report: dict[str, Any] = {}
    for name, hashes in held.items():
        old = (hashes & known) - keep
        report[name] = {
            "files": len(hashes),
            "superseded": len(old),
            "superseded_sample": sorted(old)[:5],
            "unknown_kept": len(hashes - known),
        }
    return all(entry["superseded"] == 0 for entry in report.values()), report


def bronze_hashes(source: str) -> tuple[dict[str, set[str]], set[str]]:
    """(per Bronze table: Landing SHA-256 it holds, Landing SHA-256 rejected with a reason), read once."""
    from pyiceberg.exceptions import NoSuchTableError

    from ingestion.common.bronze_files import SOURCE_TABLES, loaded_hashes, rejected_hashes
    from ingestion.common.r2 import catalog

    tables, rejections = SOURCE_TABLES[source]
    cat = catalog()
    held: dict[str, set[str]] = {}
    for table_id in tables:
        try:
            held[".".join(table_id)] = loaded_hashes(cat.load_table(table_id))
        except NoSuchTableError:
            continue
    rejected: set[str] = set()
    for table_id in rejections:
        rejected |= rejected_hashes(cat, table_id)
    return held, rejected


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", choices=SOURCES, required=True)
    args = parser.parse_args(argv)

    client = s3()
    bucket = req("STORAGE_LANDING_BUCKET_NAME")
    meta = landing_objects(client, bucket, args.source)
    held, rejected = bronze_hashes(args.source)
    passed_latest, report_latest = check_latest_only(meta, held)
    if not passed_latest:
        annotate("error", f"{args.source} Bronze latest-only breach", report_latest)
    loaded = set().union(*held.values()) if held else set()
    result = inspect_snapshot(meta, loaded, rejected)
    summary = {
        "result": "pass" if passed_latest else "fail",
        "operation": "inspect-exact-landing-iceberg-hash-membership",
        "source": args.source,
        "landing_objects": len(meta),
        "landing_bytes": sum(int(item["size"]) for item in meta.values()),
        "writes_performed": False,
        "latest_only": report_latest,
        **result,
    }
    print(json.dumps(summary, sort_keys=True))
    annotate("notice" if passed_latest else "error", f"{args.source} Landing to Bronze lineage", summary)
    report_duplicates(args.source)
    from ingestion.common import coverage

    coverage.publish(
        args.source, lambda: coverage.lineage_rows(args.source, meta, latest_snapshot(meta), loaded, rejected)
    )
    return 0 if passed_latest else 1


def report_duplicates(source: str) -> None:
    """Annotate per Bronze table the Landing files it holds twice (count and a sample)."""
    from ingestion.common.bronze_files import source_duplicates

    found = {
        table: {kind: {"count": len(hashes), "sample": sorted(hashes)[:3]} for kind, hashes in kinds.items() if hashes}
        for table, kinds in source_duplicates(source).items()
    }
    twice = any("loaded_twice" in kinds for kinds in found.values())
    annotate("warning" if twice else "notice", f"{source} Bronze duplicates", found)


if __name__ == "__main__":
    raise SystemExit(main())
