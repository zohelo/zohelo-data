#!/usr/bin/env python3
"""Platform inventory (read-only): what each source holds in every layer, and storage totals.

Per registered source (ingestion/sources/*.yaml): Landing and Archive files, bytes and
latest write; Bronze / Silver / Gold tables with rows, data files, bytes and snapshot
count (from Iceberg metadata, no data scan). Also storage per bucket and top folder,
Landing/Archive folders and Iceberg tables that no source claims, and registered tables
that do not exist. Fails fast when credentials or the catalog are broken.

Output: annotations (totals + sources), the job summary, and the full JSON in
$OPS_OUTPUT_DIR. Args: --source <id> ... to limit the per-source part.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
import fnmatch
import json
import os
from pathlib import Path
from typing import Any

import yaml

from ingestion.common.r2 import catalog, req, s3, sources
from ingestion.common.report import annotate, step_summary

ROOT = Path(__file__).resolve().parents[1]
LAYERS = ("bronze", "silver", "gold")
# R2 Standard storage (docs/operating.md): first 10 GB-month free per account, then
# $0.015 per GB-month. Operations are not estimated here. A warning is raised above
# COST_WARNING_USD so the owner hears about it before an invoice does.
FREE_GB, USD_PER_GB_MONTH, COST_WARNING_USD = 10.0, 0.015, 2.0


def folders(client, bucket: str, prefix: str, depth: int) -> dict[str, dict[str, Any]]:
    """Objects, bytes and latest write per folder at the given depth below the bucket root."""
    out: dict[str, dict[str, Any]] = defaultdict(lambda: {"objects": 0, "bytes": 0, "latest": ""})
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            parts = obj["Key"].split("/")
            name = "/".join(parts[:depth]) if len(parts) > depth else "(files)"
            entry = out[name]
            entry["objects"] += 1
            entry["bytes"] += int(obj["Size"])
            entry["latest"] = max(entry["latest"], obj["LastModified"].isoformat()[:19])
    return dict(out)


def tables(cat) -> dict[str, dict[str, Any]]:
    out = {}
    for layer in LAYERS:
        for identifier in cat.list_tables(layer):
            name = f"{layer}.{identifier[-1]}"
            table = cat.load_table(identifier)
            snapshot = table.current_snapshot()
            summary = snapshot.summary if snapshot is not None and snapshot.summary is not None else {}
            out[name] = {
                "rows": int(summary.get("total-records") or 0),
                "files": int(summary.get("total-data-files") or 0),
                "bytes": int(summary.get("total-files-size") or 0),
                "snapshots": len(table.metadata.snapshots),
            }
    return out


def gb(value: int) -> float:
    return round(value / 1e9, 2)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", nargs="*", default=None)
    args = parser.parse_args(argv)
    client, cat = s3(), catalog()
    landing_bucket, lakehouse_bucket = req("R2_LANDING_BUCKET"), req("R2_LAKEHOUSE_BUCKET")
    registry = sources()

    landing = folders(client, landing_bucket, "01_landing/", 2)
    archive = folders(client, landing_bucket, "05_archive/", 2)
    buckets = {
        landing_bucket: folders(client, landing_bucket, "", 1),
        lakehouse_bucket: folders(client, lakehouse_bucket, "", 1),
    }
    found = tables(cat)

    claimed_tables, claimed_landing, report = set(), set(), {}
    for source, config in registry.items():
        landing_folder = (config.get("landing") or f"01_landing/{source}/").rstrip("/")
        archive_folder = (config.get("archive_folder") or f"05_archive/{source}/").rstrip("/")
        claimed_landing.add(landing_folder)
        layers = {}
        for layer in LAYERS:
            names = list(config.get(layer) or [])
            claimed_tables.update(names)
            present = [found[n] for n in names if n in found]
            layers[layer] = {
                "tables": len(present),
                "rows": sum(t["rows"] for t in present),
                "files": sum(t["files"] for t in present),
                "gb": gb(sum(t["bytes"] for t in present)),
                # A rejected-files table is created with the first rejection.
                "missing": [n for n in names if n not in found and not n.endswith("_rejected_files")],
            }
        report[source] = {
            "landing": landing.get(landing_folder, {"objects": 0, "bytes": 0, "latest": ""}),
            "archive": archive.get(archive_folder, {"objects": 0, "bytes": 0, "latest": ""}),
            **layers,
            "activities": config.get("activities") or [],
        }

    # Shared Gold of the dbt project and retired-code tables still in the catalog are claimed too.
    platform = yaml.safe_load((ROOT / "warehouse" / "warehouse.yaml").read_text()) or {}
    claimed_tables.update(platform.get("gold") or [])
    legacy = [g for c in registry.values() for g in (c.get("legacy_tables") or [])]
    legacy_found = sorted(n for n in found if any(fnmatch.fnmatch(n, g) for g in legacy))
    claimed_tables.update(legacy_found)
    unclaimed_tables = sorted(set(found) - claimed_tables)
    unclaimed_landing = sorted(set(landing) - claimed_landing)
    totals = {
        "buckets_gb": {bucket: gb(sum(f["bytes"] for f in tops.values())) for bucket, tops in buckets.items()},
        "folders_gb": {
            f"{bucket}/{name}": gb(info["bytes"])
            for bucket, tops in buckets.items()
            for name, info in sorted(tops.items())
        },
        "iceberg_tables": len(found),
        "iceberg_rows": sum(t["rows"] for t in found.values()),
        "unclaimed_tables": unclaimed_tables[:30],
        "unclaimed_landing_folders": unclaimed_landing[:20],
        "legacy_tables": {"count": len(legacy_found), "gb": gb(sum(found[n]["bytes"] for n in legacy_found))},
        "registered_tables_missing": sorted(n for s in report.values() for l in LAYERS for n in s[l]["missing"])[:30],
    }
    stored_gb = sum(totals["buckets_gb"].values())
    totals["storage_estimate"] = {
        "stored_gb": round(stored_gb, 2),
        "free_gb": FREE_GB,
        "usd_per_month": round(max(0.0, stored_gb - FREE_GB) * USD_PER_GB_MONTH, 2),
    }
    annotate("notice", "inventory totals", totals)
    if totals["storage_estimate"]["usd_per_month"] > COST_WARNING_USD:
        annotate("warning", "R2 storage above free tier", totals["storage_estimate"])

    selected = [s for s in report if not args.source or s in args.source]
    compact = {
        s: {
            "landing": [
                report[s]["landing"]["objects"],
                gb(report[s]["landing"]["bytes"]),
                report[s]["landing"]["latest"][:10],
            ],
            "archive": [report[s]["archive"]["objects"], gb(report[s]["archive"]["bytes"])],
            **{l: [report[s][l]["tables"], report[s][l]["rows"], report[s][l]["gb"]] for l in LAYERS},
        }
        for s in selected
    }
    note = {"format": "landing [files, GB, latest]; archive [files, GB]; layer [tables, rows, GB]"}
    chunk: dict = {}
    for source, row in compact.items():  # pack sources into as few annotations as fit
        if len(json.dumps({**note, **chunk, source: row})) > 3800:
            annotate("notice", "inventory sources", {**note, **chunk})
            chunk = {}
        chunk[source] = row
    if chunk:
        annotate("notice", "inventory sources", {**note, **chunk})

    lines = [
        "## Platform inventory",
        "",
        "| source | Landing files | Landing GB | latest | Archive GB | Bronze rows | Silver rows | Gold rows | activities |",
        "|---|---:|---:|---|---:|---:|---:|---:|---|",
    ]
    for s in selected:
        r = report[s]
        lines.append(
            f"| {s} | {r['landing']['objects']:,} | {gb(r['landing']['bytes'])} | {r['landing']['latest'][:10]} "
            f"| {gb(r['archive']['bytes'])} | {r['bronze']['rows']:,} | {r['silver']['rows']:,} "
            f"| {r['gold']['rows']:,} | {', '.join(r['activities']) or '–'} |"
        )
    step_summary("\n".join(lines) + "\n")

    output = Path(os.environ.get("OPS_OUTPUT_DIR", ".")) / "platform-inventory.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(
            {
                "totals": totals,
                "sources": report,
                "tables": found,
                "buckets": buckets,
                "landing_folders": landing,
                "archive_folders": archive,
            },
            indent=2,
            default=str,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
