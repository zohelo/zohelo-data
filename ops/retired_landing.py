#!/usr/bin/env python3
"""Report (and with --apply delete) the retired Landing folders and zz_dev_ scratch prefixes.

Groups: every entry of `KEPT_OLD_FOLDERS` (ingestion/common/r2.py) under `01_landing/<source>/`
in the Landing bucket (gus_dbw `native/` and `_control/`, the Drive-era opendata_org copy), and
every top-level prefix starting with `zz_dev_` in both buckets. Each group shows object count,
GB and newest write. Owner decision 27 Oct 2026 (#2) approves the deletion.

Without --apply nothing is written. `--apply --expect-objects N --expect-gb G` deletes in batches
of 1000 only when the found totals equal N and round(GB, 2) == G, and only for sources that
still have objects in their current layout (`01_landing/<source>/` outside the kept entries,
plus `05_archive/<source>/`); it re-lists afterwards and fails if anything remains.

Args: --apply, --expect-objects <n>, --expect-gb <g>.
"""

from __future__ import annotations

import argparse
from typing import Any

from ingestion.common.r2 import KEPT_OLD_FOLDERS, req, s3
from ingestion.common.report import annotate, step_summary


def list_objects(client, bucket: str, prefix: str) -> list[dict[str, Any]]:
    """Every object under a prefix (paged): key, size, last modified."""
    found = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        found += [
            {"key": o["Key"], "size": int(o["Size"]), "modified": o["LastModified"]} for o in page.get("Contents", [])
        ]
    return found


def scratch_prefixes(client, bucket: str) -> list[str]:
    """Top-level prefixes of a bucket starting with zz_dev_."""
    found = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix="zz_dev_", Delimiter="/"):
        found += [p["Prefix"] for p in page.get("CommonPrefixes", [])]
    return found


def current_layout_count(client, bucket: str, source: str) -> int:
    """Objects of the source's current layout: Landing outside the kept entries, plus Archive."""
    kept = KEPT_OLD_FOLDERS[source]
    prefix = f"01_landing/{source}/"
    count = sum(1 for o in list_objects(client, bucket, prefix) if not o["key"][len(prefix) :].startswith(kept))
    return count + len(list_objects(client, bucket, f"05_archive/{source}/"))


def groups(client, landing: str, lakehouse: str) -> list[dict[str, Any]]:
    """Every group to retire: bucket, prefix label, source (kept entries only) and its objects."""
    found = []
    for source, entries in KEPT_OLD_FOLDERS.items():
        for entry in entries:
            prefix = f"01_landing/{source}/{entry}"
            found.append(
                {
                    "bucket": landing,
                    "prefix": prefix,
                    "source": source,
                    "objects": list_objects(client, landing, prefix),
                }
            )
    for bucket in dict.fromkeys([landing, lakehouse]):
        for prefix in scratch_prefixes(client, bucket):
            found.append(
                {"bucket": bucket, "prefix": prefix, "source": None, "objects": list_objects(client, bucket, prefix)}
            )
    return found


def totals(found: list[dict[str, Any]]) -> tuple[int, float]:
    """Object count and GB over all groups."""
    return sum(len(g["objects"]) for g in found), sum(o["size"] for g in found for o in g["objects"]) / 1e9


def report(found: list[dict[str, Any]], safety: dict[str, int]) -> None:
    """One annotation per group plus totals and the step summary."""
    lines = ["## retired landing", "", "| bucket | prefix | objects | GB | newest write |", "|---|---|---:|---:|---|"]
    for group in found:
        objects = group["objects"]
        newest = max((o["modified"] for o in objects), default=None)
        gb = round(sum(o["size"] for o in objects) / 1e9, 3)
        annotate(
            "notice",
            "retired landing group",
            {"prefix": group["prefix"], "bucket": group["bucket"], "objects": len(objects), "gb": gb, "newest": newest},
        )
        lines.append(f"| {group['bucket']} | `{group['prefix']}` | {len(objects):,} | {gb} | {newest} |")
    count, gb = totals(found)
    annotate(
        "notice", "retired landing total", {"objects": count, "gb": round(gb, 2), "current_layout_objects": safety}
    )
    step_summary("\n".join(lines) + "\n")


def delete(client, group: dict[str, Any]) -> None:
    """Delete a group's objects in batches of 1000."""
    objects = group["objects"]
    for start in range(0, len(objects), 1000):
        response = client.delete_objects(
            Bucket=group["bucket"],
            Delete={"Objects": [{"Key": o["key"]} for o in objects[start : start + 1000]], "Quiet": True},
        )
        if response.get("Errors"):
            raise RuntimeError(f"delete failed: {response['Errors'][:3]}")


def main(argv=None) -> int:
    """Report; with --apply and matching totals, delete and verify."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--expect-objects", type=int, default=None)
    parser.add_argument("--expect-gb", type=float, default=None)
    args = parser.parse_args(argv)
    client, landing, lakehouse = s3(), req("STORAGE_LANDING_BUCKET_NAME"), req("STORAGE_LAKEHOUSE_BUCKET_NAME")
    found = groups(client, landing, lakehouse)
    safety = {source: current_layout_count(client, landing, source) for source in KEPT_OLD_FOLDERS}
    report(found, safety)
    if not args.apply:
        return 0
    count, gb = totals(found)
    if args.expect_objects != count or args.expect_gb is None or round(gb, 2) != args.expect_gb:
        annotate(
            "error",
            "retired landing refused",
            {
                "expected_objects": args.expect_objects,
                "expected_gb": args.expect_gb,
                "found": count,
                "found_gb": round(gb, 2),
            },
        )
        return 1
    empty = sorted(source for source, n in safety.items() if n == 0)
    if empty:
        annotate("error", "retired landing refused: no current layout", {"sources": empty})
        return 1
    for group in found:
        delete(client, group)
    remaining = sum(len(g["objects"]) for g in groups(client, landing, lakehouse))
    if remaining:
        annotate("error", "retired landing: objects remain", {"remaining": remaining})
        return 1
    annotate("notice", "retired landing deleted", {"objects": count, "gb": round(gb, 2)})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
