#!/usr/bin/env python3
"""Show why Bronze rejected files, and (with --apply) let the next bronze.load retry them.

A Landing file the Bronze decoder rejects is recorded once and skipped afterwards.
Without --apply this lists the rejections grouped by reason. With --apply the matching
records are moved to <failures>/retried/<timestamp>/ (kept for history), so the next
`bronze: load` picks those files up again; use it after the decoder was fixed.
Args: --source eurostat [--dataset CODE_OR_GLOB ...] [--apply]
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import fnmatch
import json
import re

from ingestion.common.r2 import req, s3
from ingestion.common.report import annotate

FAILURES = {"eurostat": "06_control/eurostat_bulk_bronze/failures/"}


def reason(error: str) -> str:
    """Group errors by their wording, without row numbers, codes or quoted values."""
    text = re.sub(r"'[^']*'|\"[^\"]*\"", "…", error or "")
    return re.sub(r"\d+", "#", text)[:160]


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", choices=sorted(FAILURES), required=True)
    parser.add_argument("--dataset", nargs="*", default=[])
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    client, bucket = s3(), req("R2_LAKEHOUSE_BUCKET")
    prefix = FAILURES[args.source]

    records = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json"):
                body = json.loads(client.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read())
                records.append({"key": obj["Key"], **body})
    patterns = [p.lower() for p in args.dataset]
    selected = [r for r in records if not patterns
                or any(fnmatch.fnmatch(str(r.get("dataset_id", "")).lower(), p) for p in patterns)]

    reasons = Counter(reason(r.get("error", "")) for r in selected)
    families = Counter(re.split(r"[_$]", str(r.get("dataset_id", "?")))[0] for r in selected)
    moved = 0
    if args.apply:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        for record in selected:
            target = f"{prefix}retried/{stamp}/{record['key'].rsplit('/', 1)[1]}"
            client.copy_object(Bucket=bucket, Key=target, CopySource={"Bucket": bucket, "Key": record["key"]})
            client.head_object(Bucket=bucket, Key=target)
            client.delete_object(Bucket=bucket, Key=record["key"])
            moved += 1
    annotate("notice", f"{args.source} Bronze rejections", {
        "recorded": len(records), "selected": len(selected), "cleared_for_retry": moved,
        "by_dataset_family": dict(families.most_common(15)),
        "by_reason": dict(reasons.most_common(8)),
        "examples": [{"dataset": r.get("dataset_id"), "error": str(r.get("error", ""))[:200]}
                     for r in selected[:5]]})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
