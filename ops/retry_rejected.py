#!/usr/bin/env python3
"""Show why Bronze rejected files, and (with --apply) let the next bronze.load retry them.

A Landing file the Bronze decoder rejects is recorded once (an Iceberg table, e.g.
bronze.eurostat_rejected_files) and skipped afterwards. Without --apply this lists the
rejections grouped by reason. With --apply the matching rows are deleted (the table's
snapshot history keeps them), so the next `bronze: load` picks those files up again;
use it after the decoder was fixed.
Args: --source eurostat [--dataset CODE_OR_GLOB ...] [--apply]
"""

from __future__ import annotations

import argparse
from collections import Counter
import fnmatch
import re

from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import In

from ingestion.common.r2 import catalog
from ingestion.common.report import annotate

REJECTIONS = {
    "eurostat": ("bronze", "eurostat_rejected_files"),
    "gleif": ("bronze", "gleif_rejected_files"),
    "mf_biala_lista": ("bronze", "biala_lista_rejected_files"),
    "gus_bdl": ("bronze", "bdl_rejected_files"),
    "gus_dbw": ("bronze", "dbw_rejected_files"),
    "opendata_org": ("bronze", "opendata_rejected_files"),
}


def reason(error: str) -> str:
    """Group errors by their wording, without row numbers, codes or quoted values."""
    text = re.sub(r"'[^']*'|\"[^\"]*\"", "…", error or "")
    return re.sub(r"\d+", "#", text)[:160]


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", choices=sorted(REJECTIONS), required=True)
    parser.add_argument("--dataset", nargs="*", default=[])
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    try:
        table = catalog().load_table(REJECTIONS[args.source])
        records = table.scan().to_arrow().to_pylist() if table.current_snapshot() else []
    except NoSuchTableError:
        table, records = None, []
    patterns = [p.lower() for p in args.dataset]
    selected = [
        r
        for r in records
        if not patterns
        or any(fnmatch.fnmatch(str(r.get("dataset_id") or r.get("entity") or "").lower(), p) for p in patterns)
    ]

    cleared = 0
    if args.apply and selected:
        table.delete(In("raw_sha256", sorted({r["raw_sha256"] for r in selected})))
        cleared = len(selected)
    annotate(
        "notice",
        f"{args.source} Bronze rejections",
        {
            "recorded": len(records),
            "selected": len(selected),
            "cleared_for_retry": cleared,
            "by_dataset_family": dict(
                Counter(
                    re.split(r"[_$]", str(r.get("dataset_id") or r.get("entity") or "?"))[0] for r in selected
                ).most_common(15)
            ),
            "by_reason": dict(Counter(reason(r.get("error", "")) for r in selected).most_common(8)),
            "examples": [
                {"dataset": r.get("dataset_id") or r.get("entity"), "error": str(r.get("error", ""))[:200]}
                for r in selected[:5]
            ],
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
