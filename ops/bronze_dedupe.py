#!/usr/bin/env python3
"""Remove the second copies of Landing files from one Bronze table (owner approval #815).

Rows of the Landing files the table holds twice (``bronze_files.duplicated_hashes``) are
replaced, in one Iceberg overwrite, by one copy of each distinct row (equal in every
column). Nothing else in the table changes. The owner approves exact counts, so the run
states them as arguments and stops without writing when the table differs from any of
them: the number of doubled Landing files, the rows to remove and the table rows before.
Without --apply it only reports the plan. After --apply it re-reads the table and checks
the row count and that no Landing file is doubled any more.

    python -m ops.bronze_dedupe bronze.nbp_observations --hashes 352 --remove 440457 --rows 882642 [--apply]
"""

from __future__ import annotations

import argparse
import traceback

import pyarrow as pa
from pyiceberg.expressions import In

from ingestion.common.bronze_files import duplicated_hashes
from ingestion.common.r2 import catalog
from ingestion.common.report import annotate


def table_rows(table) -> int:
    """Rows of the current snapshot (its summary's total-records; 0 for an empty table)."""
    snapshot = table.current_snapshot()
    return int((snapshot.summary or {}).get("total-records", 0)) if snapshot else 0


def plan(table) -> tuple[list[str], pa.Table, pa.Table]:
    """The doubled Landing files' hashes, all their rows and one copy of each distinct row."""
    twice = sorted(duplicated_hashes(table)["loaded_twice"])
    if not twice:
        empty = table.schema().as_arrow().empty_table()
        return twice, empty, empty
    rows = table.scan(row_filter=In("raw_sha256", twice)).to_arrow()
    distinct = rows.group_by(rows.column_names).aggregate([]).select(rows.column_names).cast(rows.schema)
    return twice, rows, distinct


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("table", help="namespace.table, e.g. bronze.nbp_observations")
    parser.add_argument("--hashes", type=int, required=True, help="approved number of doubled Landing files")
    parser.add_argument("--remove", type=int, required=True, help="approved number of rows to remove")
    parser.add_argument("--rows", type=int, required=True, help="approved table rows before")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    title = f"{args.table} bronze dedupe"
    try:
        table = catalog().load_table(tuple(args.table.split(".", 1)))
        before = table_rows(table)
        twice, rows, distinct = plan(table)
        found = {"hashes": len(twice), "remove": rows.num_rows - distinct.num_rows, "rows": before}
        approved = {"hashes": args.hashes, "remove": args.remove, "rows": args.rows}
        result = {"apply": args.apply, "found": found, "approved": approved, "snapshot_before": _snapshot_id(table)}
        if found != approved:
            annotate("error", title, {**result, "outcome": "counts differ from the approval; nothing changed"})
            return 1
        if not args.apply:
            annotate("notice", title, {**result, "outcome": "plan only (dry run)"})
            return 0
        table.overwrite(distinct, overwrite_filter=In("raw_sha256", twice))
        table = table.refresh()
        after = table_rows(table)
        left = len(duplicated_hashes(table)["loaded_twice"])
        result |= {"rows_after": after, "doubled_files_after": left, "snapshot_after": _snapshot_id(table)}
        ok = after == before - args.remove and left == 0
        annotate("notice" if ok else "error", title, {**result, "outcome": "done" if ok else "check after failed"})
        return 0 if ok else 1
    except Exception:
        annotate("error", f"{title} crashed", {"traceback": traceback.format_exc()[-3000:]})
        raise


def _snapshot_id(table) -> int | None:
    snapshot = table.current_snapshot()
    return snapshot.snapshot_id if snapshot else None


if __name__ == "__main__":
    raise SystemExit(main())
