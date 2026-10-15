"""Read-only probe (#803): exact rows a Bronze clean-up would remove (second copies of Landing files)."""

from __future__ import annotations

from importlib.metadata import version
import sys

import pyarrow.compute as pc
from pyiceberg.expressions import In

from ingestion.common.bronze_files import duplicated_hashes
from ingestion.common.r2 import catalog
from ingestion.common.report import annotate

cat = catalog()
for name in sys.argv[1:]:
    table = cat.load_table(tuple(name.split(".", 1)))
    twice = sorted(duplicated_hashes(table)["loaded_twice"])
    result = {"pyiceberg": version("pyiceberg"), "hashes": len(twice), "sample": twice[:3]}
    if twice:
        rows = table.scan(row_filter=In("raw_sha256", twice)).to_arrow()
        distinct = rows.group_by(rows.column_names).aggregate([([], "count_all")])
        counts = distinct.column("count_all")
        result |= {
            "rows_of_these_files": rows.num_rows,
            "distinct_rows": distinct.num_rows,
            "rows_to_remove": rows.num_rows - distinct.num_rows,
            "copies_per_row": dict(zip(*(a.to_pylist() for a in pc.value_counts(counts).flatten()), strict=True)),
            "columns": rows.column_names,
        }
    snapshot = table.current_snapshot()
    result["table_rows"] = int((snapshot.summary or {}).get("total-records", -1)) if snapshot else 0
    result["snapshot_id"] = snapshot.snapshot_id if snapshot else None
    annotate("notice", f"{name} clean-up plan", result)
