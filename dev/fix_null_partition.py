"""One-off (#875): drop the zero-row files with no subgroup that a failed run left in the new
bronze.bdl_observations (metadata delete of the null partition; no other partition is touched)."""

from __future__ import annotations

from pyiceberg.expressions import IsNull

from ingestion.common import entity_tables as et
from ingestion.common.r2 import catalog
from ingestion.common.report import annotate
from ingestion.to_bronze.gus_bdl import entities as m


def main() -> int:
    cat = catalog()
    table = cat.load_table(m.BRONZE)
    before = et.state(table, m.ENTITY)
    nulls = before.get(None, {})
    if nulls.get("rows", 0) != 0:
        annotate("error", "fix null partition", {"refused": "null partition holds rows", "rows": nulls.get("rows")})
        return 1
    table.delete(IsNull(m.ENTITY))
    after = et.state(cat.load_table(m.BRONZE), m.ENTITY)
    annotate("notice", "fix null partition", {"files_removed": len(nulls.get("paths", [])), "entities_before": len(before), "entities_after": len(after), "rows_after": sum(v["rows"] for v in after.values()), "null_left": None in after})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
