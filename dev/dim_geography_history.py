"""Read-only check for #2201: history, properties and contents of gold.dim_geography."""

from datetime import UTC, datetime

from ingestion.common.r2 import catalog
from ingestion.common.report import annotate


def main() -> int:
    cat = catalog()
    table = cat.load_table(("gold", "dim_geography"))
    snaps = [
        {
            "id": s.snapshot_id,
            "at": datetime.fromtimestamp(s.timestamp_ms / 1000, UTC).isoformat(),
            "op": str(s.summary.operation) if s.summary else None,
            "added": (s.summary or {}).get("added-records") if s.summary else None,
            "total": (s.summary or {}).get("total-records") if s.summary else None,
        }
        for s in table.snapshots()
    ][-8:]
    annotate(
        "notice",
        "dim_geography history",
        {
            "location": table.location(),
            "properties": dict(table.properties),
            "columns": [f.name for f in table.schema().fields],
            "snapshots": snaps,
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
