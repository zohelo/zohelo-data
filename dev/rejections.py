"""Read-only: rejection records and null-partition files of the new BDL tables (#875 swap check)."""

from __future__ import annotations

from ingestion.common.r2 import catalog
from ingestion.common.report import annotate
from ingestion.to_bronze.gus_bdl import entities as m


def main() -> int:
    cat = catalog()
    rows = cat.load_table(m.REJECTED).scan().to_arrow().to_pylist()
    mine = [{"entity": r["dataset_id"], "key": (r["raw_sha256"] or "")[:16], "error": (r["error"] or "")[:90]} for r in rows if str(r["recorded_at"])[:10] == "2026-10-20"]
    annotate("notice", "bdl rejections today", {"count": len(mine), "rows": mine[:30], "empty_keys": len(m.empty_keys(cat, m.REJECTED))})
    for tid in (m.BRONZE, ("silver", "bdl_observations_new")):
        t = cat.load_table(tid).inspect.files()
        odd = [(p, n, part) for p, n, part in zip(t.column("file_path").to_pylist(), t.column("record_count").to_pylist(), t.column("partition").to_pylist(), strict=True) if not (part or {}).get("subgroup_id")]
        annotate("notice", f"null-partition files {'.'.join(tid)}", [{"path": p[-80:], "rows": n, "partition": str(part)} for p, n, part in odd][:10])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
