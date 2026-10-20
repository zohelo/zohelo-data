"""Read-only: rejection reasons recorded for GUS BDL and Eurostat (#875 swap check)."""

from __future__ import annotations

from ingestion.common.r2 import catalog
from ingestion.common.report import annotate


def main() -> int:
    cat = catalog()
    for table_id, col in ((("bronze", "bdl_rejected_files"), "dataset_id"), (("bronze", "eurostat_rejected_files"), "dataset_id")):
        rows = cat.load_table(table_id).scan().to_arrow().to_pylist()
        out = [{"entity": r[col], "key": (r["raw_sha256"] or "")[:12], "at": str(r["recorded_at"])[:10], "error": (r["error"] or "")[:160]} for r in rows]
        annotate("notice", f"rejections {'.'.join(table_id)}", {"count": len(out), "rows": out[:25]})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
