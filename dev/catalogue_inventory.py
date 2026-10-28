"""Verify-only Task #2118: dimensions, members and units of the published KPIs' measures on prod Gold."""

import csv
import json
import os
from pathlib import Path

from ingestion.common.r2 import catalog
from ingestion.common.report import annotate


def read(cat, name: str) -> list[dict]:
    return cat.load_table(("gold", name)).scan().to_arrow().to_pylist()


def main() -> int:
    cat = catalog()
    concepts = read(cat, "concept_measures")
    dims = read(cat, "measure_dimensions")
    units = {r["measure_key"]: r.get("unit_key") for r in read(cat, "dim_measure")}
    with open("warehouse/seeds/kpi.csv", encoding="utf-8", newline="") as handle:
        published = [r["kpi_key"] for r in csv.DictReader(handle) if r["status"] == "published"]
    by_measure: dict[str, list[dict]] = {}
    for row in dims:
        by_measure.setdefault(row["measure_key"], []).append(row)
    full: dict = {"kpis": {}, "breakdowns": {}}
    breakdowns: dict[str, set] = {}
    for kpi_key in published:
        result = {}
        for c in (c for c in concepts if c["concept_key"] == kpi_key):
            measure = c["measure_key"]
            entry = {"source": c["source_system"], "unit": units.get(measure), "dims": {}}
            for d in by_measure.get(measure, []):
                members = list(d["members"] or [])
                entry["dims"][d["dimension"]] = [d["member_count"], members[:6]]
                if d["dimension"] not in ("geography", "time"):
                    breakdowns.setdefault(d["dimension"], set()).update(members)
            result[measure] = entry
        annotate("notice", f"inventory: {kpi_key}", result)
        full["kpis"][kpi_key] = result
    annotate("notice", "inventory: breakdowns", {k: sorted(v)[:20] for k, v in sorted(breakdowns.items())})
    full["breakdowns"] = {k: sorted(v) for k, v in sorted(breakdowns.items())}
    out = Path(os.environ.get("DEV_OUTPUT_DIR", ".")) / "catalogue_inventory.json"
    out.write_text(json.dumps(full, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
