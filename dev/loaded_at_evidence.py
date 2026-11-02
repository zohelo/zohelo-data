"""Dev trial for #2320: coverage checks and Bronze commit times per source; writes nothing."""

from __future__ import annotations

import csv
from datetime import UTC, datetime

from pyiceberg.exceptions import NoSuchTableError
import yaml

from ingestion.common.r2 import catalog
from ingestion.common.report import annotate


def iso_ms(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def snapshot_time(cat, namespace: str, table: str) -> str | None:
    try:
        snapshot = cat.load_table((namespace, table)).current_snapshot()
    except NoSuchTableError:
        return None
    return iso_ms(snapshot.timestamp_ms) if snapshot is not None else None


def main() -> None:
    cat = catalog()
    coverage: dict[str, dict] = {}
    rows = cat.load_table(("gold", "coverage")).scan(selected_fields=("source", "status", "checked_at")).to_arrow()
    for row in rows.to_pylist():
        entry = coverage.setdefault(row["source"], {"checked_at": None, "rows": 0, "loaded": 0})
        entry["rows"] += 1
        entry["loaded"] += row["status"] == "loaded"
        stamp = row["checked_at"]
        if stamp is not None:
            stamp = stamp.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ") if stamp.tzinfo else stamp.isoformat() + "Z"
            entry["checked_at"] = max(filter(None, [entry["checked_at"], stamp]))
    with open("warehouse/models/bronze/sources.yml", encoding="utf-8") as f:
        sources = yaml.safe_load(f)["sources"]
    for source in sources:
        name = source["name"]
        times, missing = [], 0
        for table in source.get("tables") or []:
            stamp = snapshot_time(cat, "bronze", table["name"])
            if stamp is None:
                missing += 1
            else:
                times.append(stamp)
        cov = coverage.get(name, {})
        annotate(
            "notice",
            f"loaded_at: {name}",
            {
                "coverage_checked_at": cov.get("checked_at"),
                "coverage_rows": cov.get("rows", 0),
                "loaded_rows": cov.get("loaded", 0),
                "bronze_newest_snapshot": max(times) if times else None,
                "bronze_tables_found": len(times),
                "bronze_tables_missing": missing,
            },
        )
    with open("warehouse/seeds/kpi.csv", encoding="utf-8") as f:
        published = [r["kpi_key"] for r in csv.DictReader(f) if r["status"] == "published"]
    metrics = {f"gold.metrics_{k}": snapshot_time(cat, "gold", f"metrics_{k}") for k in published}
    annotate("notice", "loaded_at: gold metrics", metrics)


if __name__ == "__main__":
    main()
