"""Dev trial for #2319: dataset names per source in gold.dim_dataset and their Silver name matches; writes nothing."""

from __future__ import annotations

from collections import defaultdict

from ingestion.common.r2 import catalog
from ingestion.common.report import annotate


def rows(cat, namespace: str, table: str, fields: tuple[str, ...]) -> list[dict]:
    return cat.load_table((namespace, table)).scan(selected_fields=fields).to_arrow().to_pylist()


def main() -> None:
    cat = catalog()
    datasets = rows(cat, "gold", "dim_dataset", ("dataset_key", "source_key", "code", "name"))
    flows = {
        r["dataset_id"].upper(): r["name_en"]
        for r in rows(cat, "silver", "eurostat_dataflows", ("dataset_id", "name_en"))
    }
    wdi = {r["indicator_code"] for r in rows(cat, "silver", "wdi_indicators", ("indicator_code", "indicator_name"))}
    by_source: dict[str, list[dict]] = defaultdict(list)
    for row in datasets:
        by_source[row["source_key"]].append(row)
    for source, items in sorted(by_source.items()):
        items.sort(key=lambda r: str(r["code"]))
        data = {
            "rows": len(items),
            "empty_name": sum(1 for r in items if not (r["name"] or "").strip()),
            "first": [(r["code"], r["name"]) for r in items[:5]],
        }
        if source == "eurostat":
            matched = [r for r in items if r["code"] in flows]
            differ = [r for r in matched if r["name"] != flows[r["code"]]]
            data["dataflow_matches"] = len(matched)
            data["name_differs"] = len(differ)
            data["examples"] = [(r["code"], r["name"], flows[r["code"]]) for r in differ[:3]]
        if source == "world_bank_wdi":
            data["indicator_matches"] = sum(1 for r in items if r["code"] in wdi)
        annotate("notice", f"dataset names: {source}", data)


if __name__ == "__main__":
    main()
