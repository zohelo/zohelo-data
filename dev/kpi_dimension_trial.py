"""Dev trial for #2033: build Gold and the seeds into zz_dev_kpi_dimension; --drop removes that namespace."""

from __future__ import annotations

import csv
from pathlib import Path
import sys

from ingestion.common.r2 import catalog
from ingestion.common.report import annotate
from warehouse import build

NAMESPACE = "zz_dev_kpi_dimension"
SEEDS = Path("warehouse/seeds")


def main(argv: list[str]) -> int:
    cat = catalog()
    if "--drop" in argv:
        dropped = []
        if (NAMESPACE,) in [tuple(n) for n in cat.list_namespaces()]:
            for ident in cat.list_tables(NAMESPACE):
                cat.purge_table(ident)
                dropped.append(".".join(ident))
            cat.drop_namespace(NAMESPACE)
        annotate("notice", f"dropped {NAMESPACE}", {"tables": len(dropped)})
        return 0
    seeds = sorted(p.stem for p in SEEDS.glob("*.csv"))
    code = build.main(select=" ".join(["gold", *seeds]), target="prod", variables={"gold_schema": NAMESPACE})
    if code != 0:
        annotate("error", "kpi_dimension: build failed", {"build_exit": code})
        return 1
    table = cat.load_table((NAMESPACE, "kpi_dimension")).scan().to_arrow()
    published = [r["kpi_key"] for r in csv.DictReader((SEEDS / "kpi.csv").open()) if r["status"] == "published"]
    dims: dict[str, set[str]] = {k: set() for k in published}
    for kpi, dim in zip(table.column("kpi_key").to_pylist(), table.column("dimension_key").to_pylist(), strict=True):
        if kpi in dims:
            dims[kpi].add(dim)
    ok = all({"geography", "time"} <= d for d in dims.values()) and table.num_rows <= 10000
    annotate(
        "notice" if ok else "error",
        f"kpi_dimension: {table.num_rows} rows",
        {"published": {k: sorted(v) for k, v in dims.items()}, "build_exit": code},
    )
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
