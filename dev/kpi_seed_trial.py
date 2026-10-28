"""Dev trial for #1552: build unit, kpi, kpi_measure and poc_dim_concept into zz_dev_kpi and compare
dim_concept's row count with production poc.dim_concept; --drop removes zz_dev_kpi."""

from __future__ import annotations

import sys

from ingestion.common.r2 import catalog
from ingestion.common.report import annotate
from warehouse import build
from warehouse import catalog as data_catalog

NAMESPACE = "zz_dev_kpi"


def main(argv: list[str]) -> int:
    cat = catalog()
    if "--drop" in argv:
        dropped = []
        if (NAMESPACE,) in [tuple(n) for n in cat.list_namespaces()]:
            for ident in cat.list_tables(NAMESPACE):
                cat.purge_table(ident)
                dropped.append(".".join(ident))
            cat.drop_namespace(NAMESPACE)
        annotate("notice", f"dropped {NAMESPACE}", {"tables": dropped})
        return 0
    # A trial must not republish the production data catalogue from a branch.
    data_catalog.publish = lambda: "skipped (dev trial)"
    code = build.main(
        select="unit kpi kpi_measure poc_dim_concept",
        target="prod",
        variables={"gold_schema": NAMESPACE, "poc_schema": NAMESPACE},
    )
    trial = cat.load_table((NAMESPACE, "dim_concept")).scan().to_arrow().num_rows if code == 0 else None
    prod = cat.load_table(("poc", "dim_concept")).scan().to_arrow().num_rows
    counts = (
        {t: cat.load_table((NAMESPACE, t)).scan().to_arrow().num_rows for t in ("unit", "kpi", "kpi_measure")}
        if code == 0
        else {}
    )
    ok = code == 0 and trial == prod
    annotate(
        "notice" if ok else "error", f"dim_concept rows: {trial} trial = {prod} prod", {"build_exit": code, **counts}
    )
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
