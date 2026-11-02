"""Dev trial for #2318: build the release_calendar seed (with its parents) into zz_dev_release_calendar; drop it."""

from __future__ import annotations

import os

from ingestion.common.r2 import catalog
from ingestion.common.report import annotate
from warehouse import build

NAMESPACE = "zz_dev_release_calendar"


def drop(cat) -> list[str]:
    dropped = []
    if (NAMESPACE,) in [tuple(n) for n in cat.list_namespaces()]:
        for ident in cat.list_tables(NAMESPACE):
            cat.purge_table(ident)
            dropped.append(".".join(ident))
        cat.drop_namespace(NAMESPACE)
    return dropped


def main() -> int:
    cat = catalog()
    try:
        os.environ["DBT_INDIRECT_SELECTION"] = "cautious"  # skip tests of unselected tables (kpi_dimension)
        select = "release_calendar source kpi licence"
        code = build.main(select=select, target="prod", variables={"gold_schema": NAMESPACE})
        rows = arrow_type = None
        if code == 0:
            table = cat.load_table((NAMESPACE, "release_calendar")).scan().to_arrow()
            rows, arrow_type = table.num_rows, str(table.schema.field("release_at").type)
        ok = code == 0 and rows == 55
        annotate(
            "notice" if ok else "error",
            "release_calendar",
            {"build_exit": code, "rows": rows, "release_at": arrow_type},
        )
        return 0 if ok else 1
    finally:
        annotate("notice", f"dropped {NAMESPACE}", {"tables": drop(cat)})


if __name__ == "__main__":
    raise SystemExit(main())
