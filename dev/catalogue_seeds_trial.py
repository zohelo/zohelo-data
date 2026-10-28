"""Dev trial for #2030: build licence, source and poc_dim_source into zz_dev_licence_source; --drop removes it."""

from __future__ import annotations

import sys

from ingestion.common.r2 import catalog
from ingestion.common.report import annotate
from warehouse import build

NAMESPACE = "zz_dev_licence_source"
EXPECTED = {"licence": 8, "source": 10, "dim_source": 10}


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
    code = build.main(select="licence source poc_dim_source", target="prod", variables={"gold_schema": NAMESPACE})
    rows = {}
    if code == 0:
        for table in EXPECTED:
            rows[table] = cat.load_table((NAMESPACE, table)).scan().to_arrow().num_rows
    ok = code == 0 and rows == EXPECTED
    annotate(
        "notice" if ok else "error",
        f"rows: licence {rows.get('licence')}, source {rows.get('source')}, dim_source {rows.get('dim_source')}",
        {"build_exit": code},
    )
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
