"""Dev trial for #1550: build the frequency seed into zz_dev_seeds; --drop removes that namespace."""

from __future__ import annotations

import sys

from ingestion.common.r2 import catalog
from ingestion.common.report import annotate
from warehouse import build

NAMESPACE = "zz_dev_seeds"


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
    code = build.main(select="frequency", target="prod", variables={"gold_schema": NAMESPACE})
    rows = cat.load_table((NAMESPACE, "frequency")).scan().to_arrow().num_rows if code == 0 else None
    annotate("notice" if rows == 7 else "error", f"seed frequency: {rows} rows", {"build_exit": code})
    return 0 if rows == 7 else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
