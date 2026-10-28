"""Dev trial for #2031: build gold.dim_dataset into zz_dev_dim_dataset; --drop removes that namespace."""

from __future__ import annotations

import sys
from collections import Counter

from ingestion.common.r2 import catalog
from ingestion.common.report import annotate
from warehouse import build

NAMESPACE = "zz_dev_dim_dataset"


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
    code = build.main(select="dim_dataset", target="prod", variables={"gold_schema": NAMESPACE})
    if code != 0:
        annotate("error", "dim_dataset: build failed", {"build_exit": code})
        return 1
    table = cat.load_table((NAMESPACE, "dim_dataset")).scan().to_arrow()
    per_source = dict(sorted(Counter(table.column("source_key").to_pylist()).items()))
    annotate("notice", f"dim_dataset: {table.num_rows} rows", {"build_exit": code, "per_source": per_source})
    return 0 if table.num_rows <= 40000 else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
