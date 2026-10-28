"""Dev trial for #2083: build gold plus the seeds into zz_dev_sex; --drop removes that namespace."""

from __future__ import annotations

from collections import Counter
from pathlib import Path
import sys

from ingestion.common.r2 import catalog
from ingestion.common.report import annotate
from warehouse import build

NAMESPACE = "zz_dev_sex"


def main(argv: list[str]) -> int:
    cat = catalog()
    if "--drop" in argv:
        dropped, errors = [], []
        if (NAMESPACE,) in [tuple(n) for n in cat.list_namespaces()]:
            for ident in cat.list_tables(NAMESPACE):
                try:
                    cat.purge_table(ident)
                    dropped.append(".".join(ident))
                except Exception as exc:
                    errors.append(f"{'.'.join(ident)}: {exc}"[:200])
            if not errors:
                cat.drop_namespace(NAMESPACE)
        annotate("error" if errors else "notice", f"dropped {NAMESPACE}", {"tables": dropped, "errors": errors})
        return 1 if errors else 0
    seeds = sorted(p.stem for p in Path("warehouse/seeds").glob("*.csv"))
    code = build.main(select=" ".join(["gold", *seeds]), target="prod", variables={"gold_schema": NAMESPACE})
    counts = {}
    if code == 0:
        table = cat.load_table((NAMESPACE, "dim_series")).scan(selected_fields=("sex_key",)).to_arrow()
        counts = dict(Counter(table.column("sex_key").to_pylist()))
    ok = code == 0 and bool(counts) and set(counts) <= {"_T", "F", "M", "_Z"}
    annotate("notice" if ok else "error", "dim_series sex_key", {"build_exit": code, "counts": counts})
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
