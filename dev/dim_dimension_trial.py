"""Dev trial for #2080: build Gold and the seeds into zz_dev_dim_dimension; --drop removes that namespace."""

from __future__ import annotations

from pathlib import Path
import sys

from ingestion.common.r2 import catalog
from ingestion.common.report import annotate
from warehouse import build

NAMESPACE = "zz_dev_dim_dimension"
SEEDS = sorted(p.stem for p in Path("warehouse/seeds").glob("*.csv"))


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
    code = build.main(select=" ".join(["gold", *SEEDS]), target="prod", variables={"gold_schema": NAMESPACE})
    if code != 0:
        annotate("error", "dim_dimension: build failed", {"build_exit": code})
        return 1
    rows = cat.load_table((NAMESPACE, "dim_dimension")).scan().to_arrow().to_pylist()
    by_kind: dict[str, list[str]] = {}
    for r in rows:
        by_kind.setdefault(r["kind"], []).append(f"{r['dimension_key']}={r['measure_count']}")
    counts = {r["dimension_key"]: r["measure_count"] for r in rows}
    ok = len(by_kind.get("conformed", [])) == 5 and counts.get("geography", 0) > 0 and counts.get("time", 0) > 0
    annotate("notice" if ok else "error", "dim_dimension", {"rows": len(rows), "keys_per_kind": by_kind})
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
