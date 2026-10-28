"""Dev trial for #2150: build Gold plus every seed into zz_dev_sex_axis; --drop removes that namespace."""

from __future__ import annotations

from pathlib import Path
import sys

from ingestion.common.r2 import catalog
from ingestion.common.report import annotate
from warehouse import build

NAMESPACE = "zz_dev_sex_axis"


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
    seeds = sorted(p.stem for p in Path("warehouse/seeds").glob("*.csv"))
    code = build.main(select=" ".join(["gold", *seeds]), target="prod", variables={"gold_schema": NAMESPACE})
    result: dict = {"build_exit": code}
    try:
        dims = cat.load_table((NAMESPACE, "dim_dimension")).scan().to_arrow().to_pylist()
        result["source_specific"] = sorted(d["dimension_key"] for d in dims if d.get("kind") == "source_specific")
        rows = cat.load_table((NAMESPACE, "measure_dimensions")).scan().to_arrow().to_pylist()
        result["sex_members"] = sorted({m for r in rows if r["dimension"] == "sex" for m in (r["members"] or [])})
        result["has_plec"] = any(d["dimension_key"] == "Płeć" for d in dims)
    except Exception as exc:  # report what could be read
        result["read_error"] = repr(exc)[:300]
    annotate("notice", "sex axis", result)
    return 0 if code == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
