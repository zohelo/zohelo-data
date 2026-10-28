"""Dev trial for #2079: build Gold and the seeds into zz_dev_measure_dataset_key and check
dim_measure.dataset_key; --drop removes that namespace."""

from __future__ import annotations

from pathlib import Path
import sys

from ingestion.common.r2 import catalog
from ingestion.common.report import annotate
from warehouse import build

NAMESPACE = "zz_dev_measure_dataset_key"


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
                    errors.append(f"{'.'.join(ident)}: {type(exc).__name__}: {exc}"[:200])
            if not errors:
                cat.drop_namespace(NAMESPACE)
        annotate("error" if errors else "notice", f"dropped {NAMESPACE}", {"tables": dropped, "errors": errors})
        return 1 if errors else 0
    seeds = sorted(p.stem for p in Path("warehouse/seeds").glob("*.csv"))
    select = " ".join(["gold", *seeds])
    code = build.main(select=select, target="prod", variables={"gold_schema": NAMESPACE})
    if code != 0:
        annotate("error", "dim_measure dataset_key", {"build_exit": code})
        return 1
    t = cat.load_table((NAMESPACE, "dim_measure")).scan().to_arrow().to_pylist()
    nulls = sum(1 for r in t if r["dataset_key"] is None)
    per_source: dict[str, set] = {}
    for r in t:
        per_source.setdefault(r["source_system"], set()).add(r["dataset_key"])
    ordered = sorted(t, key=lambda r: r["measure_key"])
    first_prefix = next((r for r in ordered if r["measure_key"].startswith("eurostat:UNE_RT_A")), None)
    block = next((r for r in ordered if r["source_system"] == "eurostat" and r["block_id"] == "UNE_RT_A"), None)
    sample = block["dataset_key"] if block else None
    ok = nulls == 0 and sample == "eurostat:UNE_RT_A"
    annotate(
        "notice" if ok else "error",
        "dim_measure dataset_key",
        {
            "build_exit": code,
            "rows": len(t),
            "null_dataset_key": nulls,
            "distinct_dataset_key": {k: len(v) for k, v in sorted(per_source.items())},
            "une_rt_a_sample": sample,
            "une_rt_a_measure_key": block["measure_key"] if block else None,
            "first_prefix_match": [first_prefix["measure_key"], first_prefix["dataset_key"]] if first_prefix else None,
        },
    )
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
