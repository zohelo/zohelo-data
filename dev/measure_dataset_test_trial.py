"""Dev trial for #2217: build Gold and the seeds into zz_dev_measure_dataset_test and report the
dim_measure dataset_key relationships test; --drop removes that namespace."""

from __future__ import annotations

import json
from pathlib import Path
import sys

from ingestion.common.r2 import catalog
from ingestion.common.report import annotate
from warehouse import build

NAMESPACE = "zz_dev_measure_dataset_test"
TEST_PREFIX = "test.zohelo.relationships_poc_dim_measure_dataset_key"


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
    results = json.loads(Path("warehouse/target/run_results.json").read_text())["results"]
    found = [[r["unique_id"], r["status"], r.get("failures")] for r in results if r["unique_id"].startswith(TEST_PREFIX)]
    ok = code == 0 and len(found) == 1 and found[0][1] == "pass"
    annotate("notice" if ok else "error", "dim_measure dataset_key in dim_dataset", {"build_exit": code, "tests": found})
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
