"""Dev trial for #1596: build the folded Gold v1 (models/gold) into zz_dev_gold_v1 from real Silver and
annotate the model and test counts; --drop drops every table of zz_dev_gold_v1 and the namespace."""

from __future__ import annotations

import json
import sys

from ingestion.common.r2 import catalog
from ingestion.common.report import annotate
from warehouse import build
from warehouse import catalog as data_catalog

NAMESPACE = "zz_dev_gold_v1"


def main(argv: list[str]) -> int:
    cat = catalog()
    if "--drop" in argv:
        dropped = []
        if (NAMESPACE,) in [tuple(n) for n in cat.list_namespaces()]:
            for ident in cat.list_tables(NAMESPACE):
                cat.purge_table(ident)
                dropped.append(".".join(ident))
            cat.drop_namespace(NAMESPACE)
        annotate("notice", f"dropped {NAMESPACE}", {"tables": len(dropped)})
        return 0
    # A trial must not republish the production data catalogue from a branch.
    data_catalog.publish = lambda: "skipped (dev trial)"
    code = build.main(select="gold", target="prod", variables={"gold_schema": NAMESPACE})
    results = json.loads((build.PROJECT / "target" / "run_results.json").read_text())["results"]
    models = [r for r in results if r["unique_id"].startswith("model.")]
    tests = [r for r in results if r["unique_id"].startswith("test.")]
    models_ok = sum(r["status"] == "success" for r in models)
    tests_ok = all(r["status"] == "pass" for r in tests)
    failed = [r["unique_id"] for r in models + tests if r["status"] not in ("success", "pass")]
    level = "notice" if code == 0 and not failed else "error"
    annotate(
        level,
        f"models ok {models_ok}, tests {'ok' if tests_ok else 'failed'}",
        {"build_exit": code, "models": len(models), "tests": len(tests), "not ok": failed[:30]},
    )
    return 0 if level == "notice" else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
