"""Remove the scratch namespace zz_dev_gold3204b left by the #3204 PR B proof (Dev 45758599361): list what is in it,
drop every table and the namespace, and report. Usage: python -m dev.scratch_cleanup"""

from __future__ import annotations

import sys
import time

from ingestion.common.r2 import catalog
from ingestion.common.report import annotate

NAMESPACE = "zz_dev_gold3204b"


def main() -> int:
    cat = catalog()
    out: dict = {"namespace": NAMESPACE}
    try:
        out["namespaces"] = [".".join(n) for n in cat.list_namespaces() if n[0].startswith("zz_dev_gold3204")]
    except Exception as exc:
        out["namespaces"] = f"{type(exc).__name__}: {exc}"[:300]
    for attempt in range(3):
        try:
            tables = cat.list_tables(NAMESPACE)
        except Exception as exc:
            out[f"list_{attempt}"] = f"{type(exc).__name__}: {exc}"[:300]
            break
        out[f"tables_{attempt}"] = [".".join(t) for t in tables]
        for ident in tables:
            try:
                cat.drop_table(ident)
            except Exception as exc:
                out.setdefault("drop_errors", []).append(f"{'.'.join(ident)}: {type(exc).__name__}: {exc}"[:300])
        if not tables:
            break
        time.sleep(5)
    try:
        cat.drop_namespace(NAMESPACE)
        out["namespace_dropped"] = True
    except Exception as exc:
        out["namespace_drop"] = f"{type(exc).__name__}: {exc}"[:400]
    try:
        cat.load_namespace_properties(NAMESPACE)
        out["still_exists"] = True
    except Exception:
        out["still_exists"] = False
    annotate("notice" if not out["still_exists"] else "error", "scratch cleanup", out)
    return 1 if out["still_exists"] else 0


if __name__ == "__main__":
    sys.exit(main())
