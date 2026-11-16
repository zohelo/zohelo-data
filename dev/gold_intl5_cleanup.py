"""Dev cleanup (scratch only): drop what is left in the scratch namespace zz_dev_gold_intl5 of the trial
gold_intl5_trial (#2943): every table listed there, the namespace, and objects under the prefix. Touches
nothing else. Deleted before merge.

python -m dev.gold_intl5_cleanup
"""

from __future__ import annotations

import contextlib
import time

from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate

NS = "zz_dev_gold_intl5"


def main() -> None:
    cat, client = catalog(), s3()
    out = {"namespace_before": (NS,) in cat.list_namespaces(), "tables_before": [], "dropped": [], "errors": []}
    if out["namespace_before"]:
        out["tables_before"] = [t[1] for t in cat.list_tables(NS)]
        for name in out["tables_before"]:
            try:
                cat.drop_table((NS, name))
                out["dropped"].append(name)
            except Exception as exc:  # noqa: BLE001 - reported
                out["errors"].append(f"{name}: {exc}"[:200])
        for _ in range(5):
            try:
                cat.drop_namespace(NS)
                break
            except Exception as exc:  # noqa: BLE001 - reported, retried
                out["errors"].append(f"namespace: {exc}"[:200])
                time.sleep(10)
    bucket = req("STORAGE_LAKEHOUSE_BUCKET_NAME")
    removed = 0
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=NS + "/"):
        keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if keys:
            with contextlib.suppress(Exception):
                client.delete_objects(Bucket=bucket, Delete={"Objects": keys, "Quiet": True})
                removed += len(keys)
    out["objects_removed"] = removed
    out["namespace_left"] = (NS,) in cat.list_namespaces()
    out["objects_left"] = sum(
        len(p.get("Contents", []))
        for p in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=NS + "/")
    )
    annotate("notice", "intl5 cleanup", out)


if __name__ == "__main__":
    main()
