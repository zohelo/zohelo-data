"""Read-only check for #2179: which objects the portal index would show per layer (no writes)."""

from collections import Counter
import os

from ingestion.common.r2 import s3
from ingestion.common.report import annotate
from portal.publish_index import catalog_folders, display_key


def main() -> int:
    lakehouse = os.environ["STORAGE_LAKEHOUSE_BUCKET_NAME"]
    folders = catalog_folders()
    tables = Counter(f.split("/")[0] for f in folders.values())
    shown, hidden, gold = Counter(), 0, set()
    for page in s3().get_paginator("list_objects_v2").paginate(Bucket=lakehouse):
        for item in page.get("Contents", []):
            key = display_key(lakehouse, lakehouse, item["Key"], folders)
            if key is None:
                hidden += 1
                continue
            shown[key.split("/")[0]] += 1
            if key.startswith("04_gold/"):
                gold.add(key.split("/")[1])
    data = {
        "catalog_tables": dict(tables),
        "shown": dict(shown),
        "hidden": hidden,
        "gold_tables": len(gold),
        "gold": ",".join(sorted(gold))[:2500],
    }
    print(data)
    annotate("notice", "portal index check", data)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
