"""Read-only: what fills __r2_data_catalog in the lakehouse bucket (file kinds per table)."""
from collections import Counter, defaultdict
import traceback

from ingestion.common.r2 import req, s3
from ingestion.common.report import annotate

try:
    client, bucket = s3(), req("R2_LAKEHOUSE_BUCKET")
    by_kind, by_table = Counter(), defaultdict(Counter)
    count, sample = Counter(), []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix="__r2_data_catalog/"):
        for o in page.get("Contents", []):
            key, size = o["Key"], o["Size"]
            name = key.rsplit("/", 1)[-1]
            kind = ("metadata.json" if name.endswith(".metadata.json") else "manifest-list" if name.startswith("snap-")
                    else "manifest" if name.endswith(".avro") else "stats" if name.endswith(".stats") or "puffin" in name
                    else name.rsplit(".", 1)[-1])
            by_kind[kind] += size
            count[kind] += 1
            parts = key.split("/")
            table = "/".join(parts[1:3])
            by_table[table][kind] += size
            if len(sample) < 6:
                sample.append(key)
    gb = lambda b: round(b / 1e9, 2)
    top = sorted(by_table.items(), key=lambda kv: -sum(kv[1].values()))[:12]
    annotate("notice", "catalog metadata by kind", {"gb": {k: gb(v) for k, v in by_kind.items()}, "files": dict(count), "sample": sample})
    annotate("notice", "catalog metadata top tables", {t: {k: gb(v) for k, v in c.items()} for t, c in top})
except Exception:
    annotate("error", "catalog metadata probe", {"traceback": traceback.format_exc()[-1500:]})
    raise
