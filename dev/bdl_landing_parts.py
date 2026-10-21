"""Read-only: how the existing BDL website exports in Landing were split (parts per
subgroup version, receipt fields), and which unit code lengths Silver holds."""

from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import json

import duckdb

from ingestion.common.r2 import get_with_backoff, req, s3
from ingestion.common.report import annotate
from ingestion.to_landing.gus_bdl.inspect import DATA

client, bucket = s3(), req("R2_LANDING_BUCKET")
keys = [
    o["Key"]
    for p in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=DATA)
    for o in p.get("Contents", [])
    if o["Key"].endswith(".receipt.json")
]
with ThreadPoolExecutor(max_workers=16) as pool:
    receipts = list(pool.map(lambda k: json.loads(get_with_backoff(client, bucket, k)), keys))
totals = {}
fields = Counter()
for r in receipts:
    totals[(r["subgroup_id"], r["version_token"])] = int((r.get("part") or {}).get("total", 1))
    fields.update(r.keys())
dist = Counter(min(t, 999) for t in totals.values())
buckets = Counter(
    "1" if t == 1 else "2-5" if t <= 5 else "6-20" if t <= 20 else "21-100" if t <= 100 else ">100"
    for t in totals.values()
)
multi = [r for r in receipts if int((r.get("part") or {}).get("total", 1)) > 1][:2]
summary = {
    "receipts": len(receipts),
    "versions": len(totals),
    "parts_per_version": dict(buckets),
    "max_parts": max(totals.values()),
    "fields": dict(fields),
    "multi_part_examples": multi,
}
try:
    from pyiceberg.catalog import load_catalog

    cat = load_catalog(
        "r2", type="rest", uri=req("R2_CATALOG_URI"), warehouse=req("R2_WAREHOUSE"), token=req("R2_DATA_CATALOG_TOKEN")
    )
    files = [f for f in cat.load_table(("silver", "bdl_observations")).inspect.files().to_pylist()][:0]
    con = duckdb.connect()
    t = cat.load_table(("silver", "bdl_observations"))
    arrow = t.scan(selected_fields=("unit_code", "unit_name"), limit=30_000_000).to_arrow()
    con.register("u", arrow)
    summary["silver_units"] = con.execute(
        "select length(unit_code) l, count(distinct unit_code) codes, count(distinct unit_code||unit_name) units "
        "from u group by 1 order by 1"
    ).fetchall()
except Exception as exc:  # the Landing summary is the main result
    summary["silver_error"] = str(exc)[:300]
annotate("notice", "bdl landing parts", summary)
