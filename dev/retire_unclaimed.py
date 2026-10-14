"""Owner-approved (14 Oct 2026, #343): retire 10 unclaimed tables and 4 old NBP Landing
folders. Without --apply: report sizes only. With --apply: drop the tables (their data
files become orphans for the separate, owner-approved orphan clean-up) and delete the
Landing objects. Refuses when the Landing folders exceed 1 GB (scope larger than approved)."""
import sys
import traceback

from pyiceberg.exceptions import NoSuchTableError

from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate

TABLES = ["bronze.eurostat_observations", "gold.dim_commodity", "gold.dim_eurostat_dataset",
          "gold.dim_eurostat_geography", "gold.dim_eurostat_period", "gold.dim_source_table",
          "gold.fact_eurostat_observations", "gold.mart_eurostat_coverage",
          "silver.eurostat_observation_revisions", "silver.nbp_change_events"]
FOLDERS = ["01_landing/nbp_exchange_rates_table_a/", "01_landing/nbp_exchange_rates_table_b/",
           "01_landing/nbp_exchange_rates_table_c/", "01_landing/nbp_gold_prices/"]
MAX_BYTES = 1_000_000_000

try:
    apply = "--apply" in sys.argv[1:]
    cat, client, bucket = catalog(), s3(), req("R2_LANDING_BUCKET")
    tables = {}
    for name in TABLES:
        try:
            t = cat.load_table(tuple(name.split(".")))
            summary = t.current_snapshot().summary if t.current_snapshot() else {}
            tables[name] = {"rows": int(summary.get("total-records") or 0),
                            "gb": round(int(summary.get("total-files-size") or 0) / 1e9, 3)}
        except NoSuchTableError:
            tables[name] = "absent"
    folders, keys = {}, []
    for prefix in FOLDERS:
        n = size = 0
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            for o in page.get("Contents", []):
                n += 1
                size += o["Size"]
                keys.append(o["Key"])
        folders[prefix] = {"objects": n, "mb": round(size / 1e6, 1)}
    total = sum(v["mb"] for v in folders.values()) * 1e6
    report = {"apply": apply, "tables": tables, "folders": folders}
    if apply:
        if total > MAX_BYTES:
            raise RuntimeError(f"Landing folders hold {total / 1e9:.2f} GB, above the approved scope (1 GB)")
        dropped = []
        for name, info in tables.items():
            if info != "absent":
                cat.drop_table(tuple(name.split(".")))
                dropped.append(name)
        deleted = 0
        for start in range(0, len(keys), 1000):
            batch = keys[start:start + 1000]
            response = client.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in batch], "Quiet": True})
            if response.get("Errors"):
                raise RuntimeError(f"delete errors: {response['Errors'][:3]}")
            deleted += len(batch)
        report.update({"dropped_tables": dropped, "deleted_objects": deleted})
    annotate("notice", "retire unclaimed", report)
except Exception:
    annotate("error", "retire unclaimed", {"traceback": traceback.format_exc()[-1500:]})
    raise
