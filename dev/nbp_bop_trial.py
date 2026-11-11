"""Dev trial of nbp_bop: Landing (bulk_file) and Bronze into the scratch locations zz_dev_nbp_bop/ (both buckets)
and namespace zz_dev_nbp_bop, twice each (idempotency), then the Silver SQL applied to the scratch Bronze table
in DuckDB; findings printed to the log. Everything written is removed at the end (finally). Deleted before merge."""

from __future__ import annotations

from pathlib import Path
import re

import duckdb

from ingestion.common import bronze_files as bf
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze.nbp_bop import load
from ingestion.to_landing.patterns import bulk_file

ROOT = Path(__file__).resolve().parents[1]
SCRATCH = "zz_dev_nbp_bop"


def show(con, title, sql, limit=400):
    print(f"\n=== {title}", flush=True)
    rel = con.sql(sql)
    print("\t".join(d[0] for d in rel.description))
    rows = rel.fetchall()
    for r in rows[:limit]:
        print("\t".join("" if v is None else str(v)[:110] for v in r))
    if len(rows) > limit:
        print(f"... {len(rows)} rows")


def main() -> int:
    client = s3()
    cat = bf.retry(catalog, "catalog")
    try:
        for attempt in (1, 2):
            print(f"\n##### landing load {attempt}", flush=True)
            bulk_file.main("nbp_bop", "load", landing_root=f"{SCRATCH}/01_landing")
        for attempt in (1, 2):
            print(f"\n##### bronze load {attempt}", flush=True)
            load.load(0, landing_root=f"{SCRATCH}/01_landing", namespace=SCRATCH)
        cells = cat.load_table((SCRATCH, "nbp_bop_cells")).scan().to_arrow()
        try:
            rejected = cat.load_table((SCRATCH, "nbp_bop_rejected_files")).scan().to_arrow().to_pylist()
        except Exception:  # no rejections table when nothing was rejected
            rejected = []
        print("REJECTED", rejected)
        sql = (ROOT / "warehouse/models/silver/nbp_bop/nbp_bop_observations.sql").read_text()
        sql = re.sub(r"\{#.*?#\}", "", sql, flags=re.S).replace("{{ source('nbp_bop', 'nbp_bop_cells') }}", "cells")
        con = duckdb.connect()
        con.register("cells", cells)
        con.sql(f"create table silver as {sql}")
        show(con, "bronze per entity and sheet", "select entity, sheet, count(*) n, count(distinct source_row_number) "
             "item_rows, min(year_text), max(year_text), count(distinct source_version) versions from cells group by all "
             "order by all")
        show(con, "period texts", "select entity, period_text, count(*) from cells group by all order by all", 100)
        show(con, "silver per entity and currency", "select entity, currency, frequency, count(*) n, "
             "count(distinct item_code) items, min(date_key), max(date_key), count(*) filter (where value is null) "
             "nulls from silver group by all order by all")
        show(con, "grain duplicates", "select entity, currency, item_code, date_key, count(*) from silver group by all "
             "having count(*) > 1 limit 20")
        show(con, "items (EUR) with the newest value", "select entity, item_code, item_en, max(date_key) last, "
             "arg_max(value, date_key) value from silver where currency = 'EUR' group by all order by entity, "
             "min(source_row_number)", 500)
        annotate("notice", "nbp_bop trial", {"bronze_rows": cells.num_rows,
                                              "silver_rows": con.sql("select count(*) from silver").fetchall()[0][0],
                                              "rejected": len(rejected)})
    finally:
        removed = 0
        for bucket in (req("STORAGE_LANDING_BUCKET_NAME"), req("STORAGE_LAKEHOUSE_BUCKET_NAME")):
            for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{SCRATCH}/"):
                for obj in page.get("Contents", []):
                    client.delete_object(Bucket=bucket, Key=obj["Key"])
                    removed += 1
        dropped = []
        try:
            for ident in cat.list_tables(SCRATCH):
                cat.drop_table(ident)
                dropped.append(ident[-1])
        except Exception as exc:  # reported, not fatal
            print("drop failed", exc)
        annotate("notice", "nbp_bop trial cleanup", {"objects_removed": removed, "tables_dropped": dropped})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
