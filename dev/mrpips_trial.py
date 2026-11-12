"""Dev trial of mrpips_unemployment: Landing and Bronze into the scratch locations zz_dev_w2pl_mrpips/ (both buckets)
and namespace zz_dev_w2pl_mrpips, twice each (idempotency), then the Silver SQL applied to the scratch Bronze table
in DuckDB; findings printed to the log. Everything written is removed at the end (finally). Deleted before merge."""

from __future__ import annotations

from pathlib import Path
import re
import sys

import duckdb

from ingestion.common import bronze_files as bf
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze.mrpips_unemployment import load
from ingestion.to_landing import mrpips_unemployment as landing

ROOT = Path(__file__).resolve().parents[1]
SCRATCH = "zz_dev_w2pl_mrpips"


def show(con, title, sql, limit=200):
    print(f"\n=== {title}", flush=True)
    rel = con.sql(sql)
    print("\t".join(d[0] for d in rel.description))
    rows = rel.fetchall()
    for r in rows[:limit]:
        print("\t".join("" if v is None else str(v)[:110] for v in r))
    if len(rows) > limit:
        print(f"... {len(rows)} rows")


def main(argv) -> int:
    first = int(argv[0]) if argv else landing.FIRST_YEAR
    landing.FIRST_YEAR = first
    client = s3()
    cat = bf.retry(catalog, "catalog")
    try:
        for attempt in (1, 2):
            print(f"\n##### landing extract {attempt}", flush=True)
            landing.extract(120, landing_root=f"{SCRATCH}/01_landing")
        for attempt in (1, 2):
            print(f"\n##### bronze load {attempt}", flush=True)
            load.load(0, landing_root=f"{SCRATCH}/01_landing", namespace=SCRATCH)
        cells = cat.load_table((SCRATCH, "mrpips_unemployment_cells")).scan().to_arrow()
        try:
            rejected = cat.load_table((SCRATCH, "mrpips_unemployment_rejected_files")).scan().to_arrow().to_pylist()
        except Exception:  # no rejections table when nothing was rejected
            rejected = []
        for r in rejected:
            print("REJECTED", r.get("entity"), r.get("error"))
        sql = (ROOT / "warehouse/models/silver/mrpips_unemployment/mrpips_registered_unemployment.sql").read_text()
        sql = re.sub(r"\{#.*?#\}", "", sql, flags=re.S)
        sql = sql.replace("{{ source('mrpips_unemployment', 'mrpips_unemployment_cells') }}", "cells")
        con = duckdb.connect()
        con.register("cells", cells)
        counts = []
        for attempt in (1, 2):  # Silver twice: a rebuild gives the same rows
            con.sql("create or replace table silver as " + sql)
            counts.append(con.sql("select count(*), sum(hash(period_date, area_code, measure_code, sex_code, value)) from silver").fetchall()[0])
        n = lambda q: con.sql(q).fetchall()[0][0]  # noqa: E731
        stats = {
            "head": __import__("os").environ.get("GITHUB_SHA", "")[:7],
            "landing_months": len(landing.held_entities(f"{SCRATCH}/01_landing")),
            "bronze_rows": cells.num_rows, "bronze_files": n("select count(distinct raw_sha256) from cells"),
            "rejected": len(rejected),
            "silver_rows": counts[0][0], "silver_rebuild_identical": counts[0] == counts[1],
            "silver_dup": n("select count(*) from (select 1 from silver group by period_date, area_code, measure_code, sex_code having count(*) > 1)"),
            "silver_null_keys": n("select count(*) from silver where period_date is null or area_code is null or measure_code is null or sex_code is null or raw_sha256 is null"),
            "range": n("select min(period_date) || '..' || max(period_date) from silver"),
            "tab2_from": n("select min(period_date) from silver where measure_code = 'unemployed_persons'"),
            "pl_2026_09": n("select string_agg(measure_code || ':' || sex_code || '=' || value, '; ' order by measure_code, sex_code) from silver where area_code = '0000' and period_date = date '2026-09-01'"),
            "cells_per_month": n("select round(avg(c)) from (select count(*) c from cells group by entity)"),
        }
        print("STATS", stats)
        annotate("notice", "mrpips_unemployment trial", stats)
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
            cat.drop_namespace(SCRATCH)
        except Exception as exc:  # reported, not fatal
            print("drop failed", exc)
        annotate("notice", "mrpips trial cleanup", {"objects_removed": removed, "tables_dropped": dropped})
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
