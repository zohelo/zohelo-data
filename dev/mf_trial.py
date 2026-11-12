"""Dev trial of mf_jst_budgets: Landing and Bronze into zz_dev_w2pl_mf/ (both buckets) and namespace zz_dev_w2pl_mf,
twice each, then the workbook layouts printed (and the Silver SQL applied when it exists). Everything written is
removed at the end (finally). Deleted before merge."""

from __future__ import annotations

from pathlib import Path
import re
import sys

import duckdb

from ingestion.common import bronze_files as bf
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze.mf_jst_budgets import load
from ingestion.to_landing import mf_jst_budgets as landing

ROOT = Path(__file__).resolve().parents[1]
SCRATCH = "zz_dev_w2pl_mf"


def show(con, title, sql, limit=200):
    print(f"\n=== {title}", flush=True)
    rel = con.sql(sql)
    print("\t".join(d[0] for d in rel.description))
    rows = rel.fetchall()
    for r in rows[:limit]:
        print("\t".join("" if v is None else str(v)[:70] for v in r))
    if len(rows) > limit:
        print(f"... {len(rows)} rows")


def main(argv) -> int:
    if argv:
        landing.FIRST_YEAR = int(argv[0])
    client = s3()
    cat = bf.retry(catalog, "catalog")
    try:
        for attempt in (1, 2):
            print(f"\n##### landing extract {attempt}", flush=True)
            landing.extract(120, landing_root=f"{SCRATCH}/01_landing")
        for attempt in (1, 2):
            print(f"\n##### bronze load {attempt}", flush=True)
            load.load(0, landing_root=f"{SCRATCH}/01_landing", namespace=SCRATCH)
        cells = cat.load_table((SCRATCH, "mf_jst_budgets_cells")).scan().to_arrow()
        try:
            rejected = cat.load_table((SCRATCH, "mf_jst_budgets_rejected_files")).scan().to_arrow().to_pylist()
        except Exception:
            rejected = []
        for r in rejected:
            print("REJECTED", r.get("entity"), r.get("error"))
        con = duckdb.connect()
        con.register("cells", cells)
        sql = re.sub(r"\{#.*?#\}", "", (ROOT / "warehouse/models/silver/mf_jst_budgets/mf_jst_budget_summary.sql").read_text(), flags=re.S)
        con.sql("create table silver as " + sql.replace("{{ source('mf_jst_budgets', 'mf_jst_budgets_cells') }}", "cells"))
        gsql = re.sub(r"\{#.*?#\}|\{\{ config\(.*?\) \}\}", "", (ROOT / "warehouse/models/gold/poc_build_mf_jst_budgets.sql").read_text(), flags=re.S)
        con.sql("create table gold as " + gsql.replace("{{ ref('mf_jst_budget_summary') }}", "silver"))
        n = lambda q: con.sql(q).fetchall()[0][0]  # noqa: E731
        stats = {
            "silver_rows": n("select count(*) from silver"),
            "silver_dup": n("select count(*) from (select 1 from silver group by unit_type, period_date, section, item_label, item_occurrence, measure_code having count(*) > 1)"),
            "silver_range": n("select min(period_date) || '..' || max(period_date) from silver"),
            "gold_rows": n("select count(*) from gold"),
            "gold_dup_series_date": n("select count(*) from (select 1 from gold group by measure_key, geography_key, breakdown_json, date_key having count(*) > 1)"),
            "gold_per_measure": n("select string_agg(c, '; ') from (select split_part(measure_key, ':', 4) || ' ' || count(*) || ' ' || count(distinct breakdown_json) || 'types' c from gold group by measure_key order by 1)"),
            "jst_2025Q4": n("select string_agg(measure_codes || '=' || round(value / 1e9, 2), '; ') from gold where date_key = date '2025-10-01' and is_total"),
        }
        print("STATS", stats)
        show(con, "silver sections", "select unit_type, section, measure_code, count(*), count(distinct item_label) from silver group by all order by all")
        show(con, "gold missing per type and quarter", "select date_key, string_agg(distinct breakdown_json || measure_codes, ' ') from gold group by all having count(*) <> 24 order by all")
        show(con, "jst 2025 Q4 silver revenue", "select section, item_label, item_occurrence, measure_code, value_pln from silver where unit_type = 'jst' and period_date = date '2025-10-01' and measure_code <> 'plan' order by section, item_label limit 80")
        annotate("notice", "mf_jst_budgets trial", stats)
        annotate("notice", "mf trial bronze", {"bronze_rows": cells.num_rows, "rejected": len(rejected)})
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
        except Exception as exc:
            print("drop failed", exc)
        annotate("notice", "mf trial cleanup", {"objects_removed": removed, "tables_dropped": dropped})
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
