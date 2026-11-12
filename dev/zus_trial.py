"""Dev trial of zus_psz: Landing and Bronze into the scratch locations zz_dev_zus_psz/ (both buckets) and namespace
zz_dev_zus_psz, twice each (idempotency), then the Silver SQL and the Gold build stage applied twice to the scratch
Bronze table in DuckDB (macros expanded, map_geography replaced by its zus_psz rows); compact annotations per stage.
Everything written is removed at the end (finally). Deleted before merge."""

from __future__ import annotations

from pathlib import Path
import re
import sys

import duckdb

from ingestion.common import bronze_files as bf
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze.zus_psz import load
from ingestion.to_landing import zus_psz as landing

ROOT = Path(__file__).resolve().parents[1]
SCRATCH = "zz_dev_zus_psz"
NEWEST = """(select * from (select * from cells qualify row_number() over (partition by raw_sha256, source_row_number
order by ingested_at) = 1) qualify rank() over (partition by entity order by source_version desc, ingested_at desc,
raw_sha256) = 1)"""
GEO = """(select 'zus_psz' as source_system, c as source_code, case when c = '00' then 'PL' else 'TERC:' || c end
as geography_key from (values ('00'), ('02'), ('04'), ('06'), ('08'), ('10'), ('12'), ('14'), ('16'), ('18'), ('20'),
('22'), ('24'), ('26'), ('28'), ('30'), ('32')) as t (c))"""


def sql_of(path: str) -> str:
    sql = (ROOT / path).read_text()
    sql = re.sub(r"\{#.*?#\}", "", sql, flags=re.S)
    sql = re.sub(r"\{\{ config\(.*?\) \}\}", "", sql)
    sql = sql.replace("({{ xlsx_newest_cells(source('zus_psz', 'zus_psz_cells')) }})", NEWEST)
    sql = sql.replace("{{ ref('poc_map_geography') }}", GEO)
    return sql.replace("{{ ref('zus_psz_observations') }}", "silver")


def rows(con, sql):
    rel = con.sql(sql)
    names = [d[0] for d in rel.description]
    return [dict(zip(names, r)) for r in rel.fetchall()]


def show(con, title, sql, limit=80):
    print(f"\n=== {title}", flush=True)
    rel = con.sql(sql)
    print("\t".join(d[0] for d in rel.description))
    out = rel.fetchall()
    for r in out[:limit]:
        print("\t".join("" if v is None else str(v)[:100] for v in r))
    if len(out) > limit:
        print(f"... {len(out)} rows")


def main(argv) -> int:
    client = s3()
    cat = bf.retry(catalog, "catalog")
    try:
        for attempt in (1, 2):
            print(f"\n##### landing extract {attempt}", flush=True)
            landing.extract(60, landing_root=f"{SCRATCH}/01_landing")
        for attempt in (1, 2):
            print(f"\n##### bronze load {attempt}", flush=True)
            load.load(0, landing_root=f"{SCRATCH}/01_landing", namespace=SCRATCH)
        cells = cat.load_table((SCRATCH, "zus_psz_cells")).scan().to_arrow()
        try:
            rejected = cat.load_table((SCRATCH, "zus_psz_rejected_files")).scan().to_arrow().to_pylist()
        except Exception:  # no rejections table when nothing was rejected
            rejected = []
        con = duckdb.connect()
        con.register("cells", cells)
        silver_sql, gold_sql = sql_of("warehouse/models/silver/zus_psz/zus_psz_observations.sql"), sql_of(
            "warehouse/models/gold/poc_build_zus_psz.sql"
        )
        con.sql(f"create table silver as {silver_sql}")
        con.sql(f"create table silver2 as {silver_sql}")
        con.sql(f"create table gold as {gold_sql}")
        landed = rows(con, "select count(distinct entity) files, count(distinct raw_sha256) shas, count(*) cells from cells")[0]
        per_family = rows(
            con,
            "select family, count(*) n, min(period_label) first, max(period_label) last, count(distinct entity) files "
            "from silver group by all order by all",
        )
        grain = rows(
            con,
            "select count(*) n from (select family, period_start, area_code, sex_code, breakdown_code, measure "
            "from silver group by all having count(*) > 1)",
        )[0]["n"]
        nulls = rows(
            con,
            "select count(*) filter (where area_code is null) area, count(*) filter (where sex_code is null) sex, "
            "count(*) filter (where breakdown_code is null) breakdown, count(*) filter (where measure is null) measure, "
            "count(*) filter (where period_start is null) period, count(*) filter (where raw_sha256 is null) sha from silver",
        )[0]
        rebuild_same = rows(
            con, "select (select count(*) from (select * from silver except all select * from silver2)) = 0 "
            "and (select count(*) from silver) = (select count(*) from silver2) as same"
        )[0]["same"]
        voiv = rows(
            con,
            "select family, period_start, measure, breakdown_code, count(distinct area_code) areas from silver where "
            "family in ('insured_sickness_voiv', 'payers_voiv', 'absence_voiv') and area_code <> '_U' group by all having count(distinct "
            "area_code) <> 17",
        )
        annotate(
            "notice" if not grain and not any(nulls.values()) and not voiv else "error",
            "zus_psz trial: Bronze and Silver",
            {
                "bronze": {**landed, "rejected": [(r["entity"], r["error"][:120]) for r in rejected]},
                "silver_rows": con.sql("select count(*) from silver").fetchall()[0][0],
                "families": per_family,
                "grain_duplicates": grain,
                "null_keys": nulls,
                "voivodeship_test_failures": len(voiv),
                "rebuild_identical": rebuild_same,
            },
        )
        gold_grain = rows(
            con,
            "select count(*) n from (select measure_key, geography_key, breakdown_json, date_key from gold "
            "group by all having count(*) > 1)",
        )[0]["n"]
        freq = rows(con, "select count(*) n from (select measure_key from gold group by 1 having count(distinct frequency) > 1)")[0]["n"]
        spots = rows(
            con,
            "select measure_key, geography_key, period_label, breakdown_json, value from gold where is_total and "
            "(geography_key = 'PL' or geography_key = 'TERC:14') and date_key = (select max(date_key) from gold g2 "
            "where g2.measure_key = gold.measure_key) order by measure_key, geography_key",
        )
        annotate(
            "notice" if not gold_grain and not freq else "error",
            "zus_psz trial: Gold build stage",
            {
                "rows": con.sql("select count(*) from gold").fetchall()[0][0],
                "measures": con.sql("select count(distinct measure_key) from gold").fetchall()[0][0],
                "series": con.sql("select count(distinct measure_key || geography_key || breakdown_json) from gold").fetchall()[0][0],
                "duplicates_series_x_date": gold_grain,
                "measures_with_two_frequencies": freq,
                "unmatched_geography": con.sql("select count(*) from gold where geography_key = '_X'").fetchall()[0][0],
                "spot_values": [
                    f"{s['measure_key'].split(':', 1)[1]} {s['geography_key']} {s['period_label']} {s['value']}" for s in spots
                ][:24],
            },
        )
        show(con, "unmatched area labels", "select family, row_label, to_hex(encode(row_label)) hex, count(*) from silver "
             "where area_code is null group by all order by all")
        show(con, "payer rows", "select distinct entity, row_label from silver where family = 'payers_voiv' order by all", 60)
        show(con, "silver per family/measure", "select family, measure, frequency, unit_code, count(*) n, "
             "count(distinct area_code) areas, count(distinct breakdown_code) bds, min(period_label), max(period_label) "
             "from silver group by all order by all")
        show(con, "Poland totals, every period", "select family, measure, period_label, value from silver where "
             "area_code = '00' and sex_code = 'T' and breakdown_code = '_T' order by family, measure, period_label", 200)
        show(con, "breakdown codes", "select family, breakdown_code, sex_code, count(*) from silver group by all "
             "order by all", 120)
        show(con, "voivodeship test failures", "select * from (values (1)) where false" if not voiv else
             "select family, period_start, measure, breakdown_code, count(distinct area_code) from silver where family "
             "in ('insured_sickness_voiv', 'payers_voiv', 'absence_voiv') group by all having count(distinct area_code) <> 17")
        show(con, "headers per entity", "select c.entity, c.row_number, string_agg(c.value_text, ' | ' order by "
             "c.column_number) from cells c where c.row_number <= 9 group by all order by all", 400)
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
        annotate("notice", "zus_psz trial cleanup", {"objects_removed": removed, "tables_dropped": dropped})
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
