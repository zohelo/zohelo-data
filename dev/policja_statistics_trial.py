"""Dev trial of policja_statistics (PLG-033): Landing and Bronze into zz_dev_policja/ (both buckets) and namespace
zz_dev_policja, each twice, then the Silver and Gold SQL of the branch applied in memory with DuckDB (Jinja-rendered
with warehouse/macros), the Silver tests of _policja_statistics.yml and the completeness test, Gold checks and Poland
spot values annotated. Reads silver.teryt_terc (read-only) for the voivodeship names. Everything written is removed
at the end (finally). Deleted before merge."""

from __future__ import annotations

from pathlib import Path
import re
import sys
import time

import duckdb
import jinja2

from ingestion.common import bronze_files as bf
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_landing.patterns import bulk_file
from ingestion.to_bronze.policja_statistics import load as bronze

ROOT = Path(__file__).resolve().parents[1]
SOURCE = "policja_statistics"
SCRATCH = "zz_dev_policja"
SILVER = ROOT / "warehouse/models/silver/policja_statistics/policja_statistics_offences.sql"
GOLD = ROOT / "warehouse/models/gold/poc_build_policja_statistics.sql"
TEST = ROOT / "warehouse/tests/policja_statistics_offences_complete.sql"


def retried(what, fn):
    """Bounded retries with backoff for R2 under load (429 / SlowDown / ServiceUnavailable)."""
    for attempt in range(5):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001 - re-raised unless a throttling answer
            text = repr(exc)
            if attempt == 4 or not re.search(r"429|SlowDown|ServiceUnavailable|Too Many", text):
                raise
            print(f"RETRY {what} {attempt + 1}: {text[:200]}", flush=True)
            time.sleep(15 * (attempt + 1))
    return None


def render(path: Path, tables: dict[str, str]) -> str:
    macros = (ROOT / "warehouse/macros/xlsx_cells.sql").read_text()
    macros += (ROOT / "warehouse/macros/sources/policja_statistics.sql").read_text()
    text = macros + re.sub(r"\{\{ config\(.*?\) \}\}", "", path.read_text(), flags=re.S)
    return jinja2.Environment().from_string(text).render(source=lambda s, t: tables[t], ref=lambda m: tables[m])


def render_geo() -> str:
    """The geography CTE of macros/sources/policja_statistics.sql, run against silver.teryt_terc as `terc`."""
    macro = (ROOT / "warehouse/macros/sources/policja_statistics.sql").read_text()
    body = jinja2.Environment().from_string(macro + "{{ poc_geography__policja_statistics() }}").render().strip()
    cte = re.search(r"policja_statistics as \((.*)\)\s*$", body, re.S).group(1)
    return f"""with geo as (select 'TERC:' || terc_code as geography_key, name_pl, 'wojewodztwo' as level_name
        from terc where unit_level = 'wojewodztwo')
        select source_code, geography_key from ({cte})"""


def show(con, title, sql, limit=200):
    print(f"\n=== {title}", flush=True)
    rel = con.sql(sql)
    print("\t".join(d[0] for d in rel.description))
    rows = rel.fetchall()
    for r in rows[:limit]:
        print("\t".join("" if v is None else str(v)[:300] for v in r))
    if len(rows) > limit:
        print(f"... {len(rows)} rows")
    return rows


def model(con, cells_rel: str) -> dict:
    """Silver, its tests, Gold and the checks; returns the numbers (also used by the local dry run)."""
    n = lambda q: con.sql(q).fetchall()[0][0]  # noqa: E731
    con.sql("create or replace table silver as " + render(SILVER, {"policja_statistics_cells": cells_rel}))
    key = "category_code, area_name, measure_code, period_year"
    out = {}
    out["silver"] = {
        "rows": n("select count(*) from silver"),
        "per_category": n("""select string_agg(x, '; ' order by x) from (select category_code || ' ' || min(period_year)
            || '-' || max(period_year) || ' areas=' || count(distinct area_name) || ' n=' || count(*) x from silver group by category_code)"""),
        "status": n("select string_agg(cell_status || '=' || c, ' ' order by cell_status) from (select cell_status, count(*) c from silver group by 1)"),
        "non_numeric_published": n("""select string_agg(distinct coalesce('[' || value_published || ']', 'absent'), ' ')
            from silver where cell_status <> 'ok'"""),
        "null_values": n("select count(*) from silver where value is null"),
        "zero_values": n("select count(*) from silver where value = 0"),
        "rate_out_of_0_100": n("select count(*) from silver where measure_code = 'detection_rate' and (value < 0 or value > 100)"),
        "count_negative_or_fraction": n("select count(*) from silver where unit_code = 'offences' and (value < 0 or value <> floor(value))"),
        "detected_gt_recorded": n("""select count(*) from silver r join silver d using (category_code, area_name, period_year)
            where r.measure_code = 'recorded' and d.measure_code = 'detected' and d.value > r.value"""),
        "titles": n("select string_agg(distinct category_code || '=' || left(table_title, 90), ' | ') from silver"),
    }
    out["tests"] = {
        "unique_grain_dups": n(f"select count(*) from (select 1 from silver group by {key} having count(*) > 1)"),
        "not_null_fails": n("""select count(*) from silver where category_code is null or area_name is null
            or measure_code is null or unit_code is null or period_year is null or cell_status is null or raw_sha256 is null"""),
        "accepted_values_fails": n("""select count(*) from silver where category_code not in ('ogolem', 'wybrane_7')
            or measure_code not in ('recorded', 'detected', 'detection_rate') or unit_code not in ('offences', 'pct')
            or cell_status not in ('ok', 'not_available', 'missing')"""),
        "complete_fails": len(con.sql(render(TEST, {"policja_statistics_offences": "silver"})).fetchall()),
    }
    # areas: the 16 voivodeships against Poland, the two police areas against woj. mazowieckie (counts only)
    out["areas"] = {
        "names": n("select string_agg(distinct area_name, '; ' order by area_name) from silver"),
        "voiv_sum_ne_poland": n("""select sum(cast(pv <> sv as integer)) || ' of ' || count(*) from (
            select p.category_code, p.measure_code, p.period_year, p.value pv, sum(v.value) sv from silver p
            join silver v on v.category_code = p.category_code and v.measure_code = p.measure_code and v.period_year = p.period_year
                and v.area_name like 'woj. %'
            where p.area_name = 'Polska' and p.measure_code <> 'detection_rate' group by all)"""),
        "recorded_voiv_minus_poland": n("""select left(string_agg(x, ' ' order by x), 900) from (
            select p.category_code || ':' || p.period_year || '(' || cast(sum(v.value) - p.value as bigint) || ')' x from silver p
            join silver v on v.category_code = p.category_code and v.measure_code = p.measure_code and v.period_year = p.period_year
                and v.area_name like 'woj. %'
            where p.area_name = 'Polska' and p.measure_code = 'recorded' group by p.category_code, p.period_year, p.value
            having sum(v.value) <> p.value)"""),
        "police_areas_sum_ne_mazowieckie": n("""select sum(cast(mv <> tv as integer)) || ' of ' || count(*) from (
            select m.category_code, m.measure_code, m.period_year, m.value mv, sum(t.value) tv from silver m
            join silver t on t.category_code = m.category_code and t.measure_code = m.measure_code and t.period_year = m.period_year
                and t.area_name like 'teren działania%'
            where m.area_name = 'woj. mazowieckie' and m.measure_code <> 'detection_rate' group by all)"""),
    }
    con.sql("create or replace table gold as " + render(GOLD, {"policja_statistics_offences": "silver", "poc_map_geography": "geo"}))
    out["gold"] = {
        "rows": n("select count(*) from gold"),
        "silver_rows_with_value_official_areas": n("select count(*) from silver where value is not null and (area_name = 'Polska' or area_name like 'woj. %')"),
        "dup_series_date": n("select count(*) from (select 1 from gold group by measure_key || '|' || geography_key || '|' || breakdown_json, date_key having count(*) > 1)"),
        "null_keys": n("select count(*) from gold where measure_key is null or geography_key is null or date_key is null or value is null or measure_name is null or block_name is null"),
        "unmatched_geo": n("select count(*) from gold where geography_key = '_X'"),
        "unmatched_codes": n("select string_agg(distinct geography_source_code, '; ') from gold where geography_key = '_X'"),
        "measures": n("select string_agg(distinct measure_key, ' ' order by measure_key) from gold"),
        "geographies": n("select count(distinct geography_key) from gold"),
        "one_code_per_area": n("select count(*) from (select geography_key from gold group by 1 having count(distinct geography_source_code) > 1)"),
        "dates": n("select min(date_key) || '..' || max(date_key) from gold"),
        "max_recorded_pl": n("select arg_max(period_label || '=' || value, value) from gold where geography_key = 'PL' and measure_key = 'policja_statistics:ogolem:offences:recorded'"),
        "max_recorded_any": n("select max(value) from gold where unit_code = 'offences'"),
        "rate_range": n("select min(value) || '..' || max(value) from gold where unit_code = 'pct'"),
    }
    out["poland"] = n("""select string_agg(replace(measure_key, 'policja_statistics:', '') || ' ' || period_label || '=' || value, '; ' order by measure_key, period_label)
        from gold where geography_key = 'PL' and period_label in ('1999', '2013', '2020', '2021', '2022', '2023')""")
    return out


def main(argv) -> int:
    if argv and argv[0] == "local":  # dry run against a local cells parquet: dev/policja_statistics_trial.py local <cells> <terc>
        con = duckdb.connect()
        con.sql(f"create table cells as select * from read_parquet('{argv[1]}')")
        con.sql(f"create table terc as select * from read_parquet('{argv[2]}')")
        con.sql("create table geo as select 'policja_statistics' as source_system, source_code, geography_key from (" + render_geo() + ")")
        print(model(con, "cells"))
        return 0
    client = s3()
    cat = bf.retry(catalog, "catalog")
    con = duckdb.connect()
    land = f"{SCRATCH}/01_landing"
    lb = req("STORAGE_LANDING_BUCKET_NAME")
    try:
        landing = []
        for attempt in (1, 2):
            print(f"\n##### landing extract {attempt}", flush=True)
            retried("landing", lambda: bulk_file.main(SOURCE, "load", landing_root=land, pause_seconds=2))
            files = []
            for spec in bulk_file.declaration(SOURCE):
                held = bulk_file.landed(client, lb, bulk_file.prefix(SOURCE, spec["entity"], land))
                files.append((spec["entity"], len(held), held[-1]["size_bytes"] if held else 0,
                              held[-1]["version_token"] if held else None))
            landing.append(files)
        counts = []
        for attempt in (1, 2):
            print(f"\n##### bronze load {attempt}", flush=True)
            retried("bronze", lambda: bronze.load(10, landing_root=land, namespace=SCRATCH))
            cells = retried("scan", lambda: cat.load_table((SCRATCH, f"{SOURCE}_cells")).scan().to_arrow())
            counts.append(cells.num_rows)
        try:
            rejected = cat.load_table((SCRATCH, f"{SOURCE}_rejected_files")).scan().to_arrow().to_pylist()
        except Exception:  # noqa: BLE001 - no rejections table when nothing was rejected
            rejected = []
        con.register("cells", cells)
        annotate("notice", "policja landing and bronze", {
            "landing_files": len(landing[1]), "bytes": sum(f[2] for f in landing[1]),
            "versions_after_run1": sum(f[1] for f in landing[0]), "versions_after_run2": sum(f[1] for f in landing[1]),
            "versions": {f[0]: f[3] for f in landing[1]},
            "bronze_rows_after_run1": counts[0], "bronze_rows_after_run2": counts[1],
            "rejected": [(r.get("entity"), (r.get("error") or "")[:120]) for r in rejected],
            "per_entity": con.sql("select string_agg(entity || ':' || n, ' ' order by entity) from (select entity, count(*) n from cells group by 1)").fetchall()[0][0]})
        notes = show(con, "notes under the data", """select entity, row_number, value_text from cells
            where column_number = 1 and (value_text like 'UWAGA%' or row_number > (select max(row_number) - 3 from cells c2 where c2.entity = cells.entity))
            order by all""")
        terc = cat.load_table(("silver", "teryt_terc")).scan().to_arrow()
        con.register("terc_arrow", terc)
        con.sql("create table terc as select * from terc_arrow")
        con.sql("create table geo as select 'policja_statistics' as source_system, source_code, geography_key from (" + render_geo() + ")")
        out = model(con, "cells")
        print("RESULT", out, flush=True)
        failing = sum(out["tests"].values())
        annotate("notice" if not failing else "error", "policja silver and tests", {**out["silver"], "tests": out["tests"]})
        annotate("notice", "policja silver areas", {**out["areas"], "notes": [f"{r[0]} r{r[1]}: {r[2][:400]}" for r in notes][:6]})
        g = out["gold"]
        annotate("notice" if not (g["dup_series_date"] or g["null_keys"] or g["unmatched_geo"]) else "error", "policja gold", g)
        annotate("notice", "policja gold Poland", {"values": (out["poland"] or "")[:3500]})
        show(con, "silver non-ok", "select * exclude (raw_sha256) from silver where cell_status <> 'ok' limit 60")
        show(con, "gold sample", "select measure_key, geography_source_code, geography_key, period_label, value from gold where period_label = '2021' order by all limit 120")
    finally:
        removed = 0
        for bucket in (req("STORAGE_LANDING_BUCKET_NAME"), req("STORAGE_LAKEHOUSE_BUCKET_NAME")):
            for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{SCRATCH}/"):
                for obj in page.get("Contents", []):
                    retried("delete", lambda k=obj["Key"], b=bucket: client.delete_object(Bucket=b, Key=k))
                    removed += 1
        dropped = []
        try:
            for ident in cat.list_tables(SCRATCH):
                cat.drop_table(ident)
                dropped.append(ident[-1])
            cat.drop_namespace(SCRATCH)
        except Exception as exc:  # noqa: BLE001
            print("drop failed", exc)
        left = sum(len(p.get("Contents", [])) for b in (req("STORAGE_LANDING_BUCKET_NAME"), req("STORAGE_LAKEHOUSE_BUCKET_NAME"))
                   for p in client.get_paginator("list_objects_v2").paginate(Bucket=b, Prefix=f"{SCRATCH}/"))
        annotate("notice", "policja trial cleanup", {"objects_removed": removed, "objects_left": left, "tables_dropped": dropped})
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
