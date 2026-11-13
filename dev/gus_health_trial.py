"""Dev trial of gus_health (register PLS-004, health family): Landing and Bronze into zz_dev_gus_health/ (both buckets)
and namespace zz_dev_gus_health, each run twice, then the branch's Silver SQL, completeness test, Gold build stage
and metrics checks applied in memory (Jinja-rendered with warehouse/macros), checks and samples printed and
annotated. Reads silver.teryt_terc (read-only) for the voivodeship names. Everything written is removed at the end
(finally). Options: titles=<regex> lists sheet titles, dump=<title regex>[@<entity regex>[@<max row>]] prints grids.
Deleted before merge."""

from __future__ import annotations

import csv
from pathlib import Path
import re
import sys

import duckdb
import jinja2

from ingestion.common import bronze_files as bf
from ingestion.common import gus_publications as gp
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_landing.gus_health import SERIES

ROOT = Path(__file__).resolve().parents[1]
SOURCE = "gus_health"
SCRATCH = "zz_dev_gus_health"
MACROS = "".join((ROOT / f"warehouse/macros/{m}.sql").read_text() for m in ("xlsx_cells", "gus_tables"))
KPIS = {  # kpi -> measure_code, (min, max) as in warehouse/tests/metrics_value_range.sql
    "doctors_working": ("doctors", (0, 1e6)),
    "nurses_working": ("nurses", (0, 1e6)),
    "hospital_beds": ("beds", (0, 1e6)),
    "hospital_beds_per_10k": ("beds_per_10k", (0, 200)),
}


def render(path: Path, tables: dict[str, str]) -> str:
    env = jinja2.Environment()
    text = MACROS + re.sub(r"\{\{ config\(.*?\) \}\}", "", path.read_text(), flags=re.S)
    return env.from_string(text).render(source=lambda s, t: tables[t], ref=lambda m: tables[m])


def show(con, title, sql, limit=120):
    print(f"\n=== {title}", flush=True)
    rel = con.sql(sql)
    print("\t".join(d[0] for d in rel.description))
    rows = rel.fetchall()
    for r in rows[:limit]:
        print("\t".join("" if v is None else str(v)[:90] for v in r))
    if len(rows) > limit:
        print(f"... {len(rows)} rows")


def files(cat, name) -> int:
    try:
        return len(list(cat.load_table((SCRATCH, name)).inspect.files().to_pylist()))
    except Exception as exc:  # noqa: BLE001 - trial reports
        return f"n/a {type(exc).__name__}"


def licence_row() -> dict:
    from warehouse import publish

    seeds = ROOT / "warehouse/seeds"
    sources = list(csv.DictReader(open(seeds / "source.csv", encoding="utf-8")))
    licences = list(csv.DictReader(open(seeds / "licence.csv", encoding="utf-8")))
    row = next((s for s in sources if s["source_key"] == SOURCE), {})
    lic = next((x for x in licences if x["licence_key"] == row.get("licence_key")), {})
    return {"source_row": row.get("licence_key"), "licence_row": lic.get("name"),
            "commercial_ok": lic.get("commercial_ok"), "licensed": SOURCE in publish.licensed_sources(sources, licences)}


def main(argv) -> int:
    titles = next((a[7:] for a in argv if a.startswith("titles=")), None)
    dump = next((a[5:] for a in argv if a.startswith("dump=")), None)
    client = s3()
    cat = bf.retry(catalog, "catalog")
    con = duckdb.connect()
    land = f"{SCRATCH}/01_landing"
    try:
        terc = cat.load_table(("silver", "teryt_terc")).scan().to_arrow()
        con.register("terc", terc)
        for attempt in (1, 2):
            print(f"\n##### landing extract {attempt}", flush=True)
            gp.extract(SERIES, 60, landing_root=land)
        entities = sorted(gp.held(SERIES, land))
        bronze_files = []
        for attempt in (1, 2):
            print(f"\n##### bronze load {attempt}", flush=True)
            gp.bronze_load(SOURCE, entities, landing_root=land, namespace=SCRATCH)
            bronze_files.append(files(cat, f"{SOURCE}_cells"))
        cells = cat.load_table((SCRATCH, f"{SOURCE}_cells")).scan().to_arrow()
        try:
            rejected = cat.load_table((SCRATCH, f"{SOURCE}_rejected_files")).scan().to_arrow().to_pylist()
        except Exception:  # noqa: BLE001 - no rejection table when nothing was rejected
            rejected = []
        con.register(f"{SOURCE}_cells", cells)
        if titles:
            show(con, "titles", f"""select entity, member, sheet_name, string_agg(trim(regexp_replace(value_text, '\\s+', ' ', 'g')), ' ' order by row_number, column_number)[:230] as title
                from {SOURCE}_cells where row_number <= 3 group by all
                having regexp_matches(title, '(?i){titles}') order by all""", limit=600)
        if dump:
            parts = dump.split("@")
            pattern, ent, maxrow = parts[0], (parts[1:2] or ["."])[0], (parts[2:3] or ["16"])[0]
            show(con, "dump", f"""with t as (select entity, member, sheet_name, string_agg(value_text, ' ' order by row_number, column_number) title
                from {SOURCE}_cells where row_number <= 8 group by all having regexp_matches(title, '(?i){pattern}') and regexp_matches(entity, '{ent}'))
                select c.entity, c.sheet_name, c.row_number, string_agg(c.column_number || ':' || replace(left(c.value_text, 45), chr(10), ' '), ' | ' order by c.column_number)
                from {SOURCE}_cells c join t using (entity, member, sheet_name) where c.row_number <= {maxrow} group by all order by all""", limit=2500)
        n = lambda q: con.sql(q).fetchall()[0][0]  # noqa: E731
        # Bronze profile (look before you model)
        bronze = {
            "editions": entities,
            "bronze_rows": cells.num_rows,
            "bronze_files_after_load_1_2": bronze_files,
            "rows_per_edition": n(f"select string_agg(entity || ':' || c, ' ' order by entity) from (select entity, count(*) c from {SOURCE}_cells group by 1)"),
            "sheets": n(f"select count(distinct entity || member || sheet_name) from {SOURCE}_cells"),
            "null_values": n(f"select count(*) from {SOURCE}_cells where value_text is null"),
            "rejected": [(r.get("entity"), (r.get("error") or r.get("reason") or "")[:100]) for r in rejected],
        }
        print("BRONZE", bronze, flush=True)
        annotate("notice", f"{SOURCE} trial bronze", bronze)
        silver = ROOT / f"warehouse/models/silver/{SOURCE}/{SOURCE}_tables.sql"
        con.sql(f"create or replace table {SOURCE}_tables as " + render(silver, {f"{SOURCE}_cells": f"{SOURCE}_cells"}))
        con.sql(f"""create or replace table geo as
            select '{SOURCE}' as source_system, lower(name_pl) as source_code, 'TERC:' || terc_code as geography_key
            from terc where unit_level = 'wojewodztwo'
            union all select '{SOURCE}', 'polska', 'PL'""")
        gsql = render(ROOT / f"warehouse/models/gold/poc_build_{SOURCE}.sql",
                      {f"{SOURCE}_tables": f"{SOURCE}_tables", "poc_map_geography": "geo"})
        con.sql("create or replace table gold as " + gsql)
        complete = render(ROOT / f"warehouse/tests/{SOURCE}_tables_complete.sql", {f"{SOURCE}_tables": f"{SOURCE}_tables"})
        s = f"{SOURCE}_tables"
        stats = {
            "silver_rows": n(f"select count(*) from {s}"),
            "silver_dup": n(f"select count(*) from (select 1 from {s} group by table_code, measure_code, area_name, group_code, period_year having count(*) > 1)"),
            "silver_null_keys": n(f"select count(*) from {s} where table_code is null or measure_code is null or unit_code is null or area_name is null or group_code is null or period_year is null or raw_sha256 is null"),
            "status": n(f"select string_agg(cell_status || '=' || c, ' ') from (select cell_status, count(*) c from {s} group by 1)"),
            "non_numeric_texts": n(f"select string_agg(distinct value_text, ' ') from {s} where cell_status <> 'ok'"),
            "years": n(f"select min(period_year) || '-' || max(period_year) from {s}"),
            "per_measure_years": n(f"select string_agg(x, '; ' order by x) from (select measure_code || ' ' || string_agg(period_year || ':' || c, ',' order by period_year) x from (select measure_code, period_year, count(*) c from {s} group by all) group by measure_code)"),
            "completeness_failures": n(f"select count(*) from ({complete})"),
            "gold_rows": n("select count(*) from gold"),
            "gold_dup_series_date": n("select count(*) from (select 1 from gold group by measure_key, geography_key, breakdown_json, date_key having count(*) > 1)"),
            "gold_unmatched": n("select count(*) from gold where geography_key = '_X'"),
            "gold_dates": n("select min(date_key) || '..' || max(date_key) from gold"),
        }
        bounds = " union all ".join(f"select '{m}' m, {lo} lo, {hi} hi" for m, (lo, hi) in KPIS.values())
        stats["metrics_rows"] = n(f"select string_agg(measure_codes || '=' || c, ' ') from (select measure_codes, count(*) c from gold where measure_codes in (select m from ({bounds})) group by 1)")
        stats["metrics_max_rows_per_area_period"] = n(f"select max(c) from (select measure_codes, geography_key, date_key, count(*) c from gold group by all)")
        stats["bound_failures"] = n(f"select count(*) from gold g join ({bounds}) b on g.measure_codes = b.m where g.value < b.lo or g.value > b.hi")
        stats["licence"] = licence_row()
        print("STATS", stats, flush=True)
        annotate("notice" if not stats["completeness_failures"] and not stats["silver_dup"] else "warning", f"{SOURCE} trial silver+gold", stats)
        spot = n("select string_agg(measure_codes || ' ' || period_label || '=' || value, '; ' order by measure_codes, period_label) from gold where geography_key = 'PL'")
        annotate("notice", f"{SOURCE} trial Poland", {"values": spot[:3500] if spot else None})
        show(con, "completeness failures", complete)
        show(con, "matched sheets", f"select entity, table_code, measure_code, sheet_name, any_value(header_path) hp, count(*) from {s} group by all order by all", limit=200)
        show(con, "gold sample", "select measure_key, geography_key, period_label, value, observation_status_key from gold where geography_key in ('PL', 'TERC:1400000') order by all", limit=200)
        show(con, "notes of matched sheets", f"""select distinct c.entity, c.sheet_name, c.row_number, left(regexp_replace(c.value_text, '\\s+', ' ', 'g'), 400)
            from {SOURCE}_cells c join (select distinct entity, member, sheet_name from {s}) m using (entity, member, sheet_name)
            where regexp_matches(c.value_text, '(?i)^(u ?w ?a ?g ?a|n ?o ?t ?e|[a-d]\\))') order by all""", limit=400)
        show(con, "non-ok cells", f"select entity, table_code, measure_code, area_name, value_text, cell_status from {s} where cell_status <> 'ok' limit 60")
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
        except Exception as exc:  # noqa: BLE001
            print("drop failed", exc)
        annotate("notice", f"{SOURCE} trial cleanup", {"objects_removed": removed, "tables_dropped": dropped})
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
