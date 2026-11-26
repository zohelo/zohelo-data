"""Dev trial of gddkia_gpr (register PL-ENV-022, #2999) on the branch's final code: Landing and Bronze into zz_dev_gddkia_gpr/
(both buckets) and namespace zz_dev_gddkia_gpr, each twice; then the branch's Silver, tests and Gold SQL applied in memory
(Jinja-rendered with warehouse/macros), checks, profiles and Poland spot values printed and annotated. Reads silver.teryt_terc
(read-only) for the voivodeship codes. A rejected .xls is diagnosed (xlrd read variants). Everything written is removed at the
end (finally). Deleted before merge."""

from __future__ import annotations

from pathlib import Path
import re
import sys
import traceback

import duckdb
import jinja2

from ingestion.common import bronze_files as bf
from ingestion.common import bulk_bronze
from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze import gddkia_gpr as bronze
from ingestion.to_landing import gddkia_gpr as landing
from ingestion.to_landing.patterns import bulk_file

ROOT = Path(__file__).resolve().parents[1]
SCRATCH = "zz_dev_gddkia_gpr"
LAND = f"{SCRATCH}/01_landing"
MACROS = (ROOT / "warehouse/macros/xlsx_cells.sql").read_text() + (ROOT / "warehouse/macros/sources/gddkia_gpr.sql").read_text()


def render(path: Path, tables: dict[str, str]) -> str:
    text = MACROS + re.sub(r"\{\{ config\(.*?\) \}\}", "", path.read_text(), flags=re.S)
    return jinja2.Environment().from_string(text).render(source=lambda s, t: tables[t], ref=lambda m: tables[m])


def show(con, title, sql, limit=80):
    print(f"\n=== {title}", flush=True)
    rel = con.sql(sql)
    print("\t".join(d[0] for d in rel.description))
    rows = rel.fetchall()
    for r in rows[:limit]:
        print("\t".join("" if v is None else str(v)[:100] for v in r))
    if len(rows) > limit:
        print(f"... {len(rows)} rows")


def diagnose_xls(client, entity: str) -> None:
    """Why a landed .xls was rejected: xlrd read variants with the traceback tail."""
    import xlrd

    receipt = bulk_file.landed(client, req("STORAGE_LANDING_BUCKET_NAME"), bulk_file.prefix("gddkia_gpr", entity, LAND))[-1]
    body = bulk_bronze.read_raw(client, req("STORAGE_LANDING_BUCKET_NAME"), "gddkia_gpr", receipt)
    print(f"\n=== diagnose {entity}: {len(body)} bytes head={body[:8].hex()}", flush=True)
    for kwargs in ({}, {"ignore_workbook_corruption": True}, {"on_demand": True, "ignore_workbook_corruption": True},
                   {"formatting_info": True, "ignore_workbook_corruption": True}):
        try:
            book = xlrd.open_workbook(file_contents=body, **kwargs)
            sh = book.sheet_by_index(0)
            print(f"  {kwargs}: ok sheets={book.sheet_names()} rows={sh.nrows} cols={sh.ncols}")
        except Exception:
            print(f"  {kwargs}: failed\n" + "".join(traceback.format_exc().splitlines(True)[-10:]))


def main(argv) -> int:
    client = s3()
    cat = bf.retry(catalog, "catalog")
    con = duckdb.connect()
    stats: dict = {}
    try:
        # Landing, twice: the second run must land nothing
        for attempt in (1, 2):
            print(f"\n##### landing extract {attempt}", flush=True)
            landing.extract(60, landing_root=LAND)
        held = landing.held(LAND)
        receipts = bulk_file.landed(client, req("STORAGE_LANDING_BUCKET_NAME"), f"{LAND}/gddkia_gpr/snapshot/data/")
        stats["landing"] = {
            "files": len(receipts),
            "entities": sorted(held),
            "bytes": sum(r["size_bytes"] for r in receipts),
            "versions": {r["entity"]: r["version_token"] for r in receipts},
            "copies_per_entity_max": max([sum(1 for r in receipts if r["entity"] == e) for e in held] or [0]),
        }
        # Bronze, twice: the second run must add nothing
        rows_after = []
        for attempt in (1, 2):
            print(f"\n##### bronze load {attempt}", flush=True)
            bronze.bronze_load(sorted(held), landing_root=LAND, namespace=SCRATCH)
            rows_after.append(cat.load_table((SCRATCH, "gddkia_gpr_cells")).scan().to_arrow().num_rows)
        cells = cat.load_table((SCRATCH, "gddkia_gpr_cells")).scan().to_arrow()
        try:
            rejected = cat.load_table((SCRATCH, "gddkia_gpr_rejected_files")).scan().to_arrow().to_pylist()
        except Exception:
            rejected = []
        for r in rejected:
            diagnose_xls(client, r["entity"])
        con.register("cells_raw", cells)
        con.sql("create table cells as select * from cells_raw")
        stats["bronze"] = {
            "rows_run1": rows_after[0],
            "rows_run2": rows_after[1],
            "files": con.sql("select count(distinct raw_sha256) from cells").fetchone()[0],
            "per_entity": dict(con.sql("select entity || ' ' || string_agg(distinct sheet_name, '/'), count(*) from cells group by entity order by 1").fetchall()),
            "rejected": [(r.get("entity"), (r.get("error") or "")[:160]) for r in rejected],
        }
        annotate("notice", "gddkia_gpr trial landing+bronze", {**stats["landing"], **stats["bronze"]})
        show(con, "headers (rows 1-10)", """select entity, row_number, string_agg(column_number || ':' || replace(left(value_text, 40), chr(10), ' '), ' | ' order by column_number)
            from cells where row_number <= 10 group by all order by all""", limit=200)

        # Silver in memory + its tests
        con.sql("create table gddkia_gpr_sdrr as " + render(ROOT / "warehouse/models/silver/gddkia_gpr/gddkia_gpr_sdrr.sql", {"gddkia_gpr_cells": "cells"}))
        n = lambda q: con.sql(q).fetchone()[0]  # noqa: E731
        s = "gddkia_gpr_sdrr"
        complete = con.sql(render(ROOT / "warehouse/tests/gddkia_gpr_sdrr_complete.sql", {"gddkia_gpr_sdrr": s})).fetchall()
        woj = ["02", "04", "06", "08", "10", "12", "14", "16", "18", "20", "22", "24", "26", "28", "30", "32"]
        classes = ["motor_vehicles", "motorcycles", "cars", "cars_minibuses", "minibuses", "light_goods_vehicles",
                   "trucks_without_trailer", "trucks_with_trailer", "buses", "agricultural_tractors"]
        tests = {
            "unique": n(f"select count(*) from (select 1 from {s} group by edition_code, road_class, section_code, vehicle_class having count(*) > 1)"),
            "not_null": n(f"select count(*) from {s} where edition_code is null or road_class is null or section_code is null or voivodeship_code is null or length_km is null or vehicle_class is null or value_status is null or raw_sha256 is null"),
            "accepted_edition_road": n(f"select count(*) from {s} where edition_code not in ('2020', '2025') or road_class not in ('dk', 'dw')"),
            "accepted_voivodeship": n(f"select count(*) from {s} where voivodeship_code not in ({', '.join(repr(w) for w in woj)})"),
            "accepted_vehicle_class": n(f"select count(*) from {s} where vehicle_class not in ({', '.join(repr(c) for c in classes)})"),
            "accepted_value_status": n(f"select count(*) from {s} where value_status not in ('ok', 'not_measured')"),
            "complete_test_rows": len(complete),
        }
        stats["silver"] = {
            "rows": n(f"select count(*) from {s}"),
            "per_table": n(f"select string_agg(k, '; ' order by k) from (select edition_code || ' ' || road_class || ': ' || count(distinct section_code) || ' sections, ' || count(*) || ' rows, not_measured sections ' || count(distinct section_code) filter (where value_status = 'not_measured') || ', km ' || round(sum(length_km) filter (where vehicle_class = 'motor_vehicles'), 1) as k from {s} group by edition_code, road_class)"),
            "erratum_sections": n(f"select count(distinct section_code) from {s} where from_erratum"),
            "markers": n(f"select string_agg(value_text || '=' || c, ' ') from (select value_text, count(*) c from {s} where value_status <> 'ok' group by 1)"),
            "voivodeships": n(f"select count(distinct voivodeship_code) from {s}"),
            "tests_failing_rows": tests,
        }
        annotate("notice" if not any(tests.values()) else "warning", "gddkia_gpr trial silver", stats["silver"])
        show(con, "complete test rows", render(ROOT / "warehouse/tests/gddkia_gpr_sdrr_complete.sql", {"gddkia_gpr_sdrr": s}), limit=40)
        show(con, "remarks", f"select left(remark, 90) r, count(distinct edition_code || road_class || section_code) from {s} where remark is not null group by 1 order by 2 desc", limit=40)
        show(con, "repeated section numbers in a file (before the erratum rule)", f"select entity, section_number, count(*) / count(distinct vehicle_class) n from {s} group by all having count(*) > count(distinct vehicle_class)", limit=40)
        show(con, "top sections", f"select edition_code, road_class, section_code, road_number, section_name, length_km, value from {s} where vehicle_class = 'motor_vehicles' and value_status = 'ok' order by value desc limit 8")

        # Gold in memory: geography map from teryt_terc (read-only), build stage, grain and spot values
        terc = cat.load_table(("silver", "teryt_terc")).scan().to_arrow()
        con.register("terc", terc)
        con.sql("create table geo as select terc_code, 'TERC:' || terc_code as geography_key, 'wojewodztwo' as level_name from terc where unit_level = 'wojewodztwo'")
        geo_cte = jinja2.Environment().from_string(MACROS + "{{ poc_geography__gddkia_gpr() }}").render(ref=lambda m: m)
        con.sql("create table map_geo as with geo as (select * from geo), " + geo_cte + " select * from gddkia_gpr")
        con.sql("create table gold as " + render(ROOT / "warehouse/models/gold/poc_build_gddkia_gpr.sql", {"gddkia_gpr_sdrr": s, "poc_map_geography": "map_geo"}))
        kpi = "gddkia_gpr:sdrr:veh_per_day:dk:motor_vehicles"
        stats["gold"] = {
            "rows": n("select count(*) from gold"),
            "measures": n("select count(distinct measure_key) from gold"),
            "areas": n("select count(distinct geography_key) from gold"),
            "dup_series_date": n("select count(*) from (select 1 from gold group by measure_key || '|' || geography_key || case when geography_key = '_X' then ':' || coalesce(geography_source_code, '') else '' end || '|' || breakdown_json, date_key having count(*) > 1)"),
            "unmatched": n("select count(*) from gold where geography_key = '_X'"),
            "null_values": n("select count(*) from gold where value is null"),
            "kpi_rows": n(f"select count(*) from gold where measure_key = '{kpi}'"),
            "kpi_max_per_area_period": n(f"select max(c) from (select count(*) c from gold where measure_key = '{kpi}' group by geography_key, date_key)"),
            "kpi_range": n(f"select min(value) || ' .. ' || max(value) from gold where measure_key = '{kpi}'"),
            "kpi_outside_bounds_1000_1e5": n(f"select count(*) from gold where measure_key = '{kpi}' and (value < 1000 or value > 1e5)"),
            "dates": n("select string_agg(distinct cast(date_key as varchar) || '=' || period_label, ', ') from gold"),
        }
        annotate("notice", "gddkia_gpr trial gold", stats["gold"])
        spot = n("select string_agg(measure_codes || ' ' || period_label || '=' || value, '; ' order by measure_codes, period_label) from gold where geography_key = 'PL'")
        annotate("notice", "gddkia_gpr trial Poland", {"values": spot})
        show(con, "gold voivodeships, dk motor vehicles", f"select geography_key, period_label, value from gold where measure_key = '{kpi}' order by 1, 2", limit=40)
        from warehouse.seed_rows import rows as seed_rows

        row = next(r for r in seed_rows("source") if r["source_key"] == "gddkia_gpr")
        lic = next((r for r in seed_rows("licence") if r["licence_key"] == row["licence_key"]), None)
        annotate("notice", "gddkia_gpr trial licence", {"source_licence_key": row["licence_key"], "licence_row": lic})
        print("STATS", stats, flush=True)
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
        left = sum(len(p.get("Contents", [])) for b in (req("STORAGE_LANDING_BUCKET_NAME"), req("STORAGE_LAKEHOUSE_BUCKET_NAME"))
                   for p in client.get_paginator("list_objects_v2").paginate(Bucket=b, Prefix=f"{SCRATCH}/"))
        annotate("notice", "gddkia_gpr trial cleanup", {"objects_removed": removed, "tables_dropped": dropped, "objects_left": left})
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
