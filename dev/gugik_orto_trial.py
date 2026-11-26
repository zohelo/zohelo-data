"""Dev trial (PL-ENV-004, #3100 #3101 #3102): GUGiK orthophoto sheet index, source gugik_orto, Landing → Gold on the
final code. Landing of every year's feature type into the Landing prefix zz_dev_gugik_orto/ (a second pass lands
nothing: every year current, and two small years read again must come out unchanged), inspect and verify; Bronze
and Silver into the Iceberg namespace zz_dev_gugik_orto and the lake prefix zz_dev_gugik_orto/ twice (the second
writes nothing); a profile of Bronze, the Silver tests, the Gold build stage rendered with Jinja and run in local
DuckDB twice, the grain check by series_key x date_key and spot values for Poland. silver.prg_units and
gold.dim_geography are read (read-only) from production. Everything written is removed in `finally`. At most 10
annotations. No imagery is requested."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import re
import time
import types

import sys

import duckdb
from jinja2 import Environment

from ingestion.common.r2 import catalog, req, s3
from ingestion.common.report import annotate
from ingestion.to_bronze.gugik_orto import entities
from ingestion.to_landing import gugik_orto as landing

SOURCE, SCRATCH = "gugik_orto", "zz_dev_gugik_orto"
NOTES = {"left": 9}
YEARS = set(sys.argv[1].split(",")) if len(sys.argv) > 1 and sys.argv[1] else set()


def note(title, data, level="notice"):
    if NOTES["left"] > 0:
        NOTES["left"] -= 1
        annotate(level, title, data)
    print(f"[{title}] {data}", flush=True)


def q(con, sql):
    return [[str(v) for v in r] for r in con.execute(sql).fetchall()]


def one(con, sql):
    return con.execute(sql).fetchone()[0]


def render(path, refs, sources):
    return Environment().from_string(Path(path).read_text()).render(
        config=lambda **_k: "", ref=lambda n: refs.get(n, n), source=lambda _s, t: sources[t])


def trial(cat):
    client, bucket = s3(), req("STORAGE_LANDING_BUCKET_NAME")
    landing.LANDING_ROOT = SCRATCH
    entities.BRONZE, entities.SILVER = (SCRATCH, "gugik_orto_sheets"), (SCRATCH, "gugik_orto_sheets_silver")
    entities.REJECTED = (SCRATCH, "gugik_orto_rejected_files")
    entities.BRONZE_ROOT, entities.SILVER_ROOT = f"{SCRATCH}/02_bronze", f"{SCRATCH}/03_silver"
    entities.annotate = lambda *_a, **_k: None
    from ingestion.to_landing.patterns import bulk_file

    bulk_file.annotate = lambda *_a, **_k: None

    s = landing.session()
    started = time.monotonic()
    hits = landing.catalogue(s)
    if YEARS:  # a quick check of the code on a few years before the full load
        hits = {e: n for e, n in hits.items() if e.removeprefix("orto_") in YEARS}
    hits_seconds = round(time.monotonic() - started)
    runs = []
    for attempt in (1, 2):
        held = landing.landing_state(client, bucket)
        state = landing.classify(hits, held, datetime.now(UTC))
        todo = state["missing"] + state["stale"]
        if attempt == 2:  # read two small years again: equal bytes must land nothing
            todo += [e for e in ("orto_1971", "orto_1984") if e in hits and e not in todo]
        outcomes, t0 = {}, time.monotonic()
        for entity in todo:
            r = landing.land_year(client, bucket, s, entity, hits[entity], held.get(entity))
            outcomes[r["outcome"]] = outcomes.get(r["outcome"], 0) + 1
        runs.append({"run": attempt, "state": {k: len(v) for k, v in state.items()}, "outcomes": outcomes,
                     "minutes": round((time.monotonic() - t0) / 60, 1)})
    held = landing.landing_state(client, bucket)
    keys = [o for p in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{SCRATCH}/")
            for o in p.get("Contents", [])]
    verify = landing._run("verify", types.SimpleNamespace(budget_minutes=60))
    note("orto landing (scratch)", {
        "years": len(hits), "features_hits": sum(hits.values()), "hits_seconds": hits_seconds, "two_runs": runs,
        "files": sum(1 for k in keys if k["Key"].endswith(".gml")),
        "bytes": sum(k["Size"] for k in keys if k["Key"].endswith(".gml")),
        "complete_years": len(held), "verify_exit": verify,
        "largest_years": sorted(([e, hits[e]] for e in hits), key=lambda x: -x[1])[:3]})

    loads = []
    for _ in (1, 2):
        out = entities.run(300)
        loads.append({k: out.get(k) for k in ("entities_landed", "loaded", "bronze_rows_added", "silver_rows_written",
                                               "rejected", "rejected_sample", "minutes", "stopped_at_budget")})
    b = cat.load_table(entities.BRONZE).inspect.files()
    sv = cat.load_table(entities.SILVER).inspect.files()
    verify_b = entities.verify()
    note("orto bronze+silver (scratch)", {
        "load_two_runs": loads, "verify_exit": verify_b,
        "bronze_files_rows_bytes": [b.num_rows, sum(b.column("record_count").to_pylist()),
                                    sum(b.column("file_size_in_bytes").to_pylist())],
        "silver_files_rows_bytes": [sv.num_rows, sum(sv.column("record_count").to_pylist()),
                                    sum(sv.column("file_size_in_bytes").to_pylist())]})

    con = duckdb.connect()
    # the platform build's settings (warehouse/profiles.yml): 10 GB, 4 threads, insertion order not kept
    con.execute("SET memory_limit = '10GB'; SET threads = 4; SET preserve_insertion_order = false; "
                "SET temp_directory = '/tmp/duckdb_spill'; install spatial; load spatial")
    from warehouse import entity

    entity._remote(con)
    con.execute(f"create table bronze_b as select * from read_parquet({b.column('file_path').to_pylist()!r})")
    con.execute(f"create table silver_s as select * from read_parquet({sv.column('file_path').to_pylist()!r})")
    props = list(entities.PROPERTIES) + ["geometry_type", "srs_name", "pos_list", "rings"]
    note("orto bronze profile", {
        "rows_ids_years": q(con, "select count(*), count(distinct gml_id), count(distinct entity), min(akt_rok), "
                                 "max(akt_rok) from bronze_b"),
        "nulls": {c: n for c, n in zip(props, con.execute("select " + ", ".join(
            f"count(*) filter (where {c} is null)" for c in props) + " from bronze_b").fetchone()) if n},
        "kolor": q(con, "select kolor, count(*) from bronze_b group by 1 order by 2 desc"),
        "zrodlo": q(con, "select zrodlo_danych, count(*) from bronze_b group by 1 order by 2 desc"),
        "uklad_xy": q(con, "select uklad_xy, count(*) from bronze_b group by 1 order by 2 desc limit 12"),
        "modul": q(con, "select modul_archiwizacji, count(*) from bronze_b group by 1 order by 2 desc limit 8"),
        "filled": q(con, "select czy_ark_wypelniony, count(*) from bronze_b group by 1 order by 2 desc"),
        "geometry": q(con, "select geometry_type, rings, srs_name, count(*) from bronze_b group by all order by 4 desc"),
        "piksel": q(con, "select piksel, count(*) from bronze_b group by 1 order by 2 desc limit 15"),
        "not_numeric": q(con, "select count(*) filter (where piksel is not null and try_cast(piksel as double) is null), "
                              "count(*) filter (where wlk_pliku_mb is not null and try_cast(wlk_pliku_mb as double) is "
                              "null), count(*) filter (where try_cast(akt_data as date) is null) from bronze_b"),
        "year_vs_type": one(con, "select count(*) from bronze_b where 'orto_' || akt_rok <> entity"),
        "url_repeated": one(con, "select count(*) from (select url_do_pobrania from bronze_b group by 1 "
                                 "having count(*) > 1)")})

    tests = {
        "unique_feature_id": one(con, "select count(*) from (select feature_id from silver_s group by 1 having count(*) > 1)"),
        "not_null": q(con, "select " + ", ".join(f"count(*) filter (where {c} is null)" for c in (
            "entity", "feature_id", "sheet_id", "data_year", "raw_sha256")) + " from silver_s"),
        "accepted_values_fails": q(con, "select count(*) filter (where colour not in ('RGB', 'CIR', 'B/W')), count(*) "
                                        "filter (where data_source not in ('Zdj. cyfrowe', 'Zdj. analogowe', 'Scena "
                                        "sat.')), count(*) filter (where crs_xy not in ('PL-1992', 'PL-2000:S5', "
                                        "'PL-2000:S6', 'PL-2000:S7', 'PL-2000:S8')), count(*) filter (where "
                                        "sheet_filled not in ('TAK', 'NIE')), count(*) filter (where geometry_type <> "
                                        "'Polygon') from silver_s"),
        "complete": q(con, render("warehouse/tests/gugik_orto_sheets_complete.sql", {},
                                  {"gugik_orto_sheets": "silver_s"})),
    }
    note("orto silver", {
        "tests": tests,
        "rows_years": q(con, "select count(*), count(distinct entity), min(data_year), max(data_year), "
                             "min(acquisition_date), max(acquisition_date) from silver_s"),
        "centroid_null": one(con, "select count(*) from silver_s where centroid_x_2180 is null"),
        "centroid_range": q(con, "select min(centroid_x_2180), max(centroid_x_2180), min(centroid_y_2180), "
                                 "max(centroid_y_2180), min(footprint_area_km2), median(footprint_area_km2), "
                                 "max(footprint_area_km2) from silver_s"),
        "sheet_product_edition_repeats": one(con, "select count(*) from (select data_year, sheet_id, colour, pixel_m, "
                                                  "acquisition_date from silver_s group by all having count(*) > 1)"),
        "sheet_year_repeats": one(con, "select count(*) from (select data_year, sheet_id from silver_s group by all "
                                       "having count(*) > 1)"),
        "by_year_sample": q(con, "select data_year, count(*) from silver_s group by 1 order by 1 desc limit 4")})

    prg = cat.load_table(("silver", "prg_units")).scan(
        row_filter="unit_level == 'gmina'", selected_fields=("unit_level", "teryt_code", "geometry_wkb")).to_arrow()
    con.register("prg_a", prg)
    con.execute("create table prg_t as select * from prg_a")
    geo = cat.load_table(("gold", "dim_geography")).scan(
        selected_fields=("geography_key", "level_name", "name_pl", "terc_code")).to_arrow()
    con.register("geo_a", geo)
    con.execute("create table geo as select * from geo_a")
    cte = Environment().from_string(Path("warehouse/macros/sources/gugik_orto.sql").read_text()
                                    + "{{ poc_geography__gugik_orto() }}").render()
    con.execute(f"create table map_t as with geo as (select * from geo), {cte} select * from gugik_orto")
    gold_sql = render("warehouse/models/gold/poc_build_gugik_orto.sql",
                      {"poc_map_geography": "map_t", "prg_units": "prg_t"}, {"gugik_orto_sheets": "silver_s"})
    gold_rows = []
    for _ in (1, 2):
        t0 = time.monotonic()
        con.execute(f"create or replace table gold_t as {gold_sql}")
        gold_rows.append([one(con, "select count(*) from gold_t"), round(time.monotonic() - t0)])
    series = "measure_key || '|' || geography_key || '|' || breakdown_json"
    total = "measure_key = 'gugik_orto:sheets:map_sheets:editions' and is_total"
    note("orto gold build_gugik_orto", {
        "rows_seconds_two_builds": gold_rows, "prg_gminas": prg.num_rows,
        "dup_series_x_date": one(con, f"select count(*) from (select {series}, date_key from gold_t group by all "
                                      "having count(*) > 1)"),
        "by_measure": q(con, "select measure_key, count(*), count(distinct geography_key), min(date_key), "
                             "max(date_key), min(value), max(value) from gold_t group by 1"),
        "unmatched": q(con, "select geography_source_code, count(*) from gold_t where geography_key = '_X' group by 1"),
        "pl_total_vs_silver": one(con, f"select count(*) from (select date_key, value from gold_t where {total} and "
                                       "geography_key = 'PL' except select make_date(data_year, 1, 1), count(*) from "
                                       "silver_s group by data_year)"),
        "not_in_a_voivodeship": q(con, f"select sum(value) filter (where geography_key = 'PL') - sum(value) filter "
                                       f"(where geography_key like 'TERC:%') from gold_t where {total}"),
        "not_placed_by_year": q(con, f"select date_key, sum(value) filter (where geography_key = 'PL') - coalesce(sum("
                                     f"value) filter (where geography_key like 'TERC:%'), 0) from gold_t where {total} "
                                     "group by 1 order by 2 desc limit 6"),
        "share_sums_off": one(con, "select count(*) from (select geography_key, date_key, sum(value) s from gold_t "
                                   "where measure_key like '%share%' group by all having abs(s - 100) > 0.1)"),
        "columns": [r[0] for r in con.execute("describe gold_t").fetchall()]})
    note("orto gold spot values (Poland, voivodeships)", {
        "pl_recent": q(con, f"select date_key, value from gold_t where {total} and geography_key = 'PL' "
                            "order by 1 desc limit 5"),
        "pl_newest_shares": q(con, "select date_key, breakdown_json, value from gold_t where geography_key = 'PL' and "
                                   "date_key = (select max(date_key) - interval 1 year from gold_t) and measure_key "
                                   "like '%share%' order by 2"),
        "voivodeships_2024": q(con, f"select g.name_pl, b.value from gold_t as b join geo as g using (geography_key) "
                                    f"where {total} and b.date_key = (select max(date_key) - interval 1 year from gold_t) order by 2 desc limit 17")})

    from warehouse import seed_rows

    source_row = [r for r in seed_rows.rows("source") if r["source_key"] == SOURCE]
    note("orto licence (catalogue rows)", {
        "source": source_row,
        "licence": [r for r in seed_rows.rows("licence") if r["licence_key"] in {x["licence_key"] for x in source_row}],
        "unit_map": [r for r in seed_rows.rows("unit_map") if r["source_system"] == SOURCE]})


def cleanup(cat):
    removed = {"tables": [], "landing_objects": 0, "lake_objects": 0}
    try:
        for table in cat.list_tables(SCRATCH):
            try:
                cat.purge_table(table)
            except Exception:  # noqa: BLE001
                cat.drop_table(table)
            removed["tables"].append(".".join(table))
        cat.drop_namespace(SCRATCH)
    except Exception as exc:  # noqa: BLE001
        removed["catalog_error"] = f"{type(exc).__name__}: {str(exc)[:200]}"
    client = s3()
    for var, label in (("STORAGE_LANDING_BUCKET_NAME", "landing_objects"),
                       ("STORAGE_LAKEHOUSE_BUCKET_NAME", "lake_objects")):
        bucket = req(var)
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{SCRATCH}/"):
            keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
            if keys:
                client.delete_objects(Bucket=bucket, Delete={"Objects": keys})
                removed[label] += len(keys)
    removed["objects_left"] = sum(
        len(p.get("Contents", []))
        for b in ("STORAGE_LANDING_BUCKET_NAME", "STORAGE_LAKEHOUSE_BUCKET_NAME")
        for p in client.get_paginator("list_objects_v2").paginate(Bucket=req(b), Prefix=f"{SCRATCH}/"))
    NOTES["left"] += 1
    note("scratch removed", removed)


def main():
    failed = 0
    cat = catalog()
    try:
        try:
            trial(cat)
        except Exception as exc:
            failed = 1
            NOTES["left"] += 1
            note("orto trial failed", {"error": f"{type(exc).__name__}: {re.sub(r'\s+', ' ', str(exc))[:1500]}"}, "error")
            raise
    finally:
        cleanup(cat)
    return failed


if __name__ == "__main__":
    raise SystemExit(main())
